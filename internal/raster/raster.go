// Package raster inspects raster image containers structurally: format
// sniffing from magic bytes, dimensions from headers, and animation from
// block/chunk lists. It never decodes pixel data.
package raster

import (
	"encoding/binary"
	"errors"
)

// Canonical MIME strings for the sniffed formats.
const (
	MIMEPNG  = "image/png"
	MIMEJPEG = "image/jpeg"
	MIMEGIF  = "image/gif"
	MIMEWebP = "image/webp"
	MIMEBMP  = "image/bmp"
	MIMETIFF = "image/tiff"
)

// ErrUnknownFormat reports bytes whose magic matches no format this package
// knows.
var ErrUnknownFormat = errors.New("bytes do not sniff as a known raster format")

// ErrInvalidStructure reports a recognized container whose header yields no
// valid dimensions.
var ErrInvalidStructure = errors.New("raster header yields no valid dimensions")

// Info describes one raster's structure as read from its container.
type Info struct {
	MIME     string
	Width    int
	Height   int
	Animated bool
}

// Sniff identifies the raster format from magic bytes alone and returns its
// canonical MIME string. It recognizes PNG, JPEG, GIF, WebP, BMP, and TIFF.
func Sniff(data []byte) (string, bool) {
	switch {
	case isPNG(data):
		return MIMEPNG, true
	case isJPEG(data):
		return MIMEJPEG, true
	case isGIF(data):
		return MIMEGIF, true
	case isWebP(data):
		return MIMEWebP, true
	case len(data) >= 2 && data[0] == 'B' && data[1] == 'M':
		return MIMEBMP, true
	case len(data) >= 4 && (string(data[:4]) == "II*\x00" || string(data[:4]) == "MM\x00*"):
		return MIMETIFF, true
	default:
		return "", false
	}
}

// Inspect reads format, dimensions, and animation markers for PNG, JPEG,
// GIF, and WebP containers by walking headers and block/chunk lists.
// Corruption in regions the walk never reaches is deliberately not detected.
func Inspect(data []byte) (Info, error) {
	switch {
	case isPNG(data):
		return inspectPNG(data)
	case isJPEG(data):
		return inspectJPEG(data)
	case isGIF(data):
		return inspectGIF(data)
	case isWebP(data):
		return inspectWebP(data)
	default:
		return Info{}, ErrUnknownFormat
	}
}

func isPNG(data []byte) bool {
	return len(data) >= 8 && string(data[:8]) == "\x89PNG\r\n\x1a\n"
}

func isJPEG(data []byte) bool {
	return len(data) >= 2 && data[0] == 0xFF && data[1] == 0xD8
}

func isGIF(data []byte) bool {
	return len(data) >= 6 && (string(data[:6]) == "GIF87a" || string(data[:6]) == "GIF89a")
}

func isWebP(data []byte) bool {
	return len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP"
}

// inspectPNG reads IHDR for dimensions and walks the chunk list for an acTL
// chunk (APNG). The APNG spec constrains acTL to precede the first IDAT, so
// the walk ends there.
func inspectPNG(data []byte) (Info, error) {
	const ihdrEnd = 8 + 8 + 13 + 4

	if len(data) < ihdrEnd || binary.BigEndian.Uint32(data[8:12]) != 13 || string(data[12:16]) != "IHDR" {
		return Info{}, ErrInvalidStructure
	}

	width := binary.BigEndian.Uint32(data[16:20])
	height := binary.BigEndian.Uint32(data[20:24])

	if width == 0 || height == 0 || width > 0x7FFFFFFF || height > 0x7FFFFFFF {
		return Info{}, ErrInvalidStructure
	}

	info := Info{MIME: MIMEPNG, Width: int(width), Height: int(height)}

	for offset := ihdrEnd; offset+8 <= len(data); {
		length := binary.BigEndian.Uint32(data[offset : offset+4])
		chunkType := string(data[offset+4 : offset+8])

		if chunkType == "acTL" {
			info.Animated = true

			break
		}

		if chunkType == "IDAT" || chunkType == "IEND" {
			break
		}

		next := offset + 8 + int(length) + 4
		if length > 0x7FFFFFFF || next <= offset || next > len(data) {
			break
		}

		offset = next
	}

	return info, nil
}

// inspectJPEG walks marker segments to the first frame header for
// dimensions. JPEG carries no animation.
func inspectJPEG(data []byte) (Info, error) {
	offset := 2

	for {
		if offset+4 > len(data) || data[offset] != 0xFF {
			return Info{}, ErrInvalidStructure
		}

		marker := data[offset+1]

		switch {
		case marker == 0xFF:
			// Fill byte before a marker.
			offset++
		case marker == 0x01 || (marker >= 0xD0 && marker <= 0xD8):
			// Standalone marker without a length field.
			offset += 2
		case marker == 0xD9 || marker == 0xDA:
			// End of image or start of scan before any frame header.
			return Info{}, ErrInvalidStructure
		case isJPEGFrameMarker(marker):
			if offset+9 > len(data) {
				return Info{}, ErrInvalidStructure
			}

			height := int(binary.BigEndian.Uint16(data[offset+5 : offset+7]))
			width := int(binary.BigEndian.Uint16(data[offset+7 : offset+9]))

			if width == 0 || height == 0 {
				return Info{}, ErrInvalidStructure
			}

			return Info{MIME: MIMEJPEG, Width: width, Height: height}, nil
		default:
			length := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			if length < 2 {
				return Info{}, ErrInvalidStructure
			}

			offset += 2 + length
		}
	}
}

// isJPEGFrameMarker reports whether marker is an SOF marker carrying frame
// dimensions (0xC0-0xCF excluding DHT 0xC4, JPG 0xC8, and DAC 0xCC).
func isJPEGFrameMarker(marker byte) bool {
	return marker >= 0xC0 && marker <= 0xCF && marker != 0xC4 && marker != 0xC8 && marker != 0xCC
}

// inspectGIF reads the logical screen descriptor for dimensions and walks
// the block stream counting image descriptors; more than one means
// animation. The walk stops at the trailer, an unknown block, or
// truncation, leaving corruption beyond that point to the native surface.
func inspectGIF(data []byte) (Info, error) {
	if len(data) < 13 {
		return Info{}, ErrInvalidStructure
	}

	width := int(binary.LittleEndian.Uint16(data[6:8]))
	height := int(binary.LittleEndian.Uint16(data[8:10]))

	if width == 0 || height == 0 {
		return Info{}, ErrInvalidStructure
	}

	info := Info{MIME: MIMEGIF, Width: width, Height: height}
	offset := 13

	if data[10]&0x80 != 0 {
		offset += 3 * (1 << ((data[10] & 0x07) + 1))
	}

	descriptors := 0

walk:
	for offset < len(data) {
		switch data[offset] {
		case 0x2C: // Image descriptor.
			descriptors++
			if descriptors > 1 {
				info.Animated = true

				break walk
			}

			if offset+10 > len(data) {
				break walk
			}

			packed := data[offset+9]
			offset += 10

			if packed&0x80 != 0 {
				offset += 3 * (1 << ((packed & 0x07) + 1))
			}

			// LZW minimum code size byte, then data sub-blocks.
			offset++
			offset = skipGIFSubBlocks(data, offset)
		case 0x21: // Extension: introducer, label, then sub-blocks.
			offset = skipGIFSubBlocks(data, offset+2)
		default: // Trailer (0x3B) or unknown block.
			break walk
		}
	}

	return info, nil
}

// skipGIFSubBlocks advances past a GIF data sub-block sequence, stopping at
// the zero-length terminator or truncation.
func skipGIFSubBlocks(data []byte, offset int) int {
	for offset < len(data) {
		size := int(data[offset])
		offset += 1 + size

		if size == 0 {
			break
		}
	}

	return offset
}

// inspectWebP reads the first RIFF chunk: VP8X carries the ANIM flag and
// canvas size, VP8 and VP8L carry frame dimensions.
func inspectWebP(data []byte) (Info, error) {
	if len(data) < 20 {
		return Info{}, ErrInvalidStructure
	}

	fourCC := string(data[12:16])
	payload := data[20:]

	switch fourCC {
	case "VP8X":
		if len(payload) < 10 {
			return Info{}, ErrInvalidStructure
		}

		return Info{
			MIME:     MIMEWebP,
			Width:    1 + int(uint32(payload[4])|uint32(payload[5])<<8|uint32(payload[6])<<16),
			Height:   1 + int(uint32(payload[7])|uint32(payload[8])<<8|uint32(payload[9])<<16),
			Animated: payload[0]&0x02 != 0,
		}, nil
	case "VP8 ":
		if len(payload) < 10 || payload[3] != 0x9D || payload[4] != 0x01 || payload[5] != 0x2A {
			return Info{}, ErrInvalidStructure
		}

		width := int(binary.LittleEndian.Uint16(payload[6:8]) & 0x3FFF)
		height := int(binary.LittleEndian.Uint16(payload[8:10]) & 0x3FFF)

		if width == 0 || height == 0 {
			return Info{}, ErrInvalidStructure
		}

		return Info{MIME: MIMEWebP, Width: width, Height: height}, nil
	case "VP8L":
		if len(payload) < 5 || payload[0] != 0x2F {
			return Info{}, ErrInvalidStructure
		}

		bits := binary.LittleEndian.Uint32(payload[1:5])

		return Info{
			MIME:   MIMEWebP,
			Width:  1 + int(bits&0x3FFF),
			Height: 1 + int((bits>>14)&0x3FFF),
		}, nil
	default:
		return Info{}, ErrInvalidStructure
	}
}
