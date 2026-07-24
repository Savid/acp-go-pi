package raster

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// pngFile assembles a PNG signature plus the given chunks.
func pngFile(chunks ...[]byte) []byte {
	file := []byte("\x89PNG\r\n\x1a\n")
	for _, chunk := range chunks {
		file = append(file, chunk...)
	}

	return file
}

// pngChunk encodes one chunk with a zeroed CRC; the walk never checks CRCs.
func pngChunk(chunkType string, payload []byte) []byte {
	chunk := make([]byte, 0, 12+len(payload))
	chunk = binary.BigEndian.AppendUint32(chunk, uint32(len(payload)))
	chunk = append(chunk, chunkType...)
	chunk = append(chunk, payload...)

	return append(chunk, 0, 0, 0, 0)
}

func pngIHDR(width uint32, height uint32) []byte {
	payload := make([]byte, 13)
	binary.BigEndian.PutUint32(payload[0:4], width)
	binary.BigEndian.PutUint32(payload[4:8], height)
	payload[8] = 8
	payload[9] = 2

	return pngChunk("IHDR", payload)
}

func gifFile(descriptors int, trailer bool) []byte {
	file := []byte("GIF89a")
	file = binary.LittleEndian.AppendUint16(file, 3)
	file = binary.LittleEndian.AppendUint16(file, 2)
	file = append(file, 0x00, 0x00, 0x00)

	for range descriptors {
		file = append(file, 0x2C)
		file = binary.LittleEndian.AppendUint16(file, 0)
		file = binary.LittleEndian.AppendUint16(file, 0)
		file = binary.LittleEndian.AppendUint16(file, 3)
		file = binary.LittleEndian.AppendUint16(file, 2)
		file = append(file, 0x00)       // No local color table.
		file = append(file, 0x02)       // LZW minimum code size.
		file = append(file, 0x01, 0x00) // One data byte, then terminator.
		file = append(file, 0x00)
	}

	if trailer {
		file = append(file, 0x3B)
	}

	return file
}

func webpFile(fourCC string, payload []byte) []byte {
	file := []byte("RIFF")
	file = binary.LittleEndian.AppendUint32(file, uint32(4+8+len(payload)))
	file = append(file, "WEBP"...)
	file = append(file, fourCC...)
	file = binary.LittleEndian.AppendUint32(file, uint32(len(payload)))

	return append(file, payload...)
}

func vp8Payload(width uint16, height uint16) []byte {
	payload := []byte{0x00, 0x00, 0x00, 0x9D, 0x01, 0x2A}
	payload = binary.LittleEndian.AppendUint16(payload, width)

	return binary.LittleEndian.AppendUint16(payload, height)
}

func TestSniff(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		mime string
		ok   bool
	}{
		{name: "png", data: pngFile(pngIHDR(1, 1)), mime: MIMEPNG, ok: true},
		{name: "jpeg", data: []byte{0xFF, 0xD8, 0xFF}, mime: MIMEJPEG, ok: true},
		{name: "gif", data: gifFile(1, true), mime: MIMEGIF, ok: true},
		{name: "webp", data: webpFile("VP8 ", vp8Payload(3, 2)), mime: MIMEWebP, ok: true},
		{name: "bmp", data: []byte("BM????"), mime: MIMEBMP, ok: true},
		{name: "tiff little endian", data: []byte("II*\x00????"), mime: MIMETIFF, ok: true},
		{name: "tiff big endian", data: []byte("MM\x00*????"), mime: MIMETIFF, ok: true},
		{name: "unknown", data: []byte("not a raster"), ok: false},
		{name: "empty", data: nil, ok: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mime, ok := Sniff(test.data)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.mime, mime)
		})
	}
}

func TestInspectUnknownFormat(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("BM????"), []byte("II*\x00????"), []byte("GIF88a??????")} {
		_, err := Inspect(data)
		require.ErrorIs(t, err, ErrUnknownFormat)
	}
}

func TestInspectPNG(t *testing.T) {
	valid := func(chunks ...[]byte) []byte {
		return pngFile(append([][]byte{pngIHDR(96, 72)}, chunks...)...)
	}

	tests := []struct {
		name     string
		data     []byte
		err      error
		animated bool
	}{
		{name: "static", data: valid(pngChunk("IDAT", []byte{1}), pngChunk("IEND", nil))},
		{name: "no idat stops at iend", data: valid(pngChunk("IEND", nil))},
		{name: "actl before idat", data: valid(pngChunk("acTL", make([]byte, 8)), pngChunk("IDAT", []byte{1})), animated: true},
		{name: "intervening chunk before actl", data: valid(pngChunk("tEXt", []byte("k")), pngChunk("acTL", make([]byte, 8))), animated: true},
		{name: "truncated ihdr", data: pngFile(pngIHDR(96, 72))[:24], err: ErrInvalidStructure},
		{name: "wrong first chunk", data: pngFile(pngChunk("acTL", make([]byte, 13))), err: ErrInvalidStructure},
		{name: "wrong ihdr length", data: pngFile(pngChunk("IHDR", make([]byte, 14))), err: ErrInvalidStructure},
		{name: "zero width", data: pngFile(pngIHDR(0, 72)), err: ErrInvalidStructure},
		{name: "zero height", data: pngFile(pngIHDR(96, 0)), err: ErrInvalidStructure},
		{name: "oversize width", data: pngFile(pngIHDR(0x80000000, 72)), err: ErrInvalidStructure},
		{name: "walk stops at truncated chunk header", data: append(valid(), 0, 0, 0)},
		{name: "walk stops at overrunning chunk", data: append(valid(), 0, 0, 0, 99, 't', 'E', 'X', 't')},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := Inspect(test.data)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, Info{MIME: MIMEPNG, Width: 96, Height: 72, Animated: test.animated}, info)
		})
	}
}

func TestInspectJPEG(t *testing.T) {
	sof := []byte{0xFF, 0xC0, 0x00, 0x11, 0x08, 0x00, 0x48, 0x00, 0x60}

	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{name: "immediate frame header", data: append([]byte{0xFF, 0xD8}, sof...)},
		{name: "fill bytes before marker", data: append([]byte{0xFF, 0xD8, 0xFF, 0xFF}, sof...)},
		{name: "standalone marker", data: append([]byte{0xFF, 0xD8, 0xFF, 0x01, 0xFF, 0xD0}, sof...)},
		{name: "skipped segment", data: append([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x04, 0x00, 0x00}, sof...)},
		{name: "truncated", data: []byte{0xFF, 0xD8}, err: ErrInvalidStructure},
		{name: "garbage between segments", data: []byte{0xFF, 0xD8, 0x00, 0x00, 0x00, 0x00}, err: ErrInvalidStructure},
		{name: "end of image before frame", data: []byte{0xFF, 0xD8, 0xFF, 0xD9, 0x00, 0x00}, err: ErrInvalidStructure},
		{name: "scan before frame", data: []byte{0xFF, 0xD8, 0xFF, 0xDA, 0x00, 0x02}, err: ErrInvalidStructure},
		{name: "truncated frame header", data: []byte{0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 0x08}, err: ErrInvalidStructure},
		{name: "zero dimensions", data: []byte{0xFF, 0xD8, 0xFF, 0xC0, 0x00, 0x11, 0x08, 0x00, 0x00, 0x00, 0x00}, err: ErrInvalidStructure},
		{name: "invalid segment length", data: []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x01, 0x00, 0x00}, err: ErrInvalidStructure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := Inspect(test.data)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, Info{MIME: MIMEJPEG, Width: 96, Height: 72}, info)
		})
	}
}

func TestInspectGIF(t *testing.T) {
	withGlobalColorTable := gifFile(0, false)
	withGlobalColorTable[10] = 0x80 // Smallest table: 2 entries, 6 bytes.
	withGlobalColorTable = append(withGlobalColorTable, make([]byte, 6)...)
	withGlobalColorTable = append(withGlobalColorTable, 0x3B)

	localColorTable := gifFile(0, false)
	localColorTable = append(localColorTable, 0x2C, 0, 0, 0, 0, 3, 0, 2, 0, 0x80)
	localColorTable = append(localColorTable, make([]byte, 6)...)
	localColorTable = append(localColorTable, 0x02, 0x00, 0x3B)

	extension := gifFile(0, false)
	extension = append(extension, 0x21, 0xF9, 0x04, 0, 0, 0, 0, 0x00, 0x3B)

	truncatedDescriptor := append(gifFile(0, false), 0x2C, 0, 0)

	truncatedSubBlocks := gifFile(0, false)
	truncatedSubBlocks = append(truncatedSubBlocks, 0x21, 0xFE, 0xFF)

	tests := []struct {
		name     string
		data     []byte
		err      error
		animated bool
	}{
		{name: "single image", data: gifFile(1, true)},
		{name: "two images", data: gifFile(2, true), animated: true},
		{name: "no image", data: gifFile(0, true)},
		{name: "global color table", data: withGlobalColorTable},
		{name: "local color table", data: localColorTable},
		{name: "extension", data: extension},
		{name: "unknown block stops walk", data: append(gifFile(1, false), 0x7F)},
		{name: "truncated descriptor stops walk", data: truncatedDescriptor},
		{name: "truncated sub-blocks stop walk", data: truncatedSubBlocks},
		{name: "truncated header", data: []byte("GIF89a\x03\x00\x02\x00"), err: ErrInvalidStructure},
		{name: "zero width", data: []byte("GIF89a\x00\x00\x02\x00\x00\x00\x00"), err: ErrInvalidStructure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := Inspect(test.data)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, Info{MIME: MIMEGIF, Width: 3, Height: 2, Animated: test.animated}, info)
		})
	}
}

func TestInspectWebP(t *testing.T) {
	vp8x := func(flags byte) []byte {
		payload := make([]byte, 0, 10)
		payload = append(payload, flags, 0, 0, 0)
		payload = append(payload, 2, 0, 0) // Canvas width minus one.
		payload = append(payload, 1, 0, 0) // Canvas height minus one.

		return webpFile("VP8X", payload)
	}

	vp8l := webpFile("VP8L", []byte{0x2F, 0x02, 0x40, 0x00, 0x00})

	tests := []struct {
		name     string
		data     []byte
		err      error
		animated bool
	}{
		{name: "lossy", data: webpFile("VP8 ", vp8Payload(3, 2))},
		{name: "lossless", data: vp8l},
		{name: "extended static", data: vp8x(0x00)},
		{name: "extended animated", data: vp8x(0x02), animated: true},
		{name: "truncated chunk header", data: []byte("RIFF\x04\x00\x00\x00WEBP"), err: ErrInvalidStructure},
		{name: "short vp8x payload", data: webpFile("VP8X", []byte{0, 0, 0}), err: ErrInvalidStructure},
		{name: "bad vp8 sync code", data: webpFile("VP8 ", []byte{0, 0, 0, 0, 0, 0, 3, 0, 2, 0}), err: ErrInvalidStructure},
		{name: "short vp8 payload", data: webpFile("VP8 ", []byte{0, 0, 0}), err: ErrInvalidStructure},
		{name: "zero vp8 width", data: webpFile("VP8 ", vp8Payload(0x4000, 2)), err: ErrInvalidStructure},
		{name: "bad vp8l signature", data: webpFile("VP8L", []byte{0x00, 0x02, 0x40, 0x00, 0x00}), err: ErrInvalidStructure},
		{name: "short vp8l payload", data: webpFile("VP8L", []byte{0x2F}), err: ErrInvalidStructure},
		{name: "unknown chunk", data: webpFile("ALPH", make([]byte, 10)), err: ErrInvalidStructure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := Inspect(test.data)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, Info{MIME: MIMEWebP, Width: 3, Height: 2, Animated: test.animated}, info)
		})
	}
}
