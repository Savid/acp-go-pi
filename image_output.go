package piacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/savid/acp-go-pi/internal/raster"
)

// Image output failure envelope fields and values. Output failures ride the
// uniform pi_turn_failed shape with cause "transport" plus the optional
// stage/reason/sizeBytes/maxBytes details.
const (
	failureFieldStage  = "stage"
	failureFieldReason = "reason"

	failureStageImageOutput = "image_output"

	imageReasonNotARaster    = "not_a_raster"
	imageReasonStorageFailed = "storage_failed"
)

// imageOutputError is one adapter-side image representation failure. Every
// occurrence is turn-fatal: the adapter never silently drops an artifact it
// cannot read, validate, store, or emit.
type imageOutputError struct {
	reason    string
	message   string
	sizeBytes int64
	maxBytes  int64
}

func (e *imageOutputError) Error() string {
	return e.message
}

// imageOutputTurnFailure maps an image output failure into the uniform
// -32603 pi_turn_failed error with machine-readable image-output details.
func imageOutputTurnFailure(failure *imageOutputError) *acp.RequestError {
	data := map[string]any{
		jsonFieldError:     turnFailedError,
		failureFieldCause:  failureCauseTransport,
		jsonFieldMessage:   failure.message,
		failureFieldStage:  failureStageImageOutput,
		failureFieldReason: failure.reason,
	}

	if failure.sizeBytes > 0 {
		data[jsonFieldSizeBytes] = failure.sizeBytes
	}

	if failure.maxBytes > 0 {
		data[jsonFieldMaxBytes] = failure.maxBytes
	}

	return acp.NewInternalError(data)
}

// storageFailure builds the storage_failed envelope for replay or mirror
// durability losses.
func storageFailure(message string) *acp.RequestError {
	return imageOutputTurnFailure(&imageOutputError{
		reason:  imageReasonStorageFailed,
		message: message,
	})
}

// replayImageFailure converts a stored artifact's validation failure into the
// replay outcome: bytes over a current limit stay too_large, while corrupt or
// unsniffable stored bytes mean the store can no longer reproduce the
// artifact it once delivered.
func replayImageFailure(failure *imageOutputError) *imageOutputError {
	if failure.reason == imageErrorTooLarge {
		return failure
	}

	return &imageOutputError{
		reason:  imageReasonStorageFailed,
		message: "stored image artifact failed validation: " + failure.message,
	}
}

// outputImage is one validated emitted image: the original base64 payload,
// the truthful sniffed MIME, the decoded size, and the artifact fingerprint.
type outputImage struct {
	data        string
	mime        string
	fingerprint string
	sizeBytes   int64
}

// normalizeOutputImage validates one native image payload for emission.
// Output is not format-allowlisted: any payload that sniffs as a raster is
// emitted with its truthful sniffed MIME, bounded by the effective per-image
// limit. A declared MIME naming a different known raster than the bytes
// sniff as is a mismatch; any other declared value defers to the sniff.
func normalizeOutputImage(data string, declaredMIME string, perImageLimit int64) (outputImage, *imageOutputError) {
	if data == "" {
		return outputImage{}, &imageOutputError{
			reason:  imageReasonNotARaster,
			message: "native image content carries no data",
		}
	}

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return outputImage{}, &imageOutputError{
			reason:  imageErrorInvalidBase64,
			message: "native image content is not valid base64",
		}
	}

	mime, ok := raster.Sniff(decoded)
	if !ok {
		return outputImage{}, &imageOutputError{
			reason:  imageReasonNotARaster,
			message: "native image content does not sniff as a raster",
		}
	}

	if declaredMIMEConflicts(declaredMIME, mime) {
		return outputImage{}, &imageOutputError{
			reason:  imageErrorMediaTypeMismatch,
			message: fmt.Sprintf("native image content declared %s but sniffs as %s", declaredMIME, mime),
		}
	}

	size := int64(len(decoded))
	if size > perImageLimit {
		return outputImage{}, &imageOutputError{
			reason:    imageErrorTooLarge,
			message:   fmt.Sprintf("image output is %d decoded bytes, exceeding the %d-byte per-image limit", size, perImageLimit),
			sizeBytes: size,
			maxBytes:  perImageLimit,
		}
	}

	digest := sha256.Sum256(decoded)

	return outputImage{
		data:        data,
		mime:        mime,
		fingerprint: hex.EncodeToString(digest[:]),
		sizeBytes:   size,
	}, nil
}

// declaredMIMEConflicts reports whether a native declared MIME names a known
// raster format other than the sniffed one. Unknown or empty declarations
// defer to the sniff.
func declaredMIMEConflicts(declared string, sniffed string) bool {
	if declared == "" || declared == sniffed {
		return false
	}

	switch declared {
	case raster.MIMEPNG, raster.MIMEJPEG, raster.MIMEGIF, raster.MIMEWebP, raster.MIMEBMP, raster.MIMETIFF:
		return true
	default:
		return false
	}
}

// toolContentItem is one entry of a tool call's emitted content snapshot,
// with the identity key and decoded image size used for replace-semantics
// accounting.
type toolContentItem struct {
	content    acp.ToolCallContent
	key        string
	imageBytes int64
}

// mapToolContentBlocks converts one native tool content array into validated
// snapshot items. Empty text blocks and unmapped block types are dropped;
// image blocks pass full output validation.
func mapToolContentBlocks(blocks []pi.ContentBlock, perImageLimit int64) ([]toolContentItem, *imageOutputError) {
	items := make([]toolContentItem, 0, len(blocks))

	for index := range blocks {
		block := &blocks[index]

		switch block.Type {
		case contentBlockTypeText:
			if block.Text == "" {
				continue
			}

			items = append(items, toolContentItem{
				content: acp.ToolContent(acp.TextBlock(block.Text)),
				key:     "text:" + block.Text,
			})
		case contentBlockTypeImage:
			image, failure := normalizeOutputImage(block.Data, block.MimeType, perImageLimit)
			if failure != nil {
				return nil, failure
			}

			items = append(items, toolContentItem{
				content:    acp.ToolContent(acp.ImageBlock(image.data, image.mime)),
				key:        "image:" + image.mime + ":" + image.fingerprint,
				imageBytes: image.sizeBytes,
			})
		}
	}

	return items, nil
}

// mergeToolContent builds the next complete snapshot from the previously
// emitted one: ACP replaces a tool call's content array wholesale, so an
// already delivered item never disappears from a later array. Repeated
// occurrences of identical items are matched by count, keeping genuinely
// distinct duplicates.
func mergeToolContent(previous []toolContentItem, next []toolContentItem) []toolContentItem {
	if len(previous) == 0 {
		return next
	}

	merged := slices.Clone(previous)
	counts := make(map[string]int, len(previous))

	for _, item := range previous {
		counts[item.key]++
	}

	for _, item := range next {
		if counts[item.key] > 0 {
			counts[item.key]--

			continue
		}

		merged = append(merged, item)
	}

	return merged
}

// checkToolContentBudget enforces the per-tool-call aggregate decoded-byte
// limit over one complete snapshot, attributing the failure to the artifact
// that crosses the total.
func checkToolContentBudget(items []toolContentItem, perCallLimit int64) *imageOutputError {
	var total int64

	for _, item := range items {
		if item.imageBytes == 0 {
			continue
		}

		total += item.imageBytes
		if total > perCallLimit {
			return &imageOutputError{
				reason:    imageErrorTooLarge,
				message:   fmt.Sprintf("tool call image content totals %d decoded bytes, exceeding the %d-byte per-tool-call limit", total, perCallLimit),
				sizeBytes: total,
				maxBytes:  perCallLimit,
			}
		}
	}

	return nil
}

// buildToolContent produces the complete validated snapshot for one
// content-bearing tool call update, live and replayed alike.
func buildToolContent(
	previous []toolContentItem,
	blocks []pi.ContentBlock,
	limits ImageLimits,
) ([]toolContentItem, *imageOutputError) {
	next, failure := mapToolContentBlocks(blocks, effectiveOutputImageLimit(limits.MaxOutputBytesPerImage))
	if failure != nil {
		return nil, failure
	}

	merged := mergeToolContent(previous, next)
	if failure := checkToolContentBudget(merged, effectiveOutputImageLimit(limits.MaxOutputBytesPerToolCall)); failure != nil {
		return nil, failure
	}

	return merged, nil
}

func toolContentSnapshot(items []toolContentItem) []acp.ToolCallContent {
	content := make([]acp.ToolCallContent, 0, len(items))
	for _, item := range items {
		content = append(content, item.content)
	}

	return content
}
