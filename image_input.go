package piacp

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/savid/acp-go-pi/internal/raster"
)

// Structured image-validation error values carried in -32602 data as
// {"field":"prompt.image","error":<value>,"index":<image index>}, with
// sizeBytes/maxBytes added on limit failures.
const (
	imageErrorMissingData        = "missing_data"
	imageErrorInvalidBase64      = "invalid_base64"
	imageErrorInvalidMediaType   = "invalid_media_type"
	imageErrorMediaTypeMismatch  = "media_type_mismatch"
	imageErrorAnimated           = "animated_not_supported"
	imageErrorInvalidDimensions  = "invalid_dimensions"
	imageErrorTooLarge           = "too_large"
	imageErrorUnsupportedByModel = "unsupported_by_model"

	jsonFieldSizeBytes = "sizeBytes"
	jsonFieldMaxBytes  = "maxBytes"
)

// modelInputImage is the pi model catalog input modality naming vision.
const modelInputImage = "image"

// imageInputSupport is the prompt-time answer of the selected-model gate,
// sourced only from pi's own model catalog input list.
type imageInputSupport uint8

const (
	imageInputUnknown imageInputSupport = iota
	imageInputUnsupported
	imageInputSupported
)

// allowedInputImageMIME accepts exactly the four canonical static raster MIME
// strings; variants such as image/jpg are rejected.
func allowedInputImageMIME(mimeType string) bool {
	return slices.Contains(inputImageMIMEAllowlist(), mimeType)
}

// inboundMediaTypeRoute normalizes a declared inbound MIME for the image/
// prefix test that routes an embedded resource to the image path: trimmed,
// stripped of any parameters, and ASCII-lowercased. The allowlist still
// compares the declared value, so a non-canonical raster declaration routes to
// the image path and is rejected there as invalid_media_type.
func inboundMediaTypeRoute(declared string) string {
	essence, _, _ := strings.Cut(declared, ";")

	return strings.ToLower(strings.TrimSpace(essence))
}

// promptMediaError reports a gated-media verdict against the request member the
// block arrived on. Routing is chosen by MIME and the field is chosen by the
// inbound block type, so a resource blob reports the resource channel even when
// its declared raster type sent it through the image chain.
func promptMediaError(field string, errValue string, index int, sizeBytes int64, maxBytes int64) *acp.RequestError {
	data := map[string]any{
		jsonFieldField: field,
		jsonFieldError: errValue,
		jsonFieldIndex: index,
	}

	if sizeBytes > 0 {
		data[jsonFieldSizeBytes] = sizeBytes
	}

	if maxBytes > 0 {
		data[jsonFieldMaxBytes] = maxBytes
	}

	return acp.NewInvalidParams(data)
}

func imagePromptError(errValue string, index int, sizeBytes int64, maxBytes int64) *acp.RequestError {
	return promptMediaError(fieldPromptImage, errValue, index, sizeBytes, maxBytes)
}

// promptImageBudget validates prompt images in request order and stops on the
// first failure. Image blocks and embedded image blob resources share one
// stable index sequence and one per-prompt decoded-byte budget, whichever form
// carried their bytes.
type promptImageBudget struct {
	perImage  int64
	perPrompt int64
	total     int64
	nextIndex int
	// handoffBlocks counts the handoff-form blocks this prompt has asked the
	// adapter to read, which is bounded independently of the byte aggregate a
	// host may disable.
	handoffBlocks int
	handoffRoot   string
	// root is the opened read root, held for the life of one prompt mapping so
	// every handoff open in that prompt is relative to one kernel-checked
	// descriptor.
	root *os.Root
}

func newPromptImageBudget(limits ImageLimits, handoffRoot string) *promptImageBudget {
	return &promptImageBudget{
		perImage:    effectiveInputImageLimit(limits.MaxInputBytesPerImage),
		perPrompt:   effectiveInputPromptLimit(limits.MaxInputBytesPerPrompt),
		handoffRoot: handoffRoot,
	}
}

// validateBlock validates one ACP prompt image block in whichever form it
// carries and returns the base64 payload for the native request. Non-empty
// data is the embedded form even when a handoff envelope rides alongside it;
// empty data plus handoff intent runs the handoff pre-gate ahead of every
// embedded-form gate; empty data with neither is missing_data.
func (b *promptImageBudget) validateBlock(ctx context.Context, block *acp.ContentBlockImage) (string, error) {
	index := b.nextImageIndex()

	if block.Data != "" {
		if err := b.validateEmbedded(fieldPromptImage, index, block.Data, block.MimeType); err != nil {
			return "", err
		}

		return block.Data, nil
	}

	if !handoffIntent(block) {
		return "", imagePromptError(imageErrorMissingData, index, 0, 0)
	}

	data, failure := b.handoffBytes(ctx, block)
	if failure != nil {
		return "", imageHandoffError(failure, index)
	}

	if err := b.validateBytes(fieldPromptImage, index, data, block.MimeType); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

// validate runs the embedded-form pipeline for one image blob resource:
// required data, canonical MIME, one base64 decode, then the shared gate chain.
// The bytes arrived on a resource block, so every verdict names that member
// even though a raster declaration is what routed them here.
func (b *promptImageBudget) validate(data string, mimeType string) error {
	index := b.nextImageIndex()

	if data == "" {
		return promptMediaError(fieldPromptResource, imageErrorMissingData, index, 0, 0)
	}

	return b.validateEmbedded(fieldPromptResource, index, data, mimeType)
}

func (b *promptImageBudget) nextImageIndex() int {
	index := b.nextIndex
	b.nextIndex++

	return index
}

func (b *promptImageBudget) validateEmbedded(field string, index int, data string, mimeType string) error {
	if !allowedInputImageMIME(mimeType) {
		return promptMediaError(field, imageErrorInvalidMediaType, index, 0, 0)
	}

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return promptMediaError(field, imageErrorInvalidBase64, index, 0, 0)
	}

	return b.validateBytes(field, index, decoded, mimeType)
}

// validateBytes runs the gate chain shared by both input forms over decoded
// bytes: decode-free container inspection, animation rejection, declared-versus-sniffed agreement, then the per-image and
// aggregate decoded-byte limits. Both forms deliver every byte they account
// for — a handoff read past the per-image bound is rejected where it is read —
// so the byte the limits count is the byte that arrived. Corruption in
// container regions the structural walk never touches is deliberately
// forwarded; the native provider outcome stands for those bytes.
func (b *promptImageBudget) validateBytes(field string, index int, decoded []byte, mimeType string) error {
	size := int64(len(decoded))

	info, err := raster.Inspect(decoded)

	switch {
	case errors.Is(err, raster.ErrUnknownFormat):
		return promptMediaError(field, imageErrorMediaTypeMismatch, index, 0, 0)
	case err != nil:
		return promptMediaError(field, imageErrorInvalidDimensions, index, 0, 0)
	}

	if info.Animated {
		return promptMediaError(field, imageErrorAnimated, index, 0, 0)
	}

	if info.MIME != mimeType {
		return promptMediaError(field, imageErrorMediaTypeMismatch, index, 0, 0)
	}

	if size > b.perImage {
		return promptMediaError(field, imageErrorTooLarge, index, size, b.perImage)
	}

	b.total += size
	if b.perPrompt > 0 && b.total > b.perPrompt {
		return promptMediaError(field, imageErrorTooLarge, index, b.total, b.perPrompt)
	}

	return nil
}

// chargeText adds a text resource's bytes to the same per-prompt accumulator the
// media forms use. Bytes are bytes: declaring them as text rather than as a blob
// must not buy a prompt more of them than maxPromptBytes allows. It reports at
// the position the next media block would take without consuming it, because a
// text resource carries no media the index is meant to identify.
func (b *promptImageBudget) chargeText(size int64) error {
	b.total += size
	if b.perPrompt > 0 && b.total > b.perPrompt {
		return promptMediaError(fieldPromptResource, imageErrorTooLarge, b.nextIndex, b.total, b.perPrompt)
	}

	return nil
}

// selectedModelImageSupport evaluates the session's currently selected model
// against pi's get_available_models catalog at prompt time. A catalog entry
// with a non-empty input list is authoritative: it either names image input
// or it does not. A model absent from the catalog, or an entry without an
// input list, yields unknown and the prompt is forwarded so the native
// harness and provider stay authoritative.
func (s *agentSession) selectedModelImageSupport() imageInputSupport {
	s.mu.Lock()
	model := s.model
	available := append([]pi.Model(nil), s.availableModels...)
	s.mu.Unlock()

	ref, err := pi.ParseModelRef(model)
	if err != nil {
		return imageInputUnknown
	}

	for index := range available {
		entry := &available[index]
		if entry.Provider != ref.Provider || entry.ID != ref.ID {
			continue
		}

		if len(entry.Input) == 0 {
			return imageInputUnknown
		}

		for _, input := range entry.Input {
			if strings.EqualFold(input, modelInputImage) {
				return imageInputSupported
			}
		}

		return imageInputUnsupported
	}

	return imageInputUnknown
}

// rejectImagesForUnsupportedModel fails an image prompt before native turn
// start when the catalog authoritatively says the selected model is
// text-only. Unknown support forwards.
func (s *agentSession) rejectImagesForUnsupportedModel(mapped piPrompt) error {
	if len(mapped.Images) == 0 {
		return nil
	}

	if s.selectedModelImageSupport() == imageInputUnsupported {
		return imagePromptError(imageErrorUnsupportedByModel, 0, 0, 0)
	}

	return nil
}
