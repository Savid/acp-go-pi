package piacp

import (
	"encoding/base64"
	"errors"
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
	switch mimeType {
	case raster.MIMEPNG, raster.MIMEJPEG, raster.MIMEGIF, raster.MIMEWebP:
		return true
	default:
		return false
	}
}

func imagePromptError(errValue string, index int, sizeBytes int64, maxBytes int64) *acp.RequestError {
	data := map[string]any{
		jsonFieldField: fieldPromptImage,
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

// promptImageBudget validates prompt images in request order and stops on the
// first failure. Image blocks and embedded image blob resources share one
// stable index sequence and one per-prompt decoded-byte budget.
type promptImageBudget struct {
	perImage  int64
	perPrompt int64
	total     int64
	nextIndex int
}

func newPromptImageBudget(limits ImageLimits) *promptImageBudget {
	return &promptImageBudget{
		perImage:  limits.MaxInputBytesPerImage,
		perPrompt: limits.MaxInputBytesPerPrompt,
	}
}

// validate runs the full structural pipeline for one prompt image: required
// data, canonical MIME, one base64 decode, decode-free container inspection,
// animation rejection, declared-versus-sniffed agreement, then the per-image
// and aggregate decoded-byte limits. Corruption in container regions the
// structural walk never touches is deliberately forwarded; the native
// provider outcome stands for those bytes.
func (b *promptImageBudget) validate(data string, mimeType string) error {
	index := b.nextIndex
	b.nextIndex++

	if data == "" {
		return imagePromptError(imageErrorMissingData, index, 0, 0)
	}

	if !allowedInputImageMIME(mimeType) {
		return imagePromptError(imageErrorInvalidMediaType, index, 0, 0)
	}

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return imagePromptError(imageErrorInvalidBase64, index, 0, 0)
	}

	info, err := raster.Inspect(decoded)

	switch {
	case errors.Is(err, raster.ErrUnknownFormat):
		return imagePromptError(imageErrorMediaTypeMismatch, index, 0, 0)
	case err != nil:
		return imagePromptError(imageErrorInvalidDimensions, index, 0, 0)
	}

	if info.Animated {
		return imagePromptError(imageErrorAnimated, index, 0, 0)
	}

	if info.MIME != mimeType {
		return imagePromptError(imageErrorMediaTypeMismatch, index, 0, 0)
	}

	size := int64(len(decoded))
	if b.perImage > 0 && size > b.perImage {
		return imagePromptError(imageErrorTooLarge, index, size, b.perImage)
	}

	b.total += size
	if b.perPrompt > 0 && b.total > b.perPrompt {
		return imagePromptError(imageErrorTooLarge, index, b.total, b.perPrompt)
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
