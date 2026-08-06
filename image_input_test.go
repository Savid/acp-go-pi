package piacp

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// requireMediaParamError asserts one -32602 gated-media verdict, including the
// request member it names. The member follows the block the bytes arrived on,
// so a resource blob and an image block reporting the same gate still report
// different fields.
func requireMediaParamError(t *testing.T, err error, field string, value string, index int) map[string]any {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)

	encoded, marshalErr := json.Marshal(requestError.Data)
	require.NoError(t, marshalErr)

	var data map[string]any

	require.NoError(t, json.Unmarshal(encoded, &data))
	require.Equal(t, field, data[jsonFieldField])
	require.Equal(t, value, data[jsonFieldError])
	require.InDelta(t, float64(index), data[jsonFieldIndex], 0)

	return data
}

func requireImageParamError(t *testing.T, err error, value string, index int) map[string]any {
	t.Helper()

	return requireMediaParamError(t, err, fieldPromptImage, value, index)
}

func requireResourceParamError(t *testing.T, err error, value string, index int) map[string]any {
	t.Helper()

	return requireMediaParamError(t, err, fieldPromptResource, value, index)
}

func TestPromptImageValidationTaxonomy(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		mimeType string
		want     string
	}{
		{name: "missing data", data: "", mimeType: "image/png", want: imageErrorMissingData},
		{name: "missing mime", data: fixtureBase64(t, "valid.png"), mimeType: "", want: imageErrorInvalidMediaType},
		{name: "non-canonical mime", data: fixtureBase64(t, "valid.jpg"), mimeType: "image/jpg", want: imageErrorInvalidMediaType},
		{name: "svg excluded", data: base64.StdEncoding.EncodeToString([]byte("<svg/>")), mimeType: "image/svg+xml", want: imageErrorInvalidMediaType},
		{name: "invalid base64", data: "not base64!", mimeType: "image/png", want: imageErrorInvalidBase64},
		{name: "unsniffable bytes", data: base64.StdEncoding.EncodeToString([]byte("not a raster at all")), mimeType: "image/png", want: imageErrorMediaTypeMismatch},
		{name: "jpeg declared png", data: fixtureBase64(t, "mismatch.png"), mimeType: "image/png", want: imageErrorMediaTypeMismatch},
		{name: "truncated png", data: fixtureBase64(t, "truncated.png"), mimeType: "image/png", want: imageErrorInvalidDimensions},
		{name: "animated gif", data: fixtureBase64(t, "animated.gif"), mimeType: "image/gif", want: imageErrorAnimated},
		{name: "animated webp", data: fixtureBase64(t, "animated.webp"), mimeType: "image/webp", want: imageErrorAnimated},
		{name: "animated apng", data: fixtureBase64(t, "animated-apng.png"), mimeType: "image/png", want: imageErrorAnimated},
		{name: "single frame actl png", data: fixtureBase64(t, "single-frame-actl.png"), mimeType: "image/png", want: imageErrorAnimated},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := newPromptImageBudget(defaultImageLimits(), "")
			err := budget.validate(test.data, test.mimeType)
			requireResourceParamError(t, err, test.want, 0)
		})
	}
}

func TestPromptImageValidFormats(t *testing.T) {
	fixtures := map[string]string{
		"valid.png":  "image/png",
		"valid.jpg":  "image/jpeg",
		"valid.gif":  "image/gif",
		"valid.webp": "image/webp",
	}

	budget := newPromptImageBudget(defaultImageLimits(), "")
	for name, mimeType := range fixtures {
		require.NoError(t, budget.validate(fixtureBase64(t, name), mimeType))
	}
}

func TestPromptImagePerImageLimitBoundary(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	data := base64.StdEncoding.EncodeToString(png)
	size := int64(len(png))

	atLimit := newPromptImageBudget(ImageLimits{MaxInputBytesPerImage: size}, "")
	require.NoError(t, atLimit.validate(data, "image/png"))

	oneUnder := newPromptImageBudget(ImageLimits{MaxInputBytesPerImage: size - 1}, "")
	err := oneUnder.validate(data, "image/png")
	details := requireResourceParamError(t, err, imageErrorTooLarge, 0)
	require.InDelta(t, float64(size), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(size-1), details[jsonFieldMaxBytes], 0)
}

func TestPromptImageAggregateLimitBoundary(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")
	total := int64(len(png) + len(gif))

	validateBoth := func(budget *promptImageBudget) error {
		if err := budget.validate(base64.StdEncoding.EncodeToString(png), "image/png"); err != nil {
			return err
		}

		return budget.validate(base64.StdEncoding.EncodeToString(gif), "image/gif")
	}

	require.NoError(t, validateBoth(newPromptImageBudget(ImageLimits{MaxInputBytesPerPrompt: total}, "")))

	err := validateBoth(newPromptImageBudget(ImageLimits{MaxInputBytesPerPrompt: total - 1}, ""))
	details := requireResourceParamError(t, err, imageErrorTooLarge, 1)
	require.InDelta(t, float64(total), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(total-1), details[jsonFieldMaxBytes], 0)
}

func TestPromptImageZeroLimitsDisablePolicy(t *testing.T) {
	budget := newPromptImageBudget(ImageLimits{}, "")
	require.NoError(t, budget.validate(fixtureBase64(t, "valid.png"), "image/png"))
	require.NoError(t, budget.validate(fixtureBase64(t, "valid.webp"), "image/webp"))
}

func TestPromptToPiImageOrderAndSharedBudget(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")
	webp := fixtureBase64(t, "valid.webp")
	webpMime := "image/webp"

	mapped, err := promptToPi(t.Context(), []acp.ContentBlock{
		acp.TextBlock("before"),
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(gif, "image/gif"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///c.webp", Blob: webp, MimeType: &webpMime,
		}}),
		acp.TextBlock("after"),
	}, defaultImageLimits(), "")
	require.NoError(t, err)
	require.Len(t, mapped.Images, 3)
	require.Equal(t, "image/png", mapped.Images[0].MimeType)
	require.Equal(t, "image/gif", mapped.Images[1].MimeType)
	require.Equal(t, "image/webp", mapped.Images[2].MimeType)
	require.Equal(t, png, mapped.Images[0].Data)
	require.Equal(t, gif, mapped.Images[1].Data)
	require.Equal(t, webp, mapped.Images[2].Data)

	// The blob resource is the third image, so it carries index 2 in the
	// shared index sequence and shares the aggregate budget.
	pngBytes := int64(len(fixtureBytes(t, "valid.png")))
	gifBytes := int64(len(fixtureBytes(t, "valid.gif")))
	webpBytes := int64(len(fixtureBytes(t, "valid.webp")))
	_, err = promptToPi(t.Context(), []acp.ContentBlock{
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(gif, "image/gif"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///c.webp", Blob: webp, MimeType: &webpMime,
		}}),
	}, ImageLimits{MaxInputBytesPerPrompt: pngBytes + gifBytes + webpBytes - 1}, "")
	requireResourceParamError(t, err, imageErrorTooLarge, 2)
}

func TestPromptToPiFirstFailureWins(t *testing.T) {
	png := fixtureBase64(t, "valid.png")

	_, err := promptToPi(t.Context(), []acp.ContentBlock{
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(fixtureBase64(t, "animated.gif"), "image/gif"),
		acp.ImageBlock("", "image/png"),
	}, defaultImageLimits(), "")
	requireImageParamError(t, err, imageErrorAnimated, 1)

	svgMime := "image/svg+xml"
	_, err = promptToPi(t.Context(), []acp.ContentBlock{
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///a.svg", Blob: png, MimeType: &svgMime,
		}}),
	}, defaultImageLimits(), "")
	requireResourceParamError(t, err, imageErrorInvalidMediaType, 0)
}

// TestDisabledPerImageLimitClampsTheReadBound pins that a disabled per-image
// policy limit still bounds a handoff read, while the per-prompt aggregate is
// deliberately left unclamped.
func TestDisabledPerImageLimitClampsTheReadBound(t *testing.T) {
	disabled := newPromptImageBudget(ImageLimits{}, t.TempDir())
	require.Equal(t, maxImageFrameBytes, disabled.perImage)
	require.Zero(t, disabled.perPrompt)

	aboveFrame := newPromptImageBudget(ImageLimits{
		MaxInputBytesPerImage:  maxImageFrameBytes + 1,
		MaxInputBytesPerPrompt: maxImageFrameBytes + 1,
	}, t.TempDir())
	require.Equal(t, maxImageFrameBytes, aboveFrame.perImage)
	require.Equal(t, maxImageFrameBytes+1, aboveFrame.perPrompt)

	configured := newPromptImageBudget(defaultImageLimits(), t.TempDir())
	require.Equal(t, defaultImageLimitBytes, configured.perImage)
	require.Equal(t, defaultImageLimitBytes, configured.perPrompt)
}

// TestGatedMediaIndexCounter pins that the image index counts gated media
// blocks in request order: every blob resource the per-image byte gate claims
// consumes the counter whatever its declared MIME, and blocks that reach no
// byte gate never consume it.
func TestGatedMediaIndexCounter(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	svgMime := "image/svg+xml"
	tiffMime := "IMAGE/TIFF"

	svgBlob := acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
		Uri: "file:///a.svg", Blob: png, MimeType: &svgMime,
	}})
	tiffBlob := acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
		Uri: "file:///a.tiff", Blob: png, MimeType: &tiffMime,
	}})

	t.Run("a gated blob consumes the counter whatever its mime", func(t *testing.T) {
		_, err := promptToPi(t.Context(), []acp.ContentBlock{
			acp.ImageBlock(png, "image/png"),
			svgBlob,
		}, defaultImageLimits(), "")
		requireResourceParamError(t, err, imageErrorInvalidMediaType, 1)

		_, err = promptToPi(t.Context(), []acp.ContentBlock{
			acp.ImageBlock(png, "image/png"),
			acp.ImageBlock(png, "image/png"),
			tiffBlob,
		}, defaultImageLimits(), "")
		requireResourceParamError(t, err, imageErrorInvalidMediaType, 2)
	})

	t.Run("ungated blocks never consume the counter", func(t *testing.T) {
		_, err := promptToPi(t.Context(), []acp.ContentBlock{
			acp.TextBlock("before"),
			acp.ResourceLinkBlock("link", "https://example.test"),
			acp.ResourceBlock(acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{
				Uri: "file:///a.txt", Text: "context",
			}}),
			svgBlob,
		}, defaultImageLimits(), "")
		requireResourceParamError(t, err, imageErrorInvalidMediaType, 0)
	})
}

// TestInboundMediaTypeRoute pins the normalization the image/ prefix test
// applies before routing an embedded resource to the image path.
func TestInboundMediaTypeRoute(t *testing.T) {
	tests := []struct {
		declared string
		want     string
	}{
		{declared: "image/png", want: "image/png"},
		{declared: "IMAGE/PNG", want: "image/png"},
		{declared: "  Image/Png  ", want: "image/png"},
		{declared: "image/png; charset=binary", want: "image/png"},
		{declared: "IMAGE/PNG;q=1", want: "image/png"},
		{declared: "application/pdf", want: "application/pdf"},
		{declared: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			require.Equal(t, test.want, inboundMediaTypeRoute(test.declared))
		})
	}
}

// TestBlobResourceMediaTypeRouting pins that a raster declaration routes to the
// image path whatever its case or parameters, so an unrecognised raster
// declaration is invalid_media_type rather than falling through to another
// channel.
func TestBlobResourceMediaTypeRouting(t *testing.T) {
	png := fixtureBase64(t, "valid.png")

	tests := []struct {
		name     string
		mimeType string
		want     string
	}{
		{name: "uppercase", mimeType: "IMAGE/PNG", want: imageErrorInvalidMediaType},
		{name: "mixed case", mimeType: "Image/Png", want: imageErrorInvalidMediaType},
		{name: "padded", mimeType: " image/png ", want: imageErrorInvalidMediaType},
		{name: "parameterized", mimeType: "image/png; charset=binary", want: imageErrorInvalidMediaType},
		{name: "uppercase unsupported raster", mimeType: "IMAGE/TIFF", want: imageErrorInvalidMediaType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mimeType := test.mimeType
			_, err := promptToPi(t.Context(), []acp.ContentBlock{
				acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
					Uri: "file:///a.png", Blob: png, MimeType: &mimeType,
				}}),
			}, defaultImageLimits(), "")
			requireResourceParamError(t, err, test.want, 0)
		})
	}
}

// TestNonImageBlobResourceRefused pins that the embedded resource blob channel
// stays closed to non-image bytes: pi has no non-image native representation,
// so nothing unbounded or unvalidated reaches the harness through it.
func TestNonImageBlobResourceRefused(t *testing.T) {
	oversize := base64.StdEncoding.EncodeToString(make([]byte, defaultImageLimitBytes+4495))

	tests := []struct {
		name     string
		mimeType string
		blob     string
	}{
		{name: "pdf", mimeType: "application/pdf", blob: base64.StdEncoding.EncodeToString([]byte("%PDF-1.7\n"))},
		{name: "oversize pdf", mimeType: "application/pdf", blob: oversize},
		{name: "corrupt base64 pdf", mimeType: "application/pdf", blob: "not base64!"},
		{name: "text", mimeType: "text/plain", blob: base64.StdEncoding.EncodeToString([]byte("hello"))},
		{name: "svg", mimeType: "image/svg+xml", blob: base64.StdEncoding.EncodeToString([]byte("<svg/>"))},
		{name: "octet stream", mimeType: "application/octet-stream", blob: oversize},
		{name: "absent mime", mimeType: "", blob: base64.StdEncoding.EncodeToString([]byte("hello"))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blob := &acp.BlobResourceContents{Uri: "file:///a.bin", Blob: test.blob}
			if test.mimeType != "" {
				mimeType := test.mimeType
				blob.MimeType = &mimeType
			}

			_, err := promptToPi(t.Context(), []acp.ContentBlock{
				acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: blob}),
			}, defaultImageLimits(), "")

			var requestError *acp.RequestError

			require.ErrorAs(t, err, &requestError)
			require.Equal(t, -32602, requestError.Code)

			encoded, marshalErr := json.Marshal(requestError.Data)
			require.NoError(t, marshalErr)

			var data map[string]any

			require.NoError(t, json.Unmarshal(encoded, &data))

			// Every one of these arrived on a resource block, so every verdict
			// names that member whichever gate chain the declaration routed it
			// into: an svg declaration reaches the image allowlist, and the rest
			// are refused as resources.
			require.Equal(t, fieldPromptResource, data[jsonFieldField])

			// The refusal travels to the client and into telemetry, so it may not
			// carry the payload it refused back out with it.
			require.NotContains(t, string(encoded), test.blob)

			if test.mimeType == "image/svg+xml" {
				require.Equal(t, imageErrorInvalidMediaType, data[jsonFieldError])

				return
			}

			require.Equal(t, validationUnsupported, data[jsonFieldError])
		})
	}
}

func TestSelectedModelImageSupport(t *testing.T) {
	catalog := []pi.Model{
		{Provider: "p", ID: "vision", Input: []string{"text", "IMAGE"}},
		{Provider: "p", ID: "text-only", Input: []string{"text"}},
		{Provider: "p", ID: "mystery"},
	}

	tests := []struct {
		name  string
		model string
		want  imageInputSupport
	}{
		{name: "supported", model: "p/vision", want: imageInputSupported},
		{name: "unsupported", model: "p/text-only", want: imageInputUnsupported},
		{name: "no input list", model: "p/mystery", want: imageInputUnknown},
		{name: "absent from catalog", model: "p/absent", want: imageInputUnknown},
		{name: "unparsable selection", model: "", want: imageInputUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &agentSession{model: test.model, availableModels: catalog}
			require.Equal(t, test.want, session.selectedModelImageSupport())
		})
	}
}

// mappedImagePrompt maps one prompt through the real image pipeline, so a
// model-gate assertion runs against the provenance the mapping recorded rather
// than against a hand-built prompt.
func mappedImagePrompt(t *testing.T, blocks ...acp.ContentBlock) piPrompt {
	t.Helper()

	mapped, err := promptToPi(t.Context(), blocks, defaultImageLimits(), "")
	require.NoError(t, err)
	require.NotEmpty(t, mapped.Images)

	return mapped
}

func TestRejectImagesForUnsupportedModel(t *testing.T) {
	catalog := []pi.Model{
		{Provider: "p", ID: "vision", Input: []string{"text", "image"}},
		{Provider: "p", ID: "text-only", Input: []string{"text"}},
	}
	images := mappedImagePrompt(t, acp.ImageBlock(fixtureBase64(t, "valid.png"), "image/png"))

	unsupported := &agentSession{model: "p/text-only", availableModels: catalog}
	err := unsupported.rejectImagesForUnsupportedModel(images)
	requireImageParamError(t, err, imageErrorUnsupportedByModel, 0)
	require.NoError(t, unsupported.rejectImagesForUnsupportedModel(piPrompt{Message: "text only"}))

	require.NoError(t, (&agentSession{model: "p/vision", availableModels: catalog}).rejectImagesForUnsupportedModel(images))
	require.NoError(t, (&agentSession{model: "p/unknown", availableModels: catalog}).rejectImagesForUnsupportedModel(images))
}

// TestUnsupportedModelNamesTheChannelTheImageArrivedOn pins the field half of
// the model gate: the same raster refused for the same reason names the resource
// member when it rode a blob resource and the image member when it rode an image
// block.
func TestUnsupportedModelNamesTheChannelTheImageArrivedOn(t *testing.T) {
	session := &agentSession{
		model:           "p/text-only",
		availableModels: []pi.Model{{Provider: "p", ID: "text-only", Input: []string{"text"}}},
	}
	png := fixtureBase64(t, "valid.png")
	mimeType := "image/png"

	blob := mappedImagePrompt(t, acp.TextBlock("look"), acp.ResourceBlock(acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///shot.png", MimeType: &mimeType, Blob: png},
	}))
	requireResourceParamError(t, session.rejectImagesForUnsupportedModel(blob), imageErrorUnsupportedByModel, 0)

	block := mappedImagePrompt(t, acp.TextBlock("look"), acp.ImageBlock(png, mimeType))
	requireImageParamError(t, session.rejectImagesForUnsupportedModel(block), imageErrorUnsupportedByModel, 0)
}

func TestPromptRejectsImageForUnsupportedSelectedModel(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	client := newStubPiClient()
	session := &agentSession{
		agent:           agent,
		id:              "id",
		client:          client,
		proc:            newStubProcess(false),
		model:           "p/text-only",
		availableModels: []pi.Model{{Provider: "p", ID: "text-only", Input: []string{"text"}}},
	}

	_, err := session.Prompt(t.Context(), PromptRequest("id", "test-turn",
		acp.TextBlock("look"),
		acp.ImageBlock(fixtureBase64(t, "valid.png"), "image/png"),
	))
	requireImageParamError(t, err, imageErrorUnsupportedByModel, 0)
}

func TestPromptModelSwitchChangesGateNextPrompt(t *testing.T) {
	catalog := []pi.Model{
		{Provider: "p", ID: "vision", Input: []string{"text", "image"}},
		{Provider: "p", ID: "text-only", Input: []string{"text"}},
	}
	session := &agentSession{model: "p/vision", availableModels: catalog}
	images := mappedImagePrompt(t, acp.ImageBlock(fixtureBase64(t, "valid.png"), "image/png"))

	require.NoError(t, session.rejectImagesForUnsupportedModel(images))

	session.mu.Lock()
	session.model = "p/text-only"
	session.mu.Unlock()

	requireImageParamError(t, session.rejectImagesForUnsupportedModel(images), imageErrorUnsupportedByModel, 0)
}
