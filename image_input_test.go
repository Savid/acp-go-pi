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

func requireImageParamError(t *testing.T, err error, value string, index int) map[string]any {
	t.Helper()

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)

	encoded, marshalErr := json.Marshal(requestError.Data)
	require.NoError(t, marshalErr)

	var data map[string]any

	require.NoError(t, json.Unmarshal(encoded, &data))
	require.Equal(t, fieldPromptImage, data[jsonFieldField])
	require.Equal(t, value, data[jsonFieldError])
	require.InDelta(t, float64(index), data[jsonFieldIndex], 0)

	return data
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
			budget := newPromptImageBudget(defaultImageLimits())
			err := budget.validate(test.data, test.mimeType)
			requireImageParamError(t, err, test.want, 0)
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

	budget := newPromptImageBudget(defaultImageLimits())
	for name, mimeType := range fixtures {
		require.NoError(t, budget.validate(fixtureBase64(t, name), mimeType))
	}
}

func TestPromptImagePerImageLimitBoundary(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	data := base64.StdEncoding.EncodeToString(png)
	size := int64(len(png))

	atLimit := newPromptImageBudget(ImageLimits{MaxInputBytesPerImage: size})
	require.NoError(t, atLimit.validate(data, "image/png"))

	oneUnder := newPromptImageBudget(ImageLimits{MaxInputBytesPerImage: size - 1})
	err := oneUnder.validate(data, "image/png")
	details := requireImageParamError(t, err, imageErrorTooLarge, 0)
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

	require.NoError(t, validateBoth(newPromptImageBudget(ImageLimits{MaxInputBytesPerPrompt: total})))

	err := validateBoth(newPromptImageBudget(ImageLimits{MaxInputBytesPerPrompt: total - 1}))
	details := requireImageParamError(t, err, imageErrorTooLarge, 1)
	require.InDelta(t, float64(total), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(total-1), details[jsonFieldMaxBytes], 0)
}

func TestPromptImageZeroLimitsDisablePolicy(t *testing.T) {
	budget := newPromptImageBudget(ImageLimits{})
	require.NoError(t, budget.validate(fixtureBase64(t, "valid.png"), "image/png"))
	require.NoError(t, budget.validate(fixtureBase64(t, "valid.webp"), "image/webp"))
}

func TestPromptToPiImageOrderAndSharedBudget(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")
	webp := fixtureBase64(t, "valid.webp")
	webpMime := "image/webp"

	mapped, err := promptToPi([]acp.ContentBlock{
		acp.TextBlock("before"),
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(gif, "image/gif"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///c.webp", Blob: webp, MimeType: &webpMime,
		}}),
		acp.TextBlock("after"),
	}, defaultImageLimits())
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
	_, err = promptToPi([]acp.ContentBlock{
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(gif, "image/gif"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///c.webp", Blob: webp, MimeType: &webpMime,
		}}),
	}, ImageLimits{MaxInputBytesPerPrompt: pngBytes + gifBytes + webpBytes - 1})
	requireImageParamError(t, err, imageErrorTooLarge, 2)
}

func TestPromptToPiFirstFailureWins(t *testing.T) {
	png := fixtureBase64(t, "valid.png")

	_, err := promptToPi([]acp.ContentBlock{
		acp.ImageBlock(png, "image/png"),
		acp.ImageBlock(fixtureBase64(t, "animated.gif"), "image/gif"),
		acp.ImageBlock("", "image/png"),
	}, defaultImageLimits())
	requireImageParamError(t, err, imageErrorAnimated, 1)

	svgMime := "image/svg+xml"
	_, err = promptToPi([]acp.ContentBlock{
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///a.svg", Blob: png, MimeType: &svgMime,
		}}),
	}, defaultImageLimits())
	requireImageParamError(t, err, imageErrorInvalidMediaType, 0)
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
		{name: "unparseable selection", model: "", want: imageInputUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &agentSession{model: test.model, availableModels: catalog}
			require.Equal(t, test.want, session.selectedModelImageSupport())
		})
	}
}

func TestRejectImagesForUnsupportedModel(t *testing.T) {
	catalog := []pi.Model{
		{Provider: "p", ID: "vision", Input: []string{"text", "image"}},
		{Provider: "p", ID: "text-only", Input: []string{"text"}},
	}
	images := piPrompt{Images: []pi.ImageContent{pi.NewImageContent(fixtureBase64(t, "valid.png"), "image/png")}}

	unsupported := &agentSession{model: "p/text-only", availableModels: catalog}
	err := unsupported.rejectImagesForUnsupportedModel(images)
	requireImageParamError(t, err, imageErrorUnsupportedByModel, 0)
	require.NoError(t, unsupported.rejectImagesForUnsupportedModel(piPrompt{Message: "text only"}))

	require.NoError(t, (&agentSession{model: "p/vision", availableModels: catalog}).rejectImagesForUnsupportedModel(images))
	require.NoError(t, (&agentSession{model: "p/unknown", availableModels: catalog}).rejectImagesForUnsupportedModel(images))
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
	images := piPrompt{Images: []pi.ImageContent{pi.NewImageContent(fixtureBase64(t, "valid.png"), "image/png")}}

	require.NoError(t, session.rejectImagesForUnsupportedModel(images))

	session.mu.Lock()
	session.model = "p/text-only"
	session.mu.Unlock()

	requireImageParamError(t, session.rejectImagesForUnsupportedModel(images), imageErrorUnsupportedByModel, 0)
}
