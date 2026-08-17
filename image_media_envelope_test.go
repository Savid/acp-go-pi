package piacp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMediaEnvelopeAdvertisedShape(t *testing.T) {
	resp, err := NewAgent().Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)

	envelope, ok := resp.AgentCapabilities.Meta[mediaEnvelopeMetaKey].(map[string]any)
	require.True(t, ok, "agentCapabilities._meta must carry the media envelope")
	require.Equal(t, map[string]any{
		mediaEnvelopeFieldMaxBytes:        defaultImageLimitBytes,
		mediaEnvelopeFieldMaxPromptBytes:  defaultImageLimitBytes,
		mediaEnvelopeFieldMaxDimension:    0,
		mediaEnvelopeFieldImageFormats:    []string{"image/png", "image/jpeg", "image/gif", "image/webp"},
		mediaEnvelopeFieldDocumentFormats: []string{},
	}, envelope)

	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"maxBytes": 6291456,
		"maxPromptBytes": 6291456,
		"maxDimension": 0,
		"imageFormats": ["image/png","image/jpeg","image/gif","image/webp"],
		"documentFormats": []
	}`, string(encoded))
	require.Contains(t, string(encoded), `"documentFormats":[]`)
}

// paddedPNG grows a valid PNG to exactly size bytes. The structural walk stops
// at the first IDAT chunk, so trailing bytes leave the image readable and let a
// byte-limit case be built at any size the gates care about.
func paddedPNG(t *testing.T, png []byte, size int64) []byte {
	t.Helper()

	require.GreaterOrEqual(t, size, int64(len(png)))

	padded := make([]byte, size)
	copy(padded, png)

	return padded
}

func TestMediaEnvelopeAdvertisesTheEnforcedPerImageGate(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	size := int64(len(png))

	tests := []struct {
		name  string
		limit int64
		want  int64
	}{
		{name: "configured", limit: size, want: size},
		{name: "disabled clamps to the frame bound", limit: 0, want: maxImageFrameBytes},
		{name: "above frame clamps to the frame bound", limit: maxImageFrameBytes + 1, want: maxImageFrameBytes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := ImageLimits{MaxInputBytesPerImage: test.limit}
			advertised, ok := NewAgent(WithImageLimits(limits)).mediaEnvelope()[mediaEnvelopeFieldMaxBytes].(int64)
			require.True(t, ok)
			require.Equal(t, test.want, advertised)

			// The advertised bound is the one the gate enforces: a payload at
			// the bound passes and one byte past it is rejected against the
			// same number.
			require.NoError(t, newPromptImageBudget(limits, "").validateBytes(fieldPromptImage, 0, paddedPNG(t, png, advertised), "image/png"))

			err := newPromptImageBudget(limits, "").validateBytes(fieldPromptImage, 0, paddedPNG(t, png, advertised+1), "image/png")
			details := requireImageParamError(t, err, imageErrorTooLarge, 0)
			require.InDelta(t, float64(advertised+1), details[jsonFieldSizeBytes], 0)
			require.InDelta(t, float64(advertised), details[jsonFieldMaxBytes], 0)
		})
	}
}

func TestMediaEnvelopeAdvertisesTheEnforcedPromptGate(t *testing.T) {
	png := fixtureBytes(t, "valid.png")

	limits := ImageLimits{MaxInputBytesPerPrompt: 4096}
	advertised, ok := NewAgent(WithImageLimits(limits)).mediaEnvelope()[mediaEnvelopeFieldMaxPromptBytes].(int64)
	require.True(t, ok)
	require.Equal(t, limits.MaxInputBytesPerPrompt, advertised)

	// The advertised aggregate is the one the gate enforces: it is the number
	// the aggregate rejection reports, not the configured field restated.
	budget := newPromptImageBudget(limits, "")
	payload := paddedPNG(t, png, 3000)
	require.NoError(t, budget.validateBytes(fieldPromptImage, 0, payload, "image/png"))

	err := budget.validateBytes(fieldPromptImage, 1, payload, "image/png")
	details := requireImageParamError(t, err, imageErrorTooLarge, 1)
	require.InDelta(t, float64(6000), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(advertised), details[jsonFieldMaxBytes], 0)
}

func TestMediaEnvelopeImageFormatsAreTheAllowlist(t *testing.T) {
	for _, mimeType := range inputImageMIMEAllowlist() {
		require.True(t, allowedInputImageMIME(mimeType), mimeType)
	}

	require.False(t, allowedInputImageMIME("image/jpg"))
	require.False(t, allowedInputImageMIME("image/svg+xml"))
}

func TestHandoffCapabilityAdvertisedOnlyWithARoot(t *testing.T) {
	withoutRoot, err := NewAgent().Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	require.NotContains(t, withoutRoot.AgentCapabilities.Meta, handoffMetaKey)

	withRoot, err := NewAgent(WithInputHandoffRoot(t.TempDir())).Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{metaFieldVersions: []int{handoffVersion}},
		withRoot.AgentCapabilities.Meta[handoffMetaKey],
	)

	// The envelope is unconditional, so a host cannot infer one key from the
	// other.
	require.Contains(t, withoutRoot.AgentCapabilities.Meta, mediaEnvelopeMetaKey)
	require.Contains(t, withRoot.AgentCapabilities.Meta, mediaEnvelopeMetaKey)
}

func TestConformanceInitializeAdvertisesMediaKeys(t *testing.T) {
	tests := []struct {
		name        string
		handoffRoot bool
	}{
		{name: "without a handoff root"},
		{name: "with a handoff root", handoffRoot: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var opts []Option
			if test.handoffRoot {
				opts = append(opts, WithInputHandoffRoot(t.TempDir()))
			}

			conn := connectConformanceAgent(t, ctx, &conformanceClient{}, defaultInitializeRequest(), successfulUnitScenario(), opts...)

			response, err := conn.Initialize(ctx, defaultInitializeRequest())
			require.NoError(t, err)

			envelope, ok := response.AgentCapabilities.Meta[mediaEnvelopeMetaKey].(map[string]any)
			require.True(t, ok)
			require.InDelta(t, float64(defaultImageLimitBytes), envelope[mediaEnvelopeFieldMaxBytes], 0)
			require.InDelta(t, float64(defaultImageLimitBytes), envelope[mediaEnvelopeFieldMaxPromptBytes], 0)
			require.InDelta(t, 0, envelope[mediaEnvelopeFieldMaxDimension], 0)
			require.Equal(t, []any{"image/png", "image/jpeg", "image/gif", "image/webp"}, envelope[mediaEnvelopeFieldImageFormats])
			require.Equal(t, []any{}, envelope[mediaEnvelopeFieldDocumentFormats])

			handoff, present := response.AgentCapabilities.Meta[handoffMetaKey]
			require.Equal(t, test.handoffRoot, present)

			if test.handoffRoot {
				require.Equal(t, map[string]any{metaFieldVersions: []any{float64(handoffVersion)}}, handoff)
			}

			require.True(t, response.AgentCapabilities.PromptCapabilities.Image)
			require.Nil(t, response.Meta)
		})
	}
}
