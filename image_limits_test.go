package piacp

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestImageLimitsDefaults(t *testing.T) {
	options := applyOptions(nil)
	require.Equal(t, ImageLimits{
		MaxInputBytesPerImage:     defaultImageLimitBytes,
		MaxInputBytesPerPrompt:    defaultImageLimitBytes,
		MaxOutputBytesPerImage:    defaultImageLimitBytes,
		MaxOutputBytesPerToolCall: defaultImageLimitBytes,
	}, options.ImageLimits)
	require.EqualValues(t, 6_291_456, defaultImageLimitBytes)

	supplied := applyOptions([]Option{WithImageLimits(ImageLimits{MaxInputBytesPerImage: 1})})
	require.Equal(t, ImageLimits{MaxInputBytesPerImage: 1}, supplied.ImageLimits,
		"a supplied struct owns all four fields; unset fields stay zero and disable that policy limit")

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	require.Equal(t, defaultImageLimits(), agent.imageLimits())
}

func TestImageLimitsNegativeRejectedAtConstruction(t *testing.T) {
	err := validateImageLimits(ImageLimits{
		MaxInputBytesPerImage:     -1,
		MaxInputBytesPerPrompt:    -1,
		MaxOutputBytesPerImage:    -1,
		MaxOutputBytesPerToolCall: -1,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "MaxInputBytesPerImage")
	require.ErrorContains(t, err, "MaxInputBytesPerPrompt")
	require.ErrorContains(t, err, "MaxOutputBytesPerImage")
	require.ErrorContains(t, err, "MaxOutputBytesPerToolCall")
	require.NoError(t, validateImageLimits(ImageLimits{}))

	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithImageLimits(ImageLimits{MaxOutputBytesPerImage: -1}),
	)
	_, err = agent.Initialize(t.Context(), acp.InitializeRequest{})
	requireInvalidParams(t, err)
}

// TestSessionEstablishmentRejectsUnvalidatedOptions pins that an embedded host
// which never calls initialize still cannot open a session against options that
// failed validation at construction.
func TestSessionEstablishmentRejectsUnvalidatedOptions(t *testing.T) {
	var logs bytes.Buffer

	agent := newStubClientAgent(t, newStubPiClient(),
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithImageLimits(ImageLimits{MaxOutputBytesPerImage: -1}),
	)

	_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))

	// The client is told which option the agent refuses to serve under and
	// nothing else; the reason is the operator's, and it is in the log.
	requireUnsupportedField(t, err, optionFieldImageLimits)
	require.Contains(t, logs.String(), "MaxOutputBytesPerImage")
}

func TestEffectiveOutputImageLimit(t *testing.T) {
	require.Equal(t, maxImageFrameBytes, effectiveOutputImageLimit(0),
		"a disabled policy limit still clamps to the frame bound")
	require.Equal(t, maxImageFrameBytes, effectiveOutputImageLimit(maxImageFrameBytes+1))
	require.Equal(t, int64(1024), effectiveOutputImageLimit(1024))
	require.Equal(t, maxImageFrameBytes, effectiveOutputImageLimit(maxImageFrameBytes))
}
