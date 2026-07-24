package piacp

import (
	"errors"
	"fmt"
)

// defaultImageLimitBytes is the default for every ImageLimits field:
// 6 MiB of decoded raster bytes.
const defaultImageLimitBytes int64 = 6 * 1024 * 1024

// maxImageFrameBytes is the largest decoded image payload that still fits one
// JSON-RPC frame under the pinned ACP SDK's 10 MiB connection scanner once
// base64 expansion and envelope overhead are applied. Output emission never
// exceeds it, even when a policy limit is disabled, because one oversize
// update frame disconnects the whole consumer connection.
const maxImageFrameBytes int64 = 7_864_155

// ImageLimits bounds decoded image bytes on both prompt input and emitted
// output. Every field defaults to 6 MiB (6,291,456 bytes) when the option is
// omitted. A field explicitly set to zero disables that adapter policy limit
// but never bypasses hard framing, provider, memory, or host limits; negative
// values are rejected at agent construction.
type ImageLimits struct {
	// MaxInputBytesPerImage bounds one prompt image's decoded bytes.
	MaxInputBytesPerImage int64
	// MaxInputBytesPerPrompt bounds the decoded bytes of all images in one
	// prompt combined.
	MaxInputBytesPerPrompt int64
	// MaxOutputBytesPerImage bounds one emitted image's decoded bytes.
	MaxOutputBytesPerImage int64
	// MaxOutputBytesPerToolCall bounds the combined decoded image bytes of
	// one tool call's content array, which travels as a whole in each
	// content-bearing update.
	MaxOutputBytesPerToolCall int64
}

// WithImageLimits configures decoded-byte limits for prompt image input and
// emitted image output. Supplying the struct owns all four fields: an
// explicit zero disables that policy limit, and omitting the option leaves
// every field at its 6 MiB default.
func WithImageLimits(limits ImageLimits) Option {
	return func(options *Options) {
		options.ImageLimits = limits
		options.imageLimitsSet = true
	}
}

func defaultImageLimits() ImageLimits {
	return ImageLimits{
		MaxInputBytesPerImage:     defaultImageLimitBytes,
		MaxInputBytesPerPrompt:    defaultImageLimitBytes,
		MaxOutputBytesPerImage:    defaultImageLimitBytes,
		MaxOutputBytesPerToolCall: defaultImageLimitBytes,
	}
}

// validateImageLimits rejects negative limit fields at agent construction.
func validateImageLimits(limits ImageLimits) error {
	fields := []struct {
		name  string
		value int64
	}{
		{name: "MaxInputBytesPerImage", value: limits.MaxInputBytesPerImage},
		{name: "MaxInputBytesPerPrompt", value: limits.MaxInputBytesPerPrompt},
		{name: "MaxOutputBytesPerImage", value: limits.MaxOutputBytesPerImage},
		{name: "MaxOutputBytesPerToolCall", value: limits.MaxOutputBytesPerToolCall},
	}

	var errs []error

	for _, field := range fields {
		if field.value < 0 {
			errs = append(errs, fmt.Errorf("image limit %s must not be negative", field.name))
		}
	}

	return errors.Join(errs...)
}

// effectiveOutputImageLimit resolves one configured output policy limit into
// the enforceable bound: a disabled (zero) or above-frame limit clamps to the
// frame bound because an oversize update frame is a consumer disconnect, not
// a policy choice.
func effectiveOutputImageLimit(configured int64) int64 {
	if configured <= 0 || configured > maxImageFrameBytes {
		return maxImageFrameBytes
	}

	return configured
}

func (a *Agent) imageLimits() ImageLimits {
	return a.options.ImageLimits
}
