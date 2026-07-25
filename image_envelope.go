package piacp

import (
	"github.com/savid/acp-go-pi/internal/raster"
)

const (
	mediaEnvelopeMetaKey = "acp-go.dev/mediaEnvelope"

	mediaEnvelopeFieldMaxBytes        = "maxBytes"
	mediaEnvelopeFieldMaxPromptBytes  = "maxPromptBytes"
	mediaEnvelopeFieldMaxDimension    = "maxDimension"
	mediaEnvelopeFieldImageFormats    = "imageFormats"
	mediaEnvelopeFieldDocumentFormats = "documentFormats"
)

// mediaEnvelopeMaxDimension is zero because pi enforces no per-dimension pixel
// bound on prompt images: the structural walk reads dimensions to prove they
// are readable, never to compare them against a ceiling.
const mediaEnvelopeMaxDimension = 0

// mediaEnvelopeDocumentFormats is empty because pi maps no MIME to a native
// document representation; a non-image embedded resource is refused outright.
// It is a non-nil slice so the advertisement carries an empty JSON array.
var mediaEnvelopeDocumentFormats = []string{}

// mediaEnvelope reports the decoded-byte gates and inbound format allowlist
// this adapter enforces on prompt media, computed from the same limits and
// allowlist the gates themselves use.
func (a *Agent) mediaEnvelope() map[string]any {
	limits := a.imageLimits()

	return map[string]any{
		mediaEnvelopeFieldMaxBytes:        effectiveInputImageLimit(limits.MaxInputBytesPerImage),
		mediaEnvelopeFieldMaxPromptBytes:  limits.MaxInputBytesPerPrompt,
		mediaEnvelopeFieldMaxDimension:    mediaEnvelopeMaxDimension,
		mediaEnvelopeFieldImageFormats:    inputImageMIMEAllowlist(),
		mediaEnvelopeFieldDocumentFormats: mediaEnvelopeDocumentFormats,
	}
}

// inputImageMIMEAllowlist is the ordered inbound raster allowlist: exactly the
// four canonical static raster MIME strings, in advertisement order.
func inputImageMIMEAllowlist() []string {
	return []string{raster.MIMEPNG, raster.MIMEJPEG, raster.MIMEGIF, raster.MIMEWebP}
}
