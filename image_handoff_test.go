package piacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// handoffFileURI writes one handoff fixture under root and returns its file
// uri, mirroring a host that materialized verified bytes for this turn.
func handoffFileURI(t *testing.T, root string, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return fileURIFor(path)
}

func fileURIFor(path string) string {
	return "file://" + filepath.ToSlash(path)
}

func handoffEnvelopeFor(data []byte) map[string]any {
	sum := sha256.Sum256(data)

	return map[string]any{
		handoffFieldVersion:   handoffVersion,
		handoffFieldDigest:    hex.EncodeToString(sum[:]),
		handoffFieldSizeBytes: len(data),
	}
}

// handoffImageBlock builds one handoff-form image block: empty data, a file
// uri, and the handoff envelope.
func handoffImageBlock(uri string, mimeType string, envelope map[string]any) acp.ContentBlock {
	block := acp.ImageBlock("", mimeType)
	block.Image.Uri = &uri

	if envelope != nil {
		block.Image.Meta = map[string]any{handoffMetaKey: envelope}
	}

	return block
}

// handoffFixtureBlock writes a raster fixture into root and returns the
// matching handoff block plus the bytes it points at.
func handoffFixtureBlock(t *testing.T, root string, name string, mimeType string) (acp.ContentBlock, []byte) {
	t.Helper()

	data := fixtureBytes(t, name)
	uri := handoffFileURI(t, root, name, data)

	return handoffImageBlock(uri, mimeType, handoffEnvelopeFor(data)), data
}

func requireHandoffError(t *testing.T, err error, value string, index int, messageSubstring string) {
	t.Helper()

	data := requireImageParamError(t, err, value, index)
	message, ok := data[jsonFieldMessage].(string)
	require.True(t, ok, "handoff failure must carry a human message")
	require.NotEmpty(t, message)
	require.Contains(t, message, messageSubstring)
}

func validateHandoffBlock(t *testing.T, root string, block acp.ContentBlock, limits ImageLimits) (string, error) {
	t.Helper()

	return newPromptImageBudget(limits, root).validateBlock(block.Image)
}

func TestHandoffFormSelection(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")
	uri := handoffFileURI(t, root, "valid.png", png)

	t.Run("embedded data wins over a handoff envelope", func(t *testing.T) {
		block := acp.ImageBlock(base64.StdEncoding.EncodeToString(gif), "image/gif")
		block.Image.Uri = &uri
		block.Image.Meta = map[string]any{handoffMetaKey: handoffEnvelopeFor(png)}

		data, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		require.NoError(t, err)
		require.Equal(t, base64.StdEncoding.EncodeToString(gif), data)
	})

	t.Run("envelope alone signals handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", "image/png")
		block.Image.Meta = map[string]any{handoffMetaKey: handoffEnvelopeFor(png)}

		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		requireHandoffError(t, err, imageErrorInvalidHandoff, 0, "requires a file uri")
	})

	t.Run("file uri alone signals handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", "image/png")
		block.Image.Uri = &uri

		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		requireHandoffError(t, err, imageErrorInvalidHandoff, 0, "block metadata")
	})

	t.Run("empty data without intent stays missing data", func(t *testing.T) {
		_, err := validateHandoffBlock(t, root, acp.ImageBlock("", "image/png"), defaultImageLimits())
		requireImageParamError(t, err, imageErrorMissingData, 0)
	})

	t.Run("non-file uri without data is not handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", "image/png")
		remote := "https://example.test/a.png"
		block.Image.Uri = &remote

		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		requireImageParamError(t, err, imageErrorMissingData, 0)
	})

	t.Run("unparseable uri without data is not handoff intent", func(t *testing.T) {
		block := acp.ImageBlock("", "image/png")
		broken := "file://ho\x7fst/a.png"
		block.Image.Uri = &broken

		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		requireImageParamError(t, err, imageErrorMissingData, 0)
	})
}

func TestHandoffRootUnsetRejectsHandoffForm(t *testing.T) {
	root := t.TempDir()
	block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")

	_, err := validateHandoffBlock(t, "", block, defaultImageLimits())
	requireHandoffError(t, err, imageErrorInvalidHandoff, 0, "configured handoff root")
}

func TestHandoffEnvelopeDefects(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	uri := handoffFileURI(t, root, "valid.png", png)
	valid := handoffEnvelopeFor(png)
	validDigest, ok := valid[handoffFieldDigest].(string)
	require.True(t, ok)

	withEnvelope := func(mutate func(map[string]any)) map[string]any {
		envelope := maps.Clone(valid)
		mutate(envelope)

		return envelope
	}

	tests := []struct {
		name     string
		envelope map[string]any
		message  string
	}{
		{
			name:     "unknown field",
			envelope: withEnvelope(func(envelope map[string]any) { envelope["extra"] = true }),
			message:  "exactly version, digest, and sizeBytes",
		},
		{
			name:     "missing field",
			envelope: withEnvelope(func(envelope map[string]any) { delete(envelope, handoffFieldDigest) }),
			message:  "exactly version, digest, and sizeBytes",
		},
		{
			name:     "unsupported version",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldVersion] = 2 }),
			message:  "unsupported handoff metadata version",
		},
		{
			name:     "version wrong type",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldVersion] = "1" }),
			message:  "unsupported handoff metadata version",
		},
		{
			name:     "float version",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldVersion] = 1.5 }),
			message:  "unsupported handoff metadata version",
		},
		{
			name:     "digest wrong type",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldDigest] = 1 }),
			message:  "64 lowercase hex characters",
		},
		{
			name:     "digest too short",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldDigest] = "abcdef" }),
			message:  "64 lowercase hex characters",
		},
		{
			name: "digest uppercase",
			envelope: withEnvelope(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = strings.ToUpper(validDigest)
			}),
			message: "64 lowercase hex characters",
		},
		{
			name: "digest not hex",
			envelope: withEnvelope(func(envelope map[string]any) {
				envelope[handoffFieldDigest] = strings.Repeat("z", handoffDigestHexLength)
			}),
			message: "64 lowercase hex characters",
		},
		{
			name:     "sizeBytes wrong type",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldSizeBytes] = "12" }),
			message:  "non-negative integer",
		},
		{
			name:     "sizeBytes negative",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldSizeBytes] = -1 }),
			message:  "non-negative integer",
		},
		{
			name:     "sizeBytes negative float",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldSizeBytes] = -1.0 }),
			message:  "non-negative integer",
		},
		{
			name:     "sizeBytes fractional",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldSizeBytes] = 12.5 }),
			message:  "non-negative integer",
		},
		{
			name:     "sizeBytes beyond int64",
			envelope: withEnvelope(func(envelope map[string]any) { envelope[handoffFieldSizeBytes] = 1e30 }),
			message:  "non-negative integer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", test.envelope), defaultImageLimits())
			requireHandoffError(t, err, imageErrorInvalidHandoff, 0, test.message)
		})
	}

	t.Run("envelope is not an object", func(t *testing.T) {
		block := acp.ImageBlock("", "image/png")
		block.Image.Uri = &uri
		block.Image.Meta = map[string]any{handoffMetaKey: "handoff"}

		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		requireHandoffError(t, err, imageErrorInvalidHandoff, 0, "exactly version, digest, and sizeBytes")
	})

	t.Run("float sizeBytes and version from the wire are accepted", func(t *testing.T) {
		block := handoffImageBlock(uri, "image/png", map[string]any{
			handoffFieldVersion:   float64(handoffVersion),
			handoffFieldDigest:    valid[handoffFieldDigest],
			handoffFieldSizeBytes: float64(len(png)),
		})

		data, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		require.NoError(t, err)
		require.Equal(t, base64.StdEncoding.EncodeToString(png), data)
	})
}

func TestHandoffURIDefects(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	envelope := handoffEnvelopeFor(png)
	handoffFileURI(t, root, "valid.png", png)

	tests := []struct {
		name    string
		uri     string
		message string
	}{
		{name: "empty", uri: "", message: "requires a file uri"},
		{name: "unparseable", uri: "file://ho\x7fst/a.png", message: "not a valid uri"},
		{name: "wrong scheme", uri: "https://example.test/a.png", message: "scheme must be file"},
		{name: "remote host", uri: "file://elsewhere/a.png", message: "host is not local"},
		{name: "opaque path", uri: "file:relative.png", message: "path must be absolute"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := acp.ImageBlock("", "image/png")
			block.Image.Meta = map[string]any{handoffMetaKey: envelope}

			if test.uri != "" {
				block.Image.Uri = &test.uri
			}

			_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
			requireHandoffError(t, err, imageErrorInvalidHandoff, 0, test.message)
		})
	}

	t.Run("localhost host is local", func(t *testing.T) {
		uri := "file://" + fileURILocalHost + filepath.ToSlash(filepath.Join(root, "valid.png"))

		data, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		require.NoError(t, err)
		require.Equal(t, base64.StdEncoding.EncodeToString(png), data)
	})
}

func TestHandoffPathNotAllowed(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	envelope := handoffEnvelopeFor(png)

	t.Run("outside the root", func(t *testing.T) {
		root := t.TempDir()
		outside := handoffFileURI(t, t.TempDir(), "valid.png", png)

		_, err := validateHandoffBlock(t, root, handoffImageBlock(outside, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "outside the handoff root")
	})

	t.Run("symlink escaping the root", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(t.TempDir(), "valid.png")
		require.NoError(t, os.WriteFile(target, png, 0o600))

		link := filepath.Join(root, "link.png")
		require.NoError(t, os.Symlink(target, link))

		_, err := validateHandoffBlock(t, root, handoffImageBlock(fileURIFor(link), "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "escapes the handoff root")
	})

	t.Run("symlink inside the root resolves", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "target.png")
		require.NoError(t, os.WriteFile(target, png, 0o600))

		link := filepath.Join(root, "link.png")
		require.NoError(t, os.Symlink(target, link))

		data, err := validateHandoffBlock(t, root, handoffImageBlock(fileURIFor(link), "image/png", envelope), defaultImageLimits())
		require.NoError(t, err)
		require.Equal(t, base64.StdEncoding.EncodeToString(png), data)
	})

	t.Run("directory is not a regular file", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "nested")
		require.NoError(t, os.MkdirAll(dir, 0o700))

		_, err := validateHandoffBlock(t, root, handoffImageBlock(fileURIFor(dir), "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "not a regular file")
	})

	t.Run("path through a regular file", func(t *testing.T) {
		root := t.TempDir()
		handoffFileURI(t, root, "valid.png", png)
		nested := fileURIFor(filepath.Join(root, "valid.png", "child.png"))

		_, err := validateHandoffBlock(t, root, handoffImageBlock(nested, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "cannot be resolved safely")
	})

	t.Run("unresolvable root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "absent")
		uri := fileURIFor(filepath.Join(root, "valid.png"))

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "handoff root cannot be resolved")
	})

	t.Run("inspection failure", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := handoffLstat
		handoffLstat = func(string) (os.FileInfo, error) { return nil, errors.New("injected inspect failure") }

		t.Cleanup(func() { handoffLstat = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "cannot be inspected")
	})
}

func TestHandoffMissingFile(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	envelope := handoffEnvelopeFor(png)

	t.Run("vanished path inside the root", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)
		require.NoError(t, os.Remove(filepath.Join(root, "valid.png")))

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "does not exist")
	})

	t.Run("vanished between resolution and inspection", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := handoffLstat
		handoffLstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }

		t.Cleanup(func() { handoffLstat = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "does not exist")
	})

	t.Run("unopenable file", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := handoffOpen
		handoffOpen = func(string) (io.ReadCloser, error) { return nil, errors.New("injected open failure") }

		t.Cleanup(func() { handoffOpen = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "cannot be opened")
	})

	t.Run("unreadable file", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := handoffOpen
		handoffOpen = func(string) (io.ReadCloser, error) {
			return failingHandoffReader{err: errors.New("injected read failure")}, nil
		}

		t.Cleanup(func() { handoffOpen = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "injected read failure")
	})
}

// failingHandoffReader is an opened handoff file whose reads fail.
type failingHandoffReader struct{ err error }

func (r failingHandoffReader) Read([]byte) (int, error) { return 0, r.err }

func (r failingHandoffReader) Close() error { return nil }

func TestHandoffDigestMismatchFailsClosed(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")

	t.Run("byte tamper", func(t *testing.T) {
		root := t.TempDir()
		tampered := append([]byte(nil), png...)
		tampered[len(tampered)-1] ^= 0xFF
		uri := handoffFileURI(t, root, "valid.png", tampered)

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)), defaultImageLimits())
		requireHandoffError(t, err, imageErrorDigestMismatch, 0, "sha256")
	})

	t.Run("size mismatch", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.gif", gif)

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/gif", handoffEnvelopeFor(png)), defaultImageLimits())
		requireHandoffError(t, err, imageErrorDigestMismatch, 0, "declared")
	})
}

// TestHandoffMirrorsEmbeddedTaxonomy pins that a verified handoff file runs the
// same gate chain as embedded data, minus the two gates that are meaningless
// without base64.
func TestHandoffMirrorsEmbeddedTaxonomy(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		mimeType string
		want     string
	}{
		{name: "missing mime", fixture: "valid.png", mimeType: "", want: imageErrorInvalidMediaType},
		{name: "non-canonical mime", fixture: "valid.jpg", mimeType: "image/jpg", want: imageErrorInvalidMediaType},
		{name: "svg excluded", fixture: "valid.png", mimeType: "image/svg+xml", want: imageErrorInvalidMediaType},
		{name: "jpeg declared png", fixture: "mismatch.png", mimeType: "image/png", want: imageErrorMediaTypeMismatch},
		{name: "truncated png", fixture: "truncated.png", mimeType: "image/png", want: imageErrorInvalidDimensions},
		{name: "animated gif", fixture: "animated.gif", mimeType: "image/gif", want: imageErrorAnimated},
		{name: "animated webp", fixture: "animated.webp", mimeType: "image/webp", want: imageErrorAnimated},
		{name: "animated apng", fixture: "animated-apng.png", mimeType: "image/png", want: imageErrorAnimated},
		{name: "single frame actl png", fixture: "single-frame-actl.png", mimeType: "image/png", want: imageErrorAnimated},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			block, _ := handoffFixtureBlock(t, root, test.fixture, test.mimeType)

			_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
			requireImageParamError(t, err, test.want, 0)
		})
	}

	t.Run("unsniffable bytes", func(t *testing.T) {
		root := t.TempDir()
		data := []byte("not a raster at all")
		uri := handoffFileURI(t, root, "junk.png", data)

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", handoffEnvelopeFor(data)), defaultImageLimits())
		requireImageParamError(t, err, imageErrorMediaTypeMismatch, 0)
	})

	t.Run("every allowlisted format is accepted", func(t *testing.T) {
		root := t.TempDir()
		fixtures := map[string]string{
			"valid.png":  "image/png",
			"valid.jpg":  "image/jpeg",
			"valid.gif":  "image/gif",
			"valid.webp": "image/webp",
		}

		for name, mimeType := range fixtures {
			block, data := handoffFixtureBlock(t, root, name, mimeType)

			encoded, err := validateHandoffBlock(t, root, block, defaultImageLimits())
			require.NoError(t, err)
			require.Equal(t, base64.StdEncoding.EncodeToString(data), encoded)
		}
	})
}

func TestHandoffPerImageLimitReportsRealSize(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	size := int64(len(png))

	t.Run("at the limit", func(t *testing.T) {
		root := t.TempDir()
		block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")

		_, err := validateHandoffBlock(t, root, block, ImageLimits{MaxInputBytesPerImage: size})
		require.NoError(t, err)
	})

	t.Run("one byte over the limit", func(t *testing.T) {
		root := t.TempDir()
		block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")

		_, err := validateHandoffBlock(t, root, block, ImageLimits{MaxInputBytesPerImage: size - 1})
		details := requireImageParamError(t, err, imageErrorTooLarge, 0)
		require.InDelta(t, float64(size), details[jsonFieldSizeBytes], 0)
		require.InDelta(t, float64(size-1), details[jsonFieldMaxBytes], 0)
	})

	t.Run("bounded read still reports the real size", func(t *testing.T) {
		root := t.TempDir()
		block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")

		_, err := validateHandoffBlock(t, root, block, ImageLimits{MaxInputBytesPerImage: size - 8})
		details := requireImageParamError(t, err, imageErrorTooLarge, 0)
		require.InDelta(t, float64(size), details[jsonFieldSizeBytes], 0)
		require.InDelta(t, float64(size-8), details[jsonFieldMaxBytes], 0)
	})
}

func TestHandoffBytesCountTowardPromptAggregate(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")
	total := int64(len(png) + len(gif))

	pngURI := handoffFileURI(t, root, "valid.png", png)
	gifURI := handoffFileURI(t, root, "valid.gif", gif)

	blocks := []acp.ContentBlock{
		handoffImageBlock(pngURI, "image/png", handoffEnvelopeFor(png)),
		handoffImageBlock(gifURI, "image/gif", handoffEnvelopeFor(gif)),
	}

	mapped, err := promptToPi(blocks, ImageLimits{MaxInputBytesPerPrompt: total}, root)
	require.NoError(t, err)
	require.Len(t, mapped.Images, 2)

	_, err = promptToPi(blocks, ImageLimits{MaxInputBytesPerPrompt: total - 1}, root)
	details := requireImageParamError(t, err, imageErrorTooLarge, 1)
	require.InDelta(t, float64(total), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(total-1), details[jsonFieldMaxBytes], 0)
}

func TestHandoffSharesIndexSequenceWithEmbeddedForms(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	webpMime := "image/webp"

	_, err := promptToPi([]acp.ContentBlock{
		acp.ImageBlock(base64.StdEncoding.EncodeToString(png), "image/png"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{
			Uri: "file:///b.webp", Blob: fixtureBase64(t, "valid.webp"), MimeType: &webpMime,
		}}),
		handoffImageBlock(handoffFileURI(t, root, "animated.gif", fixtureBytes(t, "animated.gif")), "image/gif",
			handoffEnvelopeFor(fixtureBytes(t, "animated.gif"))),
	}, defaultImageLimits(), root)
	requireImageParamError(t, err, imageErrorAnimated, 2)
}

// TestHandoffNativeRequestMatchesEmbedded pins that the two input forms over
// the same bytes build byte-identical native requests and that the handoff path
// never reaches one.
func TestHandoffNativeRequestMatchesEmbedded(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	uri := handoffFileURI(t, root, "nested/deep/valid.png", png)

	handoff, err := promptToPi([]acp.ContentBlock{
		acp.TextBlock("describe this"),
		handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)),
	}, defaultImageLimits(), root)
	require.NoError(t, err)

	embedded, err := promptToPi([]acp.ContentBlock{
		acp.TextBlock("describe this"),
		acp.ImageBlock(base64.StdEncoding.EncodeToString(png), "image/png"),
	}, defaultImageLimits(), root)
	require.NoError(t, err)

	require.Equal(t, embedded, handoff)

	encoded, err := json.Marshal(handoff)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "nested/deep")
	require.NotContains(t, string(encoded), root)
	require.NotContains(t, string(encoded), fileURIScheme+":")
	require.NotContains(t, string(encoded), handoffMetaKey)
}

func TestValidateInputHandoffRoot(t *testing.T) {
	require.NoError(t, validateInputHandoffRoot(""))
	require.NoError(t, validateInputHandoffRoot(t.TempDir()))
	require.ErrorContains(t, validateInputHandoffRoot("relative/handoff"), "absolute path")
}

func TestWithInputHandoffRootRejectsRelativePath(t *testing.T) {
	_, err := NewAgent(WithInputHandoffRoot("relative/handoff")).Initialize(t.Context(), defaultInitializeRequest())

	var requestError *acp.RequestError

	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32602, requestError.Code)
}

func TestHandoffErrorReportsItsCause(t *testing.T) {
	var failure error = &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}

	require.EqualError(t, failure, handoffFileAbsentMessage)
}

func TestPathWithinRoot(t *testing.T) {
	root := filepath.Join(string(filepath.Separator), "handoff", "root")

	require.True(t, pathWithinRoot(root, root))
	require.True(t, pathWithinRoot(filepath.Join(root, "a.png"), root))
	require.False(t, pathWithinRoot(filepath.Join(string(filepath.Separator), "handoff", "rootsibling"), root))
}
