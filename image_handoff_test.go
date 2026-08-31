package piacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

func TestHandoffCapabilityScalar(t *testing.T) {
	withoutRoot, err := NewAgent().Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	require.NotContains(t, withoutRoot.AgentCapabilities.Meta, handoffMetaKey)

	withRoot, err := NewAgent(WithInputHandoffRoot(t.TempDir())).Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	require.Equal(t,
		map[string]any{"version": 1},
		withRoot.AgentCapabilities.Meta["acp-go.dev/handoff"],
	)
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

	return newPromptImageBudget(limits, root).validateBlock(t.Context(), block.Image)
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

	t.Run("unparsable uri without data is not handoff intent", func(t *testing.T) {
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
		{name: "unparsable", uri: "file://ho\x7fst/a.png", message: "not a valid uri"},
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
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "cannot be opened")
	})

	t.Run("relative symlink inside the root resolves", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(root, "target.png"), png, 0o600))

		link := filepath.Join(root, "link.png")
		require.NoError(t, os.Symlink("target.png", link))

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
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "cannot be opened")
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

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) {
			return failingHandoffReader{statErr: errors.New("injected inspect failure")}, nil
		}

		t.Cleanup(func() { openHandoffFile = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "cannot be inspected")
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

	t.Run("unopenable file", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := openHandoffFile
		openHandoffFile = func(*os.Root, string) (handoffFile, error) {
			return nil, errors.New("injected open failure")
		}

		t.Cleanup(func() { openHandoffFile = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "cannot be opened")
	})

	t.Run("unreadable file", func(t *testing.T) {
		root := t.TempDir()
		uri := handoffFileURI(t, root, "valid.png", png)

		restore := openHandoffFile
		openHandoffFile = func(root *os.Root, rel string) (handoffFile, error) {
			info, statErr := root.Stat(rel)
			if statErr != nil {
				return nil, statErr
			}

			return failingHandoffReader{err: errors.New("injected read failure"), info: info}, nil
		}

		t.Cleanup(func() { openHandoffFile = restore })

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		requireHandoffError(t, err, imageErrorMissingFile, 0, "cannot be read")
	})
}

// failingHandoffReader is an opened handoff file whose inspection or read fails,
// standing in for the descriptor-level failures a real filesystem only produces
// under a race with the host that owns the file.
type failingHandoffReader struct {
	err     error
	statErr error
	info    os.FileInfo
}

func (r failingHandoffReader) Read([]byte) (int, error) { return 0, r.err }

func (r failingHandoffReader) Close() error { return nil }

func (r failingHandoffReader) Stat() (os.FileInfo, error) {
	if r.statErr != nil {
		return nil, r.statErr
	}

	return r.info, nil
}

func TestHandoffDigestMismatchFailsClosed(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")

	t.Run("byte tamper", func(t *testing.T) {
		root := t.TempDir()
		tampered := append([]byte(nil), png...)
		tampered[len(tampered)-1] ^= 0xFF
		uri := handoffFileURI(t, root, "valid.png", tampered)

		_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)), defaultImageLimits())
		requireHandoffError(t, err, imageErrorDigestMismatch, 0, "declared digest")
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

func TestHandoffPerImageLimitRejectsOnBytesRead(t *testing.T) {
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

	t.Run("the verdict reports the declared size, never a measured one", func(t *testing.T) {
		root := t.TempDir()
		block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")
		bound := size - 8

		// The size in the verdict is the one the caller declared, so it tells a
		// host nothing it did not already know about the file.
		_, err := validateHandoffBlock(t, root, block, ImageLimits{MaxInputBytesPerImage: bound})
		details := requireImageParamError(t, err, imageErrorTooLarge, 0)
		require.InDelta(t, float64(size), details[jsonFieldSizeBytes], 0)
		require.InDelta(t, float64(bound), details[jsonFieldMaxBytes], 0)
	})
}

// TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem pins the pre-gate
// order: a declaration this adapter was never going to accept costs it no open,
// no read and no hash. The absent name proves the verdict needs no file at all;
// the name outside the root proves the declared type outranks the location, so a
// bad-MIME probe cannot learn whether the file it named is there.
func TestHandoffDeclaredMediaTypeIsJudgedBeforeTheFilesystem(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	root := t.TempDir()
	outside := handoffFileURI(t, t.TempDir(), "outside.png", png)

	for _, test := range []struct {
		name string
		uri  string
	}{
		{name: "the name does not exist", uri: fileURIFor(filepath.Join(root, "absent.png"))},
		{name: "the name leaves the root", uri: outside},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateHandoffBlock(t, root,
				handoffImageBlock(test.uri, "image/svg+xml", handoffEnvelopeFor(png)), defaultImageLimits())
			requireImageParamError(t, err, imageErrorInvalidMediaType, 0)
		})
	}
}

func TestHandoffOversizeReadIsRejectedWithoutForwardingBytes(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	bound := int64(len(png))
	limits := ImageLimits{MaxInputBytesPerImage: bound}

	t.Run("a declared size past the gate is rejected before anything is opened", func(t *testing.T) {
		root := t.TempDir()
		outside := handoffFileURI(t, t.TempDir(), "outside.png", png)

		// No file is written inside the root and the second name would be refused
		// for its location, so the only way either produces too_large is by judging
		// the caller's own declaration ahead of the open.
		envelope := handoffEnvelopeFor(png)
		envelope[handoffFieldSizeBytes] = int(bound + 1)

		for _, uri := range []string{fileURIFor(filepath.Join(root, "valid.png")), outside} {
			_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), limits)
			details := requireImageParamError(t, err, imageErrorTooLarge, 0)
			require.InDelta(t, float64(bound+1), details[jsonFieldSizeBytes], 0)
			require.InDelta(t, float64(bound), details[jsonFieldMaxBytes], 0)
		}
	})

	t.Run("a file larger than its declaration forwards nothing", func(t *testing.T) {
		root := t.TempDir()

		// The file on disk holds one byte more than the envelope describes,
		// which is what a file appended to after the block was written looks
		// like. Its bytes were never verified, so none may survive the read.
		grown := make([]byte, bound+1)
		copy(grown, png)
		uri := handoffFileURI(t, root, "valid.png", grown)

		mapped, err := promptToPi(t.Context(),
			[]acp.ContentBlock{handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png))},
			ImageLimits{MaxInputBytesPerImage: bound + 1}, root)
		requireHandoffError(t, err, imageErrorDigestMismatch, 0, "declared sizeBytes")
		require.Empty(t, mapped.Images)
		require.Empty(t, mapped.Message)
	})
}

func TestHandoffBlockCountCapRejectsWithAggregateDisabled(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	uri := handoffFileURI(t, root, "valid.png", png)

	blocks := make([]acp.ContentBlock, 0, maxHandoffBlocksPerPrompt+1)
	for range maxHandoffBlocksPerPrompt + 1 {
		blocks = append(blocks, handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)))
	}

	// The byte aggregate is disabled and every block is a small valid image, so
	// the block count is the only thing that can reject any of them.
	mapped, err := promptToPi(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: 0}, root)
	details := requireImageParamError(t, err, imageErrorTooLarge, maxHandoffBlocksPerPrompt)
	require.InDelta(t, float64(maxHandoffBlocksPerPrompt+1), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(maxHandoffBlocksPerPrompt), details[jsonFieldMaxBytes], 0)
	require.Empty(t, mapped.Images)

	accepted, err := promptToPi(t.Context(), blocks[:maxHandoffBlocksPerPrompt], ImageLimits{MaxInputBytesPerPrompt: 0}, root)
	require.NoError(t, err)
	require.Len(t, accepted.Images, maxHandoffBlocksPerPrompt)
}

func TestHandoffSymlinkContainmentIsKernelEnforced(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	envelope := handoffEnvelopeFor(png)

	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "secret.png")
	require.NoError(t, os.WriteFile(outside, png, 0o600))

	tests := []struct {
		name    string
		link    string
		target  string
		value   string
		message string
	}{
		{
			name:    "a relative link inside the root resolves",
			link:    "inside.png",
			target:  "valid.png",
			value:   "",
			message: "",
		},
		{
			name:    "a relative link out of the root is refused",
			link:    "escape.png",
			target:  filepath.Join("..", filepath.Base(outsideDir), "secret.png"),
			value:   imageErrorPathNotAllowed,
			message: "cannot be opened",
		},
		{
			name:    "an absolute link is refused even inside the root",
			link:    "absolute.png",
			target:  "",
			value:   imageErrorPathNotAllowed,
			message: "cannot be opened",
		},
		{
			name:    "a link whose target was cleaned up is missing",
			link:    "dangling.png",
			target:  "gone.png",
			value:   imageErrorMissingFile,
			message: "does not exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "valid.png"), png, 0o600))

			target := test.target
			if target == "" {
				target = filepath.Join(root, "valid.png")
			}

			require.NoError(t, os.Symlink(target, filepath.Join(root, test.link)))

			block := handoffImageBlock(fileURIFor(filepath.Join(root, test.link)), "image/png", envelope)

			encoded, err := validateHandoffBlock(t, root, block, defaultImageLimits())
			if test.value == "" {
				require.NoError(t, err)
				require.Equal(t, base64.StdEncoding.EncodeToString(png), encoded)

				return
			}

			requireHandoffError(t, err, test.value, 0, test.message)
		})
	}
}

func TestHandoffTraversalOutOfTheRootIsRefused(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")

	outside := filepath.Join(t.TempDir(), "secret.png")
	require.NoError(t, os.WriteFile(outside, png, 0o600))

	// Percent-encoded traversal decodes before the path is ever cleaned, so it
	// collapses to a path that was never under the root.
	uri := "file://" + filepath.ToSlash(root) + "/%2e%2e/" + filepath.Base(filepath.Dir(outside)) + "/secret.png"

	_, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)), defaultImageLimits())
	requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "outside the handoff root")
}

func TestHandoffReadHonoursACancelledContext(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := promptToPi(ctx, []acp.ContentBlock{block}, defaultImageLimits(), root)
	require.ErrorIs(t, err, context.Canceled)

	handle, failure := newPromptImageBudget(defaultImageLimits(), root).handoffRootHandle()
	require.Nil(t, failure)

	t.Cleanup(func() { require.NoError(t, handle.Close()) })

	_, readFailure := readHandoffFile(ctx, handle, "valid.png", int64(len(png)))
	require.NotNil(t, readFailure)
	require.Equal(t, imageErrorMissingFile, readFailure.value)
}

// rootSnapshot records every entry under root with the identity and size that
// would change if the adapter wrote, moved, or removed anything.
func rootSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()

	snapshot := map[string]string{}

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}

		snapshot[path] = fmt.Sprintf("%v|%d|%v", info.Mode(), info.Size(), info.ModTime())

		return nil
	}))

	return snapshot
}

func TestHandoffReadNeverMutatesTheRoot(t *testing.T) {
	root := t.TempDir()
	block, data := handoffFixtureBlock(t, root, "valid.png", "image/png")

	before := rootSnapshot(t, root)

	encoded, err := validateHandoffBlock(t, root, block, defaultImageLimits())
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(data), encoded)

	// The root is a read root: a turn that consumed a file from it leaves the
	// tree byte-for-byte as it found it.
	require.Equal(t, before, rootSnapshot(t, root))
}

func TestHandoffMessagesCarryNoObservedValues(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")

	uri := handoffFileURI(t, root, "valid.png", png)
	outside := filepath.Join(t.TempDir(), "outside.png")
	require.NoError(t, os.WriteFile(outside, png, 0o600))

	// Every constant this file can put in front of a client. A verdict travels
	// to the caller and into telemetry, so the set is closed by construction.
	allowed := map[string]bool{
		handoffRootUnsetMessage:                                                true,
		handoffRootUnresolvedMessage:                                           true,
		handoffOutsideRootMessage:                                              true,
		handoffNotRegularMessage:                                               true,
		handoffUninspectableMessage:                                            true,
		handoffUnopenableMessage:                                               true,
		handoffUnreadableMessage:                                               true,
		handoffSizeMismatchMessage:                                             true,
		handoffDigestMismatchMessage:                                           true,
		handoffFileAbsentMessage:                                               true,
		"handoff image requires a file uri":                                    true,
		"handoff image uri is not a valid uri":                                 true,
		"handoff image uri scheme must be file":                                true,
		"handoff image uri host is not local":                                  true,
		"handoff image uri path must be absolute":                              true,
		"unsupported handoff metadata version":                                 true,
		"handoff digest must be 64 lowercase hex characters":                   true,
		"handoff sizeBytes must be a non-negative integer":                     true,
		"handoff image requires " + handoffMetaKey + " block metadata":         true,
		"handoff metadata must contain exactly version, digest, and sizeBytes": true,
	}

	tampered := append([]byte(nil), png...)
	tampered[len(tampered)-1] ^= 0xFF
	tamperedURI := handoffFileURI(t, root, "tampered.png", tampered)

	cases := []acp.ContentBlock{
		handoffImageBlock(uri, "image/png", nil),
		handoffImageBlock("file://"+filepath.ToSlash(outside), "image/png", handoffEnvelopeFor(png)),
		handoffImageBlock(fileURIFor(filepath.Join(root, "absent.png")), "image/png", handoffEnvelopeFor(png)),
		handoffImageBlock(tamperedURI, "image/png", handoffEnvelopeFor(png)),
		handoffImageBlock(uri, "image/png", handoffEnvelopeFor(gif)),
		handoffImageBlock("http://example.test/x.png", "image/png", handoffEnvelopeFor(png)),
	}

	for _, block := range cases {
		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		require.Error(t, err)

		var requestErr *acp.RequestError
		require.ErrorAs(t, err, &requestErr)

		data, ok := requestErr.Data.(map[string]any)
		require.True(t, ok)

		message, ok := data[jsonFieldMessage].(string)
		require.True(t, ok)
		require.True(t, allowed[message], "message is not a declared constant: %q", message)

		// A message added later that interpolates something observed would not
		// be in the set above, but this also fails it outright: no verdict may
		// carry a path, a uri, a filename, a digest, or any number measured
		// from the filesystem.
		require.NotContains(t, message, root)
		require.NotContains(t, message, outside)
		require.NotContains(t, message, "valid.png")
		require.NotContains(t, message, "file://")
		require.NotRegexp(t, `[0-9a-f]{16}`, message)
	}
}

func TestTextResourceBytesCountTowardPromptAggregate(t *testing.T) {
	text := strings.Repeat("a", 4096)

	// Declaring bytes as text rather than as a blob must not buy a prompt more
	// of them than the aggregate allows.
	blocks := []acp.ContentBlock{
		acp.ResourceBlock(acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{
			Uri: "file:///a.txt", Text: text,
		}}),
	}

	mapped, err := promptToPi(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: int64(len(text))}, "")
	require.NoError(t, err)
	require.Contains(t, mapped.Message, text)

	_, err = promptToPi(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: int64(len(text)) - 1}, "")
	details := requireResourceParamError(t, err, imageErrorTooLarge, 0)
	require.InDelta(t, float64(len(text)), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(len(text)-1), details[jsonFieldMaxBytes], 0)
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

	mapped, err := promptToPi(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: total}, root)
	require.NoError(t, err)
	require.Len(t, mapped.Images, 2)

	_, err = promptToPi(t.Context(), blocks, ImageLimits{MaxInputBytesPerPrompt: total - 1}, root)
	details := requireImageParamError(t, err, imageErrorTooLarge, 1)
	require.InDelta(t, float64(total), details[jsonFieldSizeBytes], 0)
	require.InDelta(t, float64(total-1), details[jsonFieldMaxBytes], 0)
}

func TestHandoffSharesIndexSequenceWithEmbeddedForms(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	webpMime := "image/webp"

	_, err := promptToPi(t.Context(), []acp.ContentBlock{
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

	handoff, err := promptToPi(t.Context(), []acp.ContentBlock{
		acp.TextBlock("describe this"),
		handoffImageBlock(uri, "image/png", handoffEnvelopeFor(png)),
	}, defaultImageLimits(), root)
	require.NoError(t, err)

	embedded, err := promptToPi(t.Context(), []acp.ContentBlock{
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

	requireUnsupportedOption(t, err, optionFieldInputHandoffRoot)
}

func TestHandoffErrorReportsItsCause(t *testing.T) {
	var failure error = &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}

	require.EqualError(t, failure, handoffFileAbsentMessage)
}

func TestHandoffEnvelopeAcceptsNumbersFromADecoder(t *testing.T) {
	root := t.TempDir()
	png := fixtureBytes(t, "valid.png")
	uri := handoffFileURI(t, root, "valid.png", png)
	sum := sha256.Sum256(png)

	raw := fmt.Sprintf(`{"version":1,"digest":%q,"sizeBytes":%d}`, hex.EncodeToString(sum[:]), len(png))

	// The pinned SDK decodes envelope numbers to float64, but a decoder asked
	// for json.Number is one upstream flag away and must validate identically.
	for _, useNumber := range []bool{false, true} {
		decoder := json.NewDecoder(strings.NewReader(raw))
		if useNumber {
			decoder.UseNumber()
		}

		var envelope map[string]any

		require.NoError(t, decoder.Decode(&envelope))

		encoded, err := validateHandoffBlock(t, root, handoffImageBlock(uri, "image/png", envelope), defaultImageLimits())
		require.NoError(t, err, "useNumber=%v", useNumber)
		require.Equal(t, base64.StdEncoding.EncodeToString(png), encoded)
	}
}

func TestHandoffRelativePath(t *testing.T) {
	root := filepath.Join(string(filepath.Separator), "root")

	rel, failure := handoffRelativePath(root, filepath.Join(root, "sub", "a.png"))
	require.Nil(t, failure)
	require.Equal(t, filepath.Join("sub", "a.png"), rel)

	// A sibling whose name merely starts with the root is not under it.
	_, failure = handoffRelativePath(root, filepath.Join(string(filepath.Separator), "rootx", "a.png"))
	require.NotNil(t, failure)
	require.Equal(t, imageErrorPathNotAllowed, failure.value)

	_, failure = handoffRelativePath(root, filepath.Join(string(filepath.Separator), "etc", "passwd"))
	require.NotNil(t, failure)
	require.Equal(t, imageErrorPathNotAllowed, failure.value)

	// The root itself is relative to itself, and is refused later for not being
	// a regular file rather than for being out of the root.
	rel, failure = handoffRelativePath(root, root)
	require.Nil(t, failure)
	require.Equal(t, ".", rel)
}
