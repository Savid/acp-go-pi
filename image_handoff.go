package piacp

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// Handoff-form prompt image error values. They ride the same -32602 data shape
// as every other prompt image failure, plus a human message naming the real
// cause. path_not_allowed and missing_file are channel-qualified: as a -32602
// error value they describe a host-supplied inbound handoff path, while the
// identical tokens on a pi_turn_failed reason describe a harness-returned
// outbound artifact path.
const (
	imageErrorInvalidHandoff = "invalid_handoff"
	imageErrorPathNotAllowed = "path_not_allowed"
	imageErrorMissingFile    = "missing_file"
	imageErrorDigestMismatch = "handoff_digest_mismatch"
)

const (
	handoffMetaKey = "acp-go.dev/handoff"
	handoffVersion = 1

	handoffFieldVersion   = "version"
	handoffFieldDigest    = "digest"
	handoffFieldSizeBytes = "sizeBytes"

	handoffEnvelopeFieldCount = 3
	handoffDigestHexLength    = 64

	fileURIScheme    = "file"
	fileURILocalHost = "localhost"

	handoffFileAbsentMessage = "handoff image file does not exist"
)

// Filesystem seams for the handoff read. Tests substitute them to exercise
// inspection and read failures that a real filesystem only produces under a
// race with the host that owns the file.
var (
	handoffLstat = os.Lstat
	handoffOpen  = func(name string) (io.ReadCloser, error) { return os.Open(name) }
)

// validateInputHandoffRoot rejects a relative handoff root at agent
// construction. An empty root is valid and rejects the handoff form.
func validateInputHandoffRoot(root string) error {
	if root == "" || filepath.IsAbs(root) {
		return nil
	}

	return errors.New("input handoff root must be an absolute path")
}

// handoffEnvelope is one decoded acp-go.dev/handoff block envelope.
type handoffEnvelope struct {
	digest    string
	sizeBytes int64
}

// handoffError is one handoff pre-gate failure: the image error value plus
// the human message naming the real cause.
type handoffError struct {
	value   string
	message string
}

func (e *handoffError) Error() string {
	return e.message
}

func handoffInvalid(message string) *handoffError {
	return &handoffError{value: imageErrorInvalidHandoff, message: message}
}

// imageHandoffError builds the -32602 prompt image error for a handoff
// pre-gate failure, carrying the truthful cause as a human message.
func imageHandoffError(failure *handoffError, index int) *acp.RequestError {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldField:   fieldPromptImage,
		jsonFieldError:   failure.value,
		jsonFieldIndex:   index,
		jsonFieldMessage: failure.message,
	})
}

// handoffIntent reports whether an empty-data image block is trying to be a
// handoff block. Intent is a handoff envelope key or a file uri; a block with
// neither stays the embedded form's missing_data.
func handoffIntent(block *acp.ContentBlockImage) bool {
	if _, present := block.Meta[handoffMetaKey]; present {
		return true
	}

	if block.Uri == nil {
		return false
	}

	parsed, err := url.Parse(*block.Uri)

	return err == nil && parsed.Scheme == fileURIScheme
}

// parseHandoffEnvelope decodes the acp-go.dev/handoff block envelope. It
// carries exactly version, digest, and sizeBytes; anything else, a missing
// field, or an unknown field is a block malformed as a block.
func parseHandoffEnvelope(meta map[string]any) (handoffEnvelope, *handoffError) {
	value, present := meta[handoffMetaKey]
	if !present {
		return handoffEnvelope{}, handoffInvalid("handoff image requires " + handoffMetaKey + " block metadata")
	}

	object, ok := value.(map[string]any)
	if !ok || len(object) != handoffEnvelopeFieldCount {
		return handoffEnvelope{}, handoffInvalid("handoff metadata must contain exactly version, digest, and sizeBytes")
	}

	if !handoffVersionIsOne(object[handoffFieldVersion]) {
		return handoffEnvelope{}, handoffInvalid("unsupported handoff metadata version")
	}

	digest, ok := object[handoffFieldDigest].(string)
	if !ok || !validHandoffDigest(digest) {
		return handoffEnvelope{}, handoffInvalid("handoff digest must be 64 lowercase hex characters")
	}

	sizeBytes, ok := handoffSizeBytes(object[handoffFieldSizeBytes])
	if !ok {
		return handoffEnvelope{}, handoffInvalid("handoff sizeBytes must be a non-negative integer")
	}

	return handoffEnvelope{digest: digest, sizeBytes: sizeBytes}, nil
}

func handoffVersionIsOne(value any) bool {
	switch version := value.(type) {
	case int:
		return version == handoffVersion
	case float64:
		return version == handoffVersion
	default:
		return false
	}
}

func validHandoffDigest(digest string) bool {
	if len(digest) != handoffDigestHexLength {
		return false
	}

	if digest != strings.ToLower(digest) {
		return false
	}

	_, err := hex.DecodeString(digest)

	return err == nil
}

func handoffSizeBytes(value any) (int64, bool) {
	switch size := value.(type) {
	case int:
		return int64(size), size >= 0
	case float64:
		if size < 0 || size != math.Trunc(size) || size >= math.MaxInt64 {
			return 0, false
		}

		return int64(size), true
	default:
		return 0, false
	}
}

// handoffFilePath maps a handoff block uri to a cleaned local path. A missing,
// unparseable, non-file, non-local, or non-absolute uri is a block malformed
// as a block.
func handoffFilePath(uri *string) (string, *handoffError) {
	if uri == nil || *uri == "" {
		return "", handoffInvalid("handoff image requires a file uri")
	}

	parsed, err := url.Parse(*uri)
	if err != nil {
		return "", handoffInvalid("handoff image uri is not a valid uri")
	}

	if parsed.Scheme != fileURIScheme {
		return "", handoffInvalid("handoff image uri scheme must be file")
	}

	if parsed.Host != "" && parsed.Host != fileURILocalHost {
		return "", handoffInvalid("handoff image uri host is not local")
	}

	if !strings.HasPrefix(parsed.Path, "/") {
		return "", handoffInvalid("handoff image uri path must be absolute")
	}

	return filepath.Clean(filepath.FromSlash(parsed.Path)), nil
}

// resolveHandoffPath resolves one handoff path under the configured root,
// symlink-safe: containment is checked lexically on the cleaned path, symlinks
// are then resolved, and containment is re-checked on the resolved path so a
// link escaping the root cannot be read.
func resolveHandoffPath(root string, path string) (string, *handoffError) {
	cleanRoot := filepath.Clean(root)
	if !pathWithinRoot(path, cleanRoot) {
		return "", &handoffError{value: imageErrorPathNotAllowed, message: "handoff image path is outside the handoff root"}
	}

	resolvedRoot, err := filepath.EvalSymlinks(cleanRoot)
	if err != nil {
		return "", &handoffError{value: imageErrorPathNotAllowed, message: "handoff root cannot be resolved"}
	}

	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}
		}

		return "", &handoffError{value: imageErrorPathNotAllowed, message: "handoff image path cannot be resolved safely"}
	}

	if !pathWithinRoot(resolved, resolvedRoot) {
		return "", &handoffError{value: imageErrorPathNotAllowed, message: "handoff image path escapes the handoff root"}
	}

	return resolved, nil
}

// pathWithinRoot reports whether a cleaned path is the root or sits under it.
func pathWithinRoot(path string, root string) bool {
	if path == root {
		return true
	}

	return strings.HasPrefix(path, root+string(filepath.Separator))
}

// readHandoffFile reads at most bound+1 bytes of a resolved handoff path and
// reports the file's real size. The mode check runs before the open so a
// non-regular path is never opened.
func readHandoffFile(path string, bound int64) ([]byte, int64, *handoffError) {
	info, err := handoffLstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}
		}

		return nil, 0, &handoffError{value: imageErrorPathNotAllowed, message: "handoff image file cannot be inspected"}
	}

	if !info.Mode().IsRegular() {
		return nil, 0, &handoffError{value: imageErrorPathNotAllowed, message: "handoff image path is not a regular file"}
	}

	file, err := handoffOpen(path)
	if err != nil {
		return nil, 0, &handoffError{value: imageErrorMissingFile, message: "handoff image file cannot be opened"}
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, bound+1))
	if err != nil {
		return nil, 0, &handoffError{value: imageErrorMissingFile, message: "handoff image file cannot be read: " + err.Error()}
	}

	return data, info.Size(), nil
}

// verifyHandoffDigest fails closed when the read bytes do not match the
// envelope's declared size and sha256 digest.
func verifyHandoffDigest(envelope handoffEnvelope, data []byte) *handoffError {
	size := int64(len(data))
	if size != envelope.sizeBytes {
		return &handoffError{
			value:   imageErrorDigestMismatch,
			message: fmt.Sprintf("handoff image is %d bytes, declared %d", size, envelope.sizeBytes),
		}
	}

	sum := sha256.Sum256(data)

	digest := hex.EncodeToString(sum[:])
	if digest != envelope.digest {
		return &handoffError{
			value:   imageErrorDigestMismatch,
			message: "handoff image sha256 is " + digest + ", declared " + envelope.digest,
		}
	}

	return nil
}

// handoffBytes runs the handoff pre-gate for one prompt image block: envelope
// and uri strictness, bounded symlink-safe resolution under the configured
// root, a bounded read, then digest verification when the whole file fit
// inside the bound. It returns the verified bytes and the size the byte gates
// account for, which is the file's real size when the read hit the bound.
func (b *promptImageBudget) handoffBytes(block *acp.ContentBlockImage) ([]byte, int64, *handoffError) {
	if b.handoffRoot == "" {
		return nil, 0, handoffInvalid("handoff image input requires a configured handoff root")
	}

	envelope, failure := parseHandoffEnvelope(block.Meta)
	if failure != nil {
		return nil, 0, failure
	}

	path, failure := handoffFilePath(block.Uri)
	if failure != nil {
		return nil, 0, failure
	}

	resolved, failure := resolveHandoffPath(b.handoffRoot, path)
	if failure != nil {
		return nil, 0, failure
	}

	data, fileSize, failure := readHandoffFile(resolved, b.perImage)
	if failure != nil {
		return nil, 0, failure
	}

	// A file larger than the bound was only read up to it, so its digest
	// cannot be verified; the byte gates reject it on its real size instead.
	if int64(len(data)) > b.perImage {
		return data, fileSize, nil
	}

	if failure := verifyHandoffDigest(envelope, data); failure != nil {
		return nil, 0, failure
	}

	return data, int64(len(data)), nil
}
