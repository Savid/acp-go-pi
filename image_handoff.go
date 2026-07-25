package piacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
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

// handoffSizeBytesExclusiveMax is 2^63 as a float64, the first value at or above
// the int64 range. A declared size must be strictly below it.
const handoffSizeBytesExclusiveMax = 9223372036854775808.0

// maxHandoffBlocksPerPrompt bounds the handoff-form blocks one prompt may ask
// this adapter to read. Each block is a few hundred bytes on the wire and drives
// a whole file read, so without a count bound a single legal frame commits the
// adapter to arbitrarily much I/O and resident memory whenever the per-prompt
// byte aggregate is disabled. It bounds the work rather than the byte policy,
// and sits far enough above any real multi-image turn that a conforming host
// never meets it.
const maxHandoffBlocksPerPrompt = 64

// Handoff failure messages. Every one is a compile-time constant: a verdict
// travels to the client and into telemetry, so it may never carry a path, a
// uri, a filename, a digest, a byte count of a file the caller did not
// describe, or operating-system error text.
const (
	handoffRootUnsetMessage      = "handoff image input requires a configured handoff root"
	handoffRootUnresolvedMessage = "handoff root cannot be resolved"
	handoffOutsideRootMessage    = "handoff image path is outside the handoff root"
	handoffNotRegularMessage     = "handoff image path is not a regular file"
	handoffUninspectableMessage  = "handoff image file cannot be inspected"
	handoffUnopenableMessage     = "handoff image file cannot be opened"
	handoffUnreadableMessage     = "handoff image file cannot be read"
	handoffSizeMismatchMessage   = "handoff image does not match the declared sizeBytes"
	handoffDigestMismatchMessage = "handoff image does not match the declared digest"
)

// openHandoffRoot opens the configured read root. Containment is delegated to
// the kernel from here on: every handoff open is relative to this descriptor and
// a symlink beneath it may not name a location outside it, so there is no window
// between deciding a path is inside the root and reading it. It is a seam so the
// failure an unusable root produces is exercisable.
var openHandoffRoot = os.OpenRoot

// handoffFile is what the bounded read needs from an opened handoff file.
type handoffFile interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
}

// openHandoffFile opens one name inside an already-opened read root. It is a
// seam so the inspection and read failures a real filesystem only produces under
// a race with the host that owns the file stay exercisable; containment is the
// root's, not this function's, on every path.
var openHandoffFile = func(root *os.Root, rel string) (handoffFile, error) {
	return root.OpenFile(rel, os.O_RDONLY|handoffOpenFlags, 0)
}

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

// handoffError is one handoff pre-gate failure: the image error value, the
// constant message naming the cause, and the byte pair a size verdict reports.
type handoffError struct {
	value     string
	message   string
	sizeBytes int64
	maxBytes  int64
}

func (e *handoffError) Error() string {
	return e.message
}

func handoffInvalid(message string) *handoffError {
	return &handoffError{value: imageErrorInvalidHandoff, message: message}
}

// imageHandoffError builds the -32602 prompt image error for a handoff
// pre-gate failure. A byte verdict carries the size pair every other byte
// verdict carries and no message; every other verdict carries its constant
// message.
func imageHandoffError(failure *handoffError, index int) *acp.RequestError {
	data := map[string]any{
		jsonFieldField: fieldPromptImage,
		jsonFieldError: failure.value,
		jsonFieldIndex: index,
	}

	if failure.message != "" {
		data[jsonFieldMessage] = failure.message
	}

	if failure.sizeBytes > 0 {
		data[jsonFieldSizeBytes] = failure.sizeBytes
	}

	if failure.maxBytes > 0 {
		data[jsonFieldMaxBytes] = failure.maxBytes
	}

	return acp.NewInvalidParams(data)
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
	version, ok := handoffNumber(value)

	return ok && version == handoffVersion
}

// handoffNumber reads an envelope numeric as a float64 whatever shape it arrived
// in. A JSON number decodes to float64 under the pinned SDK and to json.Number
// if the decoder is ever asked for one, and an in-process host builds its own
// block metadata with a Go int. All three are the same number, and reading them
// as a float64 is what keeps the range check off an int64 conversion whose
// out-of-range behaviour Go does not define.
func handoffNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case json.Number:
		parsed, err := number.Float64()

		return parsed, err == nil
	case int:
		return float64(number), true
	default:
		return 0, false
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

// handoffSizeBytes validates a declared byte count entirely in float64, before
// any int64 conversion: non-negative, integral, and strictly below 2^63, because
// converting an out-of-range float64 to int64 is undefined in Go and wraps on
// one architecture while saturating on another.
func handoffSizeBytes(value any) (int64, bool) {
	size, ok := handoffNumber(value)
	if !ok || size < 0 || size != math.Trunc(size) || size >= handoffSizeBytesExclusiveMax {
		return 0, false
	}

	return int64(size), true
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

// handoffRelativePath maps an absolute handoff path to the name it has inside
// the read root. The lexical test only decides which member to blame for a path
// that was never under the root; the kernel, not this function, is what keeps a
// resolved path inside it.
func handoffRelativePath(root string, path string) (string, *handoffError) {
	rel, err := filepath.Rel(filepath.Clean(root), path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", &handoffError{value: imageErrorPathNotAllowed, message: handoffOutsideRootMessage}
	}

	return rel, nil
}

// readHandoffFile reads a handoff file by its name inside the read root, at most
// one byte more than the envelope declared.
//
// Nothing the filesystem reports feeds a verdict. The size the read is bounded
// by is the caller's own declared sizeBytes, already checked against the
// per-image gate, so a file that grew, shrank, or was swapped after the block
// was written fails the digest check rather than passing a size gate on a stale
// number. The descriptor is opened without blocking and required to be a regular
// file, because a root bounds where a path may lead and not what kind of object
// it names.
func readHandoffFile(ctx context.Context, root *os.Root, rel string, declared int64) ([]byte, *handoffError) {
	if ctx.Err() != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUnreadableMessage}
	}

	file, err := openHandoffFile(root, rel)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &handoffError{value: imageErrorMissingFile, message: handoffFileAbsentMessage}
		}

		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffUnopenableMessage}
	}

	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUninspectableMessage}
	}

	if !info.Mode().IsRegular() {
		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffNotRegularMessage}
	}

	data, err := io.ReadAll(io.LimitReader(file, declared+1))
	if err != nil {
		return nil, &handoffError{value: imageErrorMissingFile, message: handoffUnreadableMessage}
	}

	return data, nil
}

// verifyHandoffDigest fails closed when the read bytes do not match the
// envelope's declared size and sha256 digest. Neither message reports what was
// observed: the caller already knows what it declared, and the file it named may
// not be one it is entitled to learn the size or the content hash of.
func verifyHandoffDigest(envelope handoffEnvelope, data []byte) *handoffError {
	if int64(len(data)) != envelope.sizeBytes {
		return &handoffError{value: imageErrorDigestMismatch, message: handoffSizeMismatchMessage}
	}

	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != envelope.digest {
		return &handoffError{value: imageErrorDigestMismatch, message: handoffDigestMismatchMessage}
	}

	return nil
}

// handoffRootHandle opens the read root once per prompt. A root that cannot be
// opened is a deployment defect rather than a host cleaning a file up early, so
// it is path_not_allowed.
func (b *promptImageBudget) handoffRootHandle() (*os.Root, *handoffError) {
	if b.root != nil {
		return b.root, nil
	}

	root, err := openHandoffRoot(b.handoffRoot)
	if err != nil {
		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffRootUnresolvedMessage}
	}

	b.root = root

	return root, nil
}

// closeHandoffRoot releases the read root's descriptor at the end of the prompt
// mapping that opened it.
func (b *promptImageBudget) closeHandoffRoot() {
	if b.root != nil {
		_ = b.root.Close()
		b.root = nil
	}
}

// handoffBytes runs the handoff pre-gate for one prompt image block: the block
// count bound, envelope and uri strictness, the declared media type, the
// declared size against the per-image gate, a bounded root-relative read, then
// digest verification. Every path that returns bytes has verified them against
// the envelope the caller sent.
func (b *promptImageBudget) handoffBytes(ctx context.Context, block *acp.ContentBlockImage) ([]byte, *handoffError) {
	if b.handoffRoot == "" {
		return nil, handoffInvalid(handoffRootUnsetMessage)
	}

	b.handoffBlocks++
	if b.handoffBlocks > maxHandoffBlocksPerPrompt {
		return nil, &handoffError{
			value:     imageErrorTooLarge,
			sizeBytes: int64(b.handoffBlocks),
			maxBytes:  maxHandoffBlocksPerPrompt,
		}
	}

	envelope, failure := parseHandoffEnvelope(block.Meta)
	if failure != nil {
		return nil, failure
	}

	path, failure := handoffFilePath(block.Uri)
	if failure != nil {
		return nil, failure
	}

	rel, failure := handoffRelativePath(b.handoffRoot, path)
	if failure != nil {
		return nil, failure
	}

	root, failure := b.handoffRootHandle()
	if failure != nil {
		return nil, failure
	}

	// The declared type is judged before a byte is read, as it is in the
	// embedded form, so a block this adapter was never going to accept costs it
	// no read and no hash.
	if !allowedInputImageMIME(block.MimeType) {
		return nil, &handoffError{value: imageErrorInvalidMediaType}
	}

	// The size gate reads the caller's own declaration, so it can reject an
	// oversize handoff before opening anything and without measuring a file the
	// caller may not be entitled to measure.
	if envelope.sizeBytes > b.perImage {
		return nil, &handoffError{
			value:     imageErrorTooLarge,
			sizeBytes: envelope.sizeBytes,
			maxBytes:  b.perImage,
		}
	}

	data, failure := readHandoffFile(ctx, root, rel, envelope.sizeBytes)
	if failure != nil {
		return nil, failure
	}

	if failure := verifyHandoffDigest(envelope, data); failure != nil {
		return nil, failure
	}

	return data, nil
}
