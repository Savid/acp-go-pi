//go:build unix

package piacp

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A path whose parent component is a regular file is reported by this platform
// as a path that may not be taken: open(2) answers ENOTDIR, which is not a
// missing file, so the mapper refuses the route rather than the name.
const (
	handoffThroughFileError   = imageErrorPathNotAllowed
	handoffThroughFileMessage = handoffUnopenableMessage
)

func TestHandoffFIFOInsideRootIsRejected(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "valid.png")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	png := fixtureBytes(t, "valid.png")
	block := handoffImageBlock(fileURIFor(path), "image/png", handoffEnvelopeFor(png))

	// A FIFO with no writer blocks an ordinary open until one appears, so the
	// verdict has to arrive without the open ever waiting on it. Containment
	// bounds where a path may lead, never what kind of object it names.
	done := make(chan error, 1)
	go func() {
		_, err := validateHandoffBlock(t, root, block, defaultImageLimits())
		done <- err
	}()

	select {
	case err := <-done:
		requireHandoffError(t, err, imageErrorPathNotAllowed, 0, "not a regular file")
	case <-time.After(10 * time.Second):
		t.Fatal("opening a FIFO inside the handoff root blocked the read")
	}
}
