//go:build unix

package piacp

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
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

func TestManagedHandoffRejectsScratchAlias(t *testing.T) {
	scratch := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(scratch, alias))
	block, _ := handoffFixtureBlock(t, scratch, "valid.png", "image/png")
	block.Image.Uri = new(fileURIFor(filepath.Join(alias, "valid.png")))
	agent := NewAgent(WithHostAuthority(&edgeHostAuthority{}), WithScratchDir(scratch), WithInputHandoffRoot(alias))
	t.Cleanup(func() { _ = agent.Close() })
	_, err := mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
	requireHandoffError(t, err, imageErrorPathNotAllowed, 0, handoffRootUnresolvedMessage)
}

func TestManagedHandoffRetainsRootAfterAliasChanges(t *testing.T) {
	scratch, root := t.TempDir(), t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(root, alias))
	block, png := handoffFixtureBlock(t, root, "valid.png", "image/png")
	block.Image.Uri = new(fileURIFor(filepath.Join(alias, "valid.png")))
	agent := NewAgent(WithHostAuthority(&edgeHostAuthority{}), WithScratchDir(scratch), WithInputHandoffRoot(alias))
	t.Cleanup(func() { _ = agent.Close() })
	require.NoError(t, agent.prepareNativeTree(t.Context(), filepath.Join(scratch, "generation")))
	require.NoError(t, os.Remove(alias))
	require.NoError(t, os.Symlink(scratch, alias))
	require.NoError(t, os.WriteFile(filepath.Join(scratch, "valid.png"), fixtureBytes(t, "valid.jpg"), 0o600))
	mapped, err := mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(png), mapped.Images[0].Data)
}
