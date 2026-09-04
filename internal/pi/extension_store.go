package pi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

// The wrapper's extension sources are identical for every session, but pi
// compiles a -e extension through a filesystem cache keyed by the source file's
// own path. Sources published under a residence minted per session therefore
// never reuse a compiled extension: every launch pays the compile again and
// leaves one more cache entry behind, in a directory nothing ever prunes.
//
// They are published here instead into one content-addressed directory shared
// by every session. The path is stable, so the compile happens once per adapter
// build no matter how many sessions run. The digest names the directory, so a
// wrapper carrying new sources publishes beside the old tree instead of
// rewriting a file a running pi child was launched from.
//
// The store holds code pi executes, so an entry is reused only where the
// calling user owns it, no other user can write it, and its bytes still equal
// the sources compiled into this binary. Anything else is refused rather than
// repaired: a store this process cannot vouch for is not one to launch from.

const (
	// sharedExtensionDirName is the store root beneath the scratch parent.
	sharedExtensionDirName = "acp-go-pi-ext"
	// sharedExtensionStagingPattern names the carrier a source is written
	// through before it is linked onto its final name. It is unique per
	// attempt because the store is shared: two processes publishing the same
	// digest at once must not write through one carrier.
	sharedExtensionStagingPattern = ".staging-*"
	// sharedExtensionLooseModeBits are the write bits no entry may carry. A
	// path another user can write is a path that can be swapped for one.
	sharedExtensionLooseModeBits = 0o022
)

// errSharedExtensionAbsent reports that the store holds no entry at a path.
var errSharedExtensionAbsent = errors.New("shared extension is absent")

// Filesystem seams this store adds to the ones agentdir.go already carries.
var (
	fsCreateTemp = os.CreateTemp
	fsChmod      = os.Chmod
	fsLstat      = os.Lstat
)

// sharedExtensionPaths locates the published sources for one session.
type sharedExtensionPaths struct {
	bridge string
	path   string
	mcp    string
}

// sharedExtensionSource is one wrapper-owned source in the store.
type sharedExtensionSource struct {
	name     string
	contents []byte
}

// sharedExtensionSources are every source the store publishes, in a fixed
// order. All three are published together even when the session declares no
// MCP servers, so the digest names one directory per adapter build rather than
// one per session shape.
func sharedExtensionSources() []sharedExtensionSource {
	return []sharedExtensionSource{
		{name: BridgeExtensionFileName, contents: bridgeExtensionSource},
		{name: PathExtensionFileName, contents: pathExtensionSource},
		{name: MCPExtensionFileName, contents: mcpExtensionSource},
	}
}

// sharedExtensionDigest names the directory holding one exact set of sources.
// Name and length join the contents so no two sets can collide by
// concatenation.
func sharedExtensionDigest(sources []sharedExtensionSource) string {
	digest := sha256.New()

	for _, source := range sources {
		digest.Write([]byte(source.name))
		digest.Write([]byte{0})
		digest.Write([]byte(strconv.Itoa(len(source.contents))))
		digest.Write([]byte{0})
		digest.Write(source.contents)
		digest.Write([]byte{0})
	}

	return hex.EncodeToString(digest.Sum(nil))
}

// publishSharedExtensions publishes the wrapper-owned sources beneath root and
// reports where each one landed. It is safe to call concurrently and on a store
// an earlier session already populated: an entry that is already correct is
// reused untouched.
func publishSharedExtensions(root string) (sharedExtensionPaths, error) {
	if root == "" {
		return sharedExtensionPaths{}, errors.New("shared extension store requires a root")
	}

	sources := sharedExtensionSources()

	dir := filepath.Join(root, sharedExtensionDirName, sharedExtensionDigest(sources))
	if err := fsMkdirAll(dir, 0o700); err != nil {
		return sharedExtensionPaths{}, fmt.Errorf("create shared extension store: %w", err)
	}

	if err := requireGuardedSharedExtensionDir(filepath.Join(root, sharedExtensionDirName)); err != nil {
		return sharedExtensionPaths{}, err
	}

	if err := requireGuardedSharedExtensionDir(dir); err != nil {
		return sharedExtensionPaths{}, err
	}

	published := make(map[string]string, len(sources))

	for _, source := range sources {
		path, err := publishSharedExtension(dir, source)
		if err != nil {
			return sharedExtensionPaths{}, err
		}

		published[source.name] = path
	}

	return sharedExtensionPaths{
		bridge: published[BridgeExtensionFileName],
		path:   published[PathExtensionFileName],
		mcp:    published[MCPExtensionFileName],
	}, nil
}

// publishSharedExtension returns the path of one source in the store, writing
// it first where the store does not already hold it.
func publishSharedExtension(dir string, source sharedExtensionSource) (string, error) {
	path := filepath.Join(dir, source.name)

	switch err := verifySharedExtension(path, source.contents); {
	case err == nil:
		return path, nil
	case !errors.Is(err, errSharedExtensionAbsent):
		return "", err
	}

	staging, err := stageSharedExtension(dir, source)
	if err != nil {
		return "", err
	}

	if linkErr := fsLink(staging, path); linkErr != nil {
		removeErr := fsRemove(staging)

		// A name that already exists is the expected outcome of losing the
		// race to another process publishing the same digest. What it
		// published is only usable if it passes the same checks.
		if errors.Is(linkErr, fs.ErrExist) {
			return path, errors.Join(verifySharedExtension(path, source.contents), removeErr)
		}

		return "", fmt.Errorf("publish shared extension %q: %w", source.name, errors.Join(linkErr, removeErr))
	}

	if cleanErr := fsRemove(staging); cleanErr != nil {
		return "", fmt.Errorf("clean shared extension staging %q: %w", source.name, cleanErr)
	}

	return path, nil
}

// stageSharedExtension writes one source into a carrier beside its final name.
func stageSharedExtension(dir string, source sharedExtensionSource) (string, error) {
	file, err := fsCreateTemp(dir, source.name+sharedExtensionStagingPattern)
	if err != nil {
		return "", fmt.Errorf("stage shared extension %q: %w", source.name, err)
	}

	staging := file.Name()

	_, writeErr := file.Write(source.contents)
	if closeErr := errors.Join(writeErr, file.Close()); closeErr != nil {
		return "", fmt.Errorf("stage shared extension %q: %w", source.name, errors.Join(closeErr, fsRemove(staging)))
	}

	if chmodErr := fsChmod(staging, 0o400); chmodErr != nil {
		return "", fmt.Errorf("stage shared extension %q: %w", source.name, errors.Join(chmodErr, fsRemove(staging)))
	}

	return staging, nil
}

// verifySharedExtension admits a published source only where this process could
// have written it and nothing else could have changed it since. A path that
// does not exist reports errSharedExtensionAbsent, which is the one outcome a
// caller may answer by publishing.
func verifySharedExtension(path string, contents []byte) error {
	info, err := fsLstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errSharedExtensionAbsent
		}

		return fmt.Errorf("inspect shared extension %q: %w", path, err)
	}

	if guardErr := requireGuardedSharedExtensionPath(path, info, false); guardErr != nil {
		return guardErr
	}

	published, err := fsReadFile(path)
	if err != nil {
		return fmt.Errorf("read shared extension %q: %w", path, err)
	}

	if !bytes.Equal(published, contents) {
		return fmt.Errorf("shared extension %q does not match the source compiled into this binary", path)
	}

	return nil
}

// requireGuardedSharedExtensionDir applies the store's admission rules to a
// directory it just created or found.
func requireGuardedSharedExtensionDir(path string) error {
	info, err := fsLstat(path)
	if err != nil {
		return fmt.Errorf("inspect shared extension store %q: %w", path, err)
	}

	return requireGuardedSharedExtensionPath(path, info, true)
}

// requireGuardedSharedExtensionPath refuses anything the calling user does not
// own, anything another user could write, and anything that is not the kind of
// filesystem object the store puts there.
func requireGuardedSharedExtensionPath(path string, info fs.FileInfo, wantDir bool) error {
	switch {
	case wantDir && !info.IsDir():
		return fmt.Errorf("shared extension store %q is not a directory", path)
	case !wantDir && !info.Mode().IsRegular():
		return fmt.Errorf("shared extension %q is not a regular file", path)
	case info.Mode().Perm()&sharedExtensionLooseModeBits != 0:
		return fmt.Errorf("shared extension path %q is writable by group or world", path)
	}

	if err := sharedExtensionOwnedByCaller(info); err != nil {
		return fmt.Errorf("shared extension path %q: %w", path, err)
	}

	return nil
}
