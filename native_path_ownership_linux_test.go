//go:build linux

package piacp

import (
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"golang.org/x/sys/unix"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestGeneratedNativeTreeDistinctIdentityTraversal(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/tmp", "acp-go-pi-ownership-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })

	if chmodErr := os.Chmod(parent, 0o711); chmodErr != nil {
		t.Fatal(chmodErr)
	}

	control := filepath.Join(parent, "control")
	native := filepath.Join(parent, "native")
	if mkdirErr := os.Mkdir(control, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if writeErr := os.WriteFile(filepath.Join(control, "secret"), []byte("root"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if mkdirErr := os.Mkdir(native, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if writeErr := os.WriteFile(filepath.Join(native, "input"), []byte("ok"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if handoffErr := handoffGeneratedNativeTree(native, isolation); handoffErr != nil {
		t.Fatal(handoffErr)
	}

	command := exec.Command(
		"/bin/sh",
		"-c",
		`set -eu
test "$(cat "$1/input")" = ok
printf native >"$1/output"
if cat "$2/secret" >/dev/null 2>&1; then exit 42; fi`,
		"sh",
		native,
		control,
	)
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: isolation.UID, Gid: isolation.GID, Groups: []uint32{}},
	}
	if output, combinedErr := command.CombinedOutput(); combinedErr != nil {
		t.Fatalf("dropped-identity proof: %v: %s", combinedErr, output)
	}

	contents, err := os.ReadFile(filepath.Join(native, "output"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "native" {
		t.Fatalf("native output = %q", contents)
	}
	if contents, err := os.ReadFile(filepath.Join(control, "secret")); err != nil || string(contents) != "root" {
		t.Fatalf("trusted control changed: %q, %v", contents, err)
	}
}

func TestGeneratedNativeTreeRejectsUntraversableCallerRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/tmp", "acp-go-pi-caller-root-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })

	native := filepath.Join(parent, "native")
	if err := os.Mkdir(native, 0o700); err != nil {
		t.Fatal(err)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if err := handoffGeneratedNativeTree(native, isolation); err == nil {
		t.Fatal("0700 caller root accepted")
	}
	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree(native, isolation); err != nil {
		t.Fatalf("0711 protected caller root: %v", err)
	}
}

func TestGeneratedNativeTreeRejectsUnsafeEntries(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	for _, testCase := range []struct {
		name string
		seed func(string) error
	}{
		{name: "symlink", seed: func(root string) error {
			return os.Symlink("/etc/passwd", filepath.Join(root, "entry"))
		}},
		{name: "hardlink", seed: func(root string) error {
			first := filepath.Join(root, "first")
			if err := os.WriteFile(first, []byte("x"), 0o600); err != nil {
				return err
			}

			return os.Link(first, filepath.Join(root, "second"))
		}},
		{name: "broad mode", seed: func(root string) error {
			return os.WriteFile(filepath.Join(root, "entry"), []byte("x"), 0o644)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parent, err := os.MkdirTemp("/tmp", "acp-go-pi-unsafe-*")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(parent) })
			if err := os.Chmod(parent, 0o711); err != nil {
				t.Fatal(err)
			}

			native := filepath.Join(parent, "native")
			if err := os.Mkdir(native, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := testCase.seed(native); err != nil {
				t.Fatal(err)
			}

			isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
			if err := handoffGeneratedNativeTree(native, isolation); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe tree result = %v", err)
			}
		})
	}
}

// nativeOwnershipTestRoot builds a trusted 0700 native root under a 0711
// caller root, which is the shape the handoff accepts.
func nativeOwnershipTestRoot(t *testing.T) string {
	t.Helper()
	requireNativeOwnershipRoot(t)

	parent, err := os.MkdirTemp("/tmp", "acp-go-pi-refusal-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	native := filepath.Join(parent, "native")
	require.NoError(t, os.Mkdir(native, 0o700))

	return native
}

func requireNativeOwnershipRoot(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
}

func nativeOwnershipTestIdentity() *ProcessIsolation {
	return &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
}

func nativeOwnershipOwner(t *testing.T, path string) (uint32, uint32) {
	t.Helper()

	var stat unix.Stat_t
	require.NoError(t, unix.Stat(path, &stat))

	return stat.Uid, stat.Gid
}

// openNativeOwnershipPathDescriptor returns an O_PATH descriptor. O_PATH
// descriptors answer fstat but reject every operation that reads or writes the
// inode, which is how these tests make an already-validated descriptor stop
// answering without racing the filesystem.
func openNativeOwnershipPathDescriptor(t *testing.T, path string, directory bool) *os.File {
	t.Helper()

	flags := unix.O_PATH | unix.O_CLOEXEC
	if directory {
		flags |= unix.O_DIRECTORY
	}

	fd, err := unix.Open(path, flags, 0)
	require.NoError(t, err)

	file := os.NewFile(uintptr(fd), path)
	t.Cleanup(func() { _ = file.Close() })

	return file
}

// TestNativeOwnershipTraversalRejectsRelativeRoot proves the traversal refuses
// a relative root outright: a relative walk would resolve against the working
// directory the agent controls, not the trusted tree the caller named.
func TestNativeOwnershipTraversalRejectsRelativeRoot(t *testing.T) {
	requireNativeOwnershipRoot(t)

	err := handoffGeneratedNativeTree("relative/native", nativeOwnershipTestIdentity())
	require.ErrorContains(t, err, "native path must be absolute")
}

// TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents proves
// the filesystem root itself is validated before any component is opened, so a
// compromised "/" cannot be walked through on the way to a trusted leaf.
func TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	wantErr := errors.New("root ancestry refused")
	directory, err := openNativeOwnershipDirectory("/etc/hosts", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, directory)
	require.Equal(t, []bool{false}, seen, "traversal continued past a refused filesystem root")
}

// TestNativeOwnershipTraversalOpensFilesystemRootItself proves "/" is a valid
// traversal target and is presented to the validator as the final component
// rather than as an ancestor.
func TestNativeOwnershipTraversalOpensFilesystemRootItself(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	directory, err := openNativeOwnershipDirectory("/", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })
	require.Equal(t, []bool{true}, seen)

	var opened, root unix.Stat_t
	require.NoError(t, unix.Fstat(int(directory.Fd()), &opened))
	require.NoError(t, unix.Stat("/", &root))
	require.Equal(t, root.Ino, opened.Ino)
	require.Equal(t, root.Dev, opened.Dev)
}

// TestNativeOwnershipTraversalPropagatesMissingComponent proves a missing
// component surfaces the kernel's own error rather than being treated as an
// empty tree that needs no handoff.
func TestNativeOwnershipTraversalPropagatesMissingComponent(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)

	err := handoffGeneratedNativeTree(
		filepath.Join(filepath.Dir(native), "absent"), nativeOwnershipTestIdentity(),
	)
	require.ErrorIs(t, err, unix.ENOENT)
}

// TestNativeOwnershipTraversalFailsClosedOnKernelFaults proves each descriptor
// syscall the traversal depends on aborts the walk when it fails. A traversal
// that swallowed any of these would return a descriptor whose ancestry it never
// actually proved.
func TestNativeOwnershipTraversalFailsClosedOnKernelFaults(t *testing.T) {
	requireNativeOwnershipRoot(t)

	accept := func(unix.Stat_t, bool) error { return nil }

	t.Run("filesystem root unopenable", func(t *testing.T) {
		previous := nativeOwnershipOpenFilesystemRoot
		nativeOwnershipOpenFilesystemRoot = func() (int, error) { return -1, unix.EMFILE }

		t.Cleanup(func() { nativeOwnershipOpenFilesystemRoot = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("filesystem root unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("component unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		calls := 0
		nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
			calls++
			if calls == 1 {
				return previous(fd, stat)
			}

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
		require.Equal(t, 2, calls, "traversal statted past the faulted component")
	})

	t.Run("parent descriptor unreleasable", func(t *testing.T) {
		previous := nativeOwnershipClose
		nativeOwnershipClose = func(fd int) error {
			_ = previous(fd)

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipClose = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})
}

// TestGeneratedNativeAncestorStatesEachRefusal pins the exact reason the
// generated-tree ancestry validator refuses each unsafe shape. These reasons
// are the containment contract: an ancestor that is not trusted-owned, a leaf
// that is not exactly 0700, a non-sticky writable ancestor, or an ancestor the
// dropped identity cannot traverse.
func TestGeneratedNativeAncestorStatesEachRefusal(t *testing.T) {
	const (
		trustedUID = uint32(0)
		trustedGID = uint32(0)
		targetUID  = uint32(65534)
		targetGID  = uint32(65534)
	)

	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{
			name: "not a directory",
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "not a trusted directory",
		},
		{
			name: "ancestor owned by another identity",
			stat: directory(0o755, targetUID, targetGID),
			want: "not a trusted directory",
		},
		{
			name:  "leaf is not exactly 0700",
			stat:  directory(0o750, trustedUID, trustedGID),
			final: true,
			want:  "generated native root mode 0750 is unsafe",
		},
		{
			name: "group-writable ancestor without sticky bit",
			stat: directory(0o771, trustedUID, trustedGID),
			want: "0771 is writable without sticky protection",
		},
		{
			name: "world-writable ancestor without sticky bit",
			stat: directory(0o717, trustedUID, trustedGID),
			want: "0717 is writable without sticky protection",
		},
		{
			name: "ancestor the target identity cannot traverse",
			stat: directory(0o700, trustedUID, trustedGID),
			want: "not traversable by the target identity",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID, targetUID, targetGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o711, trustedUID, trustedGID), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o1777, trustedUID, trustedGID), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o700, trustedUID, trustedGID), true, trustedUID, trustedGID, targetUID, targetGID,
	))
}

// TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors
// proves how far the ancestry rule relaxes when the trusted identity is also
// the target identity. Nothing separates the wrapper from the identity it hands
// the tree to in that shape, so the root-owned directories every path is reached
// through are acceptable ancestors — and nothing else is: a third identity's
// ancestor, an ancestor root left writable without sticky protection, and a
// generated root root still owns are all refused.
func TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors(t *testing.T) {
	const (
		sharedUID = uint32(1000)
		sharedGID = uint32(1000)
	)

	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{
			name: "ancestor owned by a third identity",
			stat: directory(0o755, 4242, 4242),
			want: "generated native path ancestor is uid=4242 gid=4242; " +
				"run the supervisor as root to isolate the agent identity, " +
				"or place the native directory under a path the agent identity owns",
		},
		{
			name: "ancestor owned by root with a foreign group",
			stat: directory(0o755, 0, 4242),
			want: "generated native path ancestor is uid=0 gid=4242",
		},
		{
			name: "world-writable root-owned ancestor without sticky protection",
			stat: directory(0o777, 0, 0),
			want: "0777 is writable without sticky protection",
		},
		{
			name: "root-owned ancestor the shared identity cannot traverse",
			stat: directory(0o700, 0, 0),
			want: "not traversable by the target identity",
		},
		{
			name:  "generated root still owned by root",
			stat:  directory(0o700, 0, 0),
			final: true,
			want:  "generated native path ancestry is not a trusted directory",
		},
		{
			name: "not a directory",
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "generated native path ancestry is not a trusted directory",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, sharedUID, sharedGID, sharedUID, sharedGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o755, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	), "the root-owned ancestry every home directory is reached through was refused")
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o1777, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o711, sharedUID, sharedGID), false, sharedUID, sharedGID, sharedUID, sharedGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o700, sharedUID, sharedGID), true, sharedUID, sharedGID, sharedUID, sharedGID,
	))
}

// TestGeneratedNativeTreeHandoffWalksARootOwnedAncestryUnderASharedIdentity
// proves the whole handoff accepts the shape a wrapper that never dropped
// privilege presents: its own identity is the isolated identity, and the
// generated tree hangs from root-owned directories it will never own. The
// effective identity is staged through its seams so the proof does not depend on
// which identity runs the tests; the chowns behind the handoff still need the
// root the rest of this file requires.
func TestGeneratedNativeTreeHandoffWalksARootOwnedAncestryUnderASharedIdentity(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	seed := filepath.Join(native, "input")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	isolation := nativeOwnershipTestIdentity()
	require.NoError(t, os.Chown(seed, int(isolation.UID), int(isolation.GID)))
	require.NoError(t, os.Chown(native, int(isolation.UID), int(isolation.GID)))

	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return int(isolation.UID) }
	effectiveGIDSource = func() int { return int(isolation.GID) }

	require.NoError(t, handoffGeneratedNativeTree(native, isolation))

	uid, gid := nativeOwnershipOwner(t, seed)
	require.Equal(t, isolation.UID, uid)
	require.Equal(t, isolation.GID, gid)

	require.NoError(t, os.Chown(filepath.Dir(native), 4242, 4242))

	handoffErr := handoffGeneratedNativeTree(native, isolation)
	require.ErrorContains(t, handoffErr, "generated native path ancestor is uid=4242 gid=4242")
	require.ErrorContains(t, handoffErr, "run the supervisor as root to isolate the agent identity")
}

// TestNativeIdentityTraversalUsesTheApplicableModeClass proves traversability
// is decided by the single mode class the kernel would apply — owner, then
// group, then other — and never by a union of them. Reading the wrong class
// would let the handoff accept a path the dropped identity cannot enter, or
// refuse one it can.
func TestNativeIdentityTraversalUsesTheApplicableModeClass(t *testing.T) {
	const (
		uid = uint32(65534)
		gid = uint32(65535)
	)

	for _, testCase := range []struct {
		name string
		stat unix.Stat_t
		want bool
	}{
		{name: "owner execute", stat: unix.Stat_t{Uid: uid, Gid: 0, Mode: 0o100}, want: true},
		{name: "owner without execute ignores group", stat: unix.Stat_t{Uid: uid, Gid: gid, Mode: 0o011}},
		{name: "group execute", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o010}, want: true},
		{name: "group without execute ignores other", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o101}},
		{name: "other execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o001}, want: true},
		{name: "other without execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o110}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, nativeIdentityCanTraverse(testCase.stat, uid, gid))
		})
	}
}

// TestNativeOwnershipHandoffRefusesUnsafeRootMode proves a native root that is
// not exactly 0700 is refused before any inode is handed to the dropped
// identity.
func TestNativeOwnershipHandoffRefusesUnsafeRootMode(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	seed := filepath.Join(native, "input")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	directory := openNativeOwnershipPathDescriptor(t, native, true)
	require.NoError(t, os.Chmod(native, 0o750))

	err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
	require.ErrorContains(t, err, "generated native directory mode 0750 is unsafe")

	uid, gid := nativeOwnershipOwner(t, seed)
	require.Equal(t, uint32(0), uid)
	require.Equal(t, uint32(0), gid)
}

// TestNativeOwnershipHandoffRefusesUnenumerableDirectory proves a directory
// whose contents cannot be listed is refused rather than chowned blind. Handing
// the root over without enumerating it would transfer whatever it contains.
func TestNativeOwnershipHandoffRefusesUnenumerableDirectory(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	seed := filepath.Join(native, "input")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	directory := openNativeOwnershipPathDescriptor(t, native, true)

	err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
	require.Error(t, err)

	uid, gid := nativeOwnershipOwner(t, native)
	require.Equal(t, uint32(0), uid, "unenumerable root was handed over anyway")
	require.Equal(t, uint32(0), gid)
}

// nativeOwnershipTestEntry is a directory entry whose name the kernel would
// never produce through ReadDir.
type nativeOwnershipTestEntry struct {
	name string
}

func (entry nativeOwnershipTestEntry) Name() string         { return entry.name }
func (nativeOwnershipTestEntry) IsDir() bool                { return false }
func (nativeOwnershipTestEntry) Type() os.FileMode          { return 0 }
func (nativeOwnershipTestEntry) Info() (os.FileInfo, error) { return nil, unix.EBADF }

// TestNativeOwnershipHandoffRefusesEscapingEntryName proves the handoff never
// resolves an entry name that could leave the directory it is walking. Every
// name reached from here is fed straight back to openat against the directory
// descriptor, so "..", "." and any name carrying a separator would hand the
// dropped identity an inode outside the generated tree.
func TestNativeOwnershipHandoffRefusesEscapingEntryName(t *testing.T) {
	native := nativeOwnershipTestRoot(t)

	directory, err := os.Open(native)
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })

	for _, name := range []string{".", "..", "nested/leaf"} {
		t.Run(name, func(t *testing.T) {
			previous := nativeOwnershipReadDir
			nativeOwnershipReadDir = func(*os.File) ([]os.DirEntry, error) {
				return []os.DirEntry{nativeOwnershipTestEntry{name: name}}, nil
			}

			t.Cleanup(func() { nativeOwnershipReadDir = previous })

			err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
			require.ErrorContains(t, err, "invalid generated native entry")

			uid, gid := nativeOwnershipOwner(t, native)
			require.Equal(t, uint32(0), uid, "escaping entry name still reached the handoff")
			require.Equal(t, uint32(0), gid)
		})
	}
}

// TestNativeOwnershipHandoffDescendsSubdirectories proves the handoff is
// recursive: a nested directory and its contents are transferred with their
// modes intact, so the dropped identity owns the whole generated tree and
// nothing outside it.
func TestNativeOwnershipHandoffDescendsSubdirectories(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	nested := filepath.Join(native, "nested")
	require.NoError(t, os.Mkdir(nested, 0o700))

	leaf := filepath.Join(nested, "leaf")
	require.NoError(t, os.WriteFile(leaf, []byte("x"), 0o600))

	isolation := nativeOwnershipTestIdentity()
	require.NoError(t, handoffGeneratedNativeTree(native, isolation))

	for path, mode := range map[string]os.FileMode{
		native: 0o700,
		nested: 0o700,
		leaf:   0o600,
	} {
		uid, gid := nativeOwnershipOwner(t, path)
		require.Equal(t, isolation.UID, uid, path)
		require.Equal(t, isolation.GID, gid, path)

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, mode, info.Mode().Perm(), path)
	}
}

// TestNativeOwnershipHandoffRefusesNonRegularEntry proves the handoff refuses
// any inode that is neither a directory nor a regular file. Chowning a FIFO or
// device node to the dropped identity would hand it a channel the trusted
// process still holds open.
func TestNativeOwnershipHandoffRefusesNonRegularEntry(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	fifo := filepath.Join(native, "channel")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))

	err := handoffGeneratedNativeTree(native, nativeOwnershipTestIdentity())
	require.ErrorContains(t, err, "unsupported type")

	uid, gid := nativeOwnershipOwner(t, fifo)
	require.Equal(t, uint32(0), uid)
	require.Equal(t, uint32(0), gid)
}

// TestNativeOwnershipEntryRejectsUnusableDescriptor proves an entry descriptor
// the kernel no longer answers for is refused instead of being classified by a
// zero-valued stat, which would look like an unsupported type at best and a
// directory at worst.
func TestNativeOwnershipEntryRejectsUnusableDescriptor(t *testing.T) {
	requireNativeOwnershipRoot(t)

	entry, err := os.Open(os.DevNull)
	require.NoError(t, err)
	require.NoError(t, entry.Close())

	require.ErrorIs(t, handoffNativeOwnershipEntry(entry, 0, 0, 65534, 65534), unix.EBADF)
}

// TestValidateHandoffNativeInodeRefusesDriftedInodes proves the pre-chown
// revalidation catches every way the inode behind an accepted descriptor can
// stop being the trusted inode the traversal approved.
func TestValidateHandoffNativeInodeRefusesDriftedInodes(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)
	regular := filepath.Join(native, "file")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	file, err := os.Open(regular)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	t.Run("unusable descriptor", func(t *testing.T) {
		require.ErrorIs(
			t,
			validateHandoffNativeInode(-1, unix.S_IFREG, 0, 0, 65534, 65534, true),
			unix.EBADF,
		)
	})

	t.Run("inode type changed", func(t *testing.T) {
		err := validateHandoffNativeInode(int(file.Fd()), unix.S_IFDIR, 0, 0, 65534, 65534, false)
		require.ErrorContains(t, err, "inode type 0100000 changed")
	})

	t.Run("inode owner changed", func(t *testing.T) {
		require.NoError(t, unix.Fchown(int(file.Fd()), 65534, 65534))
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		err := validateHandoffNativeInode(int(file.Fd()), unix.S_IFREG, 0, 0, 65534, 65534, true)
		require.ErrorContains(t, err, "owner changed to uid=65534 gid=65534")
	})
}

// TestChownAndVerifyNativeInodeProvesTheTransfer proves the handoff never
// reports success on an unproven transfer: the chown must succeed, the re-read
// must confirm the new owner and the expected inode type, and a file must still
// have exactly one link afterwards.
func TestChownAndVerifyNativeInodeProvesTheTransfer(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)
	regular := filepath.Join(native, "file")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	t.Run("descriptor cannot be chowned", func(t *testing.T) {
		descriptor := openNativeOwnershipPathDescriptor(t, regular, false)
		require.ErrorIs(
			t,
			chownAndVerifyNativeInode(int(descriptor.Fd()), unix.S_IFREG, 65534, 65534, true),
			unix.EBADF,
		)

		uid, gid := nativeOwnershipOwner(t, regular)
		require.Equal(t, uint32(0), uid)
		require.Equal(t, uint32(0), gid)
	})

	t.Run("transferred inode is not the expected type", func(t *testing.T) {
		file, err := os.Open(regular)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFDIR, 65534, 65534, false)
		require.ErrorContains(t, err, "ownership handoff could not be proven")
	})

	t.Run("transferred inode cannot be re-read", func(t *testing.T) {
		file, err := os.Open(regular)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		previous := nativeOwnershipFstat
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true)
		require.ErrorIs(t, err, unix.EIO)
	})

	t.Run("transferred file gained a link", func(t *testing.T) {
		linked := filepath.Join(native, "linked")
		require.NoError(t, os.WriteFile(linked, []byte("x"), 0o600))

		file, err := os.Open(linked)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })

		require.NoError(t, os.Link(linked, filepath.Join(native, "alias")))

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true)
		require.ErrorContains(t, err, "has 2 links after handoff")
	})
}

// TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer proves the
// effective-id helpers refuse rather than narrow. Every caller compares their
// result against an inode's 32-bit owner, so an answer outside that width must
// not be truncated into an id a real inode could carry — the truncation of an
// answer one past the 32-bit range is 0, which is root. Linux stores its ids in
// 32 bits and cannot produce such an answer, so it is staged through the seams
// the helpers read.
func TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer(t *testing.T) {
	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return -1 }
	effectiveGIDSource = func() int { return -1 }
	require.Equal(t, uint32(math.MaxUint32), effectiveUID())
	require.Equal(t, uint32(math.MaxUint32), effectiveGID())

	effectiveUIDSource = func() int { return math.MaxUint32 + 1 }
	effectiveGIDSource = func() int { return math.MaxUint32 + 1 }

	uid, gid := effectiveUID(), effectiveGID()
	require.Equal(t, uint32(math.MaxUint32), uid)
	require.Equal(t, uint32(math.MaxUint32), gid)
	require.NotZero(t, uid, "narrowing this answer would have claimed root")
	require.NotZero(t, gid, "narrowing this answer would have claimed the root group")

	effectiveUIDSource = func() int { return 65534 }
	effectiveGIDSource = func() int { return 65533 }
	require.Equal(t, uint32(65534), effectiveUID())
	require.Equal(t, uint32(65533), effectiveGID())
}

// TestGeneratedNativeTreeHandsOffAWholeSessionTree proves the handoff accepts
// every mode a real session publishes into the tree it hands to the isolated
// identity. The per-session residence is write-once and published
// owner-read-only, which is stricter than the rest of the tree; an enumeration
// of the modes the tree happened to contain refused it and failed the session
// before pi started.
func TestGeneratedNativeTreeHandsOffAWholeSessionTree(t *testing.T) {
	native := nativeOwnershipTestRoot(t)

	agentDir := filepath.Join(native, "agent")
	sessionDir := filepath.Join(native, "sessions")
	require.NoError(t, os.MkdirAll(sessionDir, 0o700))

	require.NoError(t, (pi.AgentDir{
		Root:      agentDir,
		SeedFiles: map[string]string{"settings.json": `{"defaultProvider":"anthropic"}`},
		AuthJSON:  []byte(`{"anthropic":{"type":"api_key","key":"test-only"}}`),
	}).Write())

	residence, files, err := pi.CreateSessionResidence(agentDir, &pi.MCPConfig{
		Servers: []pi.MCPServer{{Name: "fixture", Command: "/bin/true"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = residence.Remove() })
	require.NotEmpty(t, files.ExtensionPaths)
	require.NotEmpty(t, files.MCPConfigPath)

	require.NoError(t, os.WriteFile(filepath.Join(sessionDir, "hydrated.jsonl"), []byte("{}\n"), 0o600))

	published := nativeOwnershipTreeModes(t, native)
	require.Contains(t, published, os.FileMode(0o400), "the residence must still be published write-once")
	require.Contains(t, published, os.FileMode(0o600))
	require.Contains(t, published, os.FileMode(0o700))

	require.NoError(t, handoffGeneratedNativeTree(native, nativeOwnershipTestIdentity()))

	target := nativeOwnershipTestIdentity()
	require.NoError(t, filepath.WalkDir(native, func(path string, _ os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		uid, gid := nativeOwnershipOwner(t, path)
		require.Equal(t, target.UID, uid, path)
		require.Equal(t, target.GID, gid, path)

		return nil
	}))
}

// nativeOwnershipTreeModes is the set of permission bits the tree carries, so
// a proof can name what it actually exercised instead of assuming it.
func nativeOwnershipTreeModes(t *testing.T, root string) map[os.FileMode]struct{} {
	t.Helper()

	modes := map[os.FileMode]struct{}{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		info, err := entry.Info()
		require.NoError(t, err)
		modes[info.Mode().Perm()] = struct{}{}

		return nil
	}))

	return modes
}

// TestGeneratedNativeFileModeRefusesEverythingReachableByAnotherIdentity pins
// the property the handoff enforces on a regular file, in both directions.
func TestGeneratedNativeFileModeRefusesEverythingReachableByAnotherIdentity(t *testing.T) {
	t.Parallel()

	for _, mode := range []uint32{0o400, 0o500, 0o600, 0o700} {
		require.True(t, nativeFileModeIsPrivate(mode), "%#o is owner-only and readable", mode)
	}

	for _, mode := range []uint32{
		0o644, 0o640, 0o666, 0o604, 0o060, 0o006,
		0o4600, 0o2600, 0o1600,
		0o200, 0o300, 0o000, 0o100,
	} {
		require.False(t, nativeFileModeIsPrivate(mode), "%#o must stay refused", mode)
	}
}

// TestValidateHandoffAcceptsEverySessionPublishedMode walks the tree a real
// session publishes and proves the inode rule accepts every entry in it. Only
// the chown at the end of the handoff needs privilege, which is how the
// privileged lane came to be the only thing that ever ran a session
// publishing a residence and then handing its tree off.
func TestValidateHandoffAcceptsEverySessionPublishedMode(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.Mkdir(root, 0o700))

	agentDir := filepath.Join(root, "agent")
	sessionDir := filepath.Join(root, "sessions")
	require.NoError(t, os.MkdirAll(sessionDir, 0o700))

	require.NoError(t, (pi.AgentDir{
		Root:      agentDir,
		SeedFiles: map[string]string{"settings.json": `{"defaultProvider":"anthropic"}`},
		AuthJSON:  []byte(`{"anthropic":{"type":"api_key","key":"test-only"}}`),
	}).Write())

	residence, files, err := pi.CreateSessionResidence(agentDir, &pi.MCPConfig{
		Servers: []pi.MCPServer{{Name: "fixture", Command: "/bin/true"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = residence.Remove() })
	require.NotEmpty(t, files.ExtensionPaths)

	require.NoError(t, os.WriteFile(filepath.Join(sessionDir, "hydrated.jsonl"), []byte("{}\n"), 0o600))

	uid, gid := effectiveUID(), effectiveGID()
	inspected := 0

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)

		kind := uint32(unix.S_IFREG)
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW

		if entry.IsDir() {
			kind = unix.S_IFDIR
			flags |= unix.O_DIRECTORY
		}

		fd, openErr := unix.Open(path, flags, 0)
		require.NoError(t, openErr, path)

		defer func() { require.NoError(t, unix.Close(fd)) }()

		inspected++

		info, infoErr := entry.Info()
		require.NoError(t, infoErr)

		require.NoError(t, validateHandoffNativeInode(fd, kind, uid, gid, uid, gid, kind == unix.S_IFREG),
			"session published %s as %#o and the handoff refused it", path, info.Mode().Perm())

		return nil
	}))

	require.Greater(t, inspected, 5, "the fixture must cover the whole published tree")
}
