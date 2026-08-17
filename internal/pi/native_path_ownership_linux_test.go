//go:build linux

package pi

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func restoreNativeOwnershipSeams(t *testing.T) {
	t.Helper()

	geteuid := nativeOwnershipGeteuid
	getegid := nativeOwnershipGetegid
	openRoot := nativeOwnershipOpenFilesystemRoot
	openat := nativeOwnershipOpenat
	fstat := nativeOwnershipFstat
	closeFD := nativeOwnershipClose
	fchown := nativeOwnershipFchown
	readDir := nativeOwnershipReadDir
	closeFile := nativeOwnershipFileClose

	t.Cleanup(func() {
		nativeOwnershipGeteuid = geteuid
		nativeOwnershipGetegid = getegid
		nativeOwnershipOpenFilesystemRoot = openRoot
		nativeOwnershipOpenat = openat
		nativeOwnershipFstat = fstat
		nativeOwnershipClose = closeFD
		nativeOwnershipFchown = fchown
		nativeOwnershipReadDir = readDir
		nativeOwnershipFileClose = closeFile
	})
}

func requireNativeOwnershipRoot(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
}

func nativeOwnershipTestRoot(t *testing.T) string {
	t.Helper()
	requireNativeOwnershipRoot(t)

	parent, err := os.MkdirTemp("/tmp", "acp-go-pi-internal-ownership-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	root := filepath.Join(parent, "native")
	require.NoError(t, os.Mkdir(root, 0o700))

	return root
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

type nativeOwnershipTestEntry struct {
	name string
}

func (entry nativeOwnershipTestEntry) Name() string         { return entry.name }
func (nativeOwnershipTestEntry) IsDir() bool                { return false }
func (nativeOwnershipTestEntry) Type() os.FileMode          { return 0 }
func (nativeOwnershipTestEntry) Info() (os.FileInfo, error) { return nil, unix.EBADF }

func TestNativeIdentityIDRejectsUnrepresentableValues(t *testing.T) {
	id, err := nativeIdentityID("euid", 65534)
	require.NoError(t, err)
	require.Equal(t, uint32(65534), id)

	_, err = nativeIdentityID("euid", -1)
	require.ErrorContains(t, err, "euid -1 is outside the uint32 identity range")

	maxInt := int(^uint(0) >> 1)
	if uint64(maxInt) <= math.MaxUint32 {
		t.Log("host int width cannot represent a value above math.MaxUint32")

		return
	}

	_, err = nativeIdentityID("egid", maxInt)
	require.ErrorContains(t, err, "egid")
	require.ErrorContains(t, err, "is outside the uint32 identity range")
}

func TestGeneratedNativeTreeHandoffCoversTheCompleteContract(t *testing.T) {
	requireNativeOwnershipRoot(t)

	t.Run("nil isolation owns nothing", func(t *testing.T) {
		require.NoError(t, handoffGeneratedNativeTree("relative", nil))
	})

	t.Run("relative root", func(t *testing.T) {
		require.ErrorContains(t,
			handoffGeneratedNativeTree("relative/native", nativeOwnershipTestIdentity()),
			"must be absolute",
		)
	})

	t.Run("invalid effective uid", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipGeteuid = func() int { return -1 }

		require.ErrorContains(t,
			handoffGeneratedNativeTree("/tmp/unused", nativeOwnershipTestIdentity()),
			"euid -1 is outside",
		)
	})

	t.Run("invalid effective gid", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipGetegid = func() int { return -1 }

		require.ErrorContains(t,
			handoffGeneratedNativeTree("/tmp/unused", nativeOwnershipTestIdentity()),
			"egid -1 is outside",
		)
	})

	t.Run("unopenable tree", func(t *testing.T) {
		err := handoffGeneratedNativeTree(
			"/tmp/acp-go-pi-internal-native-ownership-absent",
			nativeOwnershipTestIdentity(),
		)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("recursive handoff", func(t *testing.T) {
		root := nativeOwnershipTestRoot(t)
		nested := filepath.Join(root, "nested")
		leaf := filepath.Join(nested, "leaf")
		require.NoError(t, os.Mkdir(nested, 0o700))
		require.NoError(t, os.WriteFile(leaf, []byte("x"), 0o600))

		isolation := nativeOwnershipTestIdentity()
		require.NoError(t, handoffGeneratedNativeTree(root, isolation))

		for _, path := range []string{root, nested, leaf} {
			uid, gid := nativeOwnershipOwner(t, path)
			require.Equal(t, isolation.UID, uid, path)
			require.Equal(t, isolation.GID, gid, path)
		}
	})
}

func TestGeneratedNativeDirectoryTraversalFailsClosed(t *testing.T) {
	requireNativeOwnershipRoot(t)
	trustedUID, trustedGID := uint32(os.Geteuid()), uint32(os.Getegid())

	t.Run("filesystem root open", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipOpenFilesystemRoot = func() (int, error) { return -1, unix.EMFILE }

		directory, err := openGeneratedNativeDirectory("/etc", trustedUID, trustedGID, 65534, 65534)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("filesystem root stat", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		directory, err := openGeneratedNativeDirectory("/etc", trustedUID, trustedGID, 65534, 65534)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("filesystem root validation", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipFstat = func(_ int, stat *unix.Stat_t) error {
			*stat = unix.Stat_t{Mode: unix.S_IFREG | 0o755, Uid: trustedUID, Gid: trustedGID}

			return nil
		}

		directory, err := openGeneratedNativeDirectory("/etc", trustedUID, trustedGID, 65534, 65534)
		require.ErrorContains(t, err, "not a trusted directory")
		require.Nil(t, directory)
	})

	t.Run("filesystem root itself", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipFstat = func(_ int, stat *unix.Stat_t) error {
			*stat = unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID, Gid: trustedGID}

			return nil
		}

		directory, err := openGeneratedNativeDirectory("/", trustedUID, trustedGID, 65534, 65534)
		require.NoError(t, err)
		require.NoError(t, directory.Close())
	})

	t.Run("component open", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipOpenat = func(int, string, int, uint32) (int, error) { return -1, unix.EMFILE }

		directory, err := openGeneratedNativeDirectory("/etc", trustedUID, trustedGID, 65534, 65534)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("component stat", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		fstat := nativeOwnershipFstat
		calls := 0
		nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
			calls++
			if calls == 1 {
				return fstat(fd, stat)
			}

			return unix.EIO
		}

		directory, err := openGeneratedNativeDirectory("/etc", trustedUID, trustedGID, 65534, 65534)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("component validation", func(t *testing.T) {
		directory, err := openGeneratedNativeDirectory("/tmp", trustedUID, trustedGID, 65534, 65534)
		require.ErrorContains(t, err, "generated native root mode")
		require.Nil(t, directory)
	})

	t.Run("parent close", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		root := nativeOwnershipTestRoot(t)
		closeFD := nativeOwnershipClose
		calls := 0
		nativeOwnershipClose = func(fd int) error {
			calls++
			closeErr := closeFD(fd)
			if calls == 1 {
				return unix.EIO
			}

			return closeErr
		}

		directory, err := openGeneratedNativeDirectory(root, trustedUID, trustedGID, 65534, 65534)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})
}

func TestGeneratedNativeAncestorAndTraversalRules(t *testing.T) {
	const (
		trustedUID = uint32(0)
		trustedGID = uint32(0)
		targetUID  = uint32(65534)
		targetGID  = uint32(65533)
	)

	directory := func(mode, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{name: "wrong type", stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700}, want: "not a trusted directory"},
		{name: "wrong owner", stat: directory(0o755, 1, 1), want: "not a trusted directory"},
		{name: "unsafe root", stat: directory(0o750, 0, 0), final: true, want: "mode 0750 is unsafe"},
		{name: "writable ancestor", stat: directory(0o777, 0, 0), want: "writable without sticky"},
		{name: "untraversable ancestor", stat: directory(0o700, 0, 0), want: "not traversable"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID, targetUID, targetGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o711, 0, 0), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o1777, 0, 0), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o700, 0, 0), true, trustedUID, trustedGID, targetUID, targetGID,
	))

	for _, testCase := range []struct {
		name string
		stat unix.Stat_t
		want bool
	}{
		{name: "owner execute", stat: unix.Stat_t{Uid: targetUID, Gid: 0, Mode: 0o100}, want: true},
		{name: "owner no execute", stat: unix.Stat_t{Uid: targetUID, Gid: targetGID, Mode: 0o011}},
		{name: "group execute", stat: unix.Stat_t{Uid: 0, Gid: targetGID, Mode: 0o010}, want: true},
		{name: "group no execute", stat: unix.Stat_t{Uid: 0, Gid: targetGID, Mode: 0o101}},
		{name: "other execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o001}, want: true},
		{name: "other no execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o110}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, nativeIdentityCanTraverse(testCase.stat, targetUID, targetGID))
		})
	}
}

func TestGeneratedNativeDirectoryAndEntryRefusals(t *testing.T) {
	requireNativeOwnershipRoot(t)

	t.Run("unsafe directory", func(t *testing.T) {
		root := nativeOwnershipTestRoot(t)
		require.NoError(t, os.Chmod(root, 0o750))

		directory, err := os.Open(root)
		require.NoError(t, err)
		t.Cleanup(func() { _ = directory.Close() })

		require.ErrorContains(t,
			handoffGeneratedNativeDirectory(directory, 0, 0, 65534, 65534),
			"directory mode 0750 is unsafe",
		)
	})

	t.Run("directory cannot be enumerated", func(t *testing.T) {
		root := nativeOwnershipTestRoot(t)
		directory := openNativeOwnershipPathDescriptor(t, root, true)

		require.Error(t, handoffGeneratedNativeDirectory(directory, 0, 0, 65534, 65534))
	})

	t.Run("entry cannot be opened", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		root := nativeOwnershipTestRoot(t)
		directory, err := os.Open(root)
		require.NoError(t, err)
		t.Cleanup(func() { _ = directory.Close() })
		nativeOwnershipReadDir = func(*os.File) ([]os.DirEntry, error) {
			return []os.DirEntry{nativeOwnershipTestEntry{name: "absent"}}, nil
		}

		require.ErrorContains(t,
			handoffGeneratedNativeDirectory(directory, 0, 0, 65534, 65534),
			"open generated native entry",
		)
	})

	t.Run("entry close fails", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		root := nativeOwnershipTestRoot(t)
		require.NoError(t, os.WriteFile(filepath.Join(root, "entry"), []byte("x"), 0o600))
		directory, err := os.Open(root)
		require.NoError(t, err)
		t.Cleanup(func() { _ = directory.Close() })
		closeFile := nativeOwnershipFileClose
		nativeOwnershipFileClose = func(file *os.File) error {
			require.NoError(t, closeFile(file))

			return unix.EIO
		}

		require.ErrorIs(t,
			handoffGeneratedNativeDirectory(directory, 0, 0, 65534, 65534),
			unix.EIO,
		)
	})

	t.Run("unsupported entry", func(t *testing.T) {
		root := nativeOwnershipTestRoot(t)
		require.NoError(t, unix.Mkfifo(filepath.Join(root, "channel"), 0o600))

		require.ErrorContains(t,
			handoffGeneratedNativeTree(root, nativeOwnershipTestIdentity()),
			"unsupported type",
		)
	})

	t.Run("entry stat", func(t *testing.T) {
		entry, err := os.Open(os.DevNull)
		require.NoError(t, err)
		require.NoError(t, entry.Close())

		require.ErrorIs(t, handoffGeneratedNativeEntry(entry, 0, 0, 65534, 65534), unix.EBADF)
	})

	t.Run("regular validation", func(t *testing.T) {
		root := nativeOwnershipTestRoot(t)
		path := filepath.Join(root, "broad")
		require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
		file, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })

		require.ErrorContains(t,
			handoffGeneratedNativeEntry(file, 0, 0, 65534, 65534),
			"file mode 0644 is unsafe",
		)
	})
}

func TestGeneratedNativeInodeValidationAndChownFailures(t *testing.T) {
	requireNativeOwnershipRoot(t)
	root := nativeOwnershipTestRoot(t)
	regular := filepath.Join(root, "regular")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))
	file, err := os.Open(regular)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	require.ErrorIs(t,
		validateGeneratedNativeInode(-1, unix.S_IFREG, 0, 0, 65534, 65534, true),
		unix.EBADF,
	)
	require.ErrorContains(t,
		validateGeneratedNativeInode(int(file.Fd()), unix.S_IFDIR, 0, 0, 65534, 65534, false),
		"inode type changed",
	)
	require.ErrorContains(t,
		validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 1, 1, 65534, 65534, true),
		"owner changed",
	)

	linked := filepath.Join(root, "linked")
	require.NoError(t, os.Link(regular, linked))
	require.ErrorContains(t,
		validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 0, 0, 65534, 65534, true),
		"has 2 links",
	)
	require.NoError(t, os.Remove(linked))

	require.NoError(t, os.Chmod(root, 0o750))
	directory, err := os.Open(root)
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })
	require.ErrorContains(t,
		validateGeneratedNativeInode(int(directory.Fd()), unix.S_IFDIR, 0, 0, 65534, 65534, false),
		"directory mode 0750 is unsafe",
	)
	require.NoError(t, os.Chmod(root, 0o700))

	require.NoError(t, os.Chmod(regular, 0o644))
	require.ErrorContains(t,
		validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 0, 0, 65534, 65534, true),
		"file mode 0644 is unsafe",
	)
	require.NoError(t, os.Chmod(regular, 0o600))
	require.NoError(t,
		validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 0, 0, 65534, 65534, true),
	)

	t.Run("chown", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipFchown = func(int, int, int) error { return unix.EPERM }

		require.ErrorIs(t,
			chownGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true),
			unix.EPERM,
		)
	})

	t.Run("post-chown stat", func(t *testing.T) {
		restoreNativeOwnershipSeams(t)
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }
		t.Cleanup(func() { require.NoError(t, os.Chown(regular, 0, 0)) })

		require.ErrorIs(t,
			chownGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true),
			unix.EIO,
		)
	})

	t.Run("post-chown proof", func(t *testing.T) {
		t.Cleanup(func() { require.NoError(t, os.Chown(regular, 0, 0)) })

		require.ErrorContains(t,
			chownGeneratedNativeInode(int(file.Fd()), unix.S_IFDIR, 65534, 65534, false),
			"could not be proven",
		)
	})
}

func TestBrowserShimHandsOffItsGeneratedTree(t *testing.T) {
	requireNativeOwnershipRoot(t)
	parent, err := os.MkdirTemp("/tmp", "acp-go-pi-internal-shim-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	shim, err := NewBrowserShim(parent)
	require.NoError(t, err)
	require.NoError(t, shim.Handoff(nativeOwnershipTestIdentity()))
	require.NoError(t, shim.Remove())
}

func TestOrdinaryVersionProbeAndVanishedLeaderUseTheOrdinaryContract(t *testing.T) {
	script := filepath.Join(t.TempDir(), "pi")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho 0.80.6\n"), 0o700))
	agentDir, containment := testVersionProbeSpec(t)
	containment.Isolation = nil
	containment.DarwinBestEffort = false

	version, err := ProbeVersion(t.Context(), script, agentDir, containment)
	require.NoError(t, err)
	require.Equal(t, "0.80.6", version)

	direct := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
	native := &exec.Cmd{Process: &os.Process{Pid: 43210}}
	launch := &processTreeCommand{cmd: native, ordinary: true}
	tree, handled, err := handleVanishedProcessGroupLeader(launch, direct)
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, native.Process, tree.process)
	require.True(t, tree.ordinary)

	select {
	case <-direct.start:
	default:
		t.Fatal("ordinary direct-child wait was not released")
	}
}
