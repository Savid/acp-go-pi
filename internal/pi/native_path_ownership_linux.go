//go:build linux

package pi

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// The ownership handoff revalidates every descriptor immediately before it
// trusts or transfers it. Keep those kernel reads behind narrow seams so the
// failure paths can be proven without racing the filesystem under test.
var (
	nativeOwnershipGeteuid            = os.Geteuid
	nativeOwnershipGetegid            = os.Getegid
	nativeOwnershipOpenFilesystemRoot = func() (int, error) {
		return unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	nativeOwnershipOpenat  = unix.Openat
	nativeOwnershipFstat   = unix.Fstat
	nativeOwnershipClose   = unix.Close
	nativeOwnershipFchown  = unix.Fchown
	nativeOwnershipReadDir = func(directory *os.File) ([]os.DirEntry, error) {
		return directory.ReadDir(-1)
	}
	nativeOwnershipFileClose = (*os.File).Close
)

// nativeIdentityID narrows a process identity to the uint32 width the kernel
// ownership calls use. Geteuid/Getegid return int and report -1 where the
// platform cannot answer, so the narrowing is proven rather than assumed.
func nativeIdentityID(kind string, value int) (uint32, error) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, fmt.Errorf("generated native handoff %s %d is outside the uint32 identity range", kind, value)
	}

	return uint32(value), nil
}

func handoffGeneratedNativeTree(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	if !filepath.IsAbs(root) {
		return errors.New("generated native path must be absolute")
	}

	trustedUID, err := nativeIdentityID("euid", nativeOwnershipGeteuid())
	if err != nil {
		return err
	}

	trustedGID, err := nativeIdentityID("egid", nativeOwnershipGetegid())
	if err != nil {
		return err
	}

	directory, err := openGeneratedNativeDirectory(root, trustedUID, trustedGID, isolation.UID, isolation.GID)
	if err != nil {
		return err
	}
	defer directory.Close()

	return handoffGeneratedNativeDirectory(
		directory,
		trustedUID,
		trustedGID,
		isolation.UID,
		isolation.GID,
	)
}

func openGeneratedNativeDirectory(name string, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) (*os.File, error) {
	clean := filepath.Clean(name)

	fd, err := nativeOwnershipOpenFilesystemRoot()
	if err != nil {
		return nil, err
	}

	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")

	var rootStat unix.Stat_t

	if statErr := nativeOwnershipFstat(fd, &rootStat); statErr != nil {
		_ = nativeOwnershipClose(fd)

		return nil, statErr
	}

	if validateErr := validateGeneratedNativeAncestor(rootStat, len(components) == 1 && components[0] == "", trustedUID, trustedGID, targetUID, targetGID); validateErr != nil {
		_ = nativeOwnershipClose(fd)

		return nil, validateErr
	}

	for index, component := range components {
		if component == "" {
			continue
		}

		next, openErr := nativeOwnershipOpenat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			_ = nativeOwnershipClose(fd)

			return nil, openErr
		}

		var stat unix.Stat_t

		if statErr := nativeOwnershipFstat(next, &stat); statErr != nil {
			_ = nativeOwnershipClose(next)
			_ = nativeOwnershipClose(fd)

			return nil, statErr
		}

		if validateErr := validateGeneratedNativeAncestor(stat, index == len(components)-1, trustedUID, trustedGID, targetUID, targetGID); validateErr != nil {
			_ = nativeOwnershipClose(next)
			_ = nativeOwnershipClose(fd)

			return nil, validateErr
		}

		closeErr := nativeOwnershipClose(fd)
		if closeErr != nil {
			_ = nativeOwnershipClose(next)

			return nil, closeErr
		}

		fd = next
	}

	return os.NewFile(uintptr(fd), clean), nil
}

func validateGeneratedNativeAncestor(
	stat unix.Stat_t,
	final bool,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != trustedUID || stat.Gid != trustedGID {
		return errors.New("generated native path ancestry is not a trusted directory")
	}

	mode := stat.Mode & 0o7777
	if final && mode != 0o700 {
		return fmt.Errorf("generated native root mode %#o is unsafe", mode)
	}

	if !final && mode&0o022 != 0 && mode&unix.S_ISVTX == 0 {
		return fmt.Errorf("generated native ancestor mode %#o is writable without sticky protection", mode)
	}

	if !final && !nativeIdentityCanTraverse(stat, targetUID, targetGID) {
		return errors.New("generated native path ancestry is not traversable by the target identity")
	}

	return nil
}

func nativeIdentityCanTraverse(stat unix.Stat_t, uid uint32, gid uint32) bool {
	switch {
	case stat.Uid == uid:
		return stat.Mode&0o100 != 0
	case stat.Gid == gid:
		return stat.Mode&0o010 != 0
	default:
		return stat.Mode&0o001 != 0
	}
}

func handoffGeneratedNativeDirectory(directory *os.File, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) error {
	if err := validateGeneratedNativeInode(int(directory.Fd()), unix.S_IFDIR, trustedUID, trustedGID, targetUID, targetGID, false); err != nil {
		return err
	}

	entries, err := nativeOwnershipReadDir(directory)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	for _, entry := range entries {
		fd, openErr := nativeOwnershipOpenat(
			int(directory.Fd()),
			entry.Name(),
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
			0,
		)
		if openErr != nil {
			return fmt.Errorf("open generated native entry %q: %w", entry.Name(), openErr)
		}

		child := os.NewFile(uintptr(fd), entry.Name())
		childErr := handoffGeneratedNativeEntry(child, trustedUID, trustedGID, targetUID, targetGID)

		closeErr := nativeOwnershipFileClose(child)
		if childErr != nil || closeErr != nil {
			return errors.Join(childErr, closeErr)
		}
	}

	return chownGeneratedNativeInode(int(directory.Fd()), unix.S_IFDIR, targetUID, targetGID, false)
}

func handoffGeneratedNativeEntry(file *os.File, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32) error {
	var stat unix.Stat_t
	if err := nativeOwnershipFstat(int(file.Fd()), &stat); err != nil {
		return err
	}

	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return handoffGeneratedNativeDirectory(file, trustedUID, trustedGID, targetUID, targetGID)
	case unix.S_IFREG:
		if err := validateGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, trustedUID, trustedGID, targetUID, targetGID, true); err != nil {
			return err
		}

		return chownGeneratedNativeInode(int(file.Fd()), unix.S_IFREG, targetUID, targetGID, true)
	default:
		return fmt.Errorf("generated native inode has unsupported type %#o", stat.Mode&unix.S_IFMT)
	}
}

func validateGeneratedNativeInode(fd int, kind uint32, trustedUID uint32, trustedGID uint32, targetUID uint32, targetGID uint32, singleLink bool) error {
	var stat unix.Stat_t
	if err := nativeOwnershipFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind {
		return errors.New("generated native inode type changed")
	}

	if stat.Uid != trustedUID || stat.Gid != trustedGID {
		return fmt.Errorf("generated native inode owner changed to uid=%d gid=%d", stat.Uid, stat.Gid)
	}

	if singleLink && stat.Nlink != 1 {
		return fmt.Errorf("generated native file has %d links", stat.Nlink)
	}

	mode := stat.Mode & 0o7777
	if kind == unix.S_IFDIR && mode != 0o700 {
		return fmt.Errorf("generated native directory mode %#o is unsafe", mode)
	}

	if kind == unix.S_IFREG && mode != 0o600 && mode != 0o700 {
		return fmt.Errorf("generated native file mode %#o is unsafe", mode)
	}

	return nil
}

func chownGeneratedNativeInode(fd int, kind uint32, uid uint32, gid uint32, singleLink bool) error {
	if err := nativeOwnershipFchown(fd, int(uid), int(gid)); err != nil {
		return err
	}

	var stat unix.Stat_t
	if err := nativeOwnershipFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind || stat.Uid != uid || stat.Gid != gid || singleLink && stat.Nlink != 1 {
		return errors.New("generated native inode ownership handoff could not be proven")
	}

	return nil
}
