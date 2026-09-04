//go:build !windows

package pi

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

var sharedExtensionCurrentUID = os.Getuid

// sharedExtensionOwnedByCaller admits a store path only when the calling user
// owns it. The store holds code pi executes, so a path another user could have
// created is never one this process launches a child from.
func sharedExtensionOwnedByCaller(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("file ownership is unavailable")
	}

	if owner := int(stat.Uid); owner != sharedExtensionCurrentUID() {
		return fmt.Errorf("owned by uid %d, want the current user", owner)
	}

	return nil
}

// sharedExtensionModeLoose reports a store path an account other than its owner
// may write. The store holds code pi executes, so an entry a second account
// could rewrite between the check and the launch is never one a child starts
// from.
func sharedExtensionModeLoose(info fs.FileInfo) bool {
	return info.Mode().Perm()&sharedExtensionLooseModeBits != 0
}
