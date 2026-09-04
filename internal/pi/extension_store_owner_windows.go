//go:build windows

package pi

import "io/fs"

// sharedExtensionOwnedByCaller has no uid to compare on Windows. The store
// lives beneath the scratch parent, whose ACL already confines writes to the
// calling user, so the mode and content checks stand alone here.
func sharedExtensionOwnedByCaller(fs.FileInfo) error {
	return nil
}
