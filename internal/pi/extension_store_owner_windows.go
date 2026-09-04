//go:build windows

package pi

import "io/fs"

// sharedExtensionOwnedByCaller has no uid to compare on Windows. The store
// lives beneath the scratch parent, whose ACL already confines writes to the
// calling user, so the content check stands alone here.
func sharedExtensionOwnedByCaller(fs.FileInfo) error {
	return nil
}

// sharedExtensionModeLoose has no POSIX mode to read either. Go reports 0666
// for every writable file on Windows and 0777 for every writable directory,
// whatever the ACL says, so the group and world write bits are set on every
// path this store creates. Read literally the check refused every entry the
// store could ever hold, and because a session launch publishes these sources
// before it starts anything, no session could open on this platform at all.
// The profile ACL that stands in for the ownership check stands in for this
// one.
func sharedExtensionModeLoose(fs.FileInfo) bool { return false }
