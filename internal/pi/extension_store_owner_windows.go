//go:build windows

package pi

import "io/fs"

// sharedExtensionOwnedByCaller performs no Windows ACL or owner inspection.
// The host must protect the configured scratch parent from other writers;
// source-byte verification alone does not establish that protection.
func sharedExtensionOwnedByCaller(fs.FileInfo) error {
	return nil
}

// sharedExtensionModeLoose cannot infer ACL permissions from Go's synthetic
// Windows mode bits: writable files report 0666 and directories report 0777.
// This path therefore makes no adapter-enforced write-exclusivity claim.
func sharedExtensionModeLoose(fs.FileInfo) bool { return false }
