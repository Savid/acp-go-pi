//go:build !linux

package piacp

import (
	"fmt"
	"os"
	"runtime"
)

func handoffGeneratedNativeTreePlatform(_ string, uid uint32, gid uint32) error {
	if int64(uid) == int64(os.Geteuid()) && int64(gid) == int64(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership handoff is unsupported on %s", runtime.GOOS)
}
