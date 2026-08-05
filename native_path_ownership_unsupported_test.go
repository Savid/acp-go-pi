//go:build !linux

package piacp

import (
	"os"
	"testing"
)

func TestUnsupportedNativePathOwnership(t *testing.T) {
	if err := handoffGeneratedNativeTree("unused", nil); err != nil {
		t.Fatal(err)
	}
	current := &ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if err := handoffGeneratedNativeTree("unused", current); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree("unused", &ProcessIsolation{UID: current.UID + 1, GID: current.GID + 1}); err == nil {
		t.Fatal("unsupported ownership handoff succeeded")
	}
}
