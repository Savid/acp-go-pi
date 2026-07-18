package pi

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func testContainmentSpec(t *testing.T) ContainmentSpec {
	t.Helper()
	if runtime.GOOS != "darwin" {
		return ContainmentSpec{}
	}

	parent := t.TempDir()
	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-*")
	if err != nil {
		t.Fatal(err)
	}
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		t.Fatal(err)
	}

	return ContainmentSpec{
		DarwinBestEffort: true,
		ScratchParent:    parent,
		GenerationRoot:   filepath.Clean(root),
		RuntimeID:        hex.EncodeToString(identity),
		LifecycleKind:    "discovery",
	}
}
