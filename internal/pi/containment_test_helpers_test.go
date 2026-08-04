package pi

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

func testContainmentSpec(t *testing.T) ContainmentSpec {
	t.Helper()
	pathEnvironment := os.Getenv("PATH")
	if pathEnvironment == "" {
		pathEnvironment = "/usr/bin:/bin"
	}
	isolation := &ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()),
		BaseEnvironment:      map[string]string{"PATH": pathEnvironment, "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
	}
	if runtime.GOOS != "darwin" {
		return ContainmentSpec{Isolation: isolation}
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
		Isolation:        isolation,
	}
}

func setTestIsolationBootstrapEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envIsolationUID, strconv.Itoa(os.Geteuid()))
	t.Setenv(envIsolationGID, strconv.Itoa(os.Getegid()))
	t.Setenv(envIsolationTest, "true")
}
