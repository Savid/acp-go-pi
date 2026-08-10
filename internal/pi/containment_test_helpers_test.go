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
	pathEnvironment := os.Getenv("PATH")
	if pathEnvironment == "" {
		pathEnvironment = "/usr/bin:/bin"
	}
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	if uid == 0 {
		uid, gid = 11, 22
	}
	var isolation *ProcessIsolation
	ordinary := map[string]string{"PATH": pathEnvironment, "HOME": os.Getenv("HOME")}
	if runtime.GOOS == "linux" {
		isolation = &ProcessIsolation{
			UID: uid, GID: gid,
			BaseEnvironment:      ordinary,
			TestOnlyNoCredential: true,
		}
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
		DarwinBestEffort:    runtime.GOOS == "darwin",
		ScratchParent:       parent,
		GenerationRoot:      filepath.Clean(root),
		RuntimeID:           hex.EncodeToString(identity),
		LifecycleKind:       "discovery",
		Isolation:           isolation,
		OrdinaryEnvironment: ordinary,
	}
}

func testVersionProbeSpec(t *testing.T) (string, ContainmentSpec) {
	t.Helper()
	containment := testContainmentSpec(t)
	agentDir := filepath.Join(containment.GenerationRoot, "probe-agent")
	if err := (AgentDir{Root: agentDir}).Write(); err != nil {
		t.Fatal(err)
	}

	return agentDir, containment
}
