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
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	if uid == 0 {
		uid, gid = 11, 22
	}
	isolation := &ProcessIsolation{
		UID: uid, GID: gid,
		BaseEnvironment:      map[string]string{"PATH": pathEnvironment, "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
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
		DarwinBestEffort: runtime.GOOS == "darwin",
		ScratchParent:    parent,
		GenerationRoot:   filepath.Clean(root),
		RuntimeID:        hex.EncodeToString(identity),
		LifecycleKind:    "discovery",
		Isolation:        isolation,
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

func setTestIsolationBootstrapEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envIsolationUID, strconv.Itoa(os.Geteuid()))
	t.Setenv(envIsolationGID, strconv.Itoa(os.Getegid()))
	t.Setenv(envIsolationTest, "true")
}
