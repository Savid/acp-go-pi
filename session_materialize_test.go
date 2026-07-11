package piacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSessionFilesystemHelpers(t *testing.T) {
	agent := NewAgent(WithHome(t.TempDir()))
	session := &agentSession{agent: agent, id: "01234567-89ab-cdef-0123-456789abcdef"}
	dirs, err := agent.createSessionDirs()
	require.NoError(t, err)
	session.sessionRoot = dirs.Root
	require.True(t, filepath.IsAbs(session.sessionRoot))
	entries := []SessionStoreEntry{json.RawMessage(`{"type":"session"}`), json.RawMessage(`{"type":"message"}`)}
	path, err := writeHydratedSessionFile(dirs, string(session.id), entries)
	require.NoError(t, err)
	session.sessionFilePath = path
	require.True(t, session.sessionFileExists())
	session.sessionFilePath = filepath.Join(t.TempDir(), "missing")
	require.False(t, session.sessionFileExists())
	session.sessionFilePath = ""
	require.False(t, session.sessionFileExists())
	require.NoError(t, session.removeSessionRoot())
	require.NoError(t, session.removeSessionRoot())

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err = NewAgent(WithHome(file)).createSessionDirs()
	require.Error(t, err)
}

func TestMaterializeFaultBranches(t *testing.T) {
	originalMkdirAll := materializeMkdirAll
	originalMkdirTemp := materializeMkdirTemp
	originalRemoveAll := materializeRemoveAll
	originalWriteFile := materializeWriteFile
	t.Cleanup(func() {
		materializeMkdirAll = originalMkdirAll
		materializeMkdirTemp = originalMkdirTemp
		materializeRemoveAll = originalRemoveAll
		materializeWriteFile = originalWriteFile
	})

	agent := NewAgent(WithHome(t.TempDir()))
	materializeMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
	_, err := agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirAll = originalMkdirAll
	materializeMkdirTemp = func(string, string) (string, error) { return "", errors.New("temp") }
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirTemp = originalMkdirTemp

	calls := 0
	materializeMkdirAll = func(path string, mode os.FileMode) error {
		calls++
		if calls == 2 {
			return errors.New("child")
		}

		return originalMkdirAll(path, mode)
	}
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirAll = originalMkdirAll

	dirs, err := agent.createSessionDirs()
	require.NoError(t, err)
	materializeWriteFile = func(string, []byte, os.FileMode) error { return errors.New("write") }
	_, err = writeHydratedSessionFile(dirs, "id", []SessionStoreEntry{nil, json.RawMessage(`{}`)})
	require.Error(t, err)
	materializeWriteFile = originalWriteFile

	session := &agentSession{sessionRoot: "root"}
	materializeRemoveAll = func(string) error { return errors.New("remove") }
	require.Error(t, session.removeSessionRoot())
}

func TestSessionStoreLoadTimeoutDefaults(t *testing.T) {
	agent := NewAgent(WithSessionStoreLoadTimeout(time.Second))
	require.Equal(t, time.Second, agent.sessionStoreLoadTimeout())
	require.Equal(t, defaultSessionStoreLoadTimeout, NewAgent().sessionStoreLoadTimeout())
}
