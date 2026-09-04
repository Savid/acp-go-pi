package piacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func restoreMaterializeSeams(t *testing.T) {
	t.Helper()
	mkdirAll := materializeMkdirAll
	mkdirTemp := materializeMkdirTemp
	chmod := materializeChmod
	removeAll := materializeRemoveAll
	writeFile := materializeWriteFile
	stat := materializeStat
	readFile := materializeReadFile
	t.Cleanup(func() {
		materializeMkdirAll = mkdirAll
		materializeMkdirTemp = mkdirTemp
		materializeChmod = chmod
		materializeRemoveAll = removeAll
		materializeWriteFile = writeFile
		materializeStat = stat
		materializeReadFile = readFile
	})
}

func TestSessionFilesystemHelpers(t *testing.T) {
	agent := NewAgent(WithScratchDir(t.TempDir()))
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
	require.NoError(t, (&agentSession{}).removeSessionRoot())

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err = NewAgent(WithScratchDir(file)).createSessionDirs()
	require.Error(t, err)
}

func TestMaterializeFaultBranches(t *testing.T) {
	originalMkdirAll := materializeMkdirAll
	originalMkdirTemp := materializeMkdirTemp
	originalChmod := materializeChmod
	originalRemoveAll := materializeRemoveAll
	originalWriteFile := materializeWriteFile
	t.Cleanup(func() {
		materializeMkdirAll = originalMkdirAll
		materializeMkdirTemp = originalMkdirTemp
		materializeChmod = originalChmod
		materializeRemoveAll = originalRemoveAll
		materializeWriteFile = originalWriteFile
	})

	agent := NewAgent(WithScratchDir(t.TempDir()))
	materializeMkdirAll = func(string, os.FileMode) error { return errors.New("mkdir") }
	_, err := agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirAll = originalMkdirAll
	materializeMkdirTemp = func(string, string) (string, error) { return "", errors.New("temp") }
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeMkdirTemp = originalMkdirTemp
	materializeChmod = func(string, os.FileMode) error { return errors.New("chmod") }
	_, err = agent.createSessionDirs()
	require.Error(t, err)
	materializeChmod = originalChmod

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

func TestRuntimeGenerationMaterializeBranches(t *testing.T) {
	wantErr := errors.New("injected generation materialization failure")

	t.Run("create generation root", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeMkdirTemp = func(string, string) (string, error) { return "", wantErr }
		_, err := createSessionGeneration(t.TempDir())
		require.ErrorContains(t, err, "create session runtime generation")
	})
}

func TestSessionStoreLoadTimeoutDefaults(t *testing.T) {
	agent := NewAgent(WithSessionStoreLoadTimeout(time.Second))
	require.Equal(t, time.Second, agent.sessionStoreLoadTimeout())
	require.Equal(t, defaultSessionStoreLoadTimeout, NewAgent().sessionStoreLoadTimeout())
}

func TestDurableHomeMaterialization(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		home := filepath.Join(t.TempDir(), "durable-home")
		dirs := sessionDirs{Root: t.TempDir(), AgentDir: "/generated"}
		require.NoError(t, NewAgent(WithHome(home)).applyGenerationAgentDir(&dirs))
		require.Equal(t, home, dirs.AgentDir)
		requireRestrictedMode(t, home, 0o700)
		require.NoDirExists(t, filepath.Join(dirs.Root, "agent"))
	})

	t.Run("generation private agent directory", func(t *testing.T) {
		dirs := sessionDirs{Root: t.TempDir()}
		require.NoError(t, NewAgent().applyGenerationAgentDir(&dirs))
		require.Equal(t, filepath.Join(dirs.Root, "agent"), dirs.AgentDir)
		require.DirExists(t, dirs.AgentDir)
	})

	t.Run("generation agent directory failure", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeMkdirAll = func(string, os.FileMode) error { return errors.New("create generation") }
		err := NewAgent().applyGenerationAgentDir(&sessionDirs{Root: t.TempDir()})
		require.ErrorContains(t, err, "create session directory")
	})

	t.Run("relative path", func(t *testing.T) {
		err := NewAgent(WithHome("relative/home")).applyGenerationAgentDir(&sessionDirs{})
		requireRefusedField(t, optionFieldHome, err)
	})

	t.Run("create failure", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeMkdirAll = func(string, os.FileMode) error { return errors.New("create durable") }
		err := NewAgent(WithHome(filepath.Join(t.TempDir(), "home"))).applyGenerationAgentDir(&sessionDirs{})
		require.ErrorContains(t, err, "create durable agent directory")
	})

	t.Run("chmod failure", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeChmod = func(string, os.FileMode) error { return errors.New("chmod durable") }
		err := NewAgent(WithHome(filepath.Join(t.TempDir(), "home"))).applyGenerationAgentDir(&sessionDirs{})
		require.ErrorContains(t, err, "protect durable agent directory")
	})
}

// A settings.json pi cannot load takes effect nowhere and says so nowhere, so
// a home the wrapper cannot reconcile fails the session start rather than
// letting a launch inherit whatever is in the file.
func TestReconcileHomeStartupDefaultsFailsClosed(t *testing.T) {
	t.Run("baseline capture", func(t *testing.T) {
		home := filepath.Join(t.TempDir(), "home")
		require.NoError(t, os.MkdirAll(home, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(home, pi.SettingsFileName), []byte(`{"defaultModel":`), 0o600))

		agent := newStubClientAgent(t, newStubPiClient(), WithHome(home))
		_, err := agent.NewSession(t.Context(), NewSessionRequest(testCwd))
		require.ErrorContains(t, err, "decode pi settings")

		// The captured failure is the home's, not the session's: it holds for
		// every later session too.
		_, err = agent.NewSession(t.Context(), NewSessionRequest(testCwd))
		require.ErrorContains(t, err, "decode pi settings")
	})

	t.Run("restore", func(t *testing.T) {
		home := filepath.Join(t.TempDir(), "home")
		client := newStubPiClient()
		client.state = pi.SessionState{SessionID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
		agent := newStubClientAgent(t, client, WithHome(home))

		response, err := agent.NewSession(t.Context(), NewSessionRequest(testCwd))
		require.NoError(t, err)
		session, err := agent.session(response.SessionId)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, session.Close(t.Context())) })

		require.NoError(t, os.WriteFile(filepath.Join(home, pi.SettingsFileName), []byte(`{"defaultModel":`), 0o600))
		_, err = agent.NewSession(t.Context(), NewSessionRequest(absTestPath("other")))
		require.ErrorContains(t, err, "decode pi settings")
	})
}
