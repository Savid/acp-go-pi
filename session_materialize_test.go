package piacp

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type materializeTestDirEntry struct {
	name    string
	dir     bool
	info    os.FileInfo
	infoErr error
}

func (entry materializeTestDirEntry) Name() string               { return entry.name }
func (entry materializeTestDirEntry) IsDir() bool                { return entry.dir }
func (materializeTestDirEntry) Type() os.FileMode                { return 0 }
func (entry materializeTestDirEntry) Info() (os.FileInfo, error) { return entry.info, entry.infoErr }

func restoreMaterializeSeams(t *testing.T) {
	t.Helper()
	mkdirAll := materializeMkdirAll
	mkdirTemp := materializeMkdirTemp
	chmod := materializeChmod
	removeAll := materializeRemoveAll
	writeFile := materializeWriteFile
	stat := materializeStat
	readFile := materializeReadFile
	walkDir := materializeWalkDir
	rel := materializeRel
	t.Cleanup(func() {
		materializeMkdirAll = mkdirAll
		materializeMkdirTemp = mkdirTemp
		materializeChmod = chmod
		materializeRemoveAll = removeAll
		materializeWriteFile = writeFile
		materializeStat = stat
		materializeReadFile = readFile
		materializeWalkDir = walkDir
		materializeRel = rel
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

	t.Run("walk source", func(t *testing.T) {
		err := copyGenerationAgentDir(filepath.Join(t.TempDir(), "missing"), t.TempDir())
		require.Error(t, err)
	})

	t.Run("relative path", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeRel = func(string, string) (string, error) { return "", wantErr }
		materializeWalkDir = func(root string, walk fs.WalkDirFunc) error {
			return walk(filepath.Join(root, "child"), materializeTestDirEntry{name: "child"}, nil)
		}
		require.ErrorIs(t, copyGenerationAgentDir("source", "target"), wantErr)
		_, err := rebaseGenerationPath("path", "old", "new")
		require.ErrorContains(t, err, "outside")
	})

	t.Run("entry info", func(t *testing.T) {
		restoreMaterializeSeams(t)
		materializeWalkDir = func(root string, walk fs.WalkDirFunc) error {
			return walk(filepath.Join(root, "child"), materializeTestDirEntry{name: "child", infoErr: wantErr}, nil)
		}
		require.ErrorIs(t, copyGenerationAgentDir("source", "target"), wantErr)
	})

	t.Run("directory", func(t *testing.T) {
		restoreMaterializeSeams(t)
		source := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(source, "nested"), 0o750))
		target := t.TempDir()
		require.NoError(t, copyGenerationAgentDir(source, target))
		require.DirExists(t, filepath.Join(target, "nested"))
	})

	t.Run("non regular", func(t *testing.T) {
		restoreMaterializeSeams(t)
		source := t.TempDir()
		require.NoError(t, os.Symlink("target", filepath.Join(source, "link")))
		require.ErrorContains(t, copyGenerationAgentDir(source, t.TempDir()), "non-regular")
	})

	t.Run("read file", func(t *testing.T) {
		restoreMaterializeSeams(t)
		source := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("data"), 0o600))
		materializeReadFile = func(string) ([]byte, error) { return nil, wantErr }
		require.ErrorIs(t, copyGenerationAgentDir(source, t.TempDir()), wantErr)
	})

	t.Run("write file", func(t *testing.T) {
		restoreMaterializeSeams(t)
		source := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(source, "file"), []byte("data"), 0o600))
		materializeWriteFile = func(string, []byte, os.FileMode) error { return wantErr }
		require.ErrorIs(t, copyGenerationAgentDir(source, t.TempDir()), wantErr)
	})

	rebased, err := rebaseGenerationPath("", "/old", "/new")
	require.NoError(t, err)
	require.Empty(t, rebased)
	_, err = rebaseGenerationPath("/outside", "/old", "/new")
	require.ErrorContains(t, err, "outside")
}

func TestSessionStoreLoadTimeoutDefaults(t *testing.T) {
	agent := NewAgent(WithSessionStoreLoadTimeout(time.Second))
	require.Equal(t, time.Second, agent.sessionStoreLoadTimeout())
	require.Equal(t, defaultSessionStoreLoadTimeout, NewAgent().sessionStoreLoadTimeout())
}
