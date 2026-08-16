package pi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func readJSONFile(t *testing.T, path string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(path) // #nosec G304 -- test reads from its own temp dir.
	require.NoError(t, err)

	var values map[string]any

	require.NoError(t, json.Unmarshal(data, &values))

	return values
}

func TestAgentDirWriteManagedSettings(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "agent")

	dir := AgentDir{
		Root: root,
		ManagedSettings: map[string]any{
			"defaultProvider": "openai",
			"defaultModel":    "gpt-4o",
		},
	}
	require.NoError(t, dir.Write())

	settings := readJSONFile(t, filepath.Join(root, SettingsFileName))
	require.Equal(t, "openai", settings["defaultProvider"])
	require.Equal(t, "gpt-4o", settings["defaultModel"])

	manifest := filepath.Join(root, seedManifestFileName)
	data, err := os.ReadFile(manifest) // #nosec G304 -- test reads from its own temp dir.
	require.NoError(t, err)
	require.JSONEq(t, `["settings.json"]`, string(data))
}

func TestAgentDirWritesExplicitAuthJSON(t *testing.T) {
	root := filepath.Join(t.TempDir(), "agent")
	auth := []byte(`{"anthropic":{"type":"api_key","key":"test-only"}}`)
	require.NoError(t, (AgentDir{Root: root, AuthJSON: auth}).Write())

	path := filepath.Join(root, AuthFileName)
	contents, err := os.ReadFile(path) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.Equal(t, auth, contents)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	restoreAgentDirSeams(t)
	realWrite := fsWriteFile
	fsWriteFile = func(name string, data []byte, mode os.FileMode) error {
		if filepath.Base(name) == AuthFileName {
			return os.ErrPermission
		}

		return realWrite(name, data, mode)
	}
	require.ErrorContains(t, (AgentDir{Root: filepath.Join(t.TempDir(), "blocked"), AuthJSON: auth}).Write(), "write auth file")
}

func TestAgentDirWriteSeedMerge(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "agent")

	dir := AgentDir{
		Root: root,
		ManagedSettings: map[string]any{
			"defaultProvider": "anthropic",
			"compaction":      map[string]any{"enabled": true},
		},
		SeedFiles: map[string]string{
			SettingsFileName: `{
				"defaultProvider": "seeded-provider",
				"theme": "light",
				"compaction": {"enabled": false, "reserveTokens": 4096}
			}`,
			"notes/README.md": "seeded verbatim",
		},
	}
	require.NoError(t, dir.Write())

	settings := readJSONFile(t, filepath.Join(root, SettingsFileName))
	// Managed keys win; the seed supplies everything else, maps merge deep.
	require.Equal(t, "anthropic", settings["defaultProvider"])
	require.Equal(t, "light", settings["theme"])

	compaction, ok := settings["compaction"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, compaction["enabled"])
	require.InDelta(t, 4096.0, compaction["reserveTokens"], 0)

	verbatim, err := os.ReadFile(filepath.Join(root, "notes", "README.md")) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.Equal(t, "seeded verbatim", string(verbatim))
}

func TestAgentDirWriteInvalidSettingsSeed(t *testing.T) {
	t.Parallel()

	dir := AgentDir{
		Root:      filepath.Join(t.TempDir(), "agent"),
		SeedFiles: map[string]string{SettingsFileName: "not json"},
	}

	var seedErr *SeedFileError

	require.ErrorAs(t, dir.Write(), &seedErr)
	require.Equal(t, SettingsFileName, seedErr.Name)
}

func TestAgentDirWriteEmptyRootFailsClosed(t *testing.T) {
	t.Parallel()

	var seedErr *SeedFileError

	require.ErrorAs(t, AgentDir{Root: "  "}.Write(), &seedErr)
	require.Contains(t, seedErr.Error(), "invalid seed file configuration")
}

func TestAgentDirExplicitResources(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "agent")
	dir := AgentDir{Root: root, SeedFiles: map[string]string{
		"extensions/z.js":         "z",
		"extensions/a.ts":         "a",
		"extensions/ignored.txt":  "x",
		"skills/review/SKILL.md":  "skill",
		"skills/review/notes.md":  "notes",
		"prompts/review.md":       "prompt",
		"prompts/nested/other.md": "prompt",
		"settings.json":           `{}`,
	}}

	resources, err := dir.ExplicitResources()
	require.NoError(t, err)
	require.Equal(t, []string{
		filepath.Join(root, "extensions", "a.ts"),
		filepath.Join(root, "extensions", "z.js"),
	}, resources.Extensions)
	require.Equal(t, []string{filepath.Join(root, "skills", "review", "SKILL.md")}, resources.Skills)
	require.Equal(t, []string{
		filepath.Join(root, "prompts", "nested", "other.md"),
		filepath.Join(root, "prompts", "review.md"),
	}, resources.PromptTemplates)

	_, err = (AgentDir{Root: root, SeedFiles: map[string]string{"../escape.ts": "x"}}).ExplicitResources()
	var seedErr *SeedFileError
	require.ErrorAs(t, err, &seedErr)
}

func TestWriteSeedFilesPathValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "whitespace", path: "   "},
		{name: "absolute", path: "/etc/passwd"},
		{name: "backslash absolute", path: `\evil`},
		{name: "parent escape", path: "../outside"},
		{name: "nested parent escape", path: "nested/../../outside"},
		{name: "dot segment", path: "./settings.json"},
		{name: "nul byte", path: "bad\x00name"},
		{name: "colon segment", path: "c:evil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()

			var seedErr *SeedFileError

			err := writeSeedFiles(dir, map[string]string{test.path: "x"})
			require.ErrorAs(t, err, &seedErr)
		})
	}
}

func TestWriteSeedFilesProvenanceGuard(t *testing.T) {
	t.Parallel()

	t.Run("pre-existing unmanaged file fails closed", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		operatorFile := filepath.Join(dir, "operator.json")
		require.NoError(t, os.WriteFile(operatorFile, []byte("operator-owned"), 0o600))

		err := writeSeedFiles(dir, map[string]string{
			"operator.json": "clobber attempt",
			"new-file.txt":  "new",
		})

		var seedErr *SeedFileError

		require.ErrorAs(t, err, &seedErr)
		require.Equal(t, "operator.json", seedErr.Name)

		// Nothing was written: the rejected seed leaves every file untouched.
		current, readErr := os.ReadFile(operatorFile) // #nosec G304 -- test temp dir.
		require.NoError(t, readErr)
		require.Equal(t, "operator-owned", string(current))
		require.NoFileExists(t, filepath.Join(dir, "new-file.txt"))
	})

	t.Run("managed rewrite keeps a backup of changed bytes", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "v1"}))
		require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "v2"}))

		current, err := os.ReadFile(filepath.Join(dir, "config.txt")) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, "v2", string(current))

		backup, err := os.ReadFile(filepath.Join(dir, "config.txt"+seedBackupSuffix)) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, "v1", string(backup))
	})

	t.Run("identical rewrite skips backup", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "same"}))
		require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "same"}))
		require.NoFileExists(t, filepath.Join(dir, "config.txt"+seedBackupSuffix))
	})

	t.Run("empty file map is a no-op", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, writeSeedFiles(t.TempDir(), nil))
	})

	t.Run("corrupt manifest fails", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, seedManifestFileName), []byte("not json"), 0o600))

		err := writeSeedFiles(dir, map[string]string{"config.txt": "x"})
		require.ErrorContains(t, err, "decode seed manifest")
	})
}

func TestMergeJSONMaps(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"keep":   "base",
		"clash":  "base",
		"nested": map[string]any{"a": 1, "b": 2},
		"type":   map[string]any{"was": "map"},
	}
	overlay := map[string]any{
		"clash":  "overlay",
		"nested": map[string]any{"b": 3, "c": 4},
		"type":   "now-scalar",
		"added":  true,
	}

	merged := mergeJSONMaps(base, overlay)

	require.Equal(t, "base", merged["keep"])
	require.Equal(t, "overlay", merged["clash"])
	require.Equal(t, "now-scalar", merged["type"])
	require.Equal(t, true, merged["added"])

	nested, ok := merged["nested"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, 1, nested["a"])
	require.Equal(t, 3, nested["b"])
	require.Equal(t, 4, nested["c"])
}

func TestSeedFileErrorMessages(t *testing.T) {
	t.Parallel()

	require.Equal(t, "invalid seed file configuration", (&SeedFileError{}).Error())
	require.Equal(t, `invalid seed file "x"`, (&SeedFileError{Name: "x"}).Error())
}

func TestAgentDirWriteFaultInjection(t *testing.T) {
	tests := []struct {
		name    string
		breakFn func()
		wantErr string
	}{
		{
			name: "mkdir failure",
			breakFn: func() {
				fsMkdirAll = func(string, os.FileMode) error { return fmt.Errorf("disk gone") }
			},
			wantErr: "create agent directory",
		},
		{
			name: "settings write failure",
			breakFn: func() {
				fsWriteFile = func(string, []byte, os.FileMode) error { return fmt.Errorf("disk full") }
			},
			wantErr: "write seed file",
		},
		{
			name: "stat failure",
			breakFn: func() {
				fsStat = func(string) (os.FileInfo, error) { return nil, fmt.Errorf("io error") }
			},
			wantErr: "stat seed file",
		},
		{
			name: "manifest read failure",
			breakFn: func() {
				fsReadFile = func(string) ([]byte, error) { return nil, fmt.Errorf("io error") }
			},
			wantErr: "read seed manifest",
		},
		{
			name: "manifest write failure",
			breakFn: func() {
				realWrite := fsWriteFile
				fsWriteFile = func(path string, data []byte, perm os.FileMode) error {
					if filepath.Base(path) == seedManifestFileName {
						return fmt.Errorf("disk full")
					}

					return realWrite(path, data, perm)
				}
			},
			wantErr: "write seed manifest",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreAgentDirSeams(t)
			test.breakFn()

			dir := AgentDir{Root: filepath.Join(t.TempDir(), "agent")}
			require.ErrorContains(t, dir.Write(), test.wantErr)
		})
	}
}

func TestAgentDirWriteUnencodableManagedSettings(t *testing.T) {
	t.Parallel()

	dir := AgentDir{
		Root:            filepath.Join(t.TempDir(), "agent"),
		ManagedSettings: map[string]any{"bad": func() {}},
	}
	require.ErrorContains(t, dir.Write(), "encode settings.json")
}

func TestWriteSeedFilesBackupFailure(t *testing.T) {
	restoreAgentDirSeams(t)

	dir := t.TempDir()
	require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "v1"}))

	realWrite := fsWriteFile
	fsWriteFile = func(path string, data []byte, perm os.FileMode) error {
		if filepath.Base(path) == "config.txt"+seedBackupSuffix {
			return fmt.Errorf("disk full")
		}

		return realWrite(path, data, perm)
	}

	require.ErrorContains(t, writeSeedFiles(dir, map[string]string{"config.txt": "v2"}), "back up managed seed file")
}

func TestWriteSeedFilesManagedReadFailure(t *testing.T) {
	restoreAgentDirSeams(t)

	dir := t.TempDir()
	require.NoError(t, writeSeedFiles(dir, map[string]string{"config.txt": "v1"}))

	realRead := fsReadFile
	fsReadFile = func(path string) ([]byte, error) {
		if filepath.Base(path) == "config.txt" {
			return nil, fmt.Errorf("io error")
		}

		return realRead(path)
	}

	require.ErrorContains(t, writeSeedFiles(dir, map[string]string{"config.txt": "v2"}), "read managed seed file")
}

func TestWriteSeedFilesDirectoryFailure(t *testing.T) {
	restoreAgentDirSeams(t)

	fsMkdirAll = func(string, os.FileMode) error { return fmt.Errorf("disk gone") }

	require.ErrorContains(t, writeSeedFiles(t.TempDir(), map[string]string{"nested/config.txt": "value"}), "create seed file directory")
}

func restoreAgentDirSeams(t *testing.T) {
	t.Helper()

	readFile, writeFile, mkdirAll, stat := fsReadFile, fsWriteFile, fsMkdirAll, fsStat
	mkdirTemp, link, remove, removeAll := fsMkdirTemp, fsLink, fsRemove, fsRemoveAll

	t.Cleanup(func() {
		fsReadFile, fsWriteFile, fsMkdirAll, fsStat = readFile, writeFile, mkdirAll, stat
		fsMkdirTemp, fsLink, fsRemove, fsRemoveAll = mkdirTemp, link, remove, removeAll
	})
}
