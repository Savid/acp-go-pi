package pi

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A durable agent directory is shared by every session, and pi rewrites
// settings.json itself on each model change, so an unseeded write path must
// leave that file alone rather than author one of its own.
func TestAgentDirWriteAuthorsNoSettings(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "agent")
	require.NoError(t, AgentDir{Root: root}.Write())

	require.NoFileExists(t, filepath.Join(root, SettingsFileName))
	require.NoFileExists(t, filepath.Join(root, seedManifestFileName))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
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

// A seeded settings.json is operator configuration, identical for every
// session, so it lands verbatim like every other seed.
func TestAgentDirWriteSeedsSettingsVerbatim(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "agent")
	seeded := `{"defaultProvider": "anthropic", "theme": "light"}`

	dir := AgentDir{
		Root: root,
		SeedFiles: map[string]string{
			SettingsFileName:  seeded,
			"notes/README.md": "seeded verbatim",
		},
	}
	require.NoError(t, dir.Write())

	settings, err := os.ReadFile(filepath.Join(root, SettingsFileName)) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.Equal(t, seeded, string(settings))

	verbatim, err := os.ReadFile(filepath.Join(root, "notes", "README.md")) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.Equal(t, "seeded verbatim", string(verbatim))

	manifest, err := os.ReadFile(filepath.Join(root, seedManifestFileName)) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.JSONEq(t, `["notes/README.md","settings.json"]`, string(manifest))
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
			name: "seed write failure",
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

			dir := AgentDir{
				Root:      filepath.Join(t.TempDir(), "agent"),
				SeedFiles: map[string]string{SettingsFileName: `{}`},
			}
			require.ErrorContains(t, dir.Write(), test.wantErr)
		})
	}
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
