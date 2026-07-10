package pi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// SettingsFileName is pi's per-agent-dir settings file. It is the
	// wrapper's deep-merge seed filename: the wrapper's managed keys win, a
	// seeded settings.json supplies everything else.
	SettingsFileName = "settings.json"
	// AuthFileName is pi's provider credential file, injected at session
	// start and never persisted to the store.
	AuthFileName = "auth.json"

	// seedManifestFileName tracks the relative paths the wrapper owns inside
	// a seed root so seed writes never clobber an operator-authored file.
	seedManifestFileName = ".seed-manifest.json"
	// seedBackupSuffix names the sidecar copy kept when a managed seed file's
	// contents change.
	seedBackupSuffix = ".seed.bak"

	parentDirSegment = ".."
)

// Filesystem seams for fault-injection in tests.
var (
	fsReadFile  = os.ReadFile
	fsWriteFile = os.WriteFile
	fsMkdirAll  = os.MkdirAll
	fsStat      = os.Stat
)

// SeedFileError reports an invalid or unwritable seed file; the root package
// maps it to the uniform unsupported/invalid option error.
type SeedFileError struct {
	Name string
}

// Error implements the error interface.
func (e *SeedFileError) Error() string {
	if e.Name == "" {
		return "invalid seed file configuration"
	}

	return fmt.Sprintf("invalid seed file %q", e.Name)
}

// AgentDir describes one isolated per-session pi agent directory.
type AgentDir struct {
	// Root is the directory pi sees as PI_CODING_AGENT_DIR.
	Root string
	// ManagedSettings are the wrapper-owned settings.json keys (for example
	// defaultProvider/defaultModel). They are deep-merged on top of any
	// seeded settings.json; the wrapper wins for keys it manages.
	ManagedSettings map[string]any
	// SeedFiles maps relative paths to contents written into Root before
	// launch. settings.json participates in the deep merge; all other files
	// are written verbatim.
	SeedFiles map[string]string
	// AuthJSON, when non-empty, is written to auth.json at hydrate time. It
	// is credential material: excluded from the store, never seeded.
	AuthJSON []byte
}

// Write materializes the agent directory: validates seed paths, deep-merges
// settings.json, writes every seeded file under a provenance manifest, and
// injects auth.json.
func (d AgentDir) Write() error {
	if strings.TrimSpace(d.Root) == "" {
		return &SeedFileError{}
	}

	if err := fsMkdirAll(d.Root, 0o700); err != nil {
		return fmt.Errorf("create agent directory: %w", err)
	}

	files, err := d.effectiveFiles()
	if err != nil {
		return err
	}

	if err := writeSeedFiles(d.Root, files); err != nil {
		return err
	}

	if len(d.AuthJSON) > 0 {
		if err := fsWriteFile(filepath.Join(d.Root, AuthFileName), d.AuthJSON, 0o600); err != nil {
			return fmt.Errorf("write auth file: %w", err)
		}
	}

	return nil
}

// effectiveFiles resolves the final file contents: every seed verbatim,
// except settings.json, which is the seed deep-merged under the managed keys
// and is always present.
func (d AgentDir) effectiveFiles() (map[string]string, error) {
	files := make(map[string]string, len(d.SeedFiles)+1)
	for name, content := range d.SeedFiles {
		files[name] = content
	}

	settings := map[string]any{}

	if seed, ok := files[SettingsFileName]; ok {
		if err := json.Unmarshal([]byte(seed), &settings); err != nil {
			return nil, &SeedFileError{Name: SettingsFileName}
		}
	}

	merged := mergeJSONMaps(settings, d.ManagedSettings)

	encoded, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode settings.json: %w", err)
	}

	files[SettingsFileName] = string(encoded) + "\n"

	return files, nil
}

// mergeJSONMaps returns base with overlay applied on top: overlay wins for
// scalar conflicts, maps merge recursively.
func mergeJSONMaps(base map[string]any, overlay map[string]any) map[string]any {
	result := make(map[string]any, len(base)+len(overlay))
	for key, value := range base {
		result[key] = value
	}

	for key, value := range overlay {
		if overlayMap, ok := value.(map[string]any); ok {
			if baseMap, ok := result[key].(map[string]any); ok {
				result[key] = mergeJSONMaps(baseMap, overlayMap)

				continue
			}
		}

		result[key] = value
	}

	return result
}

// seedTarget is one resolved seed write with its precomputed disk state.
type seedTarget struct {
	name   string
	path   string
	exists bool
}

// writeSeedFiles writes each file into dir under an ownership manifest so the
// wrapper never overwrites a file it did not create: a first write records
// the relpath in the manifest; a subsequent write of a managed file keeps a
// seedBackupSuffix copy of the prior bytes when they change; a pre-existing
// unmanaged target fails closed, leaving all files untouched.
func writeSeedFiles(dir string, files map[string]string) error {
	if len(files) == 0 {
		return nil
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}

	slices.Sort(names)

	targets := make([]seedTarget, 0, len(names))

	for _, name := range names {
		target, err := resolveSeedFilePath(dir, name)
		if err != nil {
			return err
		}

		exists, err := seedTargetExists(target)
		if err != nil {
			return err
		}

		targets = append(targets, seedTarget{name: name, path: target, exists: exists})
	}

	manifest, err := loadSeedManifest(dir)
	if err != nil {
		return err
	}

	// Fail closed on any pre-existing unmanaged target before writing
	// anything, so a rejected seed leaves every file on disk untouched.
	for _, target := range targets {
		if !target.exists {
			continue
		}

		if _, managed := manifest[seedManifestKey(target.name)]; !managed {
			return &SeedFileError{Name: target.name}
		}
	}

	added := false

	for _, target := range targets {
		newBytes := []byte(files[target.name])

		if target.exists {
			// Managed target (guaranteed by the fail-closed scan above): back
			// up changed bytes, skip identical ones.
			current, readErr := fsReadFile(target.path)
			if readErr != nil {
				return fmt.Errorf("read managed seed file: %w", readErr)
			}

			if bytes.Equal(current, newBytes) {
				continue
			}

			if backupErr := fsWriteFile(target.path+seedBackupSuffix, current, 0o600); backupErr != nil {
				return fmt.Errorf("back up managed seed file: %w", backupErr)
			}
		}

		if err := fsMkdirAll(filepath.Dir(target.path), 0o700); err != nil {
			return fmt.Errorf("create seed file directory: %w", err)
		}

		if err := fsWriteFile(target.path, newBytes, 0o600); err != nil {
			return fmt.Errorf("write seed file: %w", err)
		}

		if _, managed := manifest[seedManifestKey(target.name)]; !managed {
			manifest[seedManifestKey(target.name)] = struct{}{}
			added = true
		}
	}

	if added {
		if err := writeSeedManifest(dir, manifest); err != nil {
			return err
		}
	}

	return nil
}

func seedTargetExists(path string) (bool, error) {
	if _, err := fsStat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("stat seed file: %w", err)
	}

	return true, nil
}

func seedManifestKey(name string) string {
	return filepath.ToSlash(name)
}

func loadSeedManifest(dir string) (map[string]struct{}, error) {
	data, err := fsReadFile(filepath.Join(dir, seedManifestFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[string]struct{}), nil
		}

		return nil, fmt.Errorf("read seed manifest: %w", err)
	}

	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("decode seed manifest: %w", err)
	}

	manifest := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		manifest[entry] = struct{}{}
	}

	return manifest, nil
}

func writeSeedManifest(dir string, manifest map[string]struct{}) error {
	entries := make([]string, 0, len(manifest))
	for entry := range manifest {
		entries = append(entries, entry)
	}

	slices.Sort(entries)

	// entries is a []string and cannot fail to marshal.
	data, _ := json.Marshal(entries)

	if err := fsWriteFile(filepath.Join(dir, seedManifestFileName), data, 0o600); err != nil {
		return fmt.Errorf("write seed manifest: %w", err)
	}

	return nil
}

func resolveSeedFilePath(dir string, name string) (string, error) {
	if !validSeedFilePath(name) {
		return "", &SeedFileError{Name: name}
	}

	return filepath.Join(dir, filepath.FromSlash(name)), nil
}

func validSeedFilePath(name string) bool {
	if strings.TrimSpace(name) == "" ||
		filepath.IsAbs(name) ||
		strings.HasPrefix(name, "/") ||
		strings.HasPrefix(name, "\\") ||
		strings.Contains(name, "\x00") ||
		filepath.VolumeName(name) != "" {
		return false
	}

	for _, part := range strings.FieldsFunc(name, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == "" || part == "." || part == parentDirSegment || strings.Contains(part, ":") {
			return false
		}
	}

	return true
}
