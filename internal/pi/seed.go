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
	// SettingsFileName is pi's per-agent-dir settings file.
	SettingsFileName = "settings.json"
	// seedManifestFileName lists the relative paths the adapter manages inside
	// a seed root so seed writes never clobber an operator-authored file.
	seedManifestFileName = ".seed-manifest.json"
	// seedBackupSuffix names the sidecar copy kept when a managed seed file's
	// contents change.
	seedBackupSuffix = ".seed.bak"
)

// SeedFileError reports an invalid or unwritable seed file; the root package
// maps it to the uniform unsupported error naming seedFiles.
type SeedFileError struct {
	Name string
}

func (e *SeedFileError) Error() string {
	return fmt.Sprintf("invalid seed file %q", e.Name)
}

// WriteSeedFiles writes each file into dir under an ownership manifest so the
// adapter never overwrites a file it did not create: a first write records the
// relative path in the manifest, a later write of a managed file keeps a
// backup of the prior bytes when they change, and a pre-existing unmanaged
// target fails closed before anything is written. A seeded settings.json must
// parse, because pi silently runs on its defaults when it does not.
func WriteSeedFiles(dir string, files map[string]string) error {
	if len(files) == 0 {
		return nil
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create agent directory: %w", err)
	}

	if seed, ok := files[SettingsFileName]; ok {
		var settings map[string]any
		if err := json.Unmarshal([]byte(seed), &settings); err != nil {
			return &SeedFileError{Name: SettingsFileName}
		}
	}

	names := slices.Sorted(func(yield func(string) bool) {
		for name := range files {
			if !yield(name) {
				return
			}
		}
	})

	manifest, err := loadSeedManifest(dir)
	if err != nil {
		return err
	}

	type target struct {
		name   string
		path   string
		exists bool
	}

	targets := make([]target, 0, len(names))

	for _, name := range names {
		if !validSeedFilePath(name) {
			return &SeedFileError{Name: name}
		}

		path := filepath.Join(dir, filepath.FromSlash(name))

		_, statErr := os.Stat(path)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("stat seed file: %w", statErr)
		}

		exists := statErr == nil
		if _, managed := manifest[filepath.ToSlash(name)]; exists && !managed {
			return &SeedFileError{Name: name}
		}

		targets = append(targets, target{name: name, path: path, exists: exists})
	}

	added := false

	for _, item := range targets {
		contents := []byte(files[item.name])

		if item.exists {
			current, readErr := os.ReadFile(item.path)
			if readErr != nil {
				return fmt.Errorf("read managed seed file: %w", readErr)
			}

			if bytes.Equal(current, contents) {
				continue
			}

			if backupErr := os.WriteFile(item.path+seedBackupSuffix, current, 0o600); backupErr != nil { //nolint:gosec // validSeedFilePath confines the name to dir.
				return fmt.Errorf("back up managed seed file: %w", backupErr)
			}
		}

		if err := os.MkdirAll(filepath.Dir(item.path), 0o700); err != nil {
			return fmt.Errorf("create seed file directory: %w", err)
		}

		if err := os.WriteFile(item.path, contents, 0o600); err != nil {
			return fmt.Errorf("write seed file: %w", err)
		}

		if _, managed := manifest[filepath.ToSlash(item.name)]; !managed {
			manifest[filepath.ToSlash(item.name)] = struct{}{}
			added = true
		}
	}

	if added {
		return writeSeedManifest(dir, manifest)
	}

	return nil
}

func loadSeedManifest(dir string) (map[string]struct{}, error) {
	data, err := os.ReadFile(filepath.Join(dir, seedManifestFileName))
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
	entries := slices.Sorted(func(yield func(string) bool) {
		for entry := range manifest {
			if !yield(entry) {
				return
			}
		}
	})

	// A string slice cannot fail to marshal.
	data, _ := json.Marshal(entries)

	if err := os.WriteFile(filepath.Join(dir, seedManifestFileName), data, 0o600); err != nil {
		return fmt.Errorf("write seed manifest: %w", err)
	}

	return nil
}

func validSeedFilePath(name string) bool {
	cleanName := filepath.Clean(filepath.FromSlash(name))

	if strings.TrimSpace(name) == "" ||
		filepath.IsAbs(name) ||
		strings.HasPrefix(name, "/") ||
		strings.Contains(name, "\x00") ||
		cleanName == seedManifestFileName ||
		strings.HasSuffix(cleanName, seedBackupSuffix) {
		return false
	}

	for part := range strings.SplitSeq(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}

	return true
}
