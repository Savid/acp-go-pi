package pi

import (
	"encoding/json"

	"github.com/savid/acp-go-core/process"
)

// SettingsFileName is pi's per-agent-dir settings file.
const SettingsFileName = "settings.json"

// WriteSeedFiles validates settings before seeding the native home. Pi silently
// uses defaults when a settings file cannot be parsed.
func WriteSeedFiles(dir string, files map[string]string) error {
	if seed, ok := files[SettingsFileName]; ok {
		var settings map[string]any
		if err := json.Unmarshal([]byte(seed), &settings); err != nil || settings == nil {
			return &process.SeedFileError{Name: SettingsFileName}
		}
	}

	return process.WriteSeedFiles(dir, files)
}
