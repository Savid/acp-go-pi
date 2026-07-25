package piacp

import (
	"path/filepath"

	"github.com/coder/acp-go-sdk"
)

const validationAbsolutePath = "must be an absolute path"

func validateRequiredAbsolutePath(field string, path string) error {
	if path == "" {
		return acp.NewInvalidParams(map[string]any{field: validationRequired})
	}

	if !filepath.IsAbs(path) {
		return acp.NewInvalidParams(map[string]any{field: validationAbsolutePath})
	}

	return nil
}

func validateOptionalAbsolutePath(field string, path *string) error {
	if path == nil || *path == "" {
		return nil
	}

	if !filepath.IsAbs(*path) {
		return acp.NewInvalidParams(map[string]any{field: validationAbsolutePath})
	}

	return nil
}

func validateAbsolutePaths(field string, paths []string) error {
	for i, path := range paths {
		if path == "" {
			return acp.NewInvalidParams(map[string]any{field: map[string]any{
				jsonFieldIndex: i,
				jsonFieldError: validationRequired,
			}})
		}

		if !filepath.IsAbs(path) {
			return acp.NewInvalidParams(map[string]any{field: map[string]any{
				jsonFieldIndex: i,
				"path":         path,
				jsonFieldError: validationAbsolutePath,
			}})
		}
	}

	return nil
}

func validateSessionStartPaths(cwd string, additionalDirectories []string) error {
	if err := validateRequiredAbsolutePath(jsonFieldCwd, cwd); err != nil {
		return err
	}

	return validateAbsolutePaths("additionalDirectories", additionalDirectories)
}

// sessionStartConfigurationError reports the agent configuration no session may
// start under: an option that failed validation at construction, or a configured
// Home, which pi has no native config or auth root for. The handshake reports
// the option failures too, but an embedded host can open a session and prompt
// without ever calling initialize, so unvalidated options must not survive as
// far as a native process.
func (a *Agent) sessionStartConfigurationError() error {
	if optionsErr := a.optionsError(); optionsErr != nil {
		return optionsErr
	}

	if a.options.Home != "" {
		return unsupportedField(optionFieldHome)
	}

	return nil
}
