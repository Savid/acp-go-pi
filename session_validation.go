package piacp

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/savid/acp-go-pi/internal/pi"
)

func validateDefaultModel(model string) error {
	if model == "" {
		return nil
	}

	_, err := pi.ParseModelRef(model)

	return err
}

func validateManagedHome(options Options) error {
	if options.hostAuthoritySupplied && options.Home != "" {
		return unsupportedField(optionFieldHome)
	}

	return nil
}

func validateRequiredAbsolutePath(field string, path string) error {
	if !filepath.IsAbs(path) {
		return unsupportedField(field)
	}

	return nil
}

func validateOptionalAbsolutePath(field string, path *string) error {
	if path == nil || *path == "" {
		return nil
	}

	if !filepath.IsAbs(*path) {
		return unsupportedField(field)
	}

	return nil
}

func validateAbsolutePaths(field string, paths []string) error {
	for index, path := range paths {
		if !filepath.IsAbs(path) {
			return unsupportedField(fmt.Sprintf("%s[%d]", field, index))
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
// start under. The handshake reports the option failures too, but an embedded
// host can open a session and prompt without ever calling initialize, so
// unvalidated options must not survive as far as a native process.
func (a *Agent) sessionStartConfigurationError() error {
	return errors.Join(a.optionsError(), a.nativeContainmentError(), a.nativeAdmissionError())
}
