package piacp

import (
	"errors"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

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
// start under. The handshake reports the option failures too, but an embedded
// host can open a session and prompt without ever calling initialize, so
// unvalidated options must not survive as far as a native process.
func (a *Agent) sessionStartConfigurationError() error {
	if optionsErr := a.optionsError(); optionsErr != nil {
		return optionsErr
	}

	if a.options.ProviderAuthDirectHome != "" {
		return unsupportedField(optionFieldProviderAuthDirectHome)
	}

	if isolationErr := validateProcessIsolationOption(a.options.ProcessIsolation); isolationErr != nil {
		return isolationErr
	}

	if envErr := validateEnvironment(a.options.Env, optionFieldEnv); envErr != nil {
		return acp.NewInvalidParams(map[string]any{
			jsonFieldError: envErr.Error(),
			jsonFieldField: optionFieldEnv,
		})
	}

	if pathErr := validateExtraPathDirs(a.options.ExtraPathDirs, optionFieldExtraPathDirs); pathErr != nil {
		return acp.NewInvalidParams(map[string]any{
			jsonFieldError: pathErr.Error(),
			jsonFieldField: optionFieldExtraPathDirs,
		})
	}

	return nil
}

func validateProcessIsolationOption(isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	if agentRuntimePlatform != linuxPlatform {
		return errors.New("explicit process isolation is supported only on linux")
	}

	if agentRuntimePlatform == linuxPlatform {
		if err := validateStandaloneIdentityOption(
			isolation.IdentityLock != nil, isolation.AuthorityDomain != nil,
			isolation.StandaloneOwnerID, isolation.StandaloneStateRoot,
		); err != nil {
			return err
		}
	}

	return nil
}

func validateStandaloneIdentityOption(identityLock, authorityDomain bool, ownerID, stateRoot string) error {
	if identityLock != authorityDomain {
		return errors.New("process identity lock and authority domain must be provided together")
	}

	if identityLock {
		if ownerID != "" || stateRoot != "" {
			return errors.New("borrowed process identity forbids standalone owner fields")
		}

		return nil
	}

	if !validStandaloneOwnerID(ownerID) {
		return errors.New("standalone owner id must be 1..256 valid UTF-8 bytes without whitespace or control characters")
	}

	if !validStandaloneStateRootPath(stateRoot) {
		return errors.New("standalone state root must be a clean absolute path")
	}

	return nil
}

func validStandaloneStateRootPath(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) || !filepath.IsAbs(value) ||
		filepath.Clean(value) != value || value == "/" || strings.IndexByte(value, 0) >= 0 {
		return false
	}

	const authorityRoot = "/var/lib/acp-go/agent-identities"

	if value == authorityRoot || strings.HasPrefix(value, authorityRoot+string(filepath.Separator)) {
		return false
	}

	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}

	return true
}

func validStandaloneOwnerID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}

	letterOrDigit := func(value byte) bool {
		return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
	}
	if !letterOrDigit(value[0]) {
		return false
	}

	for _, character := range []byte(value[1:]) {
		if letterOrDigit(character) || strings.ContainsRune("._:@/-", rune(character)) {
			continue
		}

		return false
	}

	return true
}
