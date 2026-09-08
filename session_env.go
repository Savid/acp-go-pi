package piacp

import (
	"maps"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	valAmbiguous = "ambiguous"

	envKeyPath        = "PATH"
	envKeyNodeOptions = "NODE_OPTIONS"
	envKeyBashEnv     = "BASH_ENV"
	envKeyEnv         = "ENV"
	envKeyAgentDir    = "PI_CODING_AGENT_DIR"
)

func validEnvName(key string) bool {
	return key != "" && !strings.ContainsAny(key, "=\x00")
}

// blockedAgentEnvKey rejects the names that could hijack the pi child process.
// The adapter's own namespaces are refused under every spelling; the loader,
// node, and shell injection names are read under an exact platform spelling,
// so they compare through the platform identity. PATH is absent: the
// agent-scoped environment is where the static native base search path is
// established.
func blockedAgentEnvKey(key string) bool {
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, privateEnvPrefix) || upper == pi.EnvExtraPathDirs {
		return true
	}

	switch name := pi.EnvironmentKey(key); name {
	case envKeyNodeOptions, envKeyBashEnv, envKeyEnv:
		return true
	default:
		return strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_")
	}
}

// blockedSessionEnvKey additionally rejects PATH and the native state root.
// extraPathDirs owns the session search order; the adapter owns AgentDir.
func blockedSessionEnvKey(key string) bool {
	name := pi.EnvironmentKey(key)

	return blockedAgentEnvKey(key) || name == envKeyPath || name == envKeyAgentDir
}

// validateEnvironment checks an environment in sorted key order, so the first
// refusal is the same on every call. A key that cannot be a variable name, a
// value carrying a NUL, and a blocked name each fail as unsupported at the key
// exactly as the caller sent it. Two keys that name one variable under the
// platform identity fail as ambiguous at the later key: a Go map carries no
// order, so the value such a map would deliver is unknowable.
func validateEnvironment(env map[string]string, path string, blocked func(key string) bool) error {
	seen := make(map[string]struct{}, len(env))

	for _, key := range slices.Sorted(maps.Keys(env)) {
		if !validEnvName(key) || strings.ContainsRune(env[key], '\x00') || blocked(key) {
			return unsupportedField(path + "." + key)
		}

		identity := pi.EnvironmentKey(key)
		if _, duplicate := seen[identity]; duplicate {
			return ambiguousField(path + "." + key)
		}

		seen[identity] = struct{}{}
	}

	return nil
}

func ambiguousField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valAmbiguous,
		jsonFieldField: path,
	})
}

// ValidatePiSessionMeta reports the refusal a session/new, session/load,
// session/resume, or fork request carrying meta receives from this package's
// _meta.pi parsing, or nil when the vendor namespace is accepted.
func ValidatePiSessionMeta(meta map[string]any) error {
	_, err := piOptionsFromMeta(meta)

	return err
}
