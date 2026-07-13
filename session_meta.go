package piacp

import (
	"fmt"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	piMetaKey              = "pi"
	metaOptionsKey         = "options"
	metaModelKey           = "model"
	metaEnvKey             = "env"
	metaOutputSchemaKey    = "outputSchema"
	metaThinkingLevelKey   = "thinkingLevel"
	metaPermissionKey      = "permission"
	metaAutoRetryKey       = "autoRetry"
	metaRawEventKey        = "rawEvent"
	metaRawEventEnabledKey = "enabled"

	envKeyPath        = "PATH"
	envKeyNodeOptions = "NODE_OPTIONS"
	envKeyBashEnv     = "BASH_ENV"
	envKeyEnv         = "ENV"
)

// PiOptions is the stable, supported pi-specific subset accepted at
// _meta.pi.options. The JSON field names below are part of this package's
// wire contract; unsupported option keys are rejected.
type PiOptions struct {
	// Model selects the pi model for this session as "provider/id".
	Model string `json:"model,omitempty"`
	// Env adds environment variables for this pi session's process.
	Env map[string]string `json:"env,omitempty"`
	// OutputSchema requests JSON Schema structured output. pi has no native
	// structured-output surface, so setting it fails closed at session start.
	OutputSchema map[string]any `json:"outputSchema,omitempty"`
	// ThinkingLevel selects the pi reasoning level for this session:
	// off, minimal, low, medium, high, xhigh, or max.
	ThinkingLevel string `json:"thinkingLevel,omitempty"`
	// Permission selects the adapter permission mode for this session:
	// "ask" (deny-by-default dialog, the default) or "allow" (auto-allow).
	Permission string `json:"permission,omitempty"`
	// AutoRetry opts this session in to pi's native automatic retry of
	// transient provider errors (5xx, timeouts). Off by default so a native
	// failure surfaces once, immediately, with the real cause; when enabled,
	// the final error after exhausted retries still carries the last cause.
	AutoRetry bool `json:"autoRetry,omitempty"`
}

// Meta returns an ACP _meta object for the supported pi-specific options.
func (options PiOptions) Meta() map[string]any {
	values := map[string]any{}

	if options.Model != "" {
		values[metaModelKey] = options.Model
	}

	if len(options.Env) > 0 {
		values[metaEnvKey] = cloneStringMap(options.Env)
	}

	if len(options.OutputSchema) > 0 {
		values[metaOutputSchemaKey] = cloneAnyMap(options.OutputSchema)
	}

	if options.ThinkingLevel != "" {
		values[metaThinkingLevelKey] = options.ThinkingLevel
	}

	if options.Permission != "" {
		values[metaPermissionKey] = options.Permission
	}

	if options.AutoRetry {
		values[metaAutoRetryKey] = true
	}

	return map[string]any{
		piMetaKey: map[string]any{
			metaOptionsKey: values,
		},
	}
}

// piOptionsFromMeta parses and validates the owned _meta.pi namespace of one
// session lifecycle request. Unknown own-namespace keys fail closed; foreign
// namespaces are ignored.
func piOptionsFromMeta(meta map[string]any) (PiOptions, error) {
	options := PiOptions{}

	piMeta, ok := meta[piMetaKey].(map[string]any)
	if !ok {
		if _, exists := meta[piMetaKey]; exists {
			return PiOptions{}, unsupportedField("_meta." + piMetaKey)
		}
	}

	if err := validatePiLifecycleMeta(piMeta); err != nil {
		return PiOptions{}, err
	}

	if rawOptions, ok := piMeta[metaOptionsKey]; ok {
		parsed, err := parsePiOptions(rawOptions)
		if err != nil {
			return PiOptions{}, err
		}

		options = parsed
	}

	return options, nil
}

func validatePiLifecycleMeta(piMeta map[string]any) error {
	if piMeta == nil {
		return nil
	}

	for key := range piMeta {
		switch key {
		case metaOptionsKey, metaRawEventKey:
		default:
			return unsupportedField("_meta." + piMetaKey + "." + key)
		}
	}

	if rawEvent, ok := piMeta[metaRawEventKey]; ok {
		if err := validateRawEventMeta(rawEvent); err != nil {
			return err
		}
	}

	return nil
}

func validateRawEventMeta(value any) error {
	raw, ok := value.(map[string]any)
	if !ok {
		return unsupportedField("_meta." + piMetaKey + "." + metaRawEventKey)
	}

	for key, item := range raw {
		switch key {
		case metaRawEventEnabledKey:
			if _, ok := item.(bool); !ok {
				return unsupportedField("_meta." + piMetaKey + "." + metaRawEventKey + "." + key)
			}
		default:
			return unsupportedField("_meta." + piMetaKey + "." + metaRawEventKey + "." + key)
		}
	}

	return nil
}

func parsePiOptions(value any) (PiOptions, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return PiOptions{}, unsupportedField("_meta." + piMetaKey + "." + metaOptionsKey)
	}

	options := PiOptions{}

	for key, item := range raw {
		switch key {
		case metaModelKey:
			model, ok := item.(string)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.Model = model
		case metaEnvKey:
			env, err := stringMapOption(item, metaOptionPath(key))
			if err != nil {
				return PiOptions{}, err
			}

			options.Env = env
		case metaOutputSchemaKey:
			schema, ok := item.(map[string]any)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.OutputSchema = cloneAnyMap(schema)
		case metaThinkingLevelKey:
			level, ok := item.(string)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.ThinkingLevel = level
		case metaPermissionKey:
			permission, ok := item.(string)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.Permission = permission
		case metaAutoRetryKey:
			enabled, ok := item.(bool)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.AutoRetry = enabled
		default:
			return PiOptions{}, unsupportedField(metaOptionPath(key))
		}
	}

	return validatePiOptions(options)
}

func validatePiOptions(options PiOptions) (PiOptions, error) {
	if len(options.OutputSchema) > 0 {
		return PiOptions{}, unsupportedField(metaOptionPath(metaOutputSchemaKey))
	}

	if options.Model != "" {
		if _, err := pi.ParseModelRef(options.Model); err != nil {
			return PiOptions{}, fmt.Errorf("%s must be \"provider/id\"", metaOptionPath(metaModelKey))
		}
	}

	if options.ThinkingLevel != "" && !pi.IsValidThinkingLevel(options.ThinkingLevel) {
		return PiOptions{}, fmt.Errorf(
			"%s must be one of %s",
			metaOptionPath(metaThinkingLevelKey),
			strings.Join(pi.ThinkingLevels(), ", "),
		)
	}

	if options.Permission != "" && options.Permission != pi.PermissionModeAsk && options.Permission != pi.PermissionModeAllow {
		return PiOptions{}, fmt.Errorf("%s must be %q or %q", metaOptionPath(metaPermissionKey), pi.PermissionModeAsk, pi.PermissionModeAllow)
	}

	for key := range options.Env {
		if !validEnvName(key) {
			return PiOptions{}, fmt.Errorf("%s.%s is not a valid environment variable name", metaOptionPath(metaEnvKey), key)
		}

		if blockedEnvKey(key) {
			return PiOptions{}, fmt.Errorf("%s.%s is not allowed", metaOptionPath(metaEnvKey), key)
		}
	}

	return options, nil
}

func unsupportedField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: validationUnsupported,
		jsonFieldField: path,
	})
}

func metaOptionPath(key string) string {
	return "_meta." + piMetaKey + "." + metaOptionsKey + "." + key
}

func stringMapOption(value any, path string) (map[string]string, error) {
	switch typed := value.(type) {
	case map[string]string:
		return cloneStringMap(typed), nil
	case map[string]any:
		result := make(map[string]string, len(typed))
		for key, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, unsupportedField(path + "." + key)
			}

			result[key] = text
		}

		return result, nil
	default:
		return nil, unsupportedField(path)
	}
}

func validEnvName(name string) bool {
	if name == "" {
		return false
	}

	for index, r := range name {
		switch {
		case r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
		case r >= '0' && r <= '9':
			if index == 0 {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// blockedEnvKey rejects env names that could hijack the pi child process
// (loader/preload and node/shell injection vectors).
func blockedEnvKey(key string) bool {
	upper := strings.ToUpper(key)
	switch upper {
	case envKeyPath, envKeyNodeOptions, envKeyBashEnv, envKeyEnv:
		return true
	default:
		return strings.HasPrefix(upper, "LD_") || strings.HasPrefix(upper, "DYLD_")
	}
}

func sessionAdditionalDirectories(primary []string) []string {
	return append([]string(nil), primary...)
}
