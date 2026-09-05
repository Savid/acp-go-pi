package piacp

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	piMetaKey              = "pi"
	jsonFieldMessageID     = "messageId"
	metaOptionsKey         = "options"
	metaModelKey           = "model"
	metaEnvKey             = "env"
	metaExtraPathDirsKey   = "extraPathDirs"
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
	privateEnvPrefix  = "ACP_" + "GO_PI_INTERNAL_"
)

// PiOptions is the stable, supported pi-specific subset accepted at
// _meta.pi.options. The JSON field names below are part of this package's
// wire contract; unsupported option keys are rejected.
type PiOptions struct {
	// Model selects the pi model for this session as "provider/id".
	Model string `json:"model,omitempty"`
	// Env adds environment variables for this pi session's process. PATH is
	// rejected; use ExtraPathDirs for executable search prefixes.
	Env map[string]string `json:"env,omitempty"`
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of this session's pi process, so the first entry resolves ahead of every
	// other while the captured native base path remains last.
	ExtraPathDirs []string `json:"extraPathDirs,omitempty"`
	// OutputSchema requests JSON Schema structured output. pi has no native
	// structured-output surface, so setting it fails closed at session start.
	OutputSchema map[string]any `json:"outputSchema,omitempty"`
	// ThinkingLevel is a non-empty reasoning-level value passed unchanged to
	// pi. The advertised levels are a host menu, not a whitelist.
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

	if options.Env != nil {
		values[metaEnvKey] = cloneStringMap(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.OutputSchema != nil {
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
	options, _, err := piOptionsFromMetaWithConfigurationPresence(meta)

	return options, err
}

type sessionConfigurationPresence struct {
	Env           bool
	ExtraPathDirs bool
}

func piOptionsFromMetaWithConfigurationPresence(
	meta map[string]any,
) (PiOptions, sessionConfigurationPresence, error) {
	options := PiOptions{}
	presence := sessionConfigurationPresence{}

	// The session lifecycle extension rides no session lifecycle request: the
	// family literal is never a foreign namespace here and never a no-op.
	if refusal := refuseLifecycleMeta(meta); refusal != nil {
		return PiOptions{}, sessionConfigurationPresence{}, refusal
	}

	piMeta, ok := meta[piMetaKey].(map[string]any)
	if !ok {
		if _, exists := meta[piMetaKey]; exists {
			return PiOptions{}, sessionConfigurationPresence{}, unsupportedField("_meta." + piMetaKey)
		}
	}

	if err := validatePiLifecycleMeta(piMeta); err != nil {
		return PiOptions{}, sessionConfigurationPresence{}, err
	}

	if rawOptions, ok := piMeta[metaOptionsKey]; ok {
		parsed, err := parsePiOptions(rawOptions)
		if err != nil {
			return PiOptions{}, sessionConfigurationPresence{}, err
		}

		options = parsed

		// parsePiOptions accepts only this exact representation.
		values, _ := rawOptions.(map[string]any)

		_, presence.Env = values[metaEnvKey]
		_, presence.ExtraPathDirs = values[metaExtraPathDirsKey]
	}

	return options, presence, nil
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
		case metaExtraPathDirsKey:
			dirs, err := stringSliceOption(item, metaOptionPath(key))
			if err != nil {
				return PiOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaOutputSchemaKey:
			schema, ok := item.(map[string]any)
			if !ok {
				return PiOptions{}, unsupportedField(metaOptionPath(key))
			}

			options.OutputSchema = cloneAnyMap(schema)
		case metaThinkingLevelKey:
			// Absence and presence-with-nothing are different requests, and
			// only this arm can tell them apart: an absent key states no
			// selection and the session takes whatever pi starts on, while a
			// key that arrived states one and names none. Empty is the empty
			// string exactly — whitespace names a level this adapter does not
			// judge, so it travels and the level pi reports back is what the
			// session advertises.
			level, ok := item.(string)
			if !ok || level == "" {
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
	if options.OutputSchema != nil {
		return PiOptions{}, unsupportedField(metaOptionPath(metaOutputSchemaKey))
	}

	if options.Model != "" {
		if _, err := pi.ParseModelRef(options.Model); err != nil {
			return PiOptions{}, unsupportedField(metaOptionPath(metaModelKey))
		}
	}

	if options.Permission != "" && options.Permission != pi.PermissionModeAsk && options.Permission != pi.PermissionModeAllow {
		return PiOptions{}, unsupportedField(metaOptionPath(metaPermissionKey))
	}

	if err := validateEnvironment(options.Env, metaOptionPath(metaEnvKey), blockedSessionEnvKey); err != nil {
		return PiOptions{}, err
	}

	if err := validateExtraPathDirs(options.ExtraPathDirs, metaOptionPath(metaExtraPathDirsKey)); err != nil {
		return PiOptions{}, err
	}

	return options, nil
}

func validateEnvironment(env map[string]string, path string, blocked func(key string) bool) error {
	return validateEnvironmentForPlatform(env, path, blocked, runtime.GOOS == "windows")
}

func validateEnvironmentForPlatform(
	env map[string]string,
	path string,
	blocked func(key string) bool,
	caseInsensitive bool,
) error {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if !validEnvName(key) || blocked(key) {
			return unsupportedField(path + "." + key)
		}

		if caseInsensitive {
			canonical := strings.ToUpper(key)
			if _, ok := seen[canonical]; ok {
				return unsupportedField(path + "." + key)
			}

			seen[canonical] = struct{}{}
		}
	}

	return nil
}

// validateExtraPathDirs rejects every entry that could not be prepended to the
// child's PATH as exactly one search directory. A relative entry resolves
// against a working directory this adapter does not own, and an embedded list
// separator would splice in directories the caller never named.
func validateExtraPathDirs(dirs []string, path string) error {
	for index, dir := range dirs {
		if !filepath.IsAbs(dir) || strings.ContainsRune(dir, os.PathListSeparator) {
			return unsupportedField(fmt.Sprintf("%s[%d]", path, index))
		}
	}

	return nil
}

func unsupportedField(path string) *acp.RequestError {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: validationUnsupported,
		jsonFieldField: path,
	})
}

// missingField refuses a reserved key the contract requires and the caller left
// out. It is a distinct verdict from unsupportedField and the two are never
// collapsed: unsupported names a value that is present and refused, missing
// names one the request had to carry. A host that reads missing adds the key;
// a host that reads unsupported on the same bare path stops sending it.
func missingField(path string) *acp.RequestError {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: validationMissing,
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

func stringSliceOption(value any, path string) ([]string, error) {
	switch typed := value.(type) {
	case []string:
		return slices.Clone(typed), nil
	case []any:
		result := make([]string, 0, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, unsupportedField(fmt.Sprintf("%s[%d]", path, index))
			}

			result = append(result, text)
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

// blockedAgentEnvKey rejects env names that could hijack the pi child process
// (loader/preload and node/shell injection vectors). PATH is absent: the
// agent-scoped environment is where the static native base search path is
// established.
func blockedAgentEnvKey(key string) bool {
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, privateEnvPrefix) {
		return true
	}

	switch upper {
	case envKeyNodeOptions, envKeyBashEnv, envKeyEnv, pi.EnvExtraPathDirs:
		return true
	default:
		return strings.HasPrefix(upper, "LD_") || strings.HasPrefix(upper, "DYLD_")
	}
}

// blockedSessionEnvKey additionally rejects PATH. The ordered extraPathDirs
// option is the only session-scoped PATH authority; a second session owner
// would make the effective search order depend on merge order.
func blockedSessionEnvKey(key string) bool {
	return blockedAgentEnvKey(key) || strings.EqualFold(key, envKeyPath)
}

func sessionAdditionalDirectories(primary []string) []string {
	return append([]string(nil), primary...)
}
