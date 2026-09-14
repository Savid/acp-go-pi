package piacp

import (
	"errors"
	"fmt"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaOutputSchemaKey  = "outputSchema"
	metaThinkingLevelKey = "thinkingLevel"
	metaPermissionKey    = "permission"
	metaAutoRetryKey     = "autoRetry"
	metaEnabledKey       = "enabled"
)

// PiOptions is the per-session options struct carried at _meta.pi.options.
type PiOptions struct {
	// Model selects the pi model for this session as "provider/id".
	Model string `json:"model,omitempty"`
	// Env overlays the session's pi process environment.
	Env map[string]string `json:"env,omitempty"`
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of this session's pi process.
	ExtraPathDirs []string `json:"extraPathDirs,omitempty"`
	// OutputSchema requests structured output. pi has no native surface for
	// it, so a session carrying it fails at session start.
	OutputSchema map[string]any `json:"outputSchema,omitempty"`
	// ThinkingLevel is a reasoning-level value passed unchanged to pi.
	ThinkingLevel string `json:"thinkingLevel,omitempty"`
	// Permission selects the permission mode: "ask" (the default) raises a
	// permission request per tool call, "allow" auto-allows every tool call.
	Permission string `json:"permission,omitempty"`
	// AutoRetry opts the session in to pi's native retry of transient
	// provider errors.
	AutoRetry    bool `json:"autoRetry,omitempty"`
	autoRetrySet bool
}

// PiOption configures PiOptions values.
type PiOption func(*PiOptions)

// NewPiOptions constructs PiOptions from functional options.
func NewPiOptions(opts ...PiOption) PiOptions {
	options := PiOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return options.clone()
}

// WithPiModel configures the session model as "provider/id".
func WithPiModel(model string) PiOption {
	return func(options *PiOptions) { options.Model = model }
}

// WithPiEnv configures the session environment overlay.
func WithPiEnv(env map[string]string) PiOption {
	cloned := cloneStringMap(env)

	return func(options *PiOptions) { options.Env = cloneStringMap(cloned) }
}

// WithPiExtraPathDirs configures the directories prepended to the session PATH.
func WithPiExtraPathDirs(dirs ...string) PiOption {
	cloned := slices.Clone(dirs)

	return func(options *PiOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
}

// WithPiOutputSchema configures structured output, which pi refuses at
// session start.
func WithPiOutputSchema(schema map[string]any) PiOption {
	cloned := cloneAnyMap(schema)

	return func(options *PiOptions) { options.OutputSchema = cloneAnyMap(cloned) }
}

// WithPiThinkingLevel configures the reasoning level passed to pi.
func WithPiThinkingLevel(level string) PiOption {
	return func(options *PiOptions) { options.ThinkingLevel = level }
}

// WithPiPermission configures the permission mode: "ask" or "allow".
func WithPiPermission(mode string) PiOption {
	return func(options *PiOptions) { options.Permission = mode }
}

// WithPiAutoRetry opts the session in to pi's native automatic retry.
func WithPiAutoRetry(enabled bool) PiOption {
	return func(options *PiOptions) { options.AutoRetry = enabled; options.autoRetrySet = true }
}

// Meta returns exactly {"pi": {"options": {...}}}, including an explicit false autoRetry.
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

	if options.AutoRetry || options.autoRetrySet {
		values[metaAutoRetryKey] = options.AutoRetry
	}

	return map[string]any{vendor: map[string]any{metaOptionsKey: values}}
}

func (options PiOptions) clone() PiOptions {
	cloned := options
	cloned.Env = cloneStringMap(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)
	cloned.OutputSchema = cloneAnyMap(options.OutputSchema)

	return cloned
}

// ValidatePiSessionMeta runs the owned-namespace parsing of a session
// lifecycle request's _meta without an Agent and returns the same refusal.
func ValidatePiSessionMeta(meta map[string]any) error {
	_, err := parseSessionMeta(meta)
	if err != nil {
		return err
	}

	return nil
}

// sessionMeta is what one session lifecycle request's _meta.pi carried.
type sessionMeta struct {
	options   PiOptions
	rawEvents bool
	// present records which carrier fields the request named, so a load or
	// resume inherits the stored value only for fields it left out.
	presentEnv           bool
	presentExtraPathDirs bool
}

// parseSessionMeta validates the owned _meta.pi namespace of one session
// lifecycle request. Unknown own-namespace keys fail closed; foreign
// namespaces are ignored; the lifecycle literal is refused by name.
func parseSessionMeta(meta map[string]any) (sessionMeta, *acp.RequestError) {
	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return sessionMeta{}, invalidParam(refusal)
	}

	raw, exists := meta[vendor]
	if !exists {
		return sessionMeta{}, nil
	}

	piMeta, ok := raw.(map[string]any)
	if !ok {
		return sessionMeta{}, wire.Unsupported("_meta." + vendor)
	}

	parsed := sessionMeta{}

	for key := range piMeta {
		switch key {
		case metaOptionsKey, metaRawEventKey:
		default:
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + key)
		}
	}

	if rawEvent, ok := piMeta[metaRawEventKey]; ok {
		values, ok := rawEvent.(map[string]any)
		if !ok {
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey)
		}

		for key, item := range values {
			enabled, ok := item.(bool)
			if key != metaEnabledKey || !ok {
				return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey + "." + key)
			}

			parsed.rawEvents = enabled
		}
	}

	rawOptions, hasOptions := piMeta[metaOptionsKey]
	if !hasOptions {
		return parsed, nil
	}

	values, isObject := rawOptions.(map[string]any)
	if !isObject {
		return sessionMeta{}, wire.Unsupported(metaOptionPath(""))
	}

	options, err := parsePiOptions(values)
	if err != nil {
		return sessionMeta{}, err
	}

	parsed.options = options
	_, parsed.presentEnv = values[metaEnvKey]
	_, parsed.presentExtraPathDirs = values[metaExtraPathDirsKey]

	return parsed, nil
}

func parsePiOptions(values map[string]any) (PiOptions, *acp.RequestError) {
	options := PiOptions{}

	for key, item := range values {
		switch key {
		case metaModelKey:
			model, ok := item.(string)
			if !ok {
				return PiOptions{}, wire.Unsupported(metaOptionPath(key))
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
				return PiOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.OutputSchema = cloneAnyMap(schema)
		case metaThinkingLevelKey:
			level, ok := item.(string)
			if !ok || level == "" {
				return PiOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.ThinkingLevel = level
		case metaPermissionKey:
			permission, ok := item.(string)
			if !ok {
				return PiOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.Permission = permission
		case metaAutoRetryKey:
			enabled, ok := item.(bool)
			if !ok {
				return PiOptions{}, wire.Unsupported(metaOptionPath(key))
			}

			options.AutoRetry = enabled
			options.autoRetrySet = true
		default:
			return PiOptions{}, wire.Unsupported(metaOptionPath(key))
		}
	}

	return options, validatePiOptions(options)
}

func validatePiOptions(options PiOptions) *acp.RequestError {
	if options.OutputSchema != nil {
		return wire.Unsupported(metaOptionPath(metaOutputSchemaKey))
	}

	if options.Model != "" {
		if _, err := pi.ParseModelRef(options.Model); err != nil {
			return wire.Unsupported(metaOptionPath(metaModelKey))
		}
	}

	if options.Permission != "" && options.Permission != pi.PermissionModeAsk && options.Permission != pi.PermissionModeAllow {
		return wire.Unsupported(metaOptionPath(metaPermissionKey))
	}

	if err := process.ValidateNames(options.Env); err != nil {
		var nameErr *process.NameError
		if errors.As(err, &nameErr) {
			return wire.Unsupported(metaOptionPath(metaEnvKey) + "." + nameErr.Key)
		}

		return wire.Unsupported(metaOptionPath(metaEnvKey))
	}

	if err := process.ValidateExtraPathDirs(options.ExtraPathDirs); err != nil {
		var dirErr *process.PathDirError
		if errors.As(err, &dirErr) {
			return wire.Unsupported(fmt.Sprintf("%s[%d]", metaOptionPath(metaExtraPathDirsKey), dirErr.Index))
		}

		return wire.Unsupported(metaOptionPath(metaExtraPathDirsKey))
	}

	return nil
}

func metaOptionPath(key string) string {
	path := "_meta." + vendor + "." + metaOptionsKey
	if key == "" {
		return path
	}

	return path + "." + key
}

func stringMapOption(value any, path string) (map[string]string, *acp.RequestError) {
	switch typed := value.(type) {
	case map[string]string:
		return cloneStringMap(typed), nil
	case map[string]any:
		result := make(map[string]string, len(typed))
		for key, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, wire.Unsupported(path + "." + key)
			}

			result[key] = text
		}

		return result, nil
	default:
		return nil, wire.Unsupported(path)
	}
}

func stringSliceOption(value any, path string) ([]string, *acp.RequestError) {
	switch typed := value.(type) {
	case []string:
		return slices.Clone(typed), nil
	case []any:
		result := make([]string, 0, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, wire.Unsupported(fmt.Sprintf("%s[%d]", path, index))
			}

			result = append(result, text)
		}

		return result, nil
	default:
		return nil, wire.Unsupported(path)
	}
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}

	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = cloneAny(value)
	}

	return cloned
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneAny(item)
		}

		return cloned
	case []string:
		return slices.Clone(typed)
	default:
		return typed
	}
}

func mergeAnyMap(base map[string]any, overlay map[string]any) map[string]any {
	result := cloneAnyMap(base)
	if result == nil {
		result = map[string]any{}
	}

	for key, value := range overlay {
		if valueMap, ok := value.(map[string]any); ok {
			if existing, ok := result[key].(map[string]any); ok {
				result[key] = mergeAnyMap(existing, valueMap)

				continue
			}
		}

		result[key] = cloneAny(value)
	}

	return result
}
