package piacp

import (
	"maps"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
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
	cloned := maps.Clone(env)

	return func(options *PiOptions) { options.Env = maps.Clone(cloned) }
}

// WithPiExtraPathDirs configures the directories prepended to the session PATH.
func WithPiExtraPathDirs(dirs ...string) PiOption {
	cloned := slices.Clone(dirs)

	return func(options *PiOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
}

// WithPiOutputSchema configures structured output, which pi refuses at
// session start.
func WithPiOutputSchema(schema map[string]any) PiOption {
	cloned := wire.CloneMap(schema)

	return func(options *PiOptions) { options.OutputSchema = wire.CloneMap(cloned) }
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
		values[metaEnvKey] = maps.Clone(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	if options.OutputSchema != nil {
		values[metaOutputSchemaKey] = wire.CloneMap(options.OutputSchema)
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
	cloned.Env = maps.Clone(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)
	cloned.OutputSchema = wire.CloneMap(options.OutputSchema)

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
		return sessionMeta{}, wire.ParamRefusal(refusal)
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
		return sessionMeta{}, wire.Unsupported(wire.MetaOptionPath(vendor, ""))
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
				return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Model = model
		case metaEnvKey:
			env, err := wire.StringMapOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return PiOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := wire.StringSliceOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return PiOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaOutputSchemaKey:
			schema, ok := item.(map[string]any)
			if !ok {
				return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.OutputSchema = wire.CloneMap(schema)
		case metaThinkingLevelKey:
			level, ok := item.(string)
			if !ok || level == "" {
				return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.ThinkingLevel = level
		case metaPermissionKey:
			permission, ok := item.(string)
			if !ok {
				return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Permission = permission
		case metaAutoRetryKey:
			enabled, ok := item.(bool)
			if !ok {
				return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.AutoRetry = enabled
			options.autoRetrySet = true
		default:
			return PiOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
		}
	}

	return options, validatePiOptions(options)
}

func validatePiOptions(options PiOptions) *acp.RequestError {
	if options.OutputSchema != nil {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaOutputSchemaKey))
	}

	if options.Model != "" {
		if _, err := pi.ParseModelRef(options.Model); err != nil {
			return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
		}
	}

	if options.Permission != "" && options.Permission != pi.PermissionModeAsk && options.Permission != pi.PermissionModeAllow {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaPermissionKey))
	}

	return wire.ValidateSessionEnvironment(options.Env, options.ExtraPathDirs, wire.MetaOptionPath(vendor, ""))
}
