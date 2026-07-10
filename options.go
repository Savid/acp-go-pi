package piacp

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Option configures the pi ACP agent.
type Option func(*Options)

// Options configures the ACP agent process and the pi RPC-mode sessions it
// starts.
type Options struct {
	// AgentName is the protocol identifier advertised during ACP initialize.
	AgentName string
	// AgentTitle is the human-readable agent name advertised during ACP initialize.
	AgentTitle string
	// AgentVersion is the agent version advertised during ACP initialize.
	AgentVersion string

	// ExecutablePath is the pi CLI executable path. If empty, PATH is searched.
	ExecutablePath string
	// Home is the parent root under which the adapter creates one isolated pi
	// agent directory per session. It never means "share this mutable native
	// home"; if empty, a temporary parent root is used.
	Home string
	// DefaultModel selects the model for newly created pi sessions when
	// non-empty, as "provider/id" (for example "openai/gpt-4o").
	DefaultModel string
	// Env is added to every launched pi process environment. pi children run
	// with a scrubbed environment, so provider API keys must travel here (or
	// per session) rather than relying on ambient variables.
	Env map[string]string

	// Logger receives structured diagnostic logs. If nil, the default logger is used.
	Logger *slog.Logger
	// TracerProvider records adapter spans. If nil, tracing is a no-op.
	TracerProvider trace.TracerProvider
	// MeterProvider records adapter metrics. If nil, metrics are no-ops.
	MeterProvider metric.MeterProvider
	// TextMapPropagator extracts ACP _meta trace context and injects pi launch
	// env. If nil, W3C trace context plus baggage propagation is used.
	TextMapPropagator propagation.TextMapPropagator

	// SessionStore mirrors pi session JSONL rows and backs store restores.
	SessionStore SessionStore
	// SessionStoreLoadTimeout bounds store load/list operations used for resume.
	SessionStoreLoadTimeout time.Duration
	// TurnTimeout bounds one pi prompt turn. Zero (the default) means no
	// deadline. On expiry the turn is aborted and fails with cause "timeout".
	TurnTimeout time.Duration
	// ConcurrencyLimits controls process-local backpressure.
	ConcurrencyLimits ConcurrencyLimits
	// SeedFiles maps paths relative to each session's isolated pi agent
	// directory to file contents written there before the pi process launches,
	// so the launched CLI reads them as its own config (e.g. settings.json,
	// which is deep-merged under the adapter's managed keys).
	SeedFiles map[string]string

	// MinimumVersion is the minimum `pi --version` accepted at startup.
	MinimumVersion string
}

// ConcurrencyLimits controls per-agent/session backpressure. Zero fields use defaults.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:      "acp-go-pi",
		AgentTitle:     "acp-go-pi",
		AgentVersion:   "0.1.0",
		MinimumVersion: pi.DefaultMinimumVersion,
	}

	for _, opt := range opts {
		opt(&options)
	}

	return options
}

// WithLogger configures structured diagnostic logging.
func WithLogger(logger *slog.Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

// WithAgentName sets the protocol identifier advertised during ACP initialize.
func WithAgentName(name string) Option {
	return func(options *Options) {
		options.AgentName = name
	}
}

// WithAgentTitle sets the human-readable agent name advertised during ACP initialize.
func WithAgentTitle(title string) Option {
	return func(options *Options) {
		options.AgentTitle = title
	}
}

// WithAgentVersion sets the agent version advertised during ACP initialize and
// used by adapter OpenTelemetry instrumentation.
func WithAgentVersion(version string) Option {
	return func(options *Options) {
		options.AgentVersion = version
	}
}

// WithExecutablePath sets the pi CLI executable path. If unset, PATH is searched.
func WithExecutablePath(path string) Option {
	return func(options *Options) {
		options.ExecutablePath = path
	}
}

// WithHome sets the parent root under which the adapter creates one isolated
// pi agent directory per session.
func WithHome(path string) Option {
	return func(options *Options) {
		options.Home = path
	}
}

// WithDefaultModel selects a pi model for newly created sessions as
// "provider/id".
func WithDefaultModel(model string) Option {
	return func(options *Options) {
		options.DefaultModel = model
	}
}

// WithEnv adds environment variables to every launched pi process. pi children
// run with a scrubbed environment, so provider API keys must travel here.
func WithEnv(env map[string]string) Option {
	return func(options *Options) {
		options.Env = cloneStringMap(env)
	}
}

// WithTracerProvider configures the OpenTelemetry tracer provider used for
// adapter spans. If unset, tracing is a no-op.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *Options) {
		options.TracerProvider = provider
	}
}

// WithMeterProvider configures the OpenTelemetry meter provider used for
// adapter metrics. If unset, metrics are no-ops.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *Options) {
		options.MeterProvider = provider
	}
}

// WithTextMapPropagator configures trace-context propagation for ACP _meta and
// pi process launch environment. If unset, W3C trace context plus baggage
// propagation is used.
func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(options *Options) {
		options.TextMapPropagator = propagator
	}
}

// WithSessionStore configures external pi session storage.
func WithSessionStore(store SessionStore) Option {
	return func(options *Options) {
		options.SessionStore = store
	}
}

// WithSessionStoreLoadTimeout bounds session store reads used during resume.
func WithSessionStoreLoadTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.SessionStoreLoadTimeout = timeout
	}
}

// WithTurnTimeout bounds one pi prompt turn. Zero (the default) disables the
// deadline. On expiry the native turn is aborted and session/prompt fails with
// a pi_turn_failed error whose cause is "timeout" (never cancelled).
func WithTurnTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.TurnTimeout = timeout
	}
}

// WithConcurrencyLimits sets process-local backpressure limits. Zero fields use defaults.
func WithConcurrencyLimits(limits ConcurrencyLimits) Option {
	return func(options *Options) {
		options.ConcurrencyLimits = limits
	}
}

// WithSeedFiles registers files written into each session's isolated pi agent
// directory before the pi process launches. Keys are paths relative to that
// directory and values are the file contents. settings.json is deep-merged
// under the adapter's managed keys; other files are written verbatim. Paths
// are confined to the agent directory: absolute paths, ".." escapes, and
// empty keys fail closed at session start.
func WithSeedFiles(files map[string]string) Option {
	return func(options *Options) {
		options.SeedFiles = cloneStringMap(files)
	}
}

// WithPiMinimumVersion overrides the minimum `pi --version` the adapter
// accepts at startup. The default is the version the adapter was verified
// against.
func WithPiMinimumVersion(version string) Option {
	return func(options *Options) {
		options.MinimumVersion = version
	}
}

const (
	piMetaKey              = "pi"
	metaOptionsKey         = "options"
	metaModelKey           = "model"
	metaEnvKey             = "env"
	metaOutputSchemaKey    = "outputSchema"
	metaThinkingLevelKey   = "thinkingLevel"
	metaPermissionKey      = "permission"
	metaRawEventKey        = "rawEvent"
	metaRawEventEnabledKey = "enabled"
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

	return map[string]any{
		piMetaKey: map[string]any{
			metaOptionsKey: values,
		},
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}

	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}
