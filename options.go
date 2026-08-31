package piacp

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
	ExecutablePath        string
	HostAuthority         HostAuthority
	hostAuthoritySupplied bool
	// Home is the durable per-instance PI_CODING_AGENT_DIR shared by ordinary-mode
	// sessions. Managed mode rejects it and always builds isolated generation
	// residences. Provider auth is advertised only with both Home and
	// ProviderAuthRoot configured in ordinary mode.
	Home string
	// ScratchDir is the parent directory for all ephemeral on-disk
	// materialization (per-session roots, hydration temp files, and the version
	// probe's isolated PI_CODING_AGENT_DIR/settings residence). Empty means the
	// system temp directory. The directory is created 0700 when missing.
	ScratchDir string
	// DefaultModel selects the model for newly created pi sessions when
	// non-empty, as "provider/id" (for example "openai/gpt-4o").
	DefaultModel string
	// Env is the static agent-scoped addition to every launched pi process
	// environment. pi children run with a scrubbed environment, so provider API
	// keys must travel here (or per session) rather than relying on ambient
	// variables. PATH here is the static native base search path every session
	// resolves against. Process-loader, shell-loader, and Node loader keys are
	// rejected at session start.
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
	// SeedFiles maps paths relative to the pi agent directory a session
	// launches against to file contents written there before the pi process
	// starts, so the launched CLI reads them as its own config. Every file is
	// written verbatim, settings.json included; that directory is the durable
	// Home when one is configured, and a per-session ephemeral directory
	// otherwise. Set via WithSeedFiles.
	SeedFiles map[string]string
	// ImageLimits bounds decoded image bytes on prompt input and emitted
	// output. Set via WithImageLimits; every field defaults to 6 MiB when the
	// option is omitted, and an explicit zero in a supplied struct disables
	// that policy limit.
	ImageLimits ImageLimits
	// InputHandoffRoot is the absolute directory under which handoff-form
	// prompt images are read. Empty (the default) rejects the handoff form.
	// The adapter only reads under it and never writes, moves, or removes
	// anything there.
	InputHandoffRoot string
	// ProviderAuthRoot is the absolute host-owned directory containing Pi's
	// values-free provider-auth ledger. Empty leaves every _pi/auth/* leg
	// unadvertised.
	ProviderAuthRoot string
	// imageLimitsSet records whether WithImageLimits supplied the struct; an
	// omitted option leaves every field at its default.
	imageLimitsSet bool
}

// ConcurrencyLimits controls per-agent/session backpressure. Zero fields use defaults.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:    "acp-go-pi",
		AgentTitle:   "acp-go-pi",
		AgentVersion: "0.1.0",
	}

	for _, opt := range opts {
		opt(&options)
	}

	if !options.imageLimitsSet {
		options.ImageLimits = defaultImageLimits()
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

func WithHostAuthority(authority HostAuthority) Option {
	return func(options *Options) {
		options.hostAuthoritySupplied = true
		options.HostAuthority = authority
	}
}

// WithHome sets the durable per-instance PI_CODING_AGENT_DIR shared by all
// ordinary-mode sessions. Managed mode rejects it. It is required for provider
// auth so Pi's native cross-process credential lock and credential residence
// survive session teardown.
func WithHome(path string) Option {
	return func(options *Options) {
		options.Home = path
	}
}

// WithProviderAuthRoot sets the durable root for the values-free provider-auth
// ledger. Omitting it leaves every _pi/auth/* leg unadvertised.
func WithProviderAuthRoot(path string) Option {
	return func(options *Options) {
		options.ProviderAuthRoot = path
	}
}

// WithScratchDir sets the parent directory for all ephemeral on-disk
// materialization (per-session roots, hydration temp files, and the version
// probe's isolated PI_CODING_AGENT_DIR/settings residence). Empty means the
// system temp directory. The directory is created 0700 when missing.
func WithScratchDir(dir string) Option {
	return func(options *Options) {
		options.ScratchDir = dir
	}
}

// WithInputHandoffRoot sets the absolute directory under which handoff-form
// prompt images are read. Omitting the option rejects the handoff form, so a
// host that expects it can tell from the absence of the handoff capability
// advertisement that its option never reached this adapter. The directory is
// read-only to the adapter: handoff files stay owned by the host, which may
// remove them as soon as session/prompt returns.
func WithInputHandoffRoot(dir string) Option {
	return func(options *Options) {
		options.InputHandoffRoot = dir
	}
}

// WithDefaultModel selects a pi model for newly created sessions as
// "provider/id".
func WithDefaultModel(model string) Option {
	return func(options *Options) {
		options.DefaultModel = model
	}
}

// WithEnv sets the static agent-scoped environment every launched pi process
// runs with. pi children run with a scrubbed environment, so provider API keys
// must travel here. A PATH entry here is the static native base search path
// used for executable lookup, version probing, and native launch; per-session
// directories from WithPiExtraPathDirs are prepended ahead of it.
// NODE_OPTIONS, BASH_ENV, ENV, LD_*, DYLD_*, and invalid names are rejected at
// session start.
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

// WithSeedFiles registers files written into the pi agent directory a session
// launches against, before the pi process starts. Keys are paths relative to
// that directory and values are the file contents.
//
// Every file, settings.json included, is written verbatim: the adapter authors
// no settings.json of its own and merges nothing into a seeded one. The only
// special handling for settings.json is a parse check that fails the seed
// closed, because pi records a load error for unparsable settings and then
// silently runs its own defaults.
//
// The directory written to is per session unless ordinary mode has a durable
// Home. With WithHome in ordinary mode, every session launches against that one
// shared directory and the seed is written there, so a seeded file is
// agent-scoped operator configuration rather than per-session state.
//
// Paths are confined to the agent directory: absolute paths, ".." escapes, and
// empty keys fail closed at session start.
func WithSeedFiles(files map[string]string) Option {
	return func(options *Options) {
		options.SeedFiles = cloneStringMap(files)
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
