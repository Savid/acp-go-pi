package piacp

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Option configures the pi ACP agent.
type Option func(*Options)

// ProcessIsolation is the mandatory operating-system identity and complete
// base environment for every native pi process.
type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

type ProcessIsolation struct {
	UID             uint32
	GID             uint32
	BaseEnvironment map[string]string
	// IdentityLock is an optional trusted-supervisor descriptor for the
	// host-global UID lock. Linux supervisors validate it and never expose it to
	// the native pi process. Standalone embeddings should leave it nil.
	IdentityLock        ProcessIdentityLockCapability
	AuthorityDomain     ProcessIdentityLockCapability
	StandaloneOwnerID   string
	StandaloneStateRoot string
}

// RuntimeResourceKind identifies the lifecycle scope consuming a host-managed resource.
type RuntimeResourceKind string

const (
	RuntimeResourceRuntime   RuntimeResourceKind = "runtime"
	RuntimeResourceSession   RuntimeResourceKind = "session"
	RuntimeResourcePrompt    RuntimeResourceKind = "prompt"
	RuntimeResourceDiscovery RuntimeResourceKind = "discovery"
)

type RuntimeProcessKind string

const (
	RuntimeProcessProviderDescendant RuntimeProcessKind = "provider_descendant"
)

type RuntimeStartupStage string

const (
	RuntimeStartupSpawn         RuntimeStartupStage = "spawn"
	RuntimeStartupReadiness     RuntimeStartupStage = "readiness"
	RuntimeStartupConfiguration RuntimeStartupStage = "configuration"
	RuntimeStartupSession       RuntimeStartupStage = "session"
)

// RuntimeContainmentMode identifies the effective native process boundary.
type RuntimeContainmentMode string

const (
	RuntimeContainmentAuthoritative RuntimeContainmentMode = "authoritative"
	RuntimeContainmentBestEffort    RuntimeContainmentMode = "best_effort"
	RuntimeContainmentUnavailable   RuntimeContainmentMode = "unavailable"
)

// RuntimeResourceHooks lets an embedding host enforce native-root and scratch-root limits.
type RuntimeResourceHooks struct {
	AcquireNativeRoot      func(context.Context, RuntimeResourceKind) (func(), error)
	ReserveScratchRoot     func(context.Context, RuntimeResourceKind) (func(), error)
	ObserveProcess         func(context.Context, RuntimeProcessKind, int64)
	ObserveProcessSnapshot func(context.Context, RuntimeProcessKind, int)
	ObserveStartupStage    func(context.Context, RuntimeResourceKind, RuntimeStartupStage, time.Duration, error)
	ObserveContainment     func(context.Context, RuntimeContainmentMode)
}

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
	// ProcessIsolation is the mandatory child identity and complete base
	// environment. It is installed with WithProcessIsolation.
	ProcessIsolation *ProcessIsolation
	// ScratchDir is the parent directory for all ephemeral on-disk
	// materialization (per-session roots, hydration temp files, and the version
	// probe's isolated PI_CODING_AGENT_DIR/settings residence). Empty means the
	// system temp directory. The directory is created 0700 when missing.
	ScratchDir string
	// DarwinBestEffortContainment explicitly selects Darwin process-group
	// containment. It is invalid on every other platform.
	DarwinBestEffortContainment bool
	// DefaultModel selects the model for newly created pi sessions when
	// non-empty, as "provider/id" (for example "openai/gpt-4o").
	DefaultModel string
	// Env is added to every launched pi process environment. pi children run
	// with a scrubbed environment, so provider API keys must travel here (or
	// per session) rather than relying on ambient variables. Process-loader,
	// shell-loader, PATH, and Node loader keys are rejected at session start.
	Env map[string]string
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of every launched pi process. Per-session directories from
	// _meta.pi.options.extraPathDirs are prepended ahead of these.
	ExtraPathDirs []string

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
	TurnTimeout          time.Duration
	RuntimeResourceHooks RuntimeResourceHooks
	// ConcurrencyLimits controls process-local backpressure.
	ConcurrencyLimits ConcurrencyLimits
	// SeedFiles maps paths relative to each session's isolated pi agent
	// directory to file contents written there before the pi process launches,
	// so the launched CLI reads them as its own config (e.g. settings.json,
	// which is deep-merged under the adapter's managed keys).
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
	// imageLimitsSet records whether WithImageLimits supplied the struct; an
	// omitted option leaves every field at its default.
	imageLimitsSet           bool
	testOnlyNoCredential     bool
	testOnlyIdentityLockRoot string
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

	if options.ProcessIsolation != nil {
		cloned := *options.ProcessIsolation
		cloned.BaseEnvironment = cloneStringMap(options.ProcessIsolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
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

// WithProcessIsolation requires every native process and probe to run as the
// supplied non-root identity with no supplementary groups. The
// base environment is a complete replacement for the adapter environment;
// WithEnv and session environment values overlay it.
func WithProcessIsolation(isolation ProcessIsolation) Option {
	return func(options *Options) {
		cloned := isolation
		cloned.BaseEnvironment = cloneStringMap(isolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
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

// WithDarwinBestEffortContainment opts into Darwin process-group containment.
// The boundary reaps the direct child and waits for the captured original
// process group to disappear, but cannot contain descendants that leave it.
func WithDarwinBestEffortContainment() Option {
	return func(options *Options) {
		options.DarwinBestEffortContainment = true
	}
}

// WithRuntimeResourceHooks installs host-facing native-root and scratch-root admission hooks.
func WithRuntimeResourceHooks(hooks RuntimeResourceHooks) Option {
	return func(options *Options) {
		options.RuntimeResourceHooks = hooks
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
// NODE_OPTIONS, BASH_ENV, ENV, LD_*, DYLD_*, and invalid names are rejected at
// session start. PATH is an explicit overlay on the isolation policy PATH.
func WithEnv(env map[string]string) Option {
	return func(options *Options) {
		options.Env = cloneStringMap(env)
	}
}

// WithExtraPathDirs prepends absolute directories, in the order given, to the
// PATH of every launched pi process, so their executables resolve ahead of
// every inherited entry. It is the sanctioned counterpart to the rejected raw
// PATH key: a caller places its own executable in front of the child without
// being able to replace the search order wholesale. Every directory must be
// absolute and free of the platform list separator; a bad entry fails session
// start. Per-session directories are prepended ahead of these.
func WithExtraPathDirs(dirs ...string) Option {
	return func(options *Options) {
		options.ExtraPathDirs = slices.Clone(dirs)
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
