package piacp

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestApplyOptionsDefaults(t *testing.T) {
	t.Parallel()

	options := applyOptions(nil)

	require.Equal(t, "acp-go-pi", options.AgentName)
	require.Equal(t, "acp-go-pi", options.AgentTitle)
	require.Equal(t, "0.1.0", options.AgentVersion)
	require.Empty(t, options.ExecutablePath)
	require.Empty(t, options.ScratchDir)
	require.Empty(t, options.DefaultModel)
	require.Nil(t, options.Env)
	require.Nil(t, options.ExtraPathDirs)
	require.Nil(t, options.SessionStore)
	require.Zero(t, options.SessionStoreLoadTimeout)
	require.Zero(t, options.TurnTimeout)
	require.Zero(t, options.ConcurrencyLimits)
	require.Nil(t, options.SeedFiles)
}

func TestApplyOptionsSetters(t *testing.T) {
	t.Parallel()

	logger := slog.Default()
	store := NewInMemorySessionStore()
	tracerProvider := tracenoop.NewTracerProvider()
	meterProvider := noop.NewMeterProvider()
	propagator := propagation.TraceContext{}
	env := map[string]string{"ANTHROPIC_API_KEY": "k"}
	extraPathDirs := []string{"/opt/shim/bin"}
	seeds := map[string]string{"settings.json": "{}"}

	options := applyOptions([]Option{
		WithLogger(logger),
		WithAgentName("name"),
		WithAgentTitle("title"),
		WithAgentVersion("9.9.9"),
		WithExecutablePath("/usr/bin/pi"),
		WithScratchDir("/srv/pi-scratch"),
		WithDefaultModel("openai/gpt-4o"),
		WithEnv(env),
		WithExtraPathDirs(extraPathDirs...),
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithTextMapPropagator(propagator),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(3 * time.Second),
		WithTurnTimeout(time.Minute),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 4, MaxConcurrentClientCalls: 2}),
		WithSeedFiles(seeds),
	})

	require.Equal(t, logger, options.Logger)
	require.Equal(t, "name", options.AgentName)
	require.Equal(t, "title", options.AgentTitle)
	require.Equal(t, "9.9.9", options.AgentVersion)
	require.Equal(t, "/usr/bin/pi", options.ExecutablePath)
	require.Equal(t, "/srv/pi-scratch", options.ScratchDir)
	require.Equal(t, "openai/gpt-4o", options.DefaultModel)
	require.Equal(t, env, options.Env)
	require.Equal(t, []string{"/opt/shim/bin"}, options.ExtraPathDirs)
	require.Equal(t, tracerProvider, options.TracerProvider)
	require.Equal(t, meterProvider, options.MeterProvider)
	require.Equal(t, propagator, options.TextMapPropagator)
	require.Same(t, store, options.SessionStore)
	require.Equal(t, 3*time.Second, options.SessionStoreLoadTimeout)
	require.Equal(t, time.Minute, options.TurnTimeout)
	require.Equal(t, ConcurrencyLimits{MaxActiveSessions: 4, MaxConcurrentClientCalls: 2}, options.ConcurrencyLimits)
	require.Equal(t, seeds, options.SeedFiles)

	// Env, extra path dirs, and seed maps are cloned, not aliased.
	env["ANTHROPIC_API_KEY"] = "mutated"
	extraPathDirs[0] = "/opt/mutated"
	seeds["settings.json"] = "mutated"
	require.Equal(t, "k", options.Env["ANTHROPIC_API_KEY"])
	require.Equal(t, "/opt/shim/bin", options.ExtraPathDirs[0])
	require.Equal(t, "{}", options.SeedFiles["settings.json"])
}

func TestProcessIsolationOptionClonesAndFailsClosed(t *testing.T) {
	base := map[string]string{"PATH": "/policy/bin", "CANARY": "base"}
	opts := applyOptions([]Option{WithProcessIsolation(ProcessIsolation{UID: 10, GID: 20, BaseEnvironment: base})})
	base["CANARY"] = "mutated"
	require.Equal(t, "base", opts.ProcessIsolation.BaseEnvironment["CANARY"])

	internal := internalProcessIsolation(opts.ProcessIsolation, false, "")
	opts.ProcessIsolation.BaseEnvironment["CANARY"] = "later"
	require.Equal(t, "base", internal.BaseEnvironment["CANARY"])
	require.Nil(t, internalProcessIsolation(nil, false, ""))

	require.Error(t, validateProcessIsolationOption(nil))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 0, GID: 1}))
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 0}))

	original := agentRuntimePlatform
	agentRuntimePlatform = "windows"
	t.Cleanup(func() { agentRuntimePlatform = original })
	require.Error(t, validateProcessIsolationOption(&ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}))
}
