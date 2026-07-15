package piacp

import (
	"log/slog"
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
	require.Empty(t, options.Home)
	require.Empty(t, options.ScratchDir)
	require.Empty(t, options.DefaultModel)
	require.Nil(t, options.Env)
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
	seeds := map[string]string{"settings.json": "{}"}

	options := applyOptions([]Option{
		WithLogger(logger),
		WithAgentName("name"),
		WithAgentTitle("title"),
		WithAgentVersion("9.9.9"),
		WithExecutablePath("/usr/bin/pi"),
		WithHome("/srv/pi-homes"),
		WithScratchDir("/srv/pi-scratch"),
		WithDefaultModel("openai/gpt-4o"),
		WithEnv(env),
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
	require.Equal(t, "/srv/pi-homes", options.Home)
	require.Equal(t, "/srv/pi-scratch", options.ScratchDir)
	require.Equal(t, "openai/gpt-4o", options.DefaultModel)
	require.Equal(t, env, options.Env)
	require.Equal(t, tracerProvider, options.TracerProvider)
	require.Equal(t, meterProvider, options.MeterProvider)
	require.Equal(t, propagator, options.TextMapPropagator)
	require.Same(t, store, options.SessionStore)
	require.Equal(t, 3*time.Second, options.SessionStoreLoadTimeout)
	require.Equal(t, time.Minute, options.TurnTimeout)
	require.Equal(t, ConcurrencyLimits{MaxActiveSessions: 4, MaxConcurrentClientCalls: 2}, options.ConcurrencyLimits)
	require.Equal(t, seeds, options.SeedFiles)

	// Env and seed maps are cloned, not aliased.
	env["ANTHROPIC_API_KEY"] = "mutated"
	seeds["settings.json"] = "mutated"
	require.Equal(t, "k", options.Env["ANTHROPIC_API_KEY"])
	require.Equal(t, "{}", options.SeedFiles["settings.json"])
}
