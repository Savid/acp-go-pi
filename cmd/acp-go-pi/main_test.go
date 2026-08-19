package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func disableTelemetry(t *testing.T) {
	t.Helper()

	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
}

func restoreMainSeams(t *testing.T) {
	t.Helper()

	realServe := serve
	realShutdown := shutdownOpenTelemetry
	realVersion := agentVersion
	realExit := exit
	realArgs := os.Args

	t.Cleanup(func() {
		serve = realServe
		shutdownOpenTelemetry = realShutdown
		agentVersion = realVersion
		exit = realExit
		os.Args = realArgs
	})
}

func TestSeedFileFlag(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/seed.json"
	require.NoError(t, os.WriteFile(path, []byte("seed contents"), 0o600))

	var flag seedFileFlag
	require.Empty(t, flag.String())
	require.Empty(t, (*seedFileFlag)(nil).String())
	require.ErrorContains(t, flag.Set("missing-separator"), "expected <relpath>=<hostpath>")
	require.ErrorContains(t, flag.Set(" = "+path), "expected <relpath>=<hostpath>")
	require.ErrorContains(t, flag.Set("config= "), "expected <relpath>=<hostpath>")
	require.ErrorContains(t, flag.Set("config=/missing/file"), "read seed file")
	require.NoError(t, flag.Set(" z.json = "+path))
	require.NoError(t, flag.Set("a.json="+path))
	require.Equal(t, "a.json,z.json", flag.String())
	require.Equal(t, "seed contents", flag.files["a.json"])
}

func TestRunSuccessAndFailures(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)
	stubProcessIsolationConfig(t)

	agentVersion = func() string { return "test-version" }

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	require.Equal(t, 2, run(t.Context(), []string{"-unknown"}, bytes.NewReader(nil), &stdout, &stderr))
	require.Contains(t, stderr.String(), "flag provided but not defined")

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 0, run(t.Context(), []string{"-version"}, bytes.NewReader(nil), &stdout, &stderr))
	require.Equal(t, "test-version\n", stdout.String())

	serve = func(context.Context, io.Reader, io.Writer, ...piacp.Option) error {
		return errors.New("serve failed")
	}
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, run(t.Context(), isolatedArgs(), bytes.NewReader(nil), &stdout, &stderr))
	require.Contains(t, stderr.String(), "serve failed")

	serve = func(context.Context, io.Reader, io.Writer, ...piacp.Option) error { return nil }
	shutdownOpenTelemetry = func(context.Context, func(context.Context) error) error {
		return errors.New("shutdown failed")
	}
	stderr.Reset()
	require.Equal(t, 1, run(t.Context(), isolatedArgs(), bytes.NewReader(nil), &stdout, &stderr))
	require.Contains(t, stderr.String(), "shutdown OpenTelemetry")
}

func TestRunTelemetryFailure(t *testing.T) {
	disableTelemetry(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "unknown")
	restoreMainSeams(t)
	stubProcessIsolationConfig(t)

	var stderr bytes.Buffer
	require.Equal(t, 1, run(t.Context(), isolatedArgs(), bytes.NewReader(nil), io.Discard, &stderr))
	require.Contains(t, stderr.String(), "configure OpenTelemetry")
}

func TestRunOptionsCancellationAndSignal(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)
	stubProcessIsolationConfig(t)

	seedPath := t.TempDir() + "/settings.json"
	require.NoError(t, os.WriteFile(seedPath, []byte(`{"theme":"dark"}`), 0o600))

	agentVersion = func() string { return "1.0.0" }
	shutdownOpenTelemetry = shutdownTelemetry
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...piacp.Option) error {
		options := piacp.Options{}
		for _, opt := range opts {
			opt(&options)
		}

		require.Equal(t, "1.0.0", options.AgentVersion)
		require.Equal(t, "/custom/pi", options.ExecutablePath)
		require.Equal(t, "/agent/home", options.Home)
		require.Equal(t, "/agent/scratch", options.ScratchDir)
		require.Equal(t, "/agent/auth-ledger", options.ProviderAuthRoot)
		require.Equal(t, "provider/model", options.DefaultModel)
		require.Equal(t, `{"theme":"dark"}`, options.SeedFiles["settings.json"])
		require.NotNil(t, options.Logger)
		require.NotNil(t, options.TextMapPropagator)
		require.NotNil(t, options.ProcessIsolation)
		require.Equal(t, uint32(20001), options.ProcessIsolation.UID)

		return nil
	}

	var stderr bytes.Buffer
	code := run(t.Context(), []string{
		"-process-isolation-config", testProcessIsolationConfigPath,
		"-debug",
		"-path", "/custom/pi",
		"-home", "/agent/home",
		"-scratch-dir", "/agent/scratch",
		"-provider-auth-root", "/agent/auth-ledger",
		"-model", "provider/model",
		"-seed-file", "settings.json=" + seedPath,
	}, bytes.NewReader(nil), io.Discard, &stderr)
	require.Zero(t, code)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	serve = func(context.Context, io.Reader, io.Writer, ...piacp.Option) error {
		return errors.New("ignored after cancellation")
	}
	require.Zero(t, run(ctx, isolatedArgs(), bytes.NewReader(nil), io.Discard, io.Discard))
}

func TestRunDispatchesContainmentSubcommand(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)
	restoreContainmentCommandSeams(t)

	containmentDiagnoseCommand = func(string) (containmentDiagnoseOutput, error) {
		return containmentDiagnoseOutput{Records: []containmentDiagnoseRecord{}}, nil
	}

	var stdout, stderr bytes.Buffer
	require.Zero(t, run(t.Context(), []string{"containment", "diagnose", "-scratch-dir", "/scratch"}, bytes.NewReader(nil), &stdout, &stderr))
	require.Contains(t, stdout.String(), `"records":[]`)
	require.Empty(t, stderr.String())
}

type namedSignal string

func (s namedSignal) String() string { return string(s) }
func (namedSignal) Signal()          {}

func TestSignalsAndPendingSignal(t *testing.T) {
	require.Equal(t, 1, signalCode(namedSignal("other")))

	ch := make(chan os.Signal, 1)
	require.Nil(t, pendingSignal(ch))
	ch <- namedSignal("pending")
	require.Equal(t, namedSignal("pending"), pendingSignal(ch))
}

func TestMain(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)

	agentVersion = func() string { return "main-test" }
	os.Args = []string{"acp-go-pi", "-version"}
	main()

	exitCode := 0
	exit = func(code int) { exitCode = code }
	os.Args = []string{"acp-go-pi", "-invalid"}
	main()
	require.Equal(t, 2, exitCode)
}

func TestVersion(t *testing.T) {
	realVersion := buildVersion
	t.Cleanup(func() { buildVersion = realVersion })

	buildVersion = ""
	require.Equal(t, "dev", version())
	buildVersion = "v1.2.3"
	require.Equal(t, "v1.2.3", version())
}

type testHandler struct {
	enabled bool
	err     error
	handled *int
	attrs   []slog.Attr
	groups  []string
}

func (h testHandler) Enabled(context.Context, slog.Level) bool { return h.enabled }

func (h testHandler) Handle(context.Context, slog.Record) error {
	*h.handled++

	return h.err
}

func (h testHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.attrs = append(h.attrs, attrs...)

	return h
}

func (h testHandler) WithGroup(name string) slog.Handler {
	h.groups = append(h.groups, name)

	return h
}
