package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestConfigureTelemetry(t *testing.T) {
	disableTelemetry(t)

	base := slog.New(slog.DiscardHandler)
	config, err := configureTelemetry(t.Context(), base, "1.2.3")
	require.NoError(t, err)
	require.Same(t, base, config.logger)
	require.Len(t, config.options, 1)
	require.NoError(t, config.shutdown(t.Context()))

	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	t.Setenv("OTEL_LOGS_EXPORTER", "console")
	config, err = configureTelemetry(t.Context(), base, "1.2.3")
	require.NoError(t, err)
	require.NotSame(t, base, config.logger)
	require.Len(t, config.options, 3)
	require.NoError(t, config.shutdown(t.Context()))
}

func TestConfigureTelemetryErrors(t *testing.T) {
	tests := []struct {
		name    string
		environ map[string]string
	}{
		{
			name: "resource",
			environ: map[string]string{
				"OTEL_RESOURCE_ATTRIBUTES": "missing-value",
			},
		},
		{
			name: "trace exporter",
			environ: map[string]string{
				"OTEL_TRACES_EXPORTER": "unknown",
			},
		},
		{
			name: "metric exporter",
			environ: map[string]string{
				"OTEL_METRICS_EXPORTER": "unknown",
			},
		},
		{
			name: "log exporter",
			environ: map[string]string{
				"OTEL_LOGS_EXPORTER": "unknown",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			disableTelemetry(t)
			for key, value := range test.environ {
				t.Setenv(key, value)
			}

			_, err := configureTelemetry(t.Context(), slog.New(slog.DiscardHandler), "dev")
			require.Error(t, err)
		})
	}
}

func TestTelemetryResource(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "region=test")
	t.Setenv("OTEL_SERVICE_NAME", "")

	res, err := telemetryResource(t.Context(), "1.0.0")
	require.NoError(t, err)
	attrs := res.Set()
	serviceName, ok := attrs.Value(attribute.Key("service.name"))
	require.True(t, ok)
	require.Equal(t, defaultServiceName, serviceName.AsString())
	serviceVersion, ok := attrs.Value(attribute.Key("service.version"))
	require.True(t, ok)
	require.Equal(t, "1.0.0", serviceVersion.AsString())

	t.Setenv("OTEL_SERVICE_NAME", "custom-service")
	res, err = telemetryResource(t.Context(), "2.0.0")
	require.NoError(t, err)
	serviceName, ok = res.Set().Value(attribute.Key("service.name"))
	require.True(t, ok)
	require.Equal(t, "custom-service", serviceName.AsString())
}

func TestTelemetrySignalEnabled(t *testing.T) {
	tests := []struct {
		name        string
		exporter    string
		endpoint    string
		genericOTLP bool
		generic     map[string]string
		want        bool
	}{
		{name: "explicit exporter", exporter: "console", want: true},
		{name: "disabled exporter", exporter: "none"},
		{name: "signal endpoint", endpoint: "http://localhost", want: true},
		{name: "logs stay disabled without signal config"},
		{name: "generic endpoint", genericOTLP: true, generic: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost"}, want: true},
		{name: "generic protocol", genericOTLP: true, generic: map[string]string{"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, want: true},
		{name: "generic headers", genericOTLP: true, generic: map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "key=value"}, want: true},
		{name: "no config", genericOTLP: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TEST_EXPORTER", test.exporter)
			t.Setenv("TEST_ENDPOINT", test.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
			t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
			for key, value := range test.generic {
				t.Setenv(key, value)
			}

			require.Equal(t, test.want, telemetrySignalEnabled("TEST_EXPORTER", "TEST_ENDPOINT", test.genericOTLP))
		})
	}
}

func TestShutdownTelemetry(t *testing.T) {
	require.NoError(t, shutdownTelemetry(t.Context(), nil))

	called := false
	wantErr := errors.New("shutdown")
	err := shutdownTelemetry(t.Context(), func(ctx context.Context) error {
		called = true
		_, hasDeadline := ctx.Deadline()
		require.True(t, hasDeadline)

		return wantErr
	})
	require.True(t, called)
	require.ErrorIs(t, err, wantErr)
}

func TestJoinedSlogHandler(t *testing.T) {
	firstCalls := 0
	secondCalls := 0
	firstErr := errors.New("first")
	first := testHandler{enabled: true, err: firstErr, handled: &firstCalls}
	second := testHandler{enabled: false, handled: &secondCalls}
	handler := joinSlogHandlers(nil, first, second)

	require.True(t, handler.Enabled(t.Context(), slog.LevelInfo))
	require.ErrorIs(t, handler.Handle(t.Context(), slog.NewRecord(testTime(), slog.LevelInfo, "message", 0)), firstErr)
	require.Equal(t, 1, firstCalls)
	require.Zero(t, secondCalls)

	withAttrs := handler.WithAttrs([]slog.Attr{slog.String("key", "value")})
	withGroup := withAttrs.WithGroup("group")
	require.True(t, withGroup.Enabled(t.Context(), slog.LevelInfo))

	empty := joinSlogHandlers(nil)
	require.False(t, empty.Enabled(t.Context(), slog.LevelInfo))
	require.NoError(t, empty.Handle(t.Context(), slog.NewRecord(testTime(), slog.LevelInfo, "message", 0)))
}

func testTime() (zeroTime time.Time) { return zeroTime }
