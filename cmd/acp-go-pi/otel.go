package main

import (
	"context"
	"log/slog"

	"github.com/savid/acp-go-core/observer/exporters"
	piacp "github.com/savid/acp-go-pi"
)

// configureTelemetry reads the OTEL_* environment and maps the providers it
// enables onto the agent's options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (exporters.Bundle, []piacp.Option, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "pi", Version: version, Logger: baseLogger})
	if err != nil {
		return exporters.Bundle{}, nil, err
	}

	options := []piacp.Option{piacp.WithTextMapPropagator(bundle.Propagator)}

	if bundle.TracerProvider != nil {
		options = append(options, piacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, piacp.WithMeterProvider(bundle.MeterProvider))
	}

	return bundle, options, nil
}
