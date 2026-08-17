package observer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type testError struct{}

func (testError) Error() string { return "test error" }

func TestObserverRecordsOperations(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	o := New(Config{
		MeterProvider:  meterProvider,
		TracerProvider: tracerProvider,
		Version:        "1.2.3",
	})
	t.Cleanup(func() {
		require.NoError(t, meterProvider.Shutdown(context.Background()))
		require.NoError(t, tracerProvider.Shutdown(context.Background()))
	})

	ctx := context.Background()
	ctx, finishACP := o.StartACP(ctx, nil, "session/new", attribute.String("test", "value"))
	finishACP(ACPResult{Extra: []attribute.KeyValue{attribute.Bool("done", true)}})

	_, finishFailedACP := o.StartACP(ctx, nil, "session/load")
	finishFailedACP(ACPResult{Err: testError{}})

	promptCtx, finishPrompt := o.StartPrompt(ctx, nil, "provider/original")
	o.ObserveFirstPromptUpdate(promptCtx)
	o.ObserveFirstPromptUpdate(promptCtx)
	finishPrompt(PromptResult{
		CachedReadTokens:  2,
		CachedWriteTokens: 3,
		Model:             "provider/final",
		Provider:          "provider",
		InputTokens:       5,
		OutputTokens:      7,
		StopReason:        "end_turn",
		TotalTokens:       17,
	})

	_, finishCanceledPrompt := o.StartPrompt(ctx, nil, "")
	finishCanceledPrompt(PromptResult{StopReason: "CANCELED"})
	_, finishFailedPrompt := o.StartPrompt(ctx, nil, "provider/model")
	finishFailedPrompt(PromptResult{Err: testError{}})
	o.ObserveFirstPromptUpdate(ctx)

	_, finishSpan := o.StartSpan(ctx, "test.ok")
	finishSpan(nil, attribute.String("result", "ok"))
	_, finishFailedSpan := o.StartSpan(ctx, "test.failed")
	finishFailedSpan(testError{})

	_, finishProcess := o.StartPiProcess(ctx, "spawn")
	finishProcess(nil)
	o.RecordPiProcessExit(ctx, "", context.Canceled)
	o.RecordPiProcessExit(ctx, "explicit", nil)
	o.AddActiveSession(ctx, 1)
	o.AddActiveSession(ctx, 0)

	_, finishPermission := o.StartPermission(ctx, "bash", "ask")
	finishPermission(PermissionResult{Behavior: "allow", Mode: "allow", ToolName: "shell"})
	_, finishFailedPermission := o.StartPermission(ctx, "", "")
	finishFailedPermission(PermissionResult{Err: testError{}})

	_, finishAcceptedElicitation := o.StartElicitation(ctx)
	finishAcceptedElicitation(ElicitationResult{Accepted: true})
	_, finishDeclinedElicitation := o.StartElicitation(ctx)
	finishDeclinedElicitation(ElicitationResult{})
	_, finishFailedElicitation := o.StartElicitation(ctx)
	finishFailedElicitation(ElicitationResult{Accepted: true, Err: testError{}})

	o.RecordSessionStore(ctx, time.Now(), "load", nil)
	o.RecordSessionStore(ctx, time.Now(), "save", testError{})
	_, finishStore := o.StartSessionStore(ctx, "delete")
	finishStore(nil)
	o.RecordRawMessageEmitFailure(ctx, nil)
	o.RecordRawMessageEmitFailure(ctx, testError{})

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &metrics))
	require.NotEmpty(t, metrics.ScopeMetrics)
	require.GreaterOrEqual(t, len(recorder.Ended()), 14)
}

func TestObserverPropagationAndNilReceiver(t *testing.T) {
	t.Parallel()

	o := New(Config{})
	ctx := context.Background()

	require.Equal(t, ctx, o.Extract(ctx, nil))
	require.Equal(t, ctx, o.Extract(ctx, map[string]any{
		metaTraceParent: 42,
		metaTraceState:  " ",
	}))

	traceID, err := oteltrace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := oteltrace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)
	traceState, err := oteltrace.ParseTraceState("vendor=value")
	require.NoError(t, err)
	member, err := baggage.NewMember("tenant", "test")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	spanCtx := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: oteltrace.FlagsSampled,
		TraceState: traceState,
		Remote:     true,
	})
	ctx = baggage.ContextWithBaggage(oteltrace.ContextWithRemoteSpanContext(ctx, spanCtx), bag)
	env := o.InjectTraceEnv(ctx, nil)
	require.Contains(t, env[envTraceParent], traceID.String())
	require.Equal(t, "vendor=value", env[envTraceState])
	require.Equal(t, "tenant=test", env[envBaggage])

	extracted := o.Extract(context.Background(), map[string]any{
		metaTraceParent: env[envTraceParent],
		metaTraceState:  env[envTraceState],
		metaBaggage:     env[envBaggage],
	})
	require.Equal(t, traceID, oteltrace.SpanContextFromContext(extracted).TraceID())
	require.Equal(t, "test", baggage.FromContext(extracted).Member("tenant").Value())

	emptyPropagation := New(Config{Propagator: propagation.NewCompositeTextMapPropagator()})
	existing := map[string]string{"KEEP": "yes"}
	require.Equal(t, existing, emptyPropagation.InjectTraceEnv(ctx, existing))

	var nilObserver *Observer
	require.Equal(t, ctx, nilObserver.Extract(ctx, map[string]any{"key": "value"}))
	require.Equal(t, existing, nilObserver.InjectTraceEnv(ctx, existing))
	_, finishACP := nilObserver.StartACP(ctx, nil, "initialize")
	finishACP(ACPResult{})
	_, finishPrompt := nilObserver.StartPrompt(ctx, nil, "model")
	finishPrompt(PromptResult{})
	nilObserver.ObserveFirstPromptUpdate(ctx)
	_, finishSpan := nilObserver.StartSpan(ctx, "span")
	finishSpan(nil)
	_, finishPermission := nilObserver.StartPermission(ctx, "", "")
	finishPermission(PermissionResult{})
	_, finishElicitation := nilObserver.StartElicitation(ctx)
	finishElicitation(ElicitationResult{})
	nilObserver.RecordPiProcessExit(ctx, "", nil)
	nilObserver.AddActiveSession(ctx, 1)
	nilObserver.RecordSessionStore(ctx, time.Now(), "load", nil)
	_, finishStore := nilObserver.StartSessionStore(ctx, "load")
	finishStore(nil)
	nilObserver.RecordRawMessageEmitFailure(ctx, nil)
}

func TestObserverHelpers(t *testing.T) {
	t.Parallel()

	require.Nil(t, modelAttrs(" "))
	require.Equal(t, "acp.session.prompt", spanNameForACPMethod("session/prompt"))
	require.Equal(t, outcomeError, outcomeFromPrompt(PromptResult{Err: errors.New("failed")}))
	require.Equal(t, outcomeCanceled, outcomeFromPrompt(PromptResult{StopReason: "cancelled"}))
	require.Equal(t, outcomeOK, outcomeFromPrompt(PromptResult{}))
	require.Equal(t, outcomeOK, outcomeFromError(nil))
	require.Equal(t, outcomeCanceled, outcomeFromError(context.Canceled))
	require.Equal(t, outcomeError, outcomeFromError(testError{}))
	require.Empty(t, ErrorType(nil))
	require.Equal(t, "context.Canceled", ErrorType(context.Canceled))
	require.Equal(t, "context.DeadlineExceeded", ErrorType(context.DeadlineExceeded))
	require.Equal(t, "observer.testError", ErrorType(testError{}))
	require.GreaterOrEqual(t, durationSeconds(time.Now()), float64(0))
	require.Equal(t, "value", firstNonEmpty("", " ", "value", "ignored"))
	require.Empty(t, firstNonEmpty("", " "))

	attrs := []attribute.KeyValue{attribute.String("keep", "yes"), attribute.String("drop", "no")}
	filtered := removeAttribute(slicesClone(attrs), "drop")
	require.Equal(t, []attribute.KeyValue{attribute.String("keep", "yes")}, filtered)
	require.Equal(t, "input", appendTokenType(attrs, "input")[2].Value.AsString())
	require.Empty(t, promptUsageAttrs(PromptResult{}))

	meter := metricnoop.NewMeterProvider().Meter("test")
	require.NotNil(t, mustInt64Counter(meter, "counter", "description"))
	require.NotNil(t, mustInt64Histogram(meter, "histogram", "unit", "description"))
	require.NotNil(t, mustFloat64Histogram(meter, "float", "description"))
	require.NotNil(t, mustInt64UpDownCounter(meter, "updown", "description"))
}
