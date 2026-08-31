// Package observer centralizes the adapter's OpenTelemetry instrumentation:
// ACP request spans/metrics, prompt-turn GenAI metrics, permission and
// elicitation dialogs, session store operations, and pi process lifecycle.
package observer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// InstrumentationName is the OpenTelemetry instrumentation scope name.
const InstrumentationName = "github.com/savid/acp-go-pi"

const (
	attrACPMethod                          = "acp.method"
	attrErrorType                          = "error.type"
	attrGenAIOperation                     = "gen_ai.operation.name"
	attrGenAIProvider                      = "gen_ai.provider.name"
	attrGenAIRequestModel                  = "gen_ai.request.model"
	attrGenAIResponseModel                 = "gen_ai.response.model"
	attrGenAIStopReason                    = "gen_ai.response.finish_reasons"
	attrGenAITokenType                     = "gen_ai.token.type"                        // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageCacheCreationInputTokens = "gen_ai.usage.cache_creation.input_tokens" // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageCacheReadInputTokens     = "gen_ai.usage.cache_read.input_tokens"     // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageInputTokens              = "gen_ai.usage.input_tokens"                // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageOutputTokens             = "gen_ai.usage.output_tokens"               // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrOperation                          = "operation"
	attrOutcome                            = "outcome"
	attrPiClient                           = "pi.client"
	attrPiPermission                       = "pi.permission.mode"
	attrSessionStoreOp                     = "session.store.operation"
	attrStopReason                         = "stop_reason"
	attrToolName                           = "pi.tool.name"

	piClientValue      = "pi-coding-agent"
	genAIOperationChat = "chat"

	metaBaggage     = "baggage"
	metaTraceParent = "traceparent"
	metaTraceState  = "tracestate"

	envBaggage     = "BAGGAGE"
	envTraceParent = "TRACEPARENT"
	envTraceState  = "TRACESTATE"

	outcomeCanceled = "canceled"
	outcomeError    = "error"
	outcomeOK       = "ok"
)

// Config wires optional OpenTelemetry providers into an Observer.
type Config struct {
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
	TracerProvider trace.TracerProvider
	Version        string
}

// Observer records adapter spans and metrics; nil providers degrade to no-ops.
type Observer struct {
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer

	acpRequestCount    metric.Int64Counter
	acpRequestDuration metric.Float64Histogram

	genAIOperationDuration        metric.Float64Histogram
	genAITimeToFirstChunk         metric.Float64Histogram
	genAITokenUsage               metric.Int64Histogram
	promptCount                   metric.Int64Counter
	promptDuration                metric.Float64Histogram
	promptCancelCount             metric.Int64Counter
	sessionActive                 metric.Int64UpDownCounter
	permissionCount               metric.Int64Counter
	permissionDuration            metric.Float64Histogram
	elicitationCount              metric.Int64Counter
	elicitationDuration           metric.Float64Histogram
	sessionStoreOperationDuration metric.Float64Histogram
	sessionStoreErrorCount        metric.Int64Counter
	rawMessageEmitErrorCount      metric.Int64Counter
	piProcessExitCount            metric.Int64Counter
}

// ACPResult finishes one ACP request observation.
type ACPResult struct {
	Err   error
	Extra []attribute.KeyValue
}

// PromptResult finishes one prompt-turn observation.
type PromptResult struct {
	CachedReadTokens  int
	CachedWriteTokens int
	Err               error
	Model             string
	Provider          string
	InputTokens       int
	OutputTokens      int
	StopReason        string
	TotalTokens       int
}

// PermissionResult finishes one permission-request observation.
type PermissionResult struct {
	Behavior string
	Err      error
	Mode     string
	ToolName string
}

// ElicitationResult finishes one elicitation-request observation.
type ElicitationResult struct {
	Accepted bool
	Err      error
}

// New constructs an Observer; nil providers degrade to no-ops.
func New(config Config) *Observer {
	tracerProvider := config.TracerProvider
	if tracerProvider == nil {
		tracerProvider = tracenoop.NewTracerProvider()
	}

	meterProvider := config.MeterProvider
	if meterProvider == nil {
		meterProvider = metricnoop.NewMeterProvider()
	}

	propagator := config.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}

	tracerOptions := []trace.TracerOption(nil)
	meterOptions := []metric.MeterOption(nil)

	if config.Version != "" {
		tracerOptions = append(tracerOptions, trace.WithInstrumentationVersion(config.Version))
		meterOptions = append(meterOptions, metric.WithInstrumentationVersion(config.Version))
	}

	meter := meterProvider.Meter(InstrumentationName, meterOptions...)
	observer := &Observer{
		propagator: propagator,
		tracer:     tracerProvider.Tracer(InstrumentationName, tracerOptions...),
	}
	observer.acpRequestCount = mustInt64Counter(meter, "acp_go_pi.acp.request.count", "ACP requests.")
	observer.acpRequestDuration = mustFloat64Histogram(meter, "acp_go_pi.acp.request.duration", "ACP request duration.")
	observer.genAIOperationDuration = mustFloat64Histogram(meter, "gen_ai.client.operation.duration", "pi prompt operation duration.")
	observer.genAITimeToFirstChunk = mustFloat64Histogram(meter, "gen_ai.client.operation.time_to_first_chunk", "Time to first ACP prompt update.")
	observer.genAITokenUsage = mustInt64Histogram(meter, "gen_ai.client.token.usage", "{token}", "pi token usage.")
	observer.promptCount = mustInt64Counter(meter, "acp_go_pi.session.prompt.count", "Prompt turns.")
	observer.promptDuration = mustFloat64Histogram(meter, "acp_go_pi.session.prompt.duration", "Prompt turn duration.")
	observer.promptCancelCount = mustInt64Counter(meter, "acp_go_pi.session.cancel.count", "Cancelled prompt turns.")
	observer.sessionActive = mustInt64UpDownCounter(meter, "acp_go_pi.session.active", "Active pi sessions.")
	observer.permissionCount = mustInt64Counter(meter, "acp_go_pi.permission.request.count", "Permission requests.")
	observer.permissionDuration = mustFloat64Histogram(meter, "acp_go_pi.permission.request.duration", "Permission request duration.")
	observer.elicitationCount = mustInt64Counter(meter, "acp_go_pi.elicitation.request.count", "Elicitation requests.")
	observer.elicitationDuration = mustFloat64Histogram(meter, "acp_go_pi.elicitation.request.duration", "Elicitation request duration.")
	observer.sessionStoreOperationDuration = mustFloat64Histogram(meter, "acp_go_pi.session_store.operation.duration", "Session store operation duration.")
	observer.sessionStoreErrorCount = mustInt64Counter(meter, "acp_go_pi.session_store.error.count", "Session store errors.")
	observer.rawMessageEmitErrorCount = mustInt64Counter(meter, "acp_go_pi.raw_message.emit.error.count", "Raw pi event emission errors.")
	observer.piProcessExitCount = mustInt64Counter(meter, "acp_go_pi.pi.process.exit.count", "pi process exits.")

	return observer
}

func mustInt64Counter(meter metric.Meter, name string, description string) metric.Int64Counter {
	instrument, _ := meter.Int64Counter(name, metric.WithDescription(description))

	return instrument
}

func mustInt64Histogram(meter metric.Meter, name string, unit string, description string) metric.Int64Histogram {
	instrument, _ := meter.Int64Histogram(name, metric.WithUnit(unit), metric.WithDescription(description))

	return instrument
}

func mustFloat64Histogram(meter metric.Meter, name string, description string) metric.Float64Histogram {
	instrument, _ := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(description))

	return instrument
}

func mustInt64UpDownCounter(meter metric.Meter, name string, description string) metric.Int64UpDownCounter {
	instrument, _ := meter.Int64UpDownCounter(name, metric.WithDescription(description))

	return instrument
}

// Extract pulls trace context from ACP _meta reserved keys into ctx.
func (o *Observer) Extract(ctx context.Context, meta map[string]any) context.Context {
	if o == nil || len(meta) == 0 {
		return ctx
	}

	carrier := propagation.MapCarrier{}

	for _, key := range []string{metaTraceParent, metaTraceState, metaBaggage} {
		value, _ := meta[key].(string)
		if strings.TrimSpace(value) != "" {
			carrier[key] = value
		}
	}

	if len(carrier) == 0 {
		return ctx
	}

	return o.propagator.Extract(ctx, carrier)
}

// InjectTraceEnv injects the active trace context into a pi launch env map.
func (o *Observer) InjectTraceEnv(ctx context.Context, env map[string]string) map[string]string {
	if o == nil {
		return env
	}

	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)

	if len(carrier) == 0 {
		return env
	}

	if env == nil {
		env = make(map[string]string, len(carrier))
	}

	if value := carrier.Get(metaTraceParent); value != "" {
		env[envTraceParent] = value
	}

	if value := carrier.Get(metaTraceState); value != "" {
		env[envTraceState] = value
	}

	if value := carrier.Get(metaBaggage); value != "" {
		env[envBaggage] = value
	}

	return env
}

// StartACP begins one ACP request observation.
func (o *Observer) StartACP(ctx context.Context, meta map[string]any, method string, attrs ...attribute.KeyValue) (context.Context, func(ACPResult)) {
	if o == nil {
		return ctx, func(ACPResult) {}
	}

	ctx = o.Extract(ctx, meta)
	start := time.Now()

	spanAttrs := make([]attribute.KeyValue, 0, 1+len(attrs))
	spanAttrs = append(spanAttrs, attribute.String(attrACPMethod, method))
	spanAttrs = append(spanAttrs, attrs...)

	ctx, span := o.tracer.Start(ctx, spanNameForACPMethod(method), trace.WithAttributes(spanAttrs...))

	return ctx, func(result ACPResult) {
		allAttrs := append(slicesClone(spanAttrs), result.Extra...)
		outcome := outcomeFromError(result.Err)
		allAttrs = append(allAttrs, attribute.String(attrOutcome, outcome))

		if errType := ErrorType(result.Err); errType != "" {
			allAttrs = append(allAttrs, attribute.String(attrErrorType, errType))

			span.RecordError(result.Err)
			span.SetStatus(codes.Error, errType)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(allAttrs...)
		span.End()

		o.acpRequestCount.Add(ctx, 1, metric.WithAttributes(allAttrs...))
		o.acpRequestDuration.Record(ctx, durationSeconds(start), metric.WithAttributes(allAttrs...))
	}
}

// StartPrompt begins one prompt-turn observation.
func (o *Observer) StartPrompt(ctx context.Context, meta map[string]any, model string) (context.Context, func(PromptResult)) {
	ctx, finishACP := o.StartACP(ctx, meta, "session/prompt", modelAttrs(model)...)
	if o == nil {
		return ctx, func(PromptResult) {}
	}

	state := &promptState{start: time.Now(), model: model}
	ctx = context.WithValue(ctx, promptStateKey{}, state)

	return ctx, func(result PromptResult) {
		promptAttrs := []attribute.KeyValue{
			attribute.String(attrPiClient, piClientValue),
			attribute.String(attrGenAIOperation, genAIOperationChat),
		}
		if result.Provider != "" {
			promptAttrs = append(promptAttrs, attribute.String(attrGenAIProvider, result.Provider))
		}

		promptAttrs = append(promptAttrs, modelAttrs(firstNonEmpty(result.Model, model))...)
		promptAttrs = append(promptAttrs, promptUsageAttrs(result)...)

		if result.StopReason != "" {
			promptAttrs = append(promptAttrs,
				attribute.String(attrStopReason, result.StopReason),
				attribute.StringSlice(attrGenAIStopReason, []string{result.StopReason}),
			)
		}

		outcome := outcomeFromPrompt(result)
		metricAttrs := append(slicesClone(promptAttrs), attribute.String(attrOutcome, outcome))

		if errType := ErrorType(result.Err); errType != "" {
			metricAttrs = append(metricAttrs, attribute.String(attrErrorType, errType))
		}

		duration := durationSeconds(state.start)

		o.promptCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		o.promptDuration.Record(ctx, duration, metric.WithAttributes(metricAttrs...))
		o.genAIOperationDuration.Record(ctx, duration, metric.WithAttributes(metricAttrs...))

		if outcome == outcomeCanceled {
			o.promptCancelCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		}

		o.recordTokenUsage(ctx, result, promptAttrs)
		finishACP(ACPResult{Err: result.Err, Extra: promptAttrs})
	}
}

// ObserveFirstPromptUpdate records time-to-first-chunk once per prompt turn.
func (o *Observer) ObserveFirstPromptUpdate(ctx context.Context) {
	if o == nil {
		return
	}

	state, _ := ctx.Value(promptStateKey{}).(*promptState)
	if state == nil {
		return
	}

	state.mu.Lock()
	if state.observed {
		state.mu.Unlock()

		return
	}

	state.observed = true
	start := state.start
	model := state.model
	state.mu.Unlock()

	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs,
		attribute.String(attrPiClient, piClientValue),
		attribute.String(attrGenAIOperation, genAIOperationChat),
	)
	attrs = append(attrs, modelAttrs(model)...)
	o.genAITimeToFirstChunk.Record(ctx, durationSeconds(start), metric.WithAttributes(attrs...))
}

// StartSpan begins one adapter span.
func (o *Observer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(error, ...attribute.KeyValue)) {
	if o == nil {
		return ctx, func(error, ...attribute.KeyValue) {}
	}

	ctx, span := o.tracer.Start(ctx, name, trace.WithAttributes(attrs...))

	return ctx, func(err error, extra ...attribute.KeyValue) {
		if errType := ErrorType(err); errType != "" {
			extra = append(extra, attribute.String(attrErrorType, errType))

			span.RecordError(err)
			span.SetStatus(codes.Error, errType)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		if len(extra) > 0 {
			span.SetAttributes(extra...)
		}

		span.End()
	}
}

// StartPiProcess begins one pi process-lifecycle span.
func (o *Observer) StartPiProcess(ctx context.Context, operation string) (context.Context, func(error)) {
	ctx, finish := o.StartSpan(ctx, "pi.process."+operation,
		attribute.String(attrOperation, operation),
		attribute.String(attrPiClient, piClientValue),
	)

	return ctx, func(err error) { finish(err) }
}

// RecordPiProcessExit counts one pi process exit.
func (o *Observer) RecordPiProcessExit(ctx context.Context, outcome string, err error) {
	if o == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String(attrOutcome, firstNonEmpty(outcome, outcomeFromError(err))),
		attribute.String(attrPiClient, piClientValue),
	}
	if errType := ErrorType(err); errType != "" {
		attrs = append(attrs, attribute.String(attrErrorType, errType))
	}

	o.piProcessExitCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// AddActiveSession adjusts the active-session gauge.
func (o *Observer) AddActiveSession(ctx context.Context, delta int64) {
	if o == nil || delta == 0 {
		return
	}

	o.sessionActive.Add(ctx, delta)
}

// StartPermission begins one permission-request observation.
func (o *Observer) StartPermission(ctx context.Context, toolName string, mode string) (context.Context, func(PermissionResult)) {
	if o == nil {
		return ctx, func(PermissionResult) {}
	}

	start := time.Now()

	attrs := []attribute.KeyValue{
		attribute.String(attrPiClient, piClientValue),
	}
	if toolName != "" {
		attrs = append(attrs, attribute.String(attrToolName, toolName))
	}

	if mode != "" {
		attrs = append(attrs, attribute.String(attrPiPermission, mode))
	}

	ctx, span := o.tracer.Start(ctx, "acp.permission.request", trace.WithAttributes(attrs...))

	return ctx, func(result PermissionResult) {
		finalAttrs := slicesClone(attrs)
		if result.Behavior != "" {
			finalAttrs = append(finalAttrs, attribute.String(attrOutcome, result.Behavior))
		} else {
			finalAttrs = append(finalAttrs, attribute.String(attrOutcome, outcomeFromError(result.Err)))
		}

		if errType := ErrorType(result.Err); errType != "" {
			finalAttrs = append(finalAttrs, attribute.String(attrErrorType, errType))

			span.RecordError(result.Err)
			span.SetStatus(codes.Error, errType)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		if result.Mode != "" && result.Mode != mode {
			finalAttrs = append(finalAttrs, attribute.String(attrPiPermission, result.Mode))
		}

		if result.ToolName != "" && result.ToolName != toolName {
			finalAttrs = append(finalAttrs, attribute.String(attrToolName, result.ToolName))
		}

		span.SetAttributes(finalAttrs...)
		span.End()

		metricAttrs := removeAttribute(finalAttrs, attrToolName)
		o.permissionCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		o.permissionDuration.Record(ctx, durationSeconds(start), metric.WithAttributes(metricAttrs...))
	}
}

// StartElicitation begins one elicitation-request observation.
func (o *Observer) StartElicitation(ctx context.Context) (context.Context, func(ElicitationResult)) {
	if o == nil {
		return ctx, func(ElicitationResult) {}
	}

	start := time.Now()
	attrs := []attribute.KeyValue{attribute.String(attrPiClient, piClientValue)}
	ctx, span := o.tracer.Start(ctx, "acp.elicitation.request", trace.WithAttributes(attrs...))

	return ctx, func(result ElicitationResult) {
		outcome := outcomeOK
		if !result.Accepted {
			outcome = "declined"
		}

		if result.Err != nil {
			outcome = outcomeError
		}

		finalAttrs := append(slicesClone(attrs), attribute.String(attrOutcome, outcome))
		if errType := ErrorType(result.Err); errType != "" {
			finalAttrs = append(finalAttrs, attribute.String(attrErrorType, errType))

			span.RecordError(result.Err)
			span.SetStatus(codes.Error, errType)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(finalAttrs...)
		span.End()
		o.elicitationCount.Add(ctx, 1, metric.WithAttributes(finalAttrs...))
		o.elicitationDuration.Record(ctx, durationSeconds(start), metric.WithAttributes(finalAttrs...))
	}
}

// RecordSessionStore records one session store operation.
func (o *Observer) RecordSessionStore(ctx context.Context, start time.Time, operation string, err error) {
	if o == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String(attrSessionStoreOp, operation),
		attribute.String(attrOutcome, outcomeFromError(err)),
	}
	if errType := ErrorType(err); errType != "" {
		attrs = append(attrs, attribute.String(attrErrorType, errType))
		o.sessionStoreErrorCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	o.sessionStoreOperationDuration.Record(ctx, durationSeconds(start), metric.WithAttributes(attrs...))
}

// StartSessionStore begins one session store operation observation.
func (o *Observer) StartSessionStore(ctx context.Context, operation string) (context.Context, func(error)) {
	start := time.Now()
	ctx, finishSpan := o.StartSpan(ctx, "acp.session_store."+operation, attribute.String(attrSessionStoreOp, operation))

	return ctx, func(err error) {
		finishSpan(err)
		o.RecordSessionStore(ctx, start, operation, err)
	}
}

// RecordRawMessageEmitFailure counts one raw-event notification failure.
func (o *Observer) RecordRawMessageEmitFailure(ctx context.Context, err error) {
	if o == nil {
		return
	}

	attrs := []attribute.KeyValue{attribute.String(attrOutcome, outcomeFromError(err))}
	if errType := ErrorType(err); errType != "" {
		attrs = append(attrs, attribute.String(attrErrorType, errType))
	}

	o.rawMessageEmitErrorCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (o *Observer) recordTokenUsage(ctx context.Context, result PromptResult, attrs []attribute.KeyValue) {
	inputTokens := result.InputTokens + result.CachedReadTokens + result.CachedWriteTokens
	if inputTokens > 0 {
		o.genAITokenUsage.Record(ctx, int64(inputTokens), metric.WithAttributes(appendTokenType(attrs, "input")...))
	}

	if result.OutputTokens > 0 {
		o.genAITokenUsage.Record(ctx, int64(result.OutputTokens), metric.WithAttributes(appendTokenType(attrs, "output")...))
	}
}

func promptUsageAttrs(result PromptResult) []attribute.KeyValue {
	attrs := []attribute.KeyValue(nil)

	inputTokens := result.InputTokens + result.CachedReadTokens + result.CachedWriteTokens
	if inputTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageInputTokens, inputTokens))
	}

	if result.OutputTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageOutputTokens, result.OutputTokens))
	}

	if result.CachedWriteTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageCacheCreationInputTokens, result.CachedWriteTokens))
	}

	if result.CachedReadTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageCacheReadInputTokens, result.CachedReadTokens))
	}

	return attrs
}

func appendTokenType(attrs []attribute.KeyValue, tokenType string) []attribute.KeyValue {
	cloned := slicesClone(attrs)

	return append(cloned, attribute.String(attrGenAITokenType, tokenType))
}

func removeAttribute(attrs []attribute.KeyValue, key string) []attribute.KeyValue {
	filtered := attrs[:0]
	for _, attr := range attrs {
		if string(attr.Key) == key {
			continue
		}

		filtered = append(filtered, attr)
	}

	return filtered
}

func modelAttrs(model string) []attribute.KeyValue {
	if strings.TrimSpace(model) == "" {
		return nil
	}

	return []attribute.KeyValue{
		attribute.String(attrGenAIRequestModel, model),
		attribute.String(attrGenAIResponseModel, model),
	}
}

func spanNameForACPMethod(method string) string {
	return "acp." + strings.ReplaceAll(method, "/", ".")
}

func outcomeFromPrompt(result PromptResult) string {
	if result.Err != nil {
		return outcomeError
	}

	if strings.EqualFold(result.StopReason, "cancelled") || strings.EqualFold(result.StopReason, "canceled") {
		return outcomeCanceled
	}

	return outcomeOK
}

func outcomeFromError(err error) string {
	if err == nil {
		return outcomeOK
	}

	if errors.Is(err, context.Canceled) {
		return outcomeCanceled
	}

	return outcomeError
}

// ErrorType names an error for metric attributes, normalizing context errors.
func ErrorType(err error) string {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.Canceled) {
		return "context.Canceled"
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return "context.DeadlineExceeded"
	}

	typ := reflect.TypeOf(err)

	return typ.String()
}

func durationSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

func slicesClone[T any](values []T) []T {
	return append([]T(nil), values...)
}

type promptStateKey struct{}

type promptState struct {
	mu       sync.Mutex
	model    string
	observed bool
	start    time.Time
}
