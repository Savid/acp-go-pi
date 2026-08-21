package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/observer"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	metaCapabilityFork      = "fork"
	elicitationScopeSession = "session"

	// metaFieldVersions carries the supported versions of one family-global
	// reserved capability literal.
	metaFieldVersions = "versions"
)

const (
	darwinPlatform  = "darwin"
	linuxPlatform   = "linux"
	windowsPlatform = "windows"
)

var agentRuntimePlatform = runtime.GOOS

var newServeAgent = NewAgent

// piProcess is the process-control seam over one running pi child.
type piProcess interface {
	CloseStdin() error
	Exited() <-chan struct{}
	WaitErr() error
	StderrTail() string
	Shutdown(ctx context.Context) error
	Kill() error
	Close() error
}

// piClient is the RPC seam over one pi child's JSONL protocol. *pi.Client
// implements it; tests substitute a scripted fake harness.
type piClient interface {
	Start(ctx context.Context) error
	Events() <-chan pi.Event
	UIRequests() <-chan pi.UIRequest
	ResponseBoundaries() <-chan pi.ResponseBoundary
	Done() <-chan struct{}
	Err() error
	RespondUI(response pi.UIResponse) error
	Prompt(ctx context.Context, message string, images []pi.ImageContent) error
	PromptWithBoundary(ctx context.Context, message string, images []pi.ImageContent, boundary pi.CallBoundary) error
	Abort(ctx context.Context) error
	Clone(ctx context.Context) (bool, error)
	GetState(ctx context.Context) (pi.SessionState, error)
	GetStateWithBoundary(ctx context.Context, boundary pi.CallBoundary) (pi.SessionState, error)
	GetAvailableModels(ctx context.Context) ([]pi.Model, error)
	SetModel(ctx context.Context, provider string, modelID string) (pi.Model, error)
	SetModelWithBoundary(ctx context.Context, provider string, modelID string, boundary pi.CallBoundary) (pi.Model, error)
	SetThinkingLevel(ctx context.Context, level string) error
	SetThinkingLevelWithBoundary(ctx context.Context, level string, boundary pi.CallBoundary) error
	SetAutoRetry(ctx context.Context, enabled bool) error
	GetSessionStats(ctx context.Context) (pi.SessionStats, error)
	GetCommands(ctx context.Context) ([]pi.SlashCommand, error)
}

var _ piClient = (*pi.Client)(nil)

// Agent exposes the pi coding agent through ACP.
type Agent struct {
	options Options
	log     *slog.Logger
	observe *observer.Observer
	// ordinaryEnvironment is captured and sanitized once at construction so
	// later ambient changes cannot cross into an existing agent generation.
	ordinaryEnvironment map[string]string

	// Lock order: acquire mu before any session lock. Do not call session
	// close methods while holding mu.
	mu                 sync.Mutex
	closed             bool
	conn               agentClient
	sessions           map[acp.SessionId]*agentSession
	retainedSessions   map[*agentSession]struct{}
	constructions      map[*nativeConstruction]struct{}
	store              SessionStore
	deleted            map[acp.SessionId]struct{}
	clientCalls        chan struct{}
	clientCapabilities acp.ClientCapabilities
	positionEncoding   acp.PositionEncodingKind
	// lifecycle is the answer this connection gave at initialize. An absent
	// answer leaves the extension dormant for every session on it.
	lifecycle            lifecycle.Negotiated
	optionErr            error
	processes            *providerProcessTracker
	nativeContainmentErr error
	closeAttempt         *agentCloseAttempt

	versionMu      sync.Mutex
	versionChecked bool

	// startupDefaults is the durable home's operator baseline for the
	// settings.json keys pi consults at process start. It is captured on the
	// first launch against the home, before any session of this agent has been
	// able to change model or thinking level there.
	startupDefaultsOnce sync.Once
	startupDefaults     pi.StartupDefaults
	startupDefaultsErr  error

	// providerAuth is nil when the durable native residence or values-free
	// ledger is not configured, or hardened distinct-identity isolation was
	// selected.
	providerAuth *providerAuth

	startPiProcess func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error)
	probeVersion   func(ctx context.Context, executablePath string, agentDir string, containment pi.ContainmentSpec) (string, error)
	lookPath       func(file string) (string, error)
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

// NewAgent creates an ACP agent for the pi coding agent CLI.
func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	observe := observer.New(observer.Config{
		MeterProvider:  options.MeterProvider,
		Propagator:     options.TextMapPropagator,
		TracerProvider: options.TracerProvider,
		Version:        options.AgentVersion,
	})
	mode := containmentMode(options)
	options.RuntimeResourceHooks = instrumentRuntimeResourceHooks(options.RuntimeResourceHooks, observe, mode)
	ordinaryEnvironment := pi.CaptureOrdinaryEnvironment()

	agent := &Agent{
		options:             options,
		log:                 log,
		observe:             observe,
		ordinaryEnvironment: ordinaryEnvironment,
		sessions:            make(map[acp.SessionId]*agentSession),
		retainedSessions:    make(map[*agentSession]struct{}),
		constructions:       make(map[*nativeConstruction]struct{}),
		store:               NewInMemorySessionStore(),
		deleted:             make(map[acp.SessionId]struct{}),
		positionEncoding:    acp.PositionEncodingKindUtf16,
		optionErr: errors.Join(
			optionFailure(log, optionFieldEnv, validateEnvironment(options.Env, optionFieldEnv, blockedAgentEnvKey)),
			optionFailure(log, optionFieldConcurrencyLimits, validateConcurrencyLimits(options.ConcurrencyLimits)),
			optionFailure(log, optionFieldContainment, validateContainmentOption(options)),
			optionFailure(log, optionFieldImageLimits, validateImageLimits(options.ImageLimits)),
			optionFailure(log, optionFieldInputHandoffRoot, validateInputHandoffRoot(options.InputHandoffRoot)),
		),
		startPiProcess: startRealPiProcess,
		probeVersion:   pi.ProbeVersion,
		lookPath: func(file string) (string, error) {
			return pi.ResolveExecutable(file, internalProcessIsolation(options.ProcessIsolation, options.testOnlyNoCredential, options.testOnlyIdentityLockRoot), ordinaryEnvironment, options.Env)
		},
	}
	agent.processes = newProviderProcessTracker(options.RuntimeResourceHooks)

	agent.optionErr = errors.Join(
		agent.optionErr,
		optionFailure(log, optionFieldProviderAuthRoot, configureProviderAuth(agent)),
	)

	observeRuntimeContainment(context.Background(), options.RuntimeResourceHooks, mode)

	if mode == RuntimeContainmentBestEffort {
		log.WarnContext(
			context.Background(),
			"Darwin process containment is best effort; escaped descendants may survive, marker correlation is not ownership and markers can be scrubbed, numeric process-group reuse can cause collateral signalling, and native-root permits do not bound escaped provider work",
			slog.String("containment", string(mode)),
		)
	}

	return agent
}

// ContainmentMode reports the effective native process boundary.
func (a *Agent) ContainmentMode() RuntimeContainmentMode {
	if a == nil {
		return RuntimeContainmentUnavailable
	}

	return containmentMode(a.options)
}

func containmentMode(options Options) RuntimeContainmentMode {
	if options.DarwinBestEffortContainment && agentRuntimePlatform != darwinPlatform {
		return RuntimeContainmentUnavailable
	}

	if options.ProcessIsolation != nil && options.DarwinBestEffortContainment {
		return RuntimeContainmentUnavailable
	}

	if options.ProcessIsolation == nil {
		if options.DarwinBestEffortContainment {
			return RuntimeContainmentBestEffort
		}

		return RuntimeContainmentSharedIdentity
	}

	if agentRuntimePlatform == linuxPlatform {
		return RuntimeContainmentAuthoritative
	}

	return RuntimeContainmentUnavailable
}

func validateContainmentOption(options Options) error {
	if options.ProcessIsolation != nil && options.DarwinBestEffortContainment {
		return errors.New("explicit process isolation cannot be combined with darwin best-effort containment")
	}

	if options.DarwinBestEffortContainment && agentRuntimePlatform != darwinPlatform {
		return errors.New("darwin best-effort containment is only valid on darwin")
	}

	return nil
}

func (a *Agent) startTrackedPiProcess(
	ctx context.Context,
	spec pi.LaunchSpec,
) (piProcess, piClient, *providerProcessRoot, error) {
	process, client, err := a.startPiProcess(ctx, spec)
	if err != nil {
		if !providerProcessTreeComplete(err) {
			root := a.processes.registerDeferred()
			a.recordNativeContainment(err)

			return process, client, root, err
		}

		return nil, nil, nil, err
	}

	root := a.processes.registerDeferred()

	return process, client, root, nil
}

func startRealPiProcess(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
	process, err := pi.StartProcess(ctx, spec)
	if err != nil {
		return nil, nil, err
	}

	return process, pi.NewClient(process.Stdin(), process.Stdout()), nil
}

// Serve runs an ACP agent over the provided streams.
func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := newServeAgent(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			agent.log.DebugContext(context.Background(), "close pi ACP agent failed")

			returnErr = closeErr
		}
	}()

	conn := newLocalAgentConnection(agent, output, input)
	agent.setConnection(conn)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

// Close cancels and closes all resources owned by the agent.
func (a *Agent) Close() error {
	attempt, owner := a.beginClose()
	if owner {
		var err error

		func() {
			defer func() {
				if recover() != nil {
					err = generationContainmentPanicError("agent close")
				}
			}()

			err = a.close(attempt)
		}()

		a.finishClose(attempt, err)
	}

	return a.awaitClose(attempt)
}

func (a *Agent) beginClose() (*agentCloseAttempt, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closeAttempt != nil {
		return a.closeAttempt, false
	}

	attempt := &agentCloseAttempt{done: make(chan struct{})}
	a.closed = true
	a.closeAttempt = attempt

	return attempt, true
}

func (a *Agent) finishClose(attempt *agentCloseAttempt, err error) {
	if !attempt.beginFinalSettlement() && attempt.settlement.Load() == closeSettlementQuarantined {
		return
	}

	attempt.finishOnce.Do(func() {
		attempt.err = err
		attempt.settlement.Store(closeSettlementFinished)
		close(attempt.done)
	})
}

func (a *Agent) quarantineClose(attempt *agentCloseAttempt, err error) bool {
	if !attempt.settlement.CompareAndSwap(closeSettlementOpen, closeSettlementQuarantined) {
		return false
	}

	attempt.finishOnce.Do(func() {
		attempt.err = err
		close(attempt.done)
	})

	a.mu.Lock()
	sessions := make([]*agentSession, 0, len(a.sessions)+len(a.retainedSessions))

	seen := make(map[*agentSession]struct{}, len(a.sessions)+len(a.retainedSessions))
	for _, session := range a.sessions {
		if _, ok := seen[session]; !ok {
			sessions = append(sessions, session)
			seen[session] = struct{}{}
		}
	}

	for session := range a.retainedSessions {
		if _, ok := seen[session]; !ok {
			sessions = append(sessions, session)
		}
	}
	a.mu.Unlock()

	for _, session := range sessions {
		session.quarantineCloseAttempt(err)
	}

	return true
}

func (a *Agent) awaitClose(attempt *agentCloseAttempt) error {
	select {
	case <-attempt.done:
		return attempt.err
	default:
	}

	waitCtx, cancelWait := sessionCloseTurnWaitContext(context.Background())
	defer cancelWait()

	select {
	case <-attempt.done:
	case <-waitCtx.Done():
		if !a.quarantineClose(attempt, fmt.Errorf("%w: join agent close attempt: %v",
			pi.ErrProcessContainmentIncomplete, waitCtx.Err())) {
			<-attempt.done
		}
	}

	return attempt.err
}

func (a *Agent) close(attempt *agentCloseAttempt) error {
	constructionErr := a.awaitNativeConstructions()
	if !pi.ProcessContainmentComplete(constructionErr) {
		a.recordNativeContainment(constructionErr)

		return constructionErr
	}

	a.mu.Lock()

	sessions := make([]*agentSession, 0, len(a.sessions)+len(a.retainedSessions))
	seen := make(map[*agentSession]struct{}, len(a.sessions)+len(a.retainedSessions))
	active := 0

	for id, session := range a.sessions {
		if _, ok := seen[session]; !ok {
			sessions = append(sessions, session)
			seen[session] = struct{}{}
		}

		if _, hidden := a.deleted[id]; !hidden {
			active++
		}
	}

	for session := range a.retainedSessions {
		if _, ok := seen[session]; ok {
			continue
		}

		sessions = append(sessions, session)
		seen[session] = struct{}{}
	}

	a.conn = nil
	a.mu.Unlock()

	if active > 0 {
		a.observe.AddActiveSession(context.Background(), -int64(active))
	}

	var closeErrs []error

	completed := make([]*agentSession, 0, len(sessions))
	for _, session := range sessions {
		if attempt.settlement.Load() == closeSettlementQuarantined {
			return attempt.result()
		}

		// Embedded shutdown detached the connection above. Suppress carrier
		// delivery without mutating lifecycle state: only a completed native
		// containment boundary may fence or terminalize the incarnation.
		session.detachLifecycleDelivery()

		if err := session.Close(context.Background()); err != nil {
			closeErrs = append(closeErrs, err)

			continue
		}

		completed = append(completed, session)
	}

	if !attempt.beginFinalSettlement() {
		return attempt.result()
	}

	a.mu.Lock()
	for _, session := range completed {
		delete(a.retainedSessions, session)

		for id, current := range a.sessions {
			if current == session {
				delete(a.sessions, id)
			}
		}
	}

	for construction := range a.constructions {
		if construction.err != nil {
			closeErrs = append(closeErrs, construction.err)
		}
	}
	a.mu.Unlock()

	closeErr := errors.Join(closeErrs...)
	if !pi.ProcessContainmentComplete(closeErr) {
		a.recordNativeContainment(closeErr)

		return closeErr
	}

	return errors.Join(closeErr, a.nativeContainmentError())
}

type nativeConstruction struct {
	done chan struct{}
	// immutable is set when Agent.Close cannot join this exact constructor.
	// The incomplete verdict never changes after that point, but the constructor
	// must still publish every handle it acquires so its late-result quarantine
	// can synchronously contain and release those exact resources.
	immutable   bool
	cleanupOnce sync.Once
	cleanupErr  error

	proc           piProcess
	client         piClient
	processRoot    *providerProcessRoot
	generationRoot string
	sessionRoot    string
	nativeRelease  func()
	scratchRelease func()
	browserShim    *pi.BrowserShim
	residence      *pi.SessionResidence
	runtime        *runtimeGeneration
	session        *agentSession
	nativeBoundary *nativeBoundaryTracker
	err            error
}

type agentCloseAttempt struct {
	done       chan struct{}
	finishOnce sync.Once
	settlement atomic.Uint32
	err        error
}

const (
	closeSettlementOpen uint32 = iota
	closeSettlementFinalizing
	closeSettlementQuarantined
	closeSettlementFinished
)

func (a *agentCloseAttempt) beginFinalSettlement() bool {
	state := a.settlement.Load()
	if state == closeSettlementFinalizing || state == closeSettlementFinished {
		return true
	}

	return a.settlement.CompareAndSwap(closeSettlementOpen, closeSettlementFinalizing)
}

func (a *agentCloseAttempt) result() error {
	<-a.done

	return a.err
}

func (a *Agent) beginNativeConstruction() (*nativeConstruction, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, errAgentClosed
	}

	construction := &nativeConstruction{
		done:           make(chan struct{}),
		nativeBoundary: newNativeBoundaryTracker(),
	}
	a.constructions[construction] = struct{}{}

	return construction, nil
}

func (a *Agent) finishNativeConstruction(construction *nativeConstruction, retain bool, err error) {
	if construction == nil {
		return
	}

	a.mu.Lock()
	if construction.immutable {
		a.mu.Unlock()

		return
	}

	if construction.err == nil {
		construction.err = err
	}

	if construction.err != nil && !pi.ProcessContainmentComplete(construction.err) {
		retain = true
	}

	if !retain {
		delete(a.constructions, construction)
	}

	close(construction.done)
	a.mu.Unlock()
}

// awaitNativeConstructions joins every construction admitted before close set
// the Agent fence. A hostile callback may never return, so the join is bounded;
// timeout memoizes one immutable containment-incomplete result on that exact
// owner and leaves its handles quarantined in a.constructions.
func (a *Agent) awaitNativeConstructions() error {
	a.mu.Lock()

	constructions := make([]*nativeConstruction, 0, len(a.constructions))
	for construction := range a.constructions {
		constructions = append(constructions, construction)
	}

	a.mu.Unlock()

	joinCtx, cancelJoin := sessionCloseTurnWaitContext(context.Background())
	defer cancelJoin()

	var joined error

	for _, construction := range constructions {
		select {
		case <-construction.done:
		case <-joinCtx.Done():
			incomplete := fmt.Errorf("%w: join native construction owner: %v",
				pi.ErrProcessContainmentIncomplete, joinCtx.Err())

			a.mu.Lock()
			if !construction.immutable {
				construction.err = incomplete
				construction.immutable = true
			}

			joined = errors.Join(joined, construction.err)
			a.mu.Unlock()

			return joined
		}
	}

	return joined
}

func (a *Agent) recordNativeContainment(err error) {
	if pi.ProcessContainmentComplete(err) {
		return
	}

	a.mu.Lock()
	if a.nativeContainmentErr == nil {
		a.nativeContainmentErr = err
	}
	a.mu.Unlock()
}

func (a *Agent) nativeContainmentError() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.nativeContainmentErr
}

func (a *Agent) setConnection(conn agentClient) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
}

// optionsError reports a construction-time option failure as an internal
// error naming the refused option, or nil when every option validated. The
// caller's params are blameless here — the embedding host built an agent this
// process cannot serve under — so the verdict keeps the option-naming data but
// carries the internal-error code. Both the handshake and session
// establishment report it, because an embedded host can open a session and
// prompt without ever calling initialize, and options that never validated
// must not reach a native process.
func (a *Agent) optionsError() error {
	var reqErr *acp.RequestError
	if !errors.As(a.optionErr, &reqErr) {
		return nil
	}

	return acp.NewInternalError(reqErr.Data)
}

// optionFailure answers a construction-time option verdict as the uniform
// two-key unsupported error naming the option. Which option the agent refuses
// to serve under is the client's business; why it refused is the operator's,
// so the reason goes to the log and never onto the wire.
func optionFailure(log *slog.Logger, field string, err error) error {
	if err == nil {
		return nil
	}

	log.ErrorContext(context.Background(), "pi agent option rejected", slog.String(jsonFieldField, field))

	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	return unsupportedField(field)
}

// Initialize implements ACP initialize.
func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, "initialize")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if optionsErr := a.optionsError(); optionsErr != nil {
		return acp.InitializeResponse{}, optionsErr
	}

	title := a.options.AgentTitle
	positionEncoding := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	// The lifecycle answer is the one family literal this adapter validates on
	// initialize itself, and the only one whose answer is resolved from the
	// active configuration rather than from a compiled-in constant.
	lifecycleAnswer, err := a.negotiateLifecycle(params.Meta)
	if err != nil {
		return acp.InitializeResponse{}, err
	}

	a.mu.Lock()
	a.clientCapabilities = params.ClientCapabilities
	a.positionEncoding = positionEncoding
	a.mu.Unlock()

	capabilityMeta := map[string]any{
		routeMetaKey:         map[string]any{metaFieldVersions: []int{routeVersion}},
		mediaEnvelopeMetaKey: a.mediaEnvelope(),
		piMetaKey: map[string]any{
			metaCapabilityFork: map[string]any{
				"unstable":      true,
				jsonFieldMethod: ForkSessionMethod,
				"request":       "acp.UnstableForkSessionRequest JSON payload only",
				"response":      "acp.UnstableForkSessionResponse JSON payload only",
			},
			"elicitation": map[string]any{
				"unstable": true,
				"scope":    elicitationScopeSession,
				"tracks":   "ACP v1 elicitation",
			},
			"rawEvent": map[string]any{
				"method":         RawEventMethod,
				"enabledBy":      "_meta.pi.rawEvent.enabled",
				"maxBytes":       rawEventMaxBytes,
				"defaultEnabled": false,
			},
			"sessionStore": map[string]any{
				"format": SessionStoreFormat,
				"key":    []string{acpFieldSessionID, "subpath"},
			},
		},
	}

	// Absence of the handoff advertisement is the actionable signal that no
	// handoff root reached this adapter, so the key is emitted only when one
	// is configured.
	if a.inputHandoffRoot() != "" {
		capabilityMeta[handoffMetaKey] = map[string]any{metaFieldVersions: []int{handoffVersion}}
	}

	if a.providerAuth != nil {
		piCapabilities, _ := capabilityMeta[piMetaKey].(map[string]any)
		piCapabilities[providerAuthCapabilityKey] = a.providerAuth.capability()
	}

	resp = acp.InitializeResponse{
		// The lifecycle answer rides the response's own _meta, never
		// agentCapabilities._meta: later protocol work relocates capability
		// objects and initialize _meta survives that move unchanged.
		Meta:            lifecycleAnswer,
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:        capabilityMeta,
			LoadSession: true,
			McpCapabilities: acp.McpCapabilities{
				Http: true,
			},
			PositionEncoding: &positionEncoding,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
			},
		},
	}

	return resp, nil
}

// Authenticate rejects agent-handled auth methods.
func (a *Agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (resp acp.AuthenticateResponse, err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, "authenticate")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	// The reserved family literal is inspected before this method's own
	// refusal, so a request carrying it is answered about the key rather than
	// about the auth method it also named.
	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.AuthenticateResponse{}, refusal
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

// Logout clears auth state owned by this adapter.
func (a *Agent) Logout(_ context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, refusal
	}

	return acp.LogoutResponse{}, nil
}

// HandleExtensionMethod handles pi-specific ACP extension methods. A closed
// agent rejects every extension call up front, before method dispatch and
// before any parameter validation.
func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	// The reserved family literal is inspected before the method is resolved,
	// so every extension call answers about the key it misplaced: the legs this
	// adapter defines whether or not they are configured, and equally a method
	// it defines nowhere, which is answered about the key rather than about the
	// name that carried it.
	if refusal := refuseLifecycleRawMeta(params); refusal != nil {
		return nil, refusal
	}

	if result, handled, err := a.handleAuthExtensionMethod(ctx, method, params); handled {
		return result, err
	}

	switch method {
	case ForkSessionMethod:
		return a.handleForkSession(ctx, params)
	default:
		return nil, acp.NewMethodNotFound(method)
	}
}

// ensureVersion probes `pi --version` once and fails fast below the minimum.
func (a *Agent) ensureVersion(ctx context.Context) (returnErr error) {
	a.versionMu.Lock()
	defer a.versionMu.Unlock()

	if a.versionChecked {
		return a.ensureOpen()
	}

	construction, err := a.beginNativeConstruction()
	if err != nil {
		return err
	}
	defer func() {
		a.mu.Lock()
		generationOwner := construction.runtime
		ownerErr := construction.err
		a.mu.Unlock()

		retain := !pi.ProcessContainmentComplete(returnErr)

		if generationOwner != nil && generationOwner.err != nil {
			ownerErr = errors.Join(ownerErr, generationOwner.err)
			retain = true
		}

		if ownerErr == nil && retain {
			ownerErr = returnErr
		}

		a.finishNativeConstruction(construction, retain, ownerErr)
	}()

	if validationErr := validateProcessIsolationOption(a.options.ProcessIsolation); validationErr != nil {
		return validationErr
	}

	if a.options.ProcessIsolation != nil && a.ContainmentMode() == RuntimeContainmentUnavailable {
		return fmt.Errorf("%w: native process containment is unavailable", ErrProcessContainmentIncomplete)
	}

	executable, err := a.resolveExecutablePath()
	if err != nil {
		return err
	}

	if closedErr := a.ensureOpen(); closedErr != nil {
		return closedErr
	}

	containment, generation, err := a.createRuntimeGeneration(ctx, RuntimeResourceDiscovery)
	if err != nil {
		return err
	}

	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.runtime = generation
		owner.generationRoot = generation.root
		owner.sessionRoot = generation.root
		owner.scratchRelease = generation.release
	})

	if closedErr := a.ensureOpen(); closedErr != nil {
		return generation.finalize(closedErr)
	}

	probeAgentDir, err := generation.prepareVersionProbeAgentDir(a.nativeOwnershipIsolation())
	if err != nil {
		return generation.finalize(err)
	}

	nativeRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		return generation.finalize(err)
	}

	a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
		owner.nativeRelease = nativeRelease
	})

	if closedErr := a.ensureOpen(); closedErr != nil {
		finalErr := generation.finalize(closedErr)
		releaseNativeRootWhenComplete(nativeRelease, finalErr)

		return finalErr
	}

	// This is the final launch gate. Nothing between it and probeVersion invokes
	// external code or drops the Agent close fence.
	if contextErr := ctx.Err(); contextErr != nil {
		finalErr := generation.finalize(contextErr)
		releaseNativeRootWhenComplete(nativeRelease, finalErr)

		return finalErr
	}

	if closedErr := a.ensureOpen(); closedErr != nil {
		finalErr := generation.finalize(closedErr)
		releaseNativeRootWhenComplete(nativeRelease, finalErr)

		return finalErr
	}

	version, err := a.probeVersion(ctx, executable, probeAgentDir, containment)
	if closedErr := a.ensureOpen(); closedErr != nil {
		err = errors.Join(err, closedErr)
	}

	err = generation.finalize(err)
	releaseNativeRootWhenComplete(nativeRelease, err)

	if pi.ProcessContainmentComplete(err) {
		a.updateNativeConstruction(construction, func(owner *nativeConstruction) {
			owner.nativeRelease = nil
			if generation.err == nil {
				owner.scratchRelease = nil
			}
		})
	}

	// The probe is a native process launch, so a probe that would not run, died,
	// or reported an unsupported version is the readiness stage of a native
	// start and carries the uniform failure shape.
	if err != nil {
		return a.nativeStartFailure(ctx, failureCauseProcessExit, err, nil)
	}

	if err := pi.CheckMinimumVersion(version, pi.DefaultMinimumVersion); err != nil {
		return a.nativeStartFailure(ctx, failureCauseProcessExit, err, nil)
	}

	a.versionChecked = true

	return nil
}

func (a *Agent) resolveExecutablePath() (string, error) {
	if a.options.ExecutablePath != "" {
		return a.options.ExecutablePath, nil
	}

	path, err := a.lookPath("pi")
	if err != nil {
		return "", errors.New("pi executable not found in PATH; set WithExecutablePath")
	}

	return path, nil
}

// sessionStart carries the validated inputs of one session lifecycle request.
type sessionStart struct {
	Cwd                   string
	AdditionalDirectories []string
	McpServers            []acp.McpServer
	ResumeID              string
	HydrateEntries        []SessionStoreEntry
	ForkSession           bool
	MetaOptions           PiOptions
	RawMessages           rawMessageConfig
	// PriorBoundary is the durable lifecycle boundary a restored session
	// resumes from. It is what lets the opening snapshot state the quiescence
	// the last incarnation actually proved instead of the class this
	// configuration advertises.
	PriorBoundary lifecycleBoundaryRecord
}
