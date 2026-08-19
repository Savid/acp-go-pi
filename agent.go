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
	Done() <-chan struct{}
	Err() error
	RespondUI(response pi.UIResponse) error
	Prompt(ctx context.Context, message string, images []pi.ImageContent) error
	Abort(ctx context.Context) error
	Clone(ctx context.Context) (bool, error)
	GetState(ctx context.Context) (pi.SessionState, error)
	GetAvailableModels(ctx context.Context) ([]pi.Model, error)
	SetModel(ctx context.Context, provider string, modelID string) (pi.Model, error)
	SetThinkingLevel(ctx context.Context, level string) error
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
	constructions        sync.WaitGroup
	closeOnce            sync.Once
	closeErr             error

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
			a.processes.register()
			a.recordNativeContainment(err)
		}

		return nil, nil, nil, err
	}

	root := a.processes.register()
	root.observe(ctx, process)

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
			agent.log.DebugContext(context.Background(), "close pi ACP agent failed", slog.String(jsonFieldError, closeErr.Error()))
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
	a.closeOnce.Do(func() {
		a.closeErr = a.close()
	})

	return a.closeErr
}

func (a *Agent) close() error {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()

	a.constructions.Wait()

	a.mu.Lock()

	sessions := make([]*agentSession, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}

	a.sessions = make(map[acp.SessionId]*agentSession)
	a.deleted = make(map[acp.SessionId]struct{})
	a.conn = nil
	a.mu.Unlock()

	if len(sessions) > 0 {
		a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
	}

	var closeErrs []error

	for _, session := range sessions {
		// Embedded shutdown detaches the connection above, so no incarnation
		// survives this call to carry an event: the stream is fenced here and
		// the boundary that follows emits nothing on it. The rungs that are not
		// emissions still run unconditionally — the containment proof, both
		// durable commits, and failing closed when a commit is refused — so
		// Agent.Close makes exactly the durable commit a wire close would have
		// made rather than dropping the state with the wrapper.
		session.fenceLifecycleStream()

		if err := session.Close(context.Background()); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}

	closeErr := errors.Join(closeErrs...)
	if !pi.ProcessContainmentComplete(closeErr) {
		a.recordNativeContainment(closeErr)

		return closeErr
	}

	return errors.Join(closeErr, a.nativeContainmentError())
}

func (a *Agent) beginNativeConstruction() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errAgentClosed
	}

	a.constructions.Add(1)

	return nil
}

func (a *Agent) endNativeConstruction() {
	a.constructions.Done()
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

	log.ErrorContext(context.Background(), "pi agent option rejected", slog.String(jsonFieldField, field), slog.Any("error", err))

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

	// Every request-bearing extension leg inspects the reserved family literal
	// before its own validation or refusal, whether or not the leg it names is
	// configured on this agent.
	if refusal := refuseLifecycleExtensionMeta(method, params); refusal != nil {
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
func (a *Agent) ensureVersion(ctx context.Context) error {
	a.versionMu.Lock()
	defer a.versionMu.Unlock()

	if a.versionChecked {
		return nil
	}

	if err := validateProcessIsolationOption(a.options.ProcessIsolation); err != nil {
		return err
	}

	if a.options.ProcessIsolation != nil && a.ContainmentMode() == RuntimeContainmentUnavailable {
		return fmt.Errorf("%w: native process containment is unavailable", ErrProcessContainmentIncomplete)
	}

	executable, err := a.resolveExecutablePath()
	if err != nil {
		return err
	}

	containment, generation, err := a.createRuntimeGeneration(ctx, RuntimeResourceDiscovery)
	if err != nil {
		return err
	}

	probeAgentDir, err := generation.prepareVersionProbeAgentDir(a.nativeOwnershipIsolation())
	if err != nil {
		return generation.finalize(err)
	}

	nativeRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		return generation.finalize(err)
	}

	version, err := a.probeVersion(ctx, executable, probeAgentDir, containment)
	err = generation.finalize(err)
	releaseNativeRootWhenComplete(nativeRelease, err)

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
