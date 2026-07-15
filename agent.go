package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"sync"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/observer"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	metaCapabilityFork      = "fork"
	elicitationScopeSession = "session"
)

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
	activeLimitErr     error

	versionMu      sync.Mutex
	versionChecked bool

	startPiProcess func(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error)
	probeVersion   func(ctx context.Context, executablePath string) (string, error)
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
	options.RuntimeResourceHooks = instrumentRuntimeResourceHooks(options.RuntimeResourceHooks, observe)

	return &Agent{
		options:          options,
		log:              log,
		observe:          observe,
		sessions:         make(map[acp.SessionId]*agentSession),
		store:            NewInMemorySessionStore(),
		deleted:          make(map[acp.SessionId]struct{}),
		positionEncoding: acp.PositionEncodingKindUtf16,
		activeLimitErr:   validateConcurrencyLimits(options.ConcurrencyLimits),
		startPiProcess:   startRealPiProcess,
		probeVersion:     pi.ProbeVersion,
		lookPath:         exec.LookPath,
	}
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
	a.mu.Lock()

	sessions := make([]*agentSession, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}

	a.sessions = make(map[acp.SessionId]*agentSession)
	a.deleted = make(map[acp.SessionId]struct{})
	a.conn = nil
	a.closed = true
	a.mu.Unlock()

	if len(sessions) > 0 {
		a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
	}

	var closeErrs []error

	for _, session := range sessions {
		if err := session.Close(context.Background()); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}

	return errors.Join(closeErrs...)
}

func (a *Agent) setConnection(conn agentClient) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
}

// Initialize implements ACP initialize.
func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, "initialize")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	if a.activeLimitErr != nil {
		return acp.InitializeResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: a.activeLimitErr.Error()})
	}

	title := a.options.AgentTitle
	positionEncoding := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	a.mu.Lock()
	a.clientCapabilities = params.ClientCapabilities
	a.positionEncoding = positionEncoding
	a.mu.Unlock()

	resp = acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta: map[string]any{
				routeMetaKey: map[string]any{"versions": []int{routeVersion}},
				piMetaKey: map[string]any{
					metaCapabilityFork: map[string]any{
						"unstable": true,
						"method":   ForkSessionMethod,
						"request":  "acp.UnstableForkSessionRequest JSON payload only",
						"response": "acp.UnstableForkSessionResponse JSON payload only",
					},
					"elicitation": map[string]any{
						"unstable": true,
						"scope":    elicitationScopeSession,
						"tracks":   "in-progress ACP elicitation RFD",
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
			},
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

// Authenticate rejects agent-handled auth methods because pi owns auth
// natively (auth.json plus provider environment variables).
func (a *Agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (resp acp.AuthenticateResponse, err error) {
	_, finish := a.observe.StartACP(ctx, params.Meta, "authenticate")
	defer func() { finish(observer.ACPResult{Err: err}) }()

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

// Logout clears auth state owned by this adapter.
func (a *Agent) Logout(_ context.Context, _ acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

// HandleExtensionMethod handles pi-specific ACP extension methods. A closed
// agent rejects every extension call up front, before method dispatch and
// before any parameter validation.
func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if err := a.ensureOpen(); err != nil {
		return nil, err
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

	executable, err := a.resolveExecutablePath()
	if err != nil {
		return err
	}

	version, err := a.probeVersion(ctx, executable)
	if err != nil {
		return err
	}

	if err := pi.CheckMinimumVersion(version, pi.DefaultMinimumVersion); err != nil {
		return err
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
}
