package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/lifecycle"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	validSessionUUID = "01234567-89ab-cdef-0123-456789abcdef"

	forkParentID = acp.SessionId("11111111-1111-4111-8111-111111111111")
	forkChildID  = "22222222-2222-4222-8222-222222222222"
)

// newStubClientAgent builds an agent whose version probe and pi process
// launch are faked so tests can drive the given stub client directly.
func newStubClientAgent(t *testing.T, client *stubPiClient, opts ...Option) *Agent {
	t.Helper()

	base := make([]Option, 0, 3+len(opts))
	base = append(base,
		testContainmentOption(),
		WithExecutablePath("/fake/pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent := NewAgent(append(base, opts...)...)
	agent.probeVersion = func(context.Context, string, string, pi.ContainmentSpec) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}

	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	return agent
}

func testContainmentOption() Option {
	return func(*Options) {}
}

// testProcessIsolationOption installs the isolated shape: a native identity
// that is never the identity running the test, so the fixture keeps describing
// a launch with a privilege boundary to cross whoever runs it.
func testProcessIsolationOption() Option {
	return func(options *Options) {
		uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
		if uid == 0 {
			uid, gid = 11, 22
		} else {
			uid, gid = uid+1, gid+1
		}
		WithProcessIsolation(ProcessIsolation{
			UID: uid, GID: gid,
			BaseEnvironment:   map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
			StandaloneOwnerID: "acp-go-pi-tests", StandaloneStateRoot: os.TempDir(),
		})(options)
		options.testOnlyNoCredential = true
		options.testOnlyIdentityLockRoot = testIdentityLockRoot()
	}
}

func testIdentityLockRoot() string {
	root := filepath.Join(os.TempDir(), "acp-go-pi-agent-identities-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		panic(err)
	}

	return root
}

// newFailingCloseProcess returns a stub process whose shutdown and close
// both fail, for exercising session-close error joins.
func newFailingCloseProcess() *stubProcess {
	process := newStubProcess(false)
	process.shutdown = errors.New("shutdown")
	process.close = errors.New("close")

	return process
}

// dialogStubClient scripts elicitation and permission responses on top of
// the direct agent client.
type dialogStubClient struct {
	*directAgentClient

	dialogMu            sync.Mutex
	elicitationResponse acp.UnstableCreateElicitationResponse
	elicitationErr      error
	elicitationRequests []acp.UnstableCreateElicitationRequest
	permissionResponse  acp.RequestPermissionResponse
	permissionErr       error
	permissionRequests  []acp.RequestPermissionRequest
}

func newDialogStubClient() *dialogStubClient {
	return &dialogStubClient{directAgentClient: newDirectAgentClient()}
}

func (c *dialogStubClient) CreateElicitation(
	ctx context.Context,
	request acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	c.dialogMu.Lock()
	c.elicitationRequests = append(c.elicitationRequests, request)
	c.dialogMu.Unlock()
	acknowledgeActionRequestWrite(ctx, nil)

	return c.elicitationResponse, c.elicitationErr
}

func (c *dialogStubClient) RequestPermission(
	ctx context.Context,
	request acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	c.dialogMu.Lock()
	c.permissionRequests = append(c.permissionRequests, request)
	c.dialogMu.Unlock()
	acknowledgeActionRequestWrite(ctx, nil)

	return c.permissionResponse, c.permissionErr
}

// faultySessionStore wraps the in-memory store with injectable append and
// load failures.
type faultySessionStore struct {
	*InMemorySessionStore

	appendErr error
	loadErr   error
}

func newFaultySessionStore() *faultySessionStore {
	return &faultySessionStore{InMemorySessionStore: NewInMemorySessionStore()}
}

func (s *faultySessionStore) Append(
	ctx context.Context,
	key SessionKey,
	entries []SessionStoreEntry,
) error {
	if s.appendErr != nil {
		return s.appendErr
	}

	return s.InMemorySessionStore.Append(ctx, key, entries)
}

func (s *faultySessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}

	return s.InMemorySessionStore.Load(ctx, key)
}

// poisonOnAdmissionContext poisons its session the first time turn admission
// inspects it, simulating a session poisoned mid-request.
type poisonOnAdmissionContext struct {
	session *agentSession
	once    sync.Once
}

func (*poisonOnAdmissionContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*poisonOnAdmissionContext) Value(any) any { return nil }

func (*poisonOnAdmissionContext) Done() <-chan struct{} { return nil }

func (c *poisonOnAdmissionContext) Err() error {
	c.once.Do(func() {
		c.session.mu.Lock()
		c.session.poisonCause = "late poison"
		c.session.mu.Unlock()
	})

	return nil
}

func forkRaw(t *testing.T, params acp.UnstableForkSessionRequest) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(params)
	require.NoError(t, err)

	return raw
}

func forkParams(t *testing.T) acp.UnstableForkSessionRequest {
	t.Helper()

	return ForkSessionRequest(forkParentID, t.TempDir())
}

func appendForkParentRows(t *testing.T, store *faultySessionStore, entries ...SessionStoreEntry) {
	t.Helper()

	require.NoError(t, store.InMemorySessionStore.Append(
		t.Context(),
		SessionKey{SessionID: string(forkParentID)},
		entries,
	))
	appendLifecycleBoundaryForRows(t, store.InMemorySessionStore, string(forkParentID), len(entries))
}

func appendLifecycleBoundaryForRows(t *testing.T, store SessionStore, sessionID string, rows int) json.RawMessage {
	t.Helper()

	encoded, err := json.Marshal(lifecycleBoundaryRecord{
		Version:             lifecycleBoundaryVersion,
		StreamID:            "stream",
		NativeRows:          rows,
		NativeState:         nativeStateCommitted,
		RecordedAtUnixMilli: 1,
	})
	require.NoError(t, err)
	require.NoError(t, store.Append(t.Context(), SessionKey{
		SessionID: sessionID,
		Subpath:   SessionStoreLifecycleSubpath,
	}, []SessionStoreEntry{encoded}))

	return encoded
}

// fixtureBytes reads one raster fixture from testdata.
func fixtureBytes(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)

	return data
}

// fixtureBase64 reads one raster fixture from testdata as standard base64.
func fixtureBase64(t *testing.T, name string) string {
	t.Helper()

	return base64.StdEncoding.EncodeToString(fixtureBytes(t, name))
}

func messageRow(t *testing.T, message pi.AgentMessage) SessionStoreEntry {
	t.Helper()
	data, err := json.Marshal(message)
	require.NoError(t, err)
	row, err := json.Marshal(storeRow{Type: storeRowTypeMessage, Message: data})
	require.NoError(t, err)

	return row
}

type stubProcess struct {
	exited        chan struct{}
	waitErr       error
	stderr        string
	shutdown      error
	kill          error
	close         error
	killFunc      func() error
	closeFunc     func() error
	shutdownFunc  func(context.Context) error
	onExited      func()
	killCalls     int
	closeCalls    int
	shutdownCalls int
}

func newStubProcess(exited bool) *stubProcess {
	process := &stubProcess{exited: make(chan struct{})}
	if exited {
		close(process.exited)
	}

	return process
}

func (*stubProcess) CloseStdin() error { return nil }
func (p *stubProcess) Exited() <-chan struct{} {
	if p.onExited != nil {
		p.onExited()
	}

	return p.exited
}
func (p *stubProcess) WaitErr() error     { return p.waitErr }
func (p *stubProcess) StderrTail() string { return p.stderr }
func (p *stubProcess) Shutdown(ctx context.Context) error {
	p.shutdownCalls++
	if p.shutdownFunc != nil {
		return p.shutdownFunc(ctx)
	}

	return p.shutdown
}
func (p *stubProcess) Kill() error {
	p.killCalls++
	if p.killFunc != nil {
		return p.killFunc()
	}

	return p.kill
}
func (p *stubProcess) Close() error {
	p.closeCalls++
	if p.closeFunc != nil {
		return p.closeFunc()
	}

	return p.close
}

type directAgentClient struct {
	done          chan struct{}
	notifyErr     error
	updateErr     error
	notified      []map[string]any
	updates       []acp.SessionUpdate
	notifications []acp.SessionNotification
}

func newDirectAgentClient() *directAgentClient {
	return &directAgentClient{done: make(chan struct{})}
}

func (c *directAgentClient) Done() <-chan struct{} { return c.done }
func (*directAgentClient) CreateElicitation(
	ctx context.Context,
	_ acp.UnstableCreateElicitationRequest,
	_ elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.UnstableCreateElicitationResponse{}, nil
}
func (*directAgentClient) RequestPermission(ctx context.Context, _ acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	acknowledgeActionRequestWrite(ctx, nil)

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}
func (c *directAgentClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.updates = append(c.updates, notification.Update)
	c.notifications = append(c.notifications, notification)

	return c.updateErr
}
func (c *directAgentClient) NotifyExtension(_ context.Context, _ string, value any) error {
	if payload, ok := value.(map[string]any); ok {
		c.notified = append(c.notified, payload)
	}

	return c.notifyErr
}

// lifecycleFailingClient fails only the notifications that carry a lifecycle
// envelope, so a test can break the lifecycle stream while every ordinary
// update still lands.
type lifecycleFailingClient struct {
	*directAgentClient
	err error
}

func (c *lifecycleFailingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if _, carries := notification.Meta[lifecycleMetaKey]; carries {
		return c.err
	}

	return c.directAgentClient.SessionUpdate(ctx, notification)
}

type appendControlledStore struct {
	SessionStore
	mu       sync.Mutex
	failures int
	calls    int
	err      error
}

func (s *appendControlledStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	s.mu.Lock()
	s.calls++
	if s.failures > 0 {
		s.failures--
		err := s.err
		s.mu.Unlock()

		return err
	}
	s.mu.Unlock()

	return s.SessionStore.Append(ctx, key, entries)
}

type errorSessionStore struct {
	SessionStore
	loadErr   error
	listErr   error
	deleteErr error
}

func (s *errorSessionStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return nil, s.loadErr
}

func (s *errorSessionStore) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, s.listErr
}

func (s *errorSessionStore) Delete(context.Context, SessionKey) error {
	return s.deleteErr
}

type stubPiClient struct {
	mu           sync.Mutex
	events       chan pi.Event
	eventsFunc   func() <-chan pi.Event
	uiRequests   chan pi.UIRequest
	done         chan struct{}
	err          error
	abortErr     error
	abortFunc    func(context.Context) error
	promptErr    error
	promptFunc   func(context.Context, string) error
	respondErr   error
	respondFunc  func(pi.UIResponse)
	responses    []pi.UIResponse
	stats        pi.SessionStats
	statsErr     error
	model        pi.Model
	setModelErr  error
	setModelFunc func(provider string, id string)
	thinkingErr  error
	thinkingFunc func(level string)
	startErr     error
	cloneCancel  bool
	cloneErr     error
	autoRetryErr error
	autoRetrySet []bool
	state        pi.SessionState
	stateErr     error
	models       []pi.Model
	modelsErr    error
	commands     []pi.SlashCommand
	commandsErr  error
}

func newStubPiClient() *stubPiClient {
	return &stubPiClient{events: make(chan pi.Event), uiRequests: make(chan pi.UIRequest), done: make(chan struct{})}
}

func (c *stubPiClient) Start(context.Context) error { return c.startErr }
func (c *stubPiClient) Events() <-chan pi.Event {
	if c.eventsFunc != nil {
		return c.eventsFunc()
	}

	return c.events
}
func (c *stubPiClient) UIRequests() <-chan pi.UIRequest { return c.uiRequests }
func (c *stubPiClient) Done() <-chan struct{}           { return c.done }
func (c *stubPiClient) Err() error                      { return c.err }
func (c *stubPiClient) RespondUI(response pi.UIResponse) error {
	c.mu.Lock()
	c.responses = append(c.responses, response)
	respond := c.respondFunc
	c.mu.Unlock()

	if respond != nil {
		respond(response)
	}

	return c.respondErr
}
func (c *stubPiClient) Prompt(ctx context.Context, message string, _ []pi.ImageContent) error {
	if c.promptFunc != nil {
		return c.promptFunc(ctx, message)
	}

	return c.promptErr
}
func (c *stubPiClient) Abort(ctx context.Context) error {
	if c.abortFunc != nil {
		return c.abortFunc(ctx)
	}

	return c.abortErr
}
func (c *stubPiClient) Clone(context.Context) (bool, error) { return c.cloneCancel, c.cloneErr }
func (c *stubPiClient) GetState(context.Context) (pi.SessionState, error) {
	return c.state, c.stateErr
}
func (c *stubPiClient) GetAvailableModels(context.Context) ([]pi.Model, error) {
	return c.models, c.modelsErr
}
func (c *stubPiClient) SetModel(_ context.Context, provider string, id string) (pi.Model, error) {
	if c.setModelFunc != nil {
		c.setModelFunc(provider, id)
	}

	return c.model, c.setModelErr
}
func (c *stubPiClient) SetThinkingLevel(_ context.Context, level string) error {
	if c.thinkingFunc != nil {
		c.thinkingFunc(level)
	}

	return c.thinkingErr
}
func (c *stubPiClient) SetAutoRetry(_ context.Context, enabled bool) error {
	c.autoRetrySet = append(c.autoRetrySet, enabled)

	return c.autoRetryErr
}
func (c *stubPiClient) GetSessionStats(context.Context) (pi.SessionStats, error) {
	return c.stats, c.statsErr
}
func (c *stubPiClient) GetCommands(context.Context) ([]pi.SlashCommand, error) {
	return c.commands, c.commandsErr
}

func requireInvalidRequest(t *testing.T, err error) {
	t.Helper()
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32600, requestError.Code)
}

// lifecycleSession builds a session attached to a recording client with the
// lifecycle extension negotiated, authoritative quiescence included only when
// the test's containment boundary can prove whole-tree vacancy.
func lifecycleSession(t *testing.T, authoritative bool) (*agentSession, *directAgentClient) {
	t.Helper()
	client := newDirectAgentClient()
	agent := NewAgent(testContainmentOption())
	agent.conn = client
	agent.lifecycle = lifecycle.Negotiated{Versions: []int{1}, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	if authoritative {
		agent.lifecycle.AuthoritativeQuiescence = true
		agent.lifecycle.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return &agentSession{agent: agent, id: "lifecycle"}, client
}
