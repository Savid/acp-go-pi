package piacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
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

// windowsGOOS names the one platform whose path spelling and environment
// folding differ from every other target this adapter builds for.
const windowsGOOS = "windows"

// absTestPath builds a host-absolute path from POSIX-looking segments, so a
// test states "an absolute working directory" rather than a spelling only one
// platform accepts.
func absTestPath(segments ...string) string {
	root := "/"
	if runtime.GOOS == windowsGOOS {
		root = `C:\`
	}

	return filepath.Join(append([]string{root}, segments...)...)
}

// testCwd is the host-absolute working directory tests open sessions under.
var testCwd = absTestPath("cwd")

// testCwdJSON is testCwd as a JSON string, quotes and separator escaping
// included, so a stored row names the same directory a request carries.
var testCwdJSON = jsonLiteral(testCwd)

// jsonLiteral encodes a value the test itself supplies, for embedding in a raw
// store row. Only a value no test constructs can fail here, so a failure is a
// programming error rather than a case a caller answers.
func jsonLiteral(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	return string(encoded)
}

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
	agent.probeVersion = func(context.Context, string, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}

	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	return agent
}

// testNativeDialog binds concise unit-test entry points to the exact native
// generation they arrange. Production dialog delivery has no unbound adapter.
func testNativeDialog(session *agentSession, request pi.UIRequest) *nativeDialog {
	session.mu.Lock()
	defer session.mu.Unlock()

	return &nativeDialog{request: request, outbox: session.outbox, client: session.client}
}

func (s *agentSession) handleUIDialog(ctx context.Context, request pi.UIRequest) {
	s.handleNativeUIDialog(ctx, testNativeDialog(s, request))
}

func (s *agentSession) handleElicitationDialog(ctx context.Context, request pi.UIRequest) {
	s.handleNativeElicitationDialog(ctx, testNativeDialog(s, request))
}

func (s *agentSession) createDialogElicitation(
	ctx context.Context,
	conn agentClient,
	request pi.UIRequest,
) (pi.UIResponse, bool) {
	return s.createBoundDialogElicitation(ctx, conn, testNativeDialog(s, request))
}

func (s *agentSession) requestPermissionAnswer(
	ctx context.Context,
	request pi.UIRequest,
	prompt pi.PermissionPrompt,
) string {
	return s.requestBoundPermissionAnswer(ctx, testNativeDialog(s, request), prompt)
}

func announcedActionRequest[T any](
	ctx context.Context,
	session *agentSession,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
	failNativeCallbacks ...func(),
) (T, error) {
	session.mu.Lock()
	outbox := session.outbox
	session.mu.Unlock()

	var failNative func()
	if len(failNativeCallbacks) > 0 {
		failNative = failNativeCallbacks[0]
	}

	return announcedBoundActionRequest(ctx, session, kind, send, resolved, outbox, failNative)
}

func testContainmentOption() Option {
	return func(*Options) {}
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
		Configuration:       sessionConfiguration(PiOptions{}),
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

func (s *agentSession) reservePromptForeground(delivery *turnDelivery) error {
	if err := s.claimPromptForeground(context.Background(), delivery); err != nil {
		return err
	}

	if _, _, err := s.reserveClaimedPromptForeground(delivery); err != nil {
		s.finishPromptForeground(delivery)

		return err
	}

	return nil
}

func reserveOutboxPrompt(outbox *sessionOutbox, delivery *turnDelivery) error {
	if err := outbox.claimPromptAdmission(delivery); err != nil {
		return err
	}

	if err := outbox.reserveClaimed(delivery); err != nil {
		outbox.releasePromptAdmission(delivery)

		return err
	}

	return nil
}

func bindTestOutbox(session *agentSession) *sessionOutbox {
	outbox := newTestSessionOutbox(1)
	if err := outbox.bindRuntime(session.proc, session.client, nil, nil, outbox.nativeBoundary); err != nil {
		panic(err)
	}
	session.outbox = outbox
	session.pumpGeneration = 1
	if session.nativeBoundary == nil {
		session.nativeBoundary = outbox.nativeBoundary
	}

	return outbox
}

// attachTestNativeBoundary gives manually assembled fixtures an exact
// construction-owned native boundary. Production constructors must never mint
// this owner implicitly.
func attachTestNativeBoundary(session *agentSession) *agentSession {
	if session == nil || session.nativeBoundary != nil {
		return session
	}
	if session.outbox != nil && session.outbox.nativeBoundary != nil {
		session.nativeBoundary = session.outbox.nativeBoundary
	} else {
		session.nativeBoundary = newNativeBoundaryTracker()
	}

	return session
}

func newTestSessionOutbox(generation uint64) *sessionOutbox {
	outbox := newSessionOutbox(generation, newNativeBoundaryTracker())
	outbox.established = true
	outbox.finishEstablishmentLocked()

	return outbox
}

func bindTestRuntime(
	outbox *sessionOutbox,
	process piProcess,
	client piClient,
	cancel context.CancelFunc,
	done chan struct{},
	_ any,
) {
	if err := outbox.bindRuntime(process, client, cancel, done, outbox.nativeBoundary); err != nil {
		panic(err)
	}
}

func bindTestEstablishingOutbox(session *agentSession, generation uint64, process piProcess, client piClient) *sessionOutbox {
	boundary := newNativeBoundaryTracker()
	outbox := newSessionOutbox(generation, boundary)
	if err := outbox.bindRuntime(process, client, nil, nil, boundary); err != nil {
		panic(err)
	}
	session.proc = process
	session.client = client
	session.nativeBoundary = boundary
	session.outbox = outbox
	session.pumpGeneration = generation

	return outbox
}

func establishTestSession(session *agentSession) {
	if session == nil || session.outbox == nil {
		return
	}

	session.outbox.mu.Lock()
	session.outbox.openingAccepted = true
	session.outbox.established = true
	session.outbox.finishEstablishmentLocked()
	session.outbox.mu.Unlock()
}

func startTestPump(session *agentSession, client piClient) uint64 {
	if session.nativeBoundary == nil {
		session.nativeBoundary = newNativeBoundaryTracker()
	}

	ctx, cancel := context.WithCancel(context.Background())
	generation, err := session.startPumpContext(ctx, cancel, client, session.nativeBoundary)
	if err != nil {
		panic(err)
	}
	session.mu.Lock()
	outbox := session.outbox
	session.mu.Unlock()
	outbox.mu.Lock()
	outbox.established = true
	outbox.finishEstablishmentLocked()
	outbox.mu.Unlock()

	return generation
}

func (o *sessionOutbox) currentCycle() *agentCycle {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.cycle
}

func (o *sessionOutbox) nativeQueueDrained() bool {
	if o == nil {
		return true
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	return o.nativeQueueDepth == 0
}

func (s *agentSession) activeTurnDelivery() *turnDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnEvents
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
	mu              sync.Mutex
	events          chan pi.Event
	eventsFunc      func() <-chan pi.Event
	uiRequests      chan pi.UIRequest
	boundaries      chan pi.ResponseBoundary
	done            chan struct{}
	err             error
	abortErr        error
	abortFunc       func(context.Context) error
	promptErr       error
	promptFunc      func(context.Context, string) error
	promptWriteFunc func(context.Context, string) error
	afterAccepted   func()
	respondErr      error
	respondFunc     func(pi.UIResponse)
	responses       []pi.UIResponse
	stats           pi.SessionStats
	statsErr        error
	model           pi.Model
	setModelErr     error
	setModelFunc    func(provider string, id string)
	thinkingErr     error
	thinkingFunc    func(level string)
	startErr        error
	startFunc       func(context.Context) error
	cloneCancel     bool
	cloneErr        error
	autoRetryErr    error
	autoRetryFunc   func(context.Context, bool) error
	autoRetrySet    []bool
	state           pi.SessionState
	stateErr        error
	stateErrAfter   int
	stateCalls      int
	models          []pi.Model
	modelsErr       error
	commands        []pi.SlashCommand
	commandsErr     error
	commandsFunc    func()
}

func newStubPiClient() *stubPiClient {
	return &stubPiClient{events: make(chan pi.Event), uiRequests: make(chan pi.UIRequest), done: make(chan struct{})}
}

func (c *stubPiClient) Start(ctx context.Context) error {
	if c.startFunc != nil {
		return c.startFunc(ctx)
	}

	return c.startErr
}
func (c *stubPiClient) Events() <-chan pi.Event {
	if c.eventsFunc != nil {
		return c.eventsFunc()
	}

	return c.events
}
func (c *stubPiClient) UIRequests() <-chan pi.UIRequest { return c.uiRequests }
func (c *stubPiClient) ResponseBoundaries() <-chan pi.ResponseBoundary {
	return c.boundaries
}
func (c *stubPiClient) Done() <-chan struct{} { return c.done }
func (c *stubPiClient) Err() error            { return c.err }
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
	return c.PromptWithBoundary(ctx, message, nil, pi.CallBoundary{})
}
func (c *stubPiClient) PromptWithBoundary(
	ctx context.Context,
	message string,
	_ []pi.ImageContent,
	boundary pi.CallBoundary,
) error {
	var release func()
	if boundary.BeforeDispatch != nil {
		var err error

		release, err = boundary.BeforeDispatch()
		if err != nil {
			return err
		}
	}

	if c.promptWriteFunc != nil {
		if err := c.promptWriteFunc(ctx, message); err != nil {
			if release != nil {
				release()
			}

			return err
		}
	}

	if release != nil {
		release()
	}

	var err error
	if c.promptFunc != nil {
		err = c.promptFunc(ctx, message)
	} else {
		err = c.promptErr
	}
	if err != nil {
		return err
	}

	if boundary.Accepted != nil {
		if err := boundary.Accepted(ctx); err != nil {
			return err
		}
	}

	if c.afterAccepted != nil {
		c.afterAccepted()
	}

	return nil
}
func (c *stubPiClient) Abort(ctx context.Context) error {
	if c.abortFunc != nil {
		return c.abortFunc(ctx)
	}

	return c.abortErr
}
func (c *stubPiClient) Clone(context.Context) (bool, error) { return c.cloneCancel, c.cloneErr }
func (c *stubPiClient) GetState(context.Context) (pi.SessionState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stateCalls++
	// stateErrAfter lets a test fail a later read while earlier ones succeed,
	// which is the only way to reach a read-back that follows a good read.
	if c.stateErr != nil && c.stateCalls > c.stateErrAfter {
		return pi.SessionState{}, c.stateErr
	}

	return c.state, nil
}
func (c *stubPiClient) GetStateWithBoundary(ctx context.Context, boundary pi.CallBoundary) (pi.SessionState, error) {
	release, err := runStubCallBoundary(boundary)
	if err != nil {
		return pi.SessionState{}, err
	}
	release()

	return c.GetState(ctx)
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
func (c *stubPiClient) SetModelWithBoundary(
	ctx context.Context,
	provider string,
	id string,
	boundary pi.CallBoundary,
) (pi.Model, error) {
	release, err := runStubCallBoundary(boundary)
	if err != nil {
		return pi.Model{}, err
	}
	release()

	return c.SetModel(ctx, provider, id)
}
func (c *stubPiClient) SetThinkingLevel(_ context.Context, level string) error {
	if c.thinkingFunc != nil {
		c.thinkingFunc(level)
	}

	if c.thinkingErr != nil {
		return c.thinkingErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Real pi acknowledges every level string and applies only the ones it
	// knows, so the double retains its effective level for anything else and a
	// read-back tells the two apart.
	if slices.Contains(pi.ThinkingLevels(), level) {
		c.state.ThinkingLevel = level
	}

	return nil
}
func (c *stubPiClient) SetThinkingLevelWithBoundary(ctx context.Context, level string, boundary pi.CallBoundary) error {
	release, err := runStubCallBoundary(boundary)
	if err != nil {
		return err
	}
	release()

	return c.SetThinkingLevel(ctx, level)
}

func runStubCallBoundary(boundary pi.CallBoundary) (func(), error) {
	if boundary.BeforeDispatch == nil {
		return func() {}, nil
	}

	release, err := boundary.BeforeDispatch()
	if err != nil {
		return nil, err
	}
	if release == nil {
		release = func() {}
	}

	return release, nil
}
func (c *stubPiClient) SetAutoRetry(ctx context.Context, enabled bool) error {
	c.autoRetrySet = append(c.autoRetrySet, enabled)
	if c.autoRetryFunc != nil {
		return c.autoRetryFunc(ctx, enabled)
	}

	return c.autoRetryErr
}
func (c *stubPiClient) GetSessionStats(context.Context) (pi.SessionStats, error) {
	return c.stats, c.statsErr
}
func (c *stubPiClient) GetCommands(context.Context) ([]pi.SlashCommand, error) {
	if c.commandsFunc != nil {
		c.commandsFunc()
	}

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
	agent.lifecycle = lifecycle.Negotiated{Version: 1, UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}}
	if authoritative {
		agent.lifecycle.AuthoritativeQuiescence = true
		agent.lifecycle.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return &agentSession{agent: agent, id: "lifecycle"}, client
}

// testSubmission is the correlation identity a negotiated prompt always carries.
// The emitter validates the notification it renders, so an acceptance stating no
// submission is refused in its own bytes exactly as a consumer would refuse it —
// which is why no fixture may stand one in for a real prompt's correlation.
func testSubmission() lifecycle.Submission {
	return lifecycle.Submission{SubmissionID: "submission", ClientNonce: "nonce"}
}

// launchEnvValue reads a composed launch environment the way the platform
// stores it. Windows folds environment names to one case, so the key a caller
// supplied is not always the key the launch carries.
func launchEnvValue(env map[string]string, key string) string {
	if value, found := env[key]; found {
		return value
	}

	for name, value := range env {
		if strings.EqualFold(name, key) {
			return value
		}
	}

	return ""
}

// testSignalTimeout bounds a rendezvous a test waits on. It is generous
// because it is not measuring anything: it exists only so a signal that will
// never arrive is reported where it was expected instead of hanging the
// package until its own timeout.
const testSignalTimeout = 30 * time.Second

// awaitTestSignal receives one rendezvous signal or fails the test. A signal
// that never comes almost always means the call meant to reach the point that
// sends it was refused before it got there, and a bounded wait names that
// failure instead of leaving a goroutine dump to be read.
func awaitTestSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(testSignalTimeout):
		require.FailNowf(t, "timed out waiting for a test signal", "%s", what)
	}
}

// negotiatedLifecycleSession builds a session on a connection whose lifecycle
// capability the host enabled, which is the only state in which the prompt
// correlation value is required.
func negotiatedLifecycleSession(t *testing.T) *agentSession {
	t.Helper()

	agent := NewAgent(testContainmentOption())

	answer, err := agent.negotiateLifecycle(map[string]any{
		lifecycleMetaKey: map[string]any{"version": json.Number("1")},
	})
	require.NoError(t, err)
	require.Contains(t, answer, lifecycleMetaKey)

	return &agentSession{agent: agent, id: "reserved-keys", cancel: func() {}, turnNonce: "turn-1"}
}

// testSubmissionValue is one well-formed submission identity in wire form.
func testSubmissionValue() map[string]any {
	return map[string]any{"submissionId": "sub-1", "clientNonce": "nonce-1"}
}

// testPromptCorrelation is one well-formed prompt correlation value in wire
// form, so a table probing the other reserved key never fails on this one.
func testPromptCorrelation(n int) map[string]any {
	return map[string]any{"version": 1, "submission": map[string]any{
		"submissionId": fmt.Sprintf("sub-%d", n),
		"clientNonce":  fmt.Sprintf("cn-%d", n),
	}}
}
