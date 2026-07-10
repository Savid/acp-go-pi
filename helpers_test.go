package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

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
		WithExecutablePath("/fake/pi"),
		WithHome(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent := NewAgent(append(base, opts...)...)
	agent.probeVersion = func(context.Context, string) (string, error) {
		return pi.DefaultMinimumVersion, nil
	}

	process := newStubProcess(false)
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		return process, client, nil
	}

	return agent
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

	elicitationResponse acp.UnstableCreateElicitationResponse
	elicitationErr      error
	permissionResponse  acp.RequestPermissionResponse
	permissionErr       error
}

func newDialogStubClient() *dialogStubClient {
	return &dialogStubClient{directAgentClient: newDirectAgentClient()}
}

func (c *dialogStubClient) CreateElicitation(
	context.Context,
	acp.UnstableCreateElicitationRequest,
	elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.elicitationResponse, c.elicitationErr
}

func (c *dialogStubClient) RequestPermission(
	context.Context,
	acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
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

// poisonOnDoneContext poisons its session the first time Done is observed,
// simulating a session poisoned mid-request.
type poisonOnDoneContext struct {
	session *agentSession
	once    sync.Once
}

func (*poisonOnDoneContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*poisonOnDoneContext) Err() error { return nil }

func (*poisonOnDoneContext) Value(any) any { return nil }

func (c *poisonOnDoneContext) Done() <-chan struct{} {
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
}

func messageRow(t *testing.T, message pi.AgentMessage) SessionStoreEntry {
	t.Helper()
	data, err := json.Marshal(message)
	require.NoError(t, err)
	row, err := json.Marshal(storeRow{Type: storeRowTypeMessage, Message: data})
	require.NoError(t, err)

	return row
}
