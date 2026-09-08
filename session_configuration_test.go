package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPiOptionsMetaPreservesExplicitEmptyConfiguration(t *testing.T) {
	meta := PiOptions{
		Env:           map[string]string{},
		ExtraPathDirs: []string{},
	}.Meta()

	options, presence, err := piOptionsFromMetaWithConfigurationPresence(meta)
	require.NoError(t, err)
	require.True(t, presence.Env)
	require.True(t, presence.ExtraPathDirs)
	require.NotNil(t, options.Env)
	require.NotNil(t, options.ExtraPathDirs)
}

func TestResolveSessionConfigurationDistinguishesOmittedAndExplicitEmpty(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "stored"},
		ExtraPathDirs: []string{absTestPath("stored", "first"), absTestPath("stored", "second")},
	}

	omitted, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, stored)
	require.NoError(t, err)
	require.Equal(t, stored.Env, omitted.Env)
	require.Equal(t, stored.ExtraPathDirs, omitted.ExtraPathDirs)

	explicit, err := resolveSessionConfiguration(
		PiOptions{Env: map[string]string{}, ExtraPathDirs: []string{}},
		sessionConfigurationPresence{Env: true, ExtraPathDirs: true},
		stored,
	)
	require.NoError(t, err)
	require.Empty(t, explicit.Env)
	require.Empty(t, explicit.ExtraPathDirs)
	require.NotNil(t, explicit.Env)
	require.NotNil(t, explicit.ExtraPathDirs)
}

func TestRecoveredEmptyConfigurationKeepsLifecycleFingerprint(t *testing.T) {
	start := sessionStart{Cwd: testCwd, ResumeID: "session"}
	recovered := start
	var err error
	recovered.MetaOptions, err = resolveSessionConfiguration(
		PiOptions{},
		sessionConfigurationPresence{},
		sessionConfiguration(PiOptions{}),
	)
	require.NoError(t, err)
	require.Equal(t, sessionStartFingerprint(start), sessionStartFingerprint(recovered))
}

func TestSessionCarrierTransitionHonorsCancellation(t *testing.T) {
	agent := NewAgent()

	releaseOwner, err := agent.acquireSessionCarrier(t.Context(), "session")
	require.NoError(t, err)

	waiterCtx, cancelWaiter := context.WithCancel(t.Context())
	cancelWaiter()

	_, err = agent.acquireSessionCarrier(waiterCtx, "session")
	require.ErrorIs(t, err, context.Canceled)

	agent.mu.Lock()
	transition := agent.sessionCarriers["session"]
	users := 0
	if transition != nil {
		users = transition.users
	}
	agent.mu.Unlock()
	require.NotNil(t, transition)
	require.Equal(t, 1, users)

	releaseIndependent, err := agent.acquireSessionCarrier(t.Context(), "independent")
	require.NoError(t, err)
	releaseIndependent()

	releaseOwner()
	releaseOwner()

	agent.mu.Lock()
	remainingTransitions := len(agent.sessionCarriers)
	agent.mu.Unlock()
	require.Zero(t, remainingTransitions)
}

func TestLifecycleBoundaryPersistsSessionConfiguration(t *testing.T) {
	store := NewInMemorySessionStore()
	session := &agentSession{
		agent: NewAgent(WithSessionStore(store)),
		id:    "session",
		configuration: sessionConfigurationRecord{
			Env:           map[string]string{"TOKEN": "durable"},
			ExtraPathDirs: []string{absTestPath("first"), absTestPath("second")},
		},
	}

	require.NoError(t, session.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{
		StreamID:    "stream",
		NativeState: nativeStateCommitted,
	}))

	entries, err := store.Load(t.Context(), SessionKey{
		SessionID: "session",
		Subpath:   SessionStoreLifecycleSubpath,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0], &raw))
	require.Contains(t, raw, "configuration")

	record, err := decodeLifecycleBoundaryRecord(entries[0])
	require.NoError(t, err)
	require.Equal(t, session.configuration, record.Configuration)
}

func TestStoredSessionConfigurationRequiresBothFields(t *testing.T) {
	tests := map[string]json.RawMessage{
		"missing configuration": json.RawMessage(`{"version":1,"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
		"missing env":           json.RawMessage(`{"version":1,"configuration":{"extraPathDirs":[]},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
		"missing path":          json.RawMessage(`{"version":1,"configuration":{"env":{}},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
	}

	for name, boundary := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "session"}, []SessionStoreEntry{
				json.RawMessage(`{"type":"session"}`),
			}))
			require.NoError(t, store.Append(t.Context(), SessionKey{
				SessionID: "session",
				Subpath:   SessionStoreLifecycleSubpath,
			}, []SessionStoreEntry{boundary}))

			agent := NewAgent(WithSessionStore(store))
			_, _, err := agent.loadCurrentStoreEntries(t.Context(), "session")
			requireSessionResumeIncompatible(t, err, "configuration")
		})
	}
}

func TestStoredSessionConfigurationRejectsUnsafeValues(t *testing.T) {
	store := NewInMemorySessionStore()
	appendStoredSessionWithConfiguration(t, store, "session", sessionConfigurationRecord{
		Env:           map[string]string{"PATH": "/untrusted"},
		ExtraPathDirs: []string{},
	})

	agent := NewAgent(WithSessionStore(store))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), "session")
	requireSessionResumeIncompatible(t, err, metaOptionPath(metaEnvKey))
}

func TestColdRestoreReconstructsOmittedConfigurationAndHonorsExplicitEmpty(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "stored"},
		ExtraPathDirs: []string{absTestPath("stored", "first"), absTestPath("stored", "second")},
	}

	tests := map[string]struct {
		meta      map[string]any
		wantEnv   bool
		wantPaths []string
	}{
		"omitted": {
			wantEnv:   true,
			wantPaths: stored.ExtraPathDirs,
		},
		"explicit empty": {
			meta:      PiOptions{Env: map[string]string{}, ExtraPathDirs: []string{}}.Meta(),
			wantPaths: []string{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			appendStoredSessionWithConfiguration(t, store, validSessionUUID, stored)

			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: validSessionUUID}
			agent := newStubClientAgent(t, client, WithSessionStore(store))
			restored, err := agent.restoreSession(t.Context(), validSessionUUID, sessionStart{
				Cwd:      t.TempDir(),
				ResumeID: validSessionUUID,
			}, test.meta)
			require.NoError(t, err)
			require.True(t, restored.started)
			restored.finish()

			require.Equal(t, test.wantPaths, restored.session.launch.ExtraPathDirs)
			value, found := restored.session.launch.Env["TOKEN"]
			require.Equal(t, test.wantEnv, found)
			if found {
				require.Equal(t, "stored", value)
			}
			require.NoError(t, restored.session.Close(t.Context()))
		})
	}
}

// TestChangedActiveCarrierContainsBeforeSuccessorConstruction pins the
// production restore cutover. While predecessor containment is blocked, the
// old carrier remains the only addressable session and successor construction
// has not begun. Only after containment completes may the replacement launch
// and become addressable.
func TestChangedActiveCarrierContainsBeforeSuccessorConstruction(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "old"},
		ExtraPathDirs: []string{absTestPath("stored", "bin")},
	}
	store := NewInMemorySessionStore()
	appendStoredSessionWithConfiguration(t, store, validSessionUUID, stored)

	initialClient := newStubPiClient()
	initialClient.state = pi.SessionState{SessionID: validSessionUUID}
	agent := newStubClientAgent(t, initialClient, WithSessionStore(store))

	start := sessionStart{Cwd: t.TempDir(), ResumeID: validSessionUUID}
	initial, err := agent.restoreSession(t.Context(), validSessionUUID, start, nil)
	require.NoError(t, err)
	initial.finish()

	predecessor := initial.session
	predecessorProcess, ok := predecessor.proc.(*stubProcess)
	require.True(t, ok)

	containmentEntered := make(chan struct{})
	releaseContainment := make(chan struct{})
	predecessorProcess.shutdownFunc = func(context.Context) error {
		close(containmentEntered)
		<-releaseContainment

		return nil
	}

	successorConstruction := make(chan struct{})
	successorClient := newStubPiClient()
	successorClient.state = pi.SessionState{SessionID: validSessionUUID}
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		close(successorConstruction)

		return newStubProcess(false), successorClient, nil
	}

	type restoreResult struct {
		restored restoredSession
		err      error
	}
	result := make(chan restoreResult, 1)
	go func() {
		restored, restoreErr := agent.restoreSession(context.Background(), validSessionUUID, start,
			PiOptions{Env: map[string]string{"TOKEN": "new"}}.Meta())
		result <- restoreResult{restored: restored, err: restoreErr}
	}()

	<-containmentEntered
	select {
	case <-successorConstruction:
		t.Fatal("successor construction began before predecessor containment")
	default:
	}

	addressed, err := agent.session(validSessionUUID)
	require.NoError(t, err)
	require.Same(t, predecessor, addressed,
		"the predecessor stopped being addressable before containment completed")

	close(releaseContainment)
	replacement := <-result
	require.NoError(t, replacement.err)
	require.True(t, replacement.restored.started)
	replacement.restored.finish()

	select {
	case <-successorConstruction:
	default:
		t.Fatal("successor construction did not begin after predecessor containment")
	}

	addressed, err = agent.session(validSessionUUID)
	require.NoError(t, err)
	require.Same(t, replacement.restored.session, addressed)
	require.NotSame(t, predecessor, addressed)
	require.Equal(t, "new", addressed.configuration.Env["TOKEN"])
	require.Equal(t, stored.ExtraPathDirs, addressed.configuration.ExtraPathDirs,
		"the omitted carrier was not reconstructed during rotation")

	require.NoError(t, agent.Close())
}

// TestChangedActiveCarrierContainmentFailurePublishesNothing pins the failure
// half of the same production cutover. An incomplete predecessor remains the
// exact addressable owner, and no successor reaches even its native launch.
func TestChangedActiveCarrierContainmentFailurePublishesNothing(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "old"},
		ExtraPathDirs: []string{absTestPath("stored", "bin")},
	}
	store := NewInMemorySessionStore()
	appendStoredSessionWithConfiguration(t, store, validSessionUUID, stored)

	initialClient := newStubPiClient()
	initialClient.state = pi.SessionState{SessionID: validSessionUUID}
	agent := newStubClientAgent(t, initialClient, WithSessionStore(store))

	start := sessionStart{Cwd: t.TempDir(), ResumeID: validSessionUUID}
	initial, err := agent.restoreSession(t.Context(), validSessionUUID, start, nil)
	require.NoError(t, err)
	initial.finish()

	predecessor := initial.session
	predecessorProcess, ok := predecessor.proc.(*stubProcess)
	require.True(t, ok)
	predecessorProcess.close = ErrContainmentIncomplete

	successorConstruction := make(chan struct{})
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		close(successorConstruction)

		return nil, nil, errors.New("successor must not launch")
	}

	_, err = agent.restoreSession(t.Context(), validSessionUUID, start,
		PiOptions{Env: map[string]string{"TOKEN": "new"}}.Meta())
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	select {
	case <-successorConstruction:
		t.Fatal("successor construction began after incomplete predecessor containment")
	default:
	}

	addressed, err := agent.session(validSessionUUID)
	require.NoError(t, err)
	require.Same(t, predecessor, addressed)
	require.Equal(t, "old", addressed.configuration.Env["TOKEN"])
	require.ErrorIs(t, agent.Close(), ErrContainmentIncomplete)
}

func TestChangedActiveCarrierRetainsFailedCloseCommit(t *testing.T) {
	store := newFaultySessionStore()
	appendStoredSessionWithConfiguration(t, store, validSessionUUID, sessionConfigurationRecord{
		Env: map[string]string{"TOKEN": "old"}, ExtraPathDirs: []string{},
	})
	client := newStubPiClient()
	client.state = pi.SessionState{SessionID: validSessionUUID}
	agent := newStubClientAgent(t, client, WithSessionStore(store))
	start := sessionStart{Cwd: t.TempDir(), ResumeID: validSessionUUID}
	initial, err := agent.restoreSession(t.Context(), validSessionUUID, start, nil)
	require.NoError(t, err)
	initial.finish()

	want := errors.New("close boundary store refused")
	store.appendErr = want
	agent.startPiProcess = func(context.Context, pi.LaunchSpec) (piProcess, piClient, error) {
		t.Fatal("replacement launched after a failed close commit")

		return nil, nil, nil
	}

	_, err = agent.restoreSession(t.Context(), validSessionUUID, start,
		PiOptions{Env: map[string]string{"TOKEN": "new"}}.Meta())
	require.ErrorIs(t, err, want)
	addressed, err := agent.session(validSessionUUID)
	require.NoError(t, err)
	require.Same(t, initial.session, addressed)
	require.DirExists(t, addressed.sessionRoot, "failed commit must retain its native state")
	require.Contains(t, agent.retainedSessions, addressed)
	require.ErrorIs(t, agent.Close(), want)
}

func appendStoredSessionWithConfiguration(
	t *testing.T,
	store SessionStore,
	sessionID string,
	configuration sessionConfigurationRecord,
) {
	t.Helper()

	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: sessionID}, []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"` + sessionID + `"}`),
	}))

	boundary, err := json.Marshal(lifecycleBoundaryRecord{
		Version:             lifecycleBoundaryVersion,
		Configuration:       configuration,
		StreamID:            "stream",
		NativeRows:          1,
		NativeState:         nativeStateCommitted,
		RecordedAtUnixMilli: 1,
	})
	require.NoError(t, err)
	require.NoError(t, store.Append(t.Context(), SessionKey{
		SessionID: sessionID,
		Subpath:   SessionStoreLifecycleSubpath,
	}, []SessionStoreEntry{boundary}))
}

func requireSessionResumeIncompatible(t *testing.T, err error, field string) {
	t.Helper()

	requireInvalidParams(t, err)
	require.ErrorContains(t, err, "session resume incompatible")
	require.ErrorContains(t, err, field)
}
