package piacp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type stagedErrorContext struct{ calls atomic.Int32 }

func (*stagedErrorContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*stagedErrorContext) Done() <-chan struct{}       { return nil }
func (c *stagedErrorContext) Err() error {
	if c.calls.Add(1) > 1 {
		return context.Canceled
	}

	return nil
}
func (*stagedErrorContext) Value(any) any { return nil }

func TestConfigurationAndAdmissionEdges(t *testing.T) {
	_, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, sessionConfigurationRecord{
		Env: map[string]string{}, ExtraPathDirs: []string{"relative"},
	})
	require.Error(t, err)

	err = validateEnvironmentForPlatform(
		map[string]string{"Token": "one", "TOKEN": "two"},
		"env",
		func(string) bool { return false },
		true,
	)
	require.Error(t, err)
	require.NoError(t, validateEnvironmentForPlatform(
		map[string]string{"TOKEN": "one"},
		"env",
		func(string) bool { return false },
		true,
	))

	agent := NewAgent()
	agent.options.ProviderAuthRoot = "/provider-auth"
	agent.options.hostAuthoritySupplied = true
	require.NoError(t, configureProviderAuth(agent))

	agent.markNativeTreeBusy("")
	require.Empty(t, agent.nativeBusyRoots)
	path, err := agent.resolveExecutablePath()
	require.NoError(t, err)
	require.Equal(t, rawEventSourceValue, path)
}

func TestGenerationRetentionEdges(t *testing.T) {
	wantErr := errors.New("cleanup fault")

	empty := &agentSession{agent: NewAgent()}
	empty.retainGeneration("", false, nil)
	require.NoError(t, empty.releaseRetainedGeneration(t.Context()))
	released, err := empty.cleanupGenerationRoot(t.Context(), "", false)
	require.False(t, released)
	require.NoError(t, err)

	retained := &agentSession{agent: NewAgent()}
	retained.retainGeneration("/first", false, nil)
	retained.retainGeneration("/ignored", true, wantErr)
	require.Equal(t, "/first", retained.retainedRoot)
	require.NoError(t, retained.retainedErr)

	incomplete := &agentSession{agent: NewAgent()}
	incomplete.retainGeneration("/incomplete", true, errors.Join(wantErr, ErrContainmentIncomplete))
	require.ErrorIs(t, incomplete.releaseRetainedGeneration(t.Context()), wantErr)
	busyRetention := &agentSession{agent: NewAgent()}
	busyRetention.retainGeneration("/busy", true, ErrNativeTreeBusy)
	require.NoError(t, busyRetention.retainedErr)

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	removed := ""
	materializeRemoveAll = func(path string) error {
		removed = path

		return nil
	}
	released, err = retained.cleanupGenerationRoot(t.Context(), "/ordinary", false)
	require.False(t, released)
	require.NoError(t, err)
	require.Equal(t, "/ordinary", removed)

	managedAgent := edgeManagedAgent(&edgeHostAuthority{})
	managed := &agentSession{agent: managedAgent}
	released, err = managed.cleanupGenerationRoot(t.Context(), "/prepared", true)
	require.False(t, released)
	require.NoError(t, err)
	require.Equal(t, "/prepared", removed)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	released, err = managed.cleanupGenerationRoot(t.Context(), "/prepared", true)
	require.True(t, released)
	require.ErrorIs(t, err, wantErr)

	retainedErr := &agentSession{agent: managedAgent, retainedRoot: "/retained", retainedErr: wantErr}
	require.ErrorIs(t, retainedErr.releaseRetainedGeneration(t.Context()), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	busy := &agentSession{agent: managedAgent, retainedRoot: "/busy", retainedPrepared: true}
	require.ErrorIs(t, busy.releaseRetainedGeneration(t.Context()), ErrNativeTreeBusy)
	_, markedBusy := managedAgent.nativeBusyRoots["/busy"]
	require.True(t, markedBusy)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	otherFailure := &agentSession{agent: managedAgent, retainedRoot: "/other", retainedPrepared: true}
	require.ErrorIs(t, otherFailure.releaseRetainedGeneration(t.Context()), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{}
	materializeRemoveAll = func(string) error { return wantErr }
	removeFailure := &agentSession{agent: managedAgent, retainedRoot: "/remove", retainedPrepared: true}
	require.ErrorIs(t, removeFailure.releaseRetainedGeneration(t.Context()), wantErr)

	materializeRemoveAll = func(string) error { return nil }
	success := &agentSession{agent: managedAgent, retainedRoot: "/success", retainedPrepared: true}
	managedAgent.markNativeTreeBusy("/success")
	require.NoError(t, success.releaseRetainedGeneration(t.Context()))
	require.Empty(t, success.retainedRoot)
	_, stillBusy := managedAgent.nativeBusyRoots["/success"]
	require.False(t, stillBusy)

	changed := &agentSession{agent: managedAgent, retainedRoot: "/changed"}
	materializeRemoveAll = func(string) error {
		changed.mu.Lock()
		changed.retainedRoot = "/replacement"
		changed.mu.Unlock()

		return nil
	}
	require.NoError(t, changed.releaseRetainedGeneration(t.Context()))
	require.Equal(t, "/replacement", changed.retainedRoot)
}

func TestManagedGenerationRetirementEdges(t *testing.T) {
	wantErr := errors.New("retirement fault")
	require.ErrorIs(t, finalizeSessionNativeResources(nil, ErrContainmentIncomplete, "", false, "", nil, nil), ErrContainmentIncomplete)

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	materializeRemoveAll = func(string) error { return wantErr }
	require.ErrorIs(t, finalizeSessionNativeResources(NewAgent(), nil, "/generation", false, "", nil, nil), wantErr)
	require.ErrorIs(t, finalizeSessionNativeResources(nil, nil, "", false, "/session", nil, nil), wantErr)
	materializeRemoveAll = func(string) error { return nil }
	require.NoError(t, finalizeSessionNativeResources(nil, nil, "", false, "/session", nil, nil))
	disposeAgent := edgeManagedAgent(&edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }})
	require.ErrorIs(t, finalizeSessionNativeResources(disposeAgent, nil, "/prepared", true, "", nil, nil), wantErr)
	disposeAgent.options.HostAuthority = &edgeHostAuthority{}
	require.NoError(t, finalizeSessionNativeResources(disposeAgent, nil, "/prepared", true, "", nil, nil))

	ordinary := &agentSession{agent: NewAgent()}
	require.NoError(t, ordinary.retireManagedGeneration(t.Context(), nil))
	require.NoError(t, (&agentSession{}).retireManagedGeneration(t.Context(), nil))

	managedAgent := edgeManagedAgent(&edgeHostAuthority{})
	managed := &agentSession{agent: managedAgent}
	require.ErrorIs(t, managed.retireManagedGeneration(t.Context(), nil), ErrContainmentIncomplete)
	require.True(t, managed.managedCommitPending)

	missing := newTestSessionOutbox(1)
	require.ErrorIs(t, managed.stopSettledManagedGeneration(t.Context(), missing), ErrContainmentIncomplete)
	require.ErrorIs(t, managed.retireManagedGeneration(t.Context(), missing), ErrContainmentIncomplete)

	closedPump := make(chan struct{})
	close(closedPump)
	cancelled := false
	process := newStubProcess(true)
	process.shutdown = wantErr
	complete := newTestSessionOutbox(2)
	complete.proc = process
	complete.pumpDone = closedPump
	complete.pumpCancel = func() { cancelled = true }
	require.NoError(t, managed.stopSettledManagedGeneration(t.Context(), complete))
	require.True(t, cancelled)
	ownerSuccess := newTestSessionOutbox(20)
	ownerSuccess.proc = newStubProcess(true)
	managed.outbox = ownerSuccess
	require.NoError(t, managed.retireManagedGeneration(t.Context(), ownerSuccess))

	process = newStubProcess(true)
	process.shutdown = wantErr
	process.close = wantErr
	failing := newTestSessionOutbox(3)
	failing.proc = process
	require.ErrorIs(t, managed.stopSettledManagedGeneration(t.Context(), failing), wantErr)

	blockedPump := make(chan struct{})
	process = newStubProcess(true)
	blocked := newTestSessionOutbox(4)
	blocked.proc = process
	blocked.pumpDone = blockedPump
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, managed.stopSettledManagedGeneration(ctx, blocked), ErrContainmentIncomplete)

	nonOwner := newTestSessionOutbox(5)
	nonOwner.mu.Lock()
	containment, owner := nonOwner.claimContainmentLocked(containmentOwnerTurn)
	nonOwner.mu.Unlock()
	require.True(t, owner)
	nonOwner.finishContainment(containment, nil)
	managed.outbox = nonOwner
	require.NoError(t, managed.retireManagedGeneration(t.Context(), nonOwner))

	other := newTestSessionOutbox(6)
	managed.outbox = nonOwner
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrContainmentIncomplete)
	managed.outbox = other
	managed.launch.NativeRoot = ""
	managed.generationPrepared = true
	require.NoError(t, managed.reclaimManagedGeneration(t.Context(), other))

	managed.launch.NativeRoot = "/busy"
	managed.generationPrepared = true
	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrNativeTreeBusy)
	_, busy := managedAgent.nativeBusyRoots["/busy"]
	require.True(t, busy)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error { return wantErr }}
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), wantErr)

	managedAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		managed.mu.Lock()
		managed.launch.NativeRoot = "/replacement"
		managed.mu.Unlock()

		return nil
	}}
	managed.launch.NativeRoot = "/changed"
	managed.generationPrepared = true
	require.ErrorIs(t, managed.reclaimManagedGeneration(t.Context(), other), ErrContainmentIncomplete)

	managedAgent.options.HostAuthority = &edgeHostAuthority{}
	managed.launch.NativeRoot = "/success"
	managed.generationPrepared = true
	managedAgent.markNativeTreeBusy("/success")
	require.NoError(t, managed.reclaimManagedGeneration(t.Context(), other))
	require.False(t, managed.generationPrepared)
}

func TestSessionCarrierAndLookupEdges(t *testing.T) {
	id := acp.SessionId(validSessionUUID)
	agent := NewAgent()
	agent.sessionCarriers = nil
	cancelledCtx, cancelImmediately := context.WithCancel(t.Context())
	cancelImmediately()
	_, err := agent.acquireSessionCarrier(cancelledCtx, id)
	require.ErrorIs(t, err, context.Canceled)

	release, err := agent.acquireSessionCarrier(t.Context(), id)
	require.NoError(t, err)
	release()
	release()
	require.Empty(t, agent.sessionCarriers)

	agent.sessionCarriers = nil
	_, err = agent.acquireSessionCarrier(&stagedErrorContext{}, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, agent.sessionCarriers)

	blocked := &sessionCarrierTransition{token: make(chan struct{}, 1)}
	agent.sessionCarriers = map[acp.SessionId]*sessionCarrierTransition{id: blocked}
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()
	_, err = agent.acquireSessionCarrier(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, blocked.users)

	session := &agentSession{id: id, configuration: sessionConfigurationRecord{
		Env: map[string]string{"TOKEN": "value"}, ExtraPathDirs: []string{"/bin"},
	}}
	agent.sessions[id] = session
	agent.deleted[id] = struct{}{}
	require.Nil(t, agent.activeSession(id))
	_, ok := agent.activeSessionConfiguration(id)
	require.False(t, ok)

	delete(agent.deleted, id)
	require.Same(t, session, agent.activeSession(id))
	configuration, ok := agent.activeSessionConfiguration(id)
	require.True(t, ok)
	require.Equal(t, session.configuration, configuration)
	delete(agent.sessions, id)
	_, ok = agent.activeSessionConfiguration(id)
	require.False(t, ok)
}

func TestTurnGenerationStopAndFenceEdges(t *testing.T) {
	wantErr := errors.New("turn fault")
	session := &agentSession{agent: NewAgent()}
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), nil), ErrContainmentIncomplete)

	process := newStubProcess(true)
	client := newStubPiClient()
	complete := newTestSessionOutbox(1)
	complete.proc = process
	complete.client = client
	pumpCancelled := false
	complete.pumpCancel = func() { pumpCancelled = true }
	require.NoError(t, session.stopTurnGeneration(t.Context(), complete))
	require.True(t, pumpCancelled)

	process = newStubProcess(true)
	process.close = ErrContainmentIncomplete
	client = newStubPiClient()
	client.abortErr = errors.Join(wantErr, ErrContainmentIncomplete)
	incomplete := newTestSessionOutbox(2)
	incomplete.proc = process
	incomplete.client = client
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), incomplete), ErrContainmentIncomplete)
	require.ErrorIs(t, session.stopTurnGeneration(t.Context(), incomplete), wantErr)

	process = newStubProcess(true)
	blockedPump := make(chan struct{})
	blocked := newTestSessionOutbox(3)
	blocked.proc = process
	blocked.pumpDone = blockedPump
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, session.stopTurnGeneration(ctx, blocked), ErrContainmentIncomplete)

	require.NoError(t, session.awaitTurnFence())
	session.turnFenceStarted = true
	require.NoError(t, session.awaitTurnFence())
	done := make(chan struct{})
	close(done)
	session.turnFenceDone = done
	session.turnFenceErr = wantErr
	require.ErrorIs(t, session.awaitTurnFence(), wantErr)
}

func TestPumpNativeBoundaryEdges(t *testing.T) {
	wantErr := errors.New("boundary fault")
	require.ErrorIs(t, runNativeBoundaryStep(t.Context(), "error", func() error { return wantErr }), wantErr)
	require.ErrorIs(t, runNativeBoundaryStep(t.Context(), "panic", func() error { panic("boundary") }), ErrContainmentIncomplete)
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runNativeBoundaryStep(cancelledCtx, "cancelled", func() error { return nil }), context.Canceled)

	tracker := newNativeBoundaryTracker()
	require.ErrorIs(t, tracker.run(t.Context(), "panic", func() error { panic("tracker") }), ErrContainmentIncomplete)

	session := &agentSession{agent: NewAgent(), id: acp.SessionId(validSessionUUID)}
	missing := newTestSessionOutbox(1)
	require.NoError(t, session.stopNativeGeneration(t.Context(), missing))

	process := newStubProcess(true)
	client := newStubPiClient()
	client.abortErr = wantErr
	complete := newTestSessionOutbox(2)
	complete.proc = process
	complete.client = client
	pumpCancelled := false
	complete.pumpCancel = func() { pumpCancelled = true }
	require.NoError(t, session.stopNativeGeneration(t.Context(), complete))
	require.True(t, pumpCancelled)

	process = newStubProcess(true)
	process.close = ErrContainmentIncomplete
	client = newStubPiClient()
	client.abortErr = errors.Join(wantErr, ErrContainmentIncomplete)
	incomplete := newTestSessionOutbox(3)
	incomplete.proc = process
	incomplete.client = client
	require.ErrorIs(t, session.stopNativeGeneration(t.Context(), incomplete), wantErr)

	process = newStubProcess(true)
	pumpDone := make(chan struct{})
	join := newTestSessionOutbox(4)
	join.proc = process
	ctx, cancelDuringPump := context.WithCancel(t.Context())
	join.pumpCancel = func() {
		cancelDuringPump()
		go func() {
			time.Sleep(time.Millisecond)
			close(pumpDone)
		}()
	}
	join.pumpDone = pumpDone
	require.NoError(t, session.stopNativeGeneration(ctx, join))

	containmentOutbox := newTestSessionOutbox(5)
	containmentProcess := newStubProcess(true)
	containmentProcess.close = ErrContainmentIncomplete
	containmentOutbox.proc = containmentProcess
	require.ErrorIs(t, session.containGenerationSync(t.Context(), containmentOutbox, "coverage"), ErrContainmentIncomplete)
}
