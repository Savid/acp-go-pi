package piacp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

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
