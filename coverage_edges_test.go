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
