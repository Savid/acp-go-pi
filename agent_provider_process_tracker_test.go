package piacp

import (
	"context"
	"errors"
	"sync"
	"testing"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

type testProviderInventory struct {
	count     int
	available bool
}

type inventoryPiProcess struct {
	*stubProcess
	count int
}

func (p inventoryPiProcess) ProviderDescendantCount() (int, bool) {
	return p.count, true
}

func (i testProviderInventory) ProviderDescendantCount() (int, bool) {
	return i.count, i.available
}

func TestProviderProcessTrackerAggregatesOnlyCompleteInventories(t *testing.T) {
	var (
		mu        sync.Mutex
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, kind RuntimeProcessKind, count int) {
			require.Equal(t, RuntimeProcessProviderDescendant, kind)
			mu.Lock()
			snapshots = append(snapshots, count)
			mu.Unlock()
		},
	})

	unknown := tracker.register()
	known := tracker.register()
	unknown.observe(t.Context(), struct{}{})
	unknown.observe(t.Context(), testProviderInventory{count: 9})
	unknown.observe(t.Context(), testProviderInventory{count: -1, available: true})
	known.observe(t.Context(), testProviderInventory{count: 2, available: true})
	require.Empty(t, snapshots)

	unknown.observe(t.Context(), testProviderInventory{count: 3, available: true})
	require.Equal(t, []int{5}, snapshots)

	unknown.retire(t.Context(), false)
	require.Equal(t, []int{5}, snapshots)
	require.False(t, providerProcessTreeProven(internalpi.ErrProcessTreeNotQuiescent))
	require.True(t, providerProcessTreeProven(errors.New("ordinary close error")))

	unknown.retire(t.Context(), true)
	known.retire(t.Context(), true)
	known.retire(t.Context(), true)
	known.observe(t.Context(), testProviderInventory{count: 7, available: true})
	require.Equal(t, []int{5, 2, 0}, snapshots)
}

func TestProviderProcessTrackerConcurrentLifecycle(t *testing.T) {
	const roots = 16

	var (
		mu        sync.Mutex
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			mu.Lock()
			snapshots = append(snapshots, count)
			mu.Unlock()
		},
	})
	registered := make([]*providerProcessRoot, roots)
	for index := range registered {
		registered[index] = tracker.register()
	}

	var wg sync.WaitGroup
	for _, root := range registered {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root.observe(t.Context(), testProviderInventory{count: 1, available: true})
		}()
	}
	wg.Wait()

	for _, root := range registered {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root.retire(t.Context(), true)
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, snapshots, roots+1)
	require.Equal(t, roots, snapshots[0])
	require.Equal(t, 0, snapshots[len(snapshots)-1])
}

func TestPiProductionProcessSnapshotLifecycle(t *testing.T) {
	tests := []struct {
		name          string
		closeErr      error
		wantSnapshots []int
	}{
		{name: "proven close resets zero", wantSnapshots: []int{4, 4, 0}},
		{
			name:          "unproven close preserves nonzero",
			closeErr:      internalpi.ErrProcessTreeNotQuiescent,
			wantSnapshots: []int{4, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var snapshots []int
			client := newStubPiClient()
			client.state = internalpi.SessionState{SessionID: "snapshot-session"}
			agent := newStubClientAgent(t, client, WithRuntimeResourceHooks(RuntimeResourceHooks{
				ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
					snapshots = append(snapshots, count)
				},
			}))
			process := inventoryPiProcess{stubProcess: newStubProcess(false), count: 4}
			process.close = test.closeErr
			agent.startPiProcess = func(context.Context, internalpi.LaunchSpec) (piProcess, piClient, error) {
				return process, client, nil
			}

			session, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
			require.NoError(t, err)
			require.ErrorIs(t, session.Close(t.Context()), test.closeErr)
			require.Equal(t, test.wantSnapshots, snapshots)
		})
	}
}
