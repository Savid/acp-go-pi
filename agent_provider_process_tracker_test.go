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

type mutableProviderInventory struct {
	mu        sync.Mutex
	count     int
	available bool
}

func (i *mutableProviderInventory) ProviderDescendantCount() (int, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.count, i.available
}

func (i *mutableProviderInventory) set(count int, available bool) {
	i.mu.Lock()
	i.count = count
	i.available = available
	i.mu.Unlock()
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
	require.False(t, providerProcessTreeComplete(internalpi.ErrProcessContainmentIncomplete))
	require.True(t, providerProcessTreeComplete(errors.New("ordinary close error")))

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
	require.GreaterOrEqual(t, len(snapshots), 2)
	require.LessOrEqual(t, len(snapshots), roots+1)
	require.Equal(t, roots, snapshots[0])
	require.Equal(t, 0, snapshots[len(snapshots)-1])
}

func TestProviderProcessTrackerRequeriesEveryRoot(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})
	rootA := tracker.register()
	rootB := tracker.register()
	inventoryA := &mutableProviderInventory{count: 1, available: true}
	inventoryB := &mutableProviderInventory{count: 2, available: true}

	rootA.observe(t.Context(), inventoryA)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3}, snapshots)

	inventoryA.set(5, true)
	inventoryB.set(4, true)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9}, snapshots)

	inventoryA.set(5, false)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9}, snapshots)

	inventoryA.set(6, true)
	rootB.observe(t.Context(), inventoryB)
	require.Equal(t, []int{3, 9, 10}, snapshots)
}

func TestProviderProcessTrackerHookCanReenter(t *testing.T) {
	var (
		root      *providerProcessRoot
		snapshots []int
	)
	tracker := newProviderProcessTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(ctx context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
			if count == 1 {
				root.retire(ctx, true)
			}
		},
	})
	root = tracker.register()
	root.observe(t.Context(), testProviderInventory{count: 1, available: true})

	require.Equal(t, []int{1, 0}, snapshots)
}

func TestPiProductionProcessSnapshotLifecycle(t *testing.T) {
	tests := []struct {
		name          string
		closeErr      error
		wantSnapshots []int
	}{
		{name: "complete close resets zero", wantSnapshots: []int{4, 4, 0}},
		{
			name:          "incomplete close preserves nonzero",
			closeErr:      internalpi.ErrProcessContainmentIncomplete,
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
			if agent.ContainmentMode() == RuntimeContainmentBestEffort {
				require.Empty(t, snapshots)
			} else {
				require.Equal(t, test.wantSnapshots, snapshots)
			}
		})
	}
}
