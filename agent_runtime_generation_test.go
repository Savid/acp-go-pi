package piacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func restoreRuntimeGenerationSeams(t *testing.T) {
	t.Helper()

	ensureParent := runtimeGenerationEnsureScratchParent
	abs := runtimeGenerationAbs
	mkdirTemp := runtimeGenerationMkdirTemp
	chmod := runtimeGenerationChmod
	removeAll := runtimeGenerationRemoveAll
	randRead := runtimeGenerationRandRead
	t.Cleanup(func() {
		runtimeGenerationEnsureScratchParent = ensureParent
		runtimeGenerationAbs = abs
		runtimeGenerationMkdirTemp = mkdirTemp
		runtimeGenerationChmod = chmod
		runtimeGenerationRemoveAll = removeAll
		runtimeGenerationRandRead = randRead
	})
}

func TestRuntimeGenerationConstructionFailures(t *testing.T) {
	wantErr := errors.New("injected runtime generation failure")

	t.Run("reservation", func(t *testing.T) {
		agent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return nil, wantErr
			},
		}))
		_, _, err := agent.createRuntimeGeneration(t.Context(), RuntimeResourceDiscovery)
		require.ErrorIs(t, err, wantErr)
	})

	for _, test := range []struct {
		name  string
		setup func()
	}{
		{name: "scratch parent", setup: func() {
			runtimeGenerationEnsureScratchParent = func(string) (string, error) { return "", wantErr }
		}},
		{name: "absolute parent", setup: func() {
			runtimeGenerationAbs = func(string) (string, error) { return "", wantErr }
		}},
		{name: "mkdir", setup: func() {
			runtimeGenerationMkdirTemp = func(string, string) (string, error) { return "", wantErr }
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreRuntimeGenerationSeams(t)
			releases := 0
			agent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
				ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
					return func() { releases++ }, nil
				},
			}))
			test.setup()
			_, _, err := agent.createRuntimeGeneration(t.Context(), RuntimeResourceDiscovery)
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, 1, releases)
		})
	}

	t.Run("chmod", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		root := filepath.Join(t.TempDir(), "acp-go-pi-runtime-chmod")
		runtimeGenerationMkdirTemp = func(string, string) (string, error) {
			require.NoError(t, os.Mkdir(root, 0o700))

			return root, nil
		}
		runtimeGenerationChmod = func(string, os.FileMode) error { return wantErr }
		removed, releases := 0, 0
		runtimeGenerationRemoveAll = func(path string) error {
			require.Equal(t, root, path)
			removed++

			return nil
		}
		agent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { releases++ }, nil
			},
		}))
		_, _, err := agent.createRuntimeGeneration(t.Context(), RuntimeResourceDiscovery)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, 1, removed)
		require.Equal(t, 1, releases)
	})

	t.Run("identity", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		runtimeGenerationRandRead = func([]byte) (int, error) { return 0, wantErr }
		removed, releases := 0, 0
		runtimeGenerationRemoveAll = func(string) error {
			removed++

			return nil
		}
		agent := NewAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { releases++ }, nil
			},
		}))
		_, _, err := agent.createRuntimeGeneration(t.Context(), RuntimeResourceDiscovery)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, 1, removed)
		require.Equal(t, 1, releases)
	})
}

func TestContainmentSpecConstructionFailures(t *testing.T) {
	wantErr := errors.New("injected containment identity failure")
	agent := NewAgent(testContainmentOption())

	t.Run("random identity", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		runtimeGenerationRandRead = func([]byte) (int, error) { return 0, wantErr }
		_, err := agent.containmentSpecForRoot("/scratch", "/scratch/acp-go-pi-runtime-one", RuntimeResourceRuntime)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("absolute parent", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		runtimeGenerationAbs = func(string) (string, error) { return "", wantErr }
		_, err := agent.containmentSpecForRoot("/scratch", "/scratch/acp-go-pi-runtime-one", RuntimeResourceRuntime)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("absolute root", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		calls := 0
		runtimeGenerationAbs = func(path string) (string, error) {
			calls++
			if calls == 2 {
				return "", wantErr
			}

			return path, nil
		}
		_, err := agent.containmentSpecForRoot("/scratch", "/scratch/acp-go-pi-runtime-one", RuntimeResourceRuntime)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestRuntimeGenerationFinalizeBoundaries(t *testing.T) {
	require.ErrorIs(t, (*runtimeGeneration)(nil).finalize(internalpi.ErrProcessContainmentIncomplete), internalpi.ErrProcessContainmentIncomplete)

	t.Run("incomplete retains", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		removed, released := 0, 0
		runtimeGenerationRemoveAll = func(string) error {
			removed++

			return nil
		}
		generation := &runtimeGeneration{root: "/scratch/acp-go-pi-runtime-one", release: func() { released++ }}
		err := generation.finalize(internalpi.ErrProcessContainmentIncomplete)
		require.ErrorIs(t, err, internalpi.ErrProcessContainmentIncomplete)
		require.Zero(t, removed)
		require.Zero(t, released)
	})

	t.Run("remove failure is memoized", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		wantErr := errors.New("remove generation")
		removed, released := 0, 0
		runtimeGenerationRemoveAll = func(string) error {
			removed++

			return wantErr
		}
		generation := &runtimeGeneration{root: "/scratch/acp-go-pi-runtime-one", release: func() { released++ }}
		require.ErrorIs(t, generation.finalize(nil), wantErr)
		require.ErrorIs(t, generation.finalize(nil), wantErr)
		require.Equal(t, 1, removed)
		require.Zero(t, released)
	})

	t.Run("success releases", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		removed, released := 0, 0
		runtimeGenerationRemoveAll = func(string) error {
			removed++

			return nil
		}
		generation := &runtimeGeneration{root: "/scratch/acp-go-pi-runtime-one", release: func() { released++ }}
		require.NoError(t, generation.finalize(nil))
		require.Equal(t, 1, removed)
		require.Equal(t, 1, released)
	})
}
