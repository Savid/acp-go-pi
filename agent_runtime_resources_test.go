package piacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

func TestRuntimeResourceHooks(t *testing.T) {
	options := Options{}
	WithRuntimeResourceHooks(RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return func() {}, nil
	}})(&options)
	require.NotNil(t, options.RuntimeResourceHooks.AcquireNativeRoot)

	release, err := acquireNativeRoot(t.Context(), RuntimeResourceHooks{}, RuntimeResourceSession)
	require.NoError(t, err)
	release()

	wantErr := errors.New("full")
	_, err = reserveScratchRoot(t.Context(), RuntimeResourceHooks{ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}}, RuntimeResourceSession)
	require.ErrorIs(t, err, wantErr)

	_, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, nil //nolint:nilnil // A nil release is the invalid hook result under test.
	}}, RuntimeResourcePrompt)
	require.ErrorContains(t, err, "nil release")

	releases := 0
	release, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
		require.Equal(t, RuntimeResourceDiscovery, kind)

		return func() { releases++ }, nil
	}}, RuntimeResourceDiscovery)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)
}

func TestNativeRootReleaseRequiresProcessTreeQuiescence(t *testing.T) {
	releases := 0
	release := func() { releases++ }

	releaseNativeRootWhenComplete(release, internalpi.ErrProcessContainmentIncomplete)
	require.Zero(t, releases)

	releaseNativeRootWhenComplete(release, errors.New("native command failed"))
	require.Equal(t, 1, releases)

	releaseNativeRootWhenComplete(nil, nil)
}

func TestSessionResourceAdmissionFailsBeforeNativeStart(t *testing.T) {
	wantErr := errors.New("resource exhausted")
	discoveryBlocked := newStubClientAgent(t, newStubPiClient(), WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err := discoveryBlocked.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, wantErr)

	scratchBlocked := newStubClientAgent(t, newStubPiClient(), WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot:  func(context.Context, RuntimeResourceKind) (func(), error) { return func() {}, nil },
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err = scratchBlocked.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, wantErr)

	sessionScratchBlocked := newStubClientAgent(t, newStubPiClient(), WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return func() {}, nil },
		ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
			if kind == RuntimeResourceSession {
				return nil, wantErr
			}

			return func() {}, nil
		},
	}))
	_, err = sessionScratchBlocked.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, wantErr)

	scratchReleases := 0
	sessionBlocked := newStubClientAgent(t, newStubPiClient(), WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
			if kind == RuntimeResourceSession {
				return nil, wantErr
			}

			return func() {}, nil
		},
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { scratchReleases++ }, nil
		},
	}))
	_, err = sessionBlocked.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, scratchReleases)

	restoreRuntimeGenerationSeams(t)
	randomCalls := 0
	runtimeGenerationRandRead = func(destination []byte) (int, error) {
		randomCalls++
		if randomCalls == 2 {
			return 0, wantErr
		}

		for index := range destination {
			destination[index] = byte(index + 1)
		}

		return len(destination), nil
	}
	containmentBlocked := newStubClientAgent(t, newStubPiClient())
	_, err = containmentBlocked.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, wantErr)
}

func TestSessionRuntimeCleanupProofBoundaries(t *testing.T) {
	t.Run("ordinary runtime error releases after root deletion", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "session")
		require.NoError(t, os.Mkdir(root, 0o700))
		nativeReleases, scratchReleases := 0, 0
		runtimeErr := errors.New("ordinary shutdown error")

		err := finalizeSessionRuntimeResources(
			runtimeErr,
			func() { nativeReleases++ },
			root,
			func() { scratchReleases++ },
			nil,
		)

		require.ErrorIs(t, err, runtimeErr)
		require.Equal(t, 1, nativeReleases)
		require.Equal(t, 1, scratchReleases)
		require.NoDirExists(t, root)
	})

	t.Run("incomplete containment retains root and both admissions", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "session")
		require.NoError(t, os.Mkdir(root, 0o700))
		nativeReleases, scratchReleases := 0, 0

		err := finalizeSessionRuntimeResources(
			internalpi.ErrProcessContainmentIncomplete,
			func() { nativeReleases++ },
			root,
			func() { scratchReleases++ },
			nil,
		)

		require.ErrorIs(t, err, internalpi.ErrProcessContainmentIncomplete)
		require.Zero(t, nativeReleases)
		require.Zero(t, scratchReleases)
		require.DirExists(t, root)
	})

	t.Run("root deletion failure releases native and retains scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "session")
		require.NoError(t, os.Mkdir(root, 0o700))
		originalRemoveAll := materializeRemoveAll
		deleteErr := errors.New("delete session root")
		materializeRemoveAll = func(path string) error {
			require.Equal(t, root, path)

			return deleteErr
		}
		t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
		nativeReleases, scratchReleases := 0, 0

		err := finalizeSessionRuntimeResources(
			nil,
			func() { nativeReleases++ },
			root,
			func() { scratchReleases++ },
			nil,
		)

		require.ErrorIs(t, err, deleteErr)
		require.Equal(t, 1, nativeReleases)
		require.Zero(t, scratchReleases)
		require.DirExists(t, root)
	})
}

func TestSessionStartRetainsScratchWhenSpawnContainmentIsIncomplete(t *testing.T) {
	scratch := t.TempDir()
	nativeReleases, scratchReleases := 0, 0
	agent := newStubClientAgent(
		t,
		newStubPiClient(),
		WithScratchDir(scratch),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				if kind != RuntimeResourceSession {
					return func() {}, nil
				}

				return func() { nativeReleases++ }, nil
			},
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
		}),
	)
	agent.startPiProcess = func(context.Context, internalpi.LaunchSpec) (piProcess, piClient, error) {
		return nil, nil, internalpi.ErrProcessContainmentIncomplete
	}

	_, err := agent.startSession(t.Context(), sessionStart{Cwd: t.TempDir()})
	require.ErrorIs(t, err, internalpi.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
	require.Zero(t, nativeReleases)
	require.Equal(t, 1, scratchReleases)
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.NotEmpty(t, entries)
}
