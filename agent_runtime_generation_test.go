package piacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	writeProbeAgentDir := runtimeGenerationWriteProbeAgentDir
	handoffNativeTree := runtimeGenerationHandoffNativeTree
	t.Cleanup(func() {
		runtimeGenerationEnsureScratchParent = ensureParent
		runtimeGenerationAbs = abs
		runtimeGenerationMkdirTemp = mkdirTemp
		runtimeGenerationChmod = chmod
		runtimeGenerationRemoveAll = removeAll
		runtimeGenerationRandRead = randRead
		runtimeGenerationWriteProbeAgentDir = writeProbeAgentDir
		runtimeGenerationHandoffNativeTree = handoffNativeTree
	})
}

func TestRuntimeGenerationPreparesVersionProbeAgentDir(t *testing.T) {
	wantErr := errors.New("injected version probe residence failure")
	if _, err := (*runtimeGeneration)(nil).prepareVersionProbeAgentDir(nil); err == nil {
		t.Fatal("nil runtime generation accepted")
	}

	t.Run("materialization", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		runtimeGenerationWriteProbeAgentDir = func(string) error { return wantErr }
		_, err := (&runtimeGeneration{root: filepath.Join(t.TempDir(), "acp-go-pi-runtime-probe")}).prepareVersionProbeAgentDir(nil)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("ownership handoff", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		root := filepath.Join(t.TempDir(), "acp-go-pi-runtime-probe")
		runtimeGenerationWriteProbeAgentDir = func(path string) error {
			require.Equal(t, filepath.Join(root, "probe-agent"), path)

			return nil
		}
		runtimeGenerationHandoffNativeTree = func(string, *ProcessIsolation) error { return wantErr }
		_, err := (&runtimeGeneration{root: root}).prepareVersionProbeAgentDir(nil)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("success", func(t *testing.T) {
		restoreRuntimeGenerationSeams(t)
		root := filepath.Join(t.TempDir(), "acp-go-pi-runtime-probe")
		isolation := &ProcessIsolation{UID: 11, GID: 22}
		wrote, handedOff := false, false
		runtimeGenerationWriteProbeAgentDir = func(path string) error {
			require.Equal(t, filepath.Join(root, "probe-agent"), path)
			wrote = true

			return nil
		}
		runtimeGenerationHandoffNativeTree = func(path string, got *ProcessIsolation) error {
			require.True(t, wrote)
			require.Equal(t, root, path)
			require.Same(t, isolation, got)
			handedOff = true

			return nil
		}
		agentDir, err := (&runtimeGeneration{root: root}).prepareVersionProbeAgentDir(isolation)
		require.NoError(t, err)
		require.True(t, handedOff)
		require.Equal(t, filepath.Join(root, "probe-agent"), agentDir)
	})
}

func TestEnsureVersionProbeResidenceLifetime(t *testing.T) {
	for _, test := range []struct {
		name       string
		probeErr   error
		wantRetain bool
	}{
		{name: "success removes and releases"},
		{name: "incomplete containment quarantines", probeErr: internalpi.ErrProcessContainmentIncomplete, wantRetain: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scratch := t.TempDir()
			reserved, releasedScratch := 0, 0
			acquired, releasedNative := 0, 0
			agent := NewAgent(
				testContainmentOption(),
				WithExecutablePath("/fake/pi"),
				WithScratchDir(scratch),
				WithRuntimeResourceHooks(RuntimeResourceHooks{
					ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
						require.Equal(t, RuntimeResourceDiscovery, kind)
						reserved++

						return func() { releasedScratch++ }, nil
					},
					AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
						require.Equal(t, RuntimeResourceDiscovery, kind)
						acquired++

						return func() { releasedNative++ }, nil
					},
				}),
			)

			var generationRoot string
			agent.probeVersion = func(_ context.Context, _ string, agentDir string, containment internalpi.ContainmentSpec) (string, error) {
				generationRoot = containment.GenerationRoot
				require.Equal(t, filepath.Join(generationRoot, "probe-agent"), agentDir)
				require.Equal(t, scratch, filepath.Dir(generationRoot))
				require.Equal(t, agentDir, environmentValue(internalpi.LaunchSpec{AgentDir: agentDir, Containment: containment}.Environ(), "PI_CODING_AGENT_DIR"))
				settings, err := os.ReadFile(filepath.Join(agentDir, internalpi.SettingsFileName))
				require.NoError(t, err)
				require.JSONEq(t, `{}`, string(settings))

				return internalpi.DefaultMinimumVersion, test.probeErr
			}

			err := agent.ensureVersion(t.Context())
			if test.probeErr != nil {
				require.ErrorIs(t, err, test.probeErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, reserved)
			require.Equal(t, 1, acquired)

			_, statErr := os.Stat(generationRoot)
			if test.wantRetain {
				require.NoError(t, statErr)
				require.Zero(t, releasedScratch)
				require.Zero(t, releasedNative)
			} else {
				require.ErrorIs(t, statErr, os.ErrNotExist)
				require.Equal(t, 1, releasedScratch)
				require.Equal(t, 1, releasedNative)
			}
		})
	}
}

func TestEnsureVersionFinalizesFailedProbeResidence(t *testing.T) {
	restoreRuntimeGenerationSeams(t)
	wantErr := errors.New("materialize probe")
	runtimeGenerationWriteProbeAgentDir = func(string) error { return wantErr }
	released := 0
	agent := NewAgent(
		testContainmentOption(),
		WithExecutablePath("/fake/pi"),
		WithScratchDir(t.TempDir()),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { released++ }, nil
			},
		}),
	)

	require.ErrorIs(t, agent.ensureVersion(t.Context()), wantErr)
	require.Equal(t, 1, released)
}

func TestNativeOwnershipIsolation(t *testing.T) {
	require.Nil(t, (*Agent)(nil).nativeOwnershipIsolation())
	require.Nil(t, (&Agent{options: Options{testOnlyNoCredential: true}}).nativeOwnershipIsolation())
	isolation := &ProcessIsolation{UID: 11, GID: 22}
	require.Same(t, isolation, (&Agent{options: Options{ProcessIsolation: isolation}}).nativeOwnershipIsolation())
}

func environmentValue(environment []string, key string) string {
	prefix := key + "="
	for _, entry := range environment {
		if value, ok := strings.CutPrefix(entry, prefix); ok {
			return value
		}
	}

	return ""
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
