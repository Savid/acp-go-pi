package pi

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeVersion(t *testing.T) {
	t.Parallel()

	t.Run("reports the trimmed version", func(t *testing.T) {
		t.Parallel()

		script := filepath.Join(t.TempDir(), "fake-pi")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho ' 0.80.6 '\n"), 0o700))

		version, err := probeVersionTestScript(t, t.Context(), script)
		require.NoError(t, err)
		require.Equal(t, "0.80.6", version)
	})

	t.Run("empty output fails", func(t *testing.T) {
		t.Parallel()

		script := filepath.Join(t.TempDir(), "fake-pi")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700))

		_, err := probeVersionTestScript(t, t.Context(), script)
		require.ErrorContains(t, err, "empty output")
	})

	t.Run("exec failure is wrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ProbeVersion(t.Context(), filepath.Join(t.TempDir(), "missing"), testContainmentSpec(t))
		require.ErrorContains(t, err, "probe pi version")
	})

	t.Run("running probe cancellation is wrapped", func(t *testing.T) {
		t.Parallel()

		script := filepath.Join(t.TempDir(), "fake-pi")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0o700))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, err := probeVersionTestScript(t, ctx, script)
		require.Error(t, err)
	})
}

// probeVersionTestScript retries ETXTBSY: a concurrently forked child of a
// parallel test can transiently inherit the just-written script's descriptor
// across its own fork/exec window. Installed pi binaries do not have this
// freshly-created-file race.
func probeVersionTestScript(t *testing.T, ctx context.Context, script string) (string, error) {
	t.Helper()

	for attempt := 0; ; attempt++ {
		version, err := ProbeVersion(ctx, script, testContainmentSpec(t))
		if err == nil || attempt >= 50 || !errors.Is(err, syscall.ETXTBSY) {
			return version, err
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func restoreVersionSeams(t *testing.T) {
	t.Helper()
	command := execCommand
	prepareTree := versionPrepareTreeCommand
	prepareRecord := versionPrepareContainmentRecord
	startTree := versionStartTree
	afterPrepare := versionAfterPrepare
	kill := versionTreeKill
	terminateAndWait := versionTreeTerminateAndWait
	t.Cleanup(func() {
		execCommand = command
		versionPrepareTreeCommand = prepareTree
		versionPrepareContainmentRecord = prepareRecord
		versionStartTree = startTree
		versionAfterPrepare = afterPrepare
		versionTreeKill = kill
		versionTreeTerminateAndWait = terminateAndWait
	})
}

func closedVersionTree(waitErr error) *processTree {
	done := make(chan struct{})
	close(done)

	return &processTree{direct: &directChildWait{done: done, err: waitErr}}
}

func TestProbeVersionStageBranches(t *testing.T) {
	wantErr := errors.New("injected version stage failure")
	require.NoError(t, versionTreeKill(&processTree{}))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := ProbeVersion(cancelled, "/usr/bin/true", ContainmentSpec{})
	require.ErrorIs(t, err, context.Canceled)

	t.Run("prepare command", func(t *testing.T) {
		restoreVersionSeams(t)
		versionPrepareTreeCommand = func(*exec.Cmd, ContainmentSpec) (*processTreeCommand, error) { return nil, wantErr }
		_, err := ProbeVersion(t.Context(), "/usr/bin/true", ContainmentSpec{})
		require.ErrorContains(t, err, "prepare pi version probe")
	})

	t.Run("prepare record", func(t *testing.T) {
		restoreVersionSeams(t)
		versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		versionPrepareContainmentRecord = func(ContainmentSpec) (containmentRecord, error) { return containmentRecord{}, wantErr }
		_, err := ProbeVersion(t.Context(), "/usr/bin/true", ContainmentSpec{})
		require.ErrorContains(t, err, "prepare pi version containment record")
	})

	t.Run("cancel after prepare", func(t *testing.T) {
		restoreVersionSeams(t)
		ctx, cancel := context.WithCancel(t.Context())
		versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		versionPrepareContainmentRecord = func(ContainmentSpec) (containmentRecord, error) { return containmentRecord{}, nil }
		versionAfterPrepare = cancel
		_, err := ProbeVersion(ctx, "/usr/bin/true", ContainmentSpec{})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("start tree", func(t *testing.T) {
		restoreVersionSeams(t)
		versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		versionPrepareContainmentRecord = func(ContainmentSpec) (containmentRecord, error) { return containmentRecord{}, nil }
		versionStartTree = func(*processTreeCommand) (*processTree, error) { return nil, wantErr }
		_, err := ProbeVersion(t.Context(), "/usr/bin/true", ContainmentSpec{})
		require.ErrorContains(t, err, "probe pi version")
	})

	for _, test := range []struct {
		name           string
		waitErr        error
		containmentErr error
		output         string
		wantErr        bool
	}{
		{name: "wait delay is normalized", waitErr: exec.ErrWaitDelay, output: " 0.80.6 ", wantErr: false},
		{name: "wait failure", waitErr: wantErr, output: "0.80.6", wantErr: true},
		{name: "containment failure", containmentErr: wantErr, output: "0.80.6", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreVersionSeams(t)
			versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
				return &processTreeCommand{cmd: cmd}, nil
			}
			versionPrepareContainmentRecord = func(ContainmentSpec) (containmentRecord, error) { return containmentRecord{}, nil }
			versionStartTree = func(launch *processTreeCommand) (*processTree, error) {
				_, err := launch.cmd.Stdout.Write([]byte(test.output))
				require.NoError(t, err)

				return closedVersionTree(test.waitErr), nil
			}
			versionTreeTerminateAndWait = func(*processTree, time.Duration) error { return test.containmentErr }
			version, err := ProbeVersion(t.Context(), "/usr/bin/true", ContainmentSpec{})
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, "0.80.6", version)
			}
		})
	}

	t.Run("cancellation callback", func(t *testing.T) {
		restoreVersionSeams(t)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		versionPrepareTreeCommand = func(cmd *exec.Cmd, _ ContainmentSpec) (*processTreeCommand, error) {
			return &processTreeCommand{cmd: cmd}, nil
		}
		versionPrepareContainmentRecord = func(ContainmentSpec) (containmentRecord, error) { return containmentRecord{}, nil }
		versionStartTree = func(launch *processTreeCommand) (*processTree, error) {
			_, err := launch.cmd.Stdout.Write([]byte("0.80.6"))
			require.NoError(t, err)

			return &processTree{direct: &directChildWait{done: done}}, nil
		}
		versionTreeKill = func(*processTree) error {
			close(done)

			return nil
		}
		versionTreeTerminateAndWait = func(*processTree, time.Duration) error { return nil }
		go func() {
			time.Sleep(time.Millisecond)
			cancel()
		}()
		version, err := ProbeVersion(ctx, "/usr/bin/true", ContainmentSpec{})
		require.NoError(t, err)
		require.Equal(t, "0.80.6", version)
	})
}

func TestCheckMinimumVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		minimum string
		wantErr string
	}{
		{name: "equal", version: "0.80.6", minimum: "0.80.6"},
		{name: "patch above", version: "0.80.7", minimum: "0.80.6"},
		{name: "minor above", version: "0.81.0", minimum: "0.80.6"},
		{name: "major above", version: "1.0.0", minimum: "0.80.6"},
		{name: "v prefix accepted", version: "v0.80.6", minimum: "0.80.6"},
		{name: "prerelease suffix ignored", version: "0.80.6-rc.1", minimum: "0.80.6"},
		{name: "shorter version padded", version: "1", minimum: "0.80.6"},
		{
			name:    "patch below",
			version: "0.80.5",
			minimum: "0.80.6",
			wantErr: "below the minimum supported version",
		},
		{
			name:    "minor below",
			version: "0.79.9",
			minimum: "0.80.6",
			wantErr: "below the minimum supported version",
		},
		{
			name:    "invalid version",
			version: "abc",
			minimum: "0.80.6",
			wantErr: "invalid version",
		},
		{
			name:    "invalid minimum",
			version: "0.80.6",
			minimum: "",
			wantErr: "invalid version",
		},
		{
			name:    "negative segment",
			version: "0.-1.0",
			minimum: "0.80.6",
			wantErr: "invalid version",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := CheckMinimumVersion(test.version, test.minimum)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDefaultMinimumVersionIsValid(t *testing.T) {
	t.Parallel()

	require.NoError(t, CheckMinimumVersion(DefaultMinimumVersion, DefaultMinimumVersion))
}
