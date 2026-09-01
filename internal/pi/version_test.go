package pi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeOrdinaryVersion(t *testing.T) {
	_, err := ProbeOrdinaryVersion(t.Context(), "", nil)
	require.ErrorContains(t, err, "resolve pi version executable")

	script := filepath.Join(t.TempDir(), "fake-pi")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho ' 0.80.6 '\n"), 0o700))
	version, err := ProbeOrdinaryVersion(t.Context(), script, CaptureOrdinaryEnvironmentEntries())
	require.NoError(t, err)
	require.Equal(t, "0.80.6", version)

	empty := filepath.Join(t.TempDir(), "empty-pi")
	require.NoError(t, os.WriteFile(empty, []byte("#!/bin/sh\nexit 0\n"), 0o700))
	_, err = ProbeOrdinaryVersion(t.Context(), empty, CaptureOrdinaryEnvironmentEntries())
	require.ErrorContains(t, err, "empty output")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	slow := filepath.Join(t.TempDir(), "slow-pi")
	require.NoError(t, os.WriteFile(slow, []byte("#!/bin/sh\nsleep 30\n"), 0o700))
	_, err = ProbeOrdinaryVersion(ctx, slow, CaptureOrdinaryEnvironmentEntries())
	require.Error(t, err)
}

func CaptureOrdinaryEnvironmentEntries() []string {
	return environmentEntries(CaptureOrdinaryEnvironment())
}

func TestCheckMinimumVersion(t *testing.T) {
	tests := []struct{ version, minimum, want string }{
		{"0.80.6", "0.80.6", ""}, {"0.80.7", "0.80.6", ""}, {"1.0.0", "0.80.6", ""},
		{"v0.80.6", "0.80.6", ""}, {"0.80.6-rc.1", "0.80.6", ""}, {"1", "0.80.6", ""},
		{"0.80.5", "0.80.6", "below the minimum"}, {"abc", "0.80.6", "invalid version"},
		{"0.80.6", "", "invalid version"}, {"0.-1.0", "0.80.6", "invalid version"},
	}
	for _, test := range tests {
		err := CheckMinimumVersion(test.version, test.minimum)
		if test.want == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, test.want)
		}
	}
}

func TestDefaultMinimumVersionIsValid(t *testing.T) {
	require.NoError(t, CheckMinimumVersion(DefaultMinimumVersion, DefaultMinimumVersion))
}
