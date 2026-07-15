package pi

import (
	"context"
	"os"
	"path/filepath"
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

		version, err := ProbeVersion(t.Context(), script)
		require.NoError(t, err)
		require.Equal(t, "0.80.6", version)
	})

	t.Run("empty output fails", func(t *testing.T) {
		t.Parallel()

		script := filepath.Join(t.TempDir(), "fake-pi")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700))

		_, err := ProbeVersion(t.Context(), script)
		require.ErrorContains(t, err, "empty output")
	})

	t.Run("exec failure is wrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ProbeVersion(t.Context(), filepath.Join(t.TempDir(), "missing"))
		require.ErrorContains(t, err, "probe pi version")
	})

	t.Run("running probe cancellation is wrapped", func(t *testing.T) {
		t.Parallel()

		script := filepath.Join(t.TempDir(), "fake-pi")
		require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0o700))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, err := ProbeVersion(ctx, script)
		require.Error(t, err)
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
