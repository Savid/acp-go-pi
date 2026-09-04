package pi

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeOrdinaryVersion(t *testing.T) {
	_, err := ProbeOrdinaryVersion(t.Context(), "", nil)
	require.ErrorContains(t, err, "resolve pi version executable")

	executable := fakeOrdinaryExecutable(t)

	version, err := ProbeOrdinaryVersion(t.Context(), executable, childProbeEnvironment(ordinaryChildVersion))
	require.NoError(t, err)
	require.Equal(t, "0.80.6", version)

	_, err = ProbeOrdinaryVersion(t.Context(), executable, childProbeEnvironment(ordinaryChildSilent))
	require.ErrorContains(t, err, "empty output")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = ProbeOrdinaryVersion(ctx, executable, childProbeEnvironment(ordinaryChildSleep))
	require.Error(t, err)
}

func CaptureOrdinaryEnvironmentEntries() []string {
	return environmentEntries(CaptureOrdinaryEnvironment())
}

// childProbeEnvironment is a captured ordinary environment with the fake
// child's mode appended. The capture filter drops names pi has no use for,
// which includes this one, so the probe is handed it directly.
func childProbeEnvironment(mode string) []string {
	return append(CaptureOrdinaryEnvironmentEntries(), ordinaryChildEnvKey+"="+mode)
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
