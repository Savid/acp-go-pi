package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComposeEnvironmentPlatformIdentityAndPrecedence(t *testing.T) {
	previous := Platform
	t.Cleanup(func() { Platform = previous })

	for _, test := range []struct {
		platform string
		want     map[string]string
	}{
		{"linux", map[string]string{"PATH": "base", "Path": "explicit", "path": "session", "EMPTY": ""}},
		{"windows", map[string]string{"PATH": "session", "EMPTY": ""}},
	} {
		t.Run(test.platform, func(t *testing.T) {
			Platform = test.platform
			base := map[string]string{"PATH": "base", "EMPTY": "inherited"}
			explicit := map[string]string{"Path": "explicit", "EMPTY": ""}
			session := map[string]string{"path": "session"}

			require.Equal(t, test.want, ComposeEnvironment(base, explicit, session))
			require.Equal(t, "inherited", base["EMPTY"], "composition changed the caller's environment")
			require.Equal(t, "explicit", explicit["Path"])
		})
	}
}

func TestCaptureOrdinaryEnvironmentFoldsKeysUnderThePlatformIdentity(t *testing.T) {
	previous := Platform
	t.Cleanup(func() { Platform = previous })

	entries := []string{"Path=/first", "PATH=/last", "HOME=/home/pi", "OPENAI_API_KEY=secret"}

	Platform = "windows"
	require.Equal(t, map[string]string{"PATH": "/last", "HOME": "/home/pi"}, CaptureOrdinaryEnvironment(entries),
		"the later spelling in block order wins where the platform folds names")

	Platform = "linux"
	require.Equal(t, map[string]string{"Path": "/first", "PATH": "/last", "HOME": "/home/pi"}, CaptureOrdinaryEnvironment(entries))
}
