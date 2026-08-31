package pi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrowserShimEnviron(t *testing.T) {
	t.Parallel()

	separator := string(os.PathListSeparator)

	cases := map[string]struct {
		env  []string
		want []string
	}{
		"empty environment gains both mechanisms": {
			env:  nil,
			want: []string{"PATH=/shim", "BROWSER=" + filepath.Join("/shim", browserLauncherNames[0])},
		},
		"existing PATH is kept behind the shim": {
			env:  []string{"HOME=/home/user", "PATH=/usr/bin" + separator + "/bin"},
			want: []string{"HOME=/home/user", "PATH=/shim" + separator + "/usr/bin" + separator + "/bin", "BROWSER=" + filepath.Join("/shim", browserLauncherNames[0])},
		},
		"existing BROWSER is replaced": {
			env:  []string{"BROWSER=/usr/bin/firefox", "TERM=xterm"},
			want: []string{"TERM=xterm", "PATH=/shim", "BROWSER=" + filepath.Join("/shim", browserLauncherNames[0])},
		},
		"entry without a separator is passed through": {
			env:  []string{"MALFORMED"},
			want: []string{"MALFORMED", "PATH=/shim", "BROWSER=" + filepath.Join("/shim", browserLauncherNames[0])},
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, testCase.want, browserShimEnviron(testCase.env, "/shim"))
		})
	}
}

func TestBrowserShimNilHandleOwnsNothing(t *testing.T) {
	t.Parallel()

	var (
		inner  *browserShim
		handle *BrowserShim
	)

	require.NoError(t, inner.remove())
	require.NoError(t, handle.Remove())
	require.Equal(t, []string{"PATH=/usr/bin"}, handle.Environ([]string{"PATH=/usr/bin"}))
}
