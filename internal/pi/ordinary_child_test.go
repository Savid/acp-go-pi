package pi

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A fake native child. The behaviours below are what the process tests need a
// pi to do — drain its stdin, outlive it, fail loudly, report its environment —
// and they are performed by this test binary re-executed as the child rather
// than by a shell script, because a shebang is not an executable image on every
// platform this adapter runs on.
const (
	// ordinaryChildEnvKey names the behaviour the child performs. It survives
	// LaunchSpec.Environ's explicit-environment filter, which is what lets a
	// test steer a child the product itself started.
	ordinaryChildEnvKey = "TEST_ORDINARY_CHILD"

	ordinaryChildDrainAndReport = "drain-and-report"
	ordinaryChildDrain          = "drain"
	ordinaryChildOutliveStdin   = "outlive-stdin"
	ordinaryChildFail           = "fail"
	ordinaryChildReportEnv      = "report-env"
	ordinaryChildExit           = "exit"
	ordinaryChildVersion        = "version"
	ordinaryChildSilent         = "silent"
	ordinaryChildSleep          = "sleep"

	// ordinaryChildReply is what a drained child prints, and
	// ordinaryChildStderr what a failing one writes to its error stream.
	ordinaryChildReply  = "done"
	ordinaryChildStderr = "boom: real cause"
	// ordinaryChildVersionOutput is padded exactly as a real pi pads it, so the
	// probe's trimming is what the test is judging.
	ordinaryChildVersionOutput = " 0.80.6 "
)

// runOrdinaryChild performs one fake native behaviour and never returns. It is
// reached from TestMain before the test binary looks at its own arguments,
// because the product launches this executable with pi's arguments rather than
// the test framework's.
func runOrdinaryChild(mode string) {
	switch mode {
	case ordinaryChildDrainAndReport:
		_, _ = io.Copy(io.Discard, os.Stdin)
		fmt.Println(ordinaryChildReply)
	case ordinaryChildDrain:
	case ordinaryChildOutliveStdin, ordinaryChildSleep:
		// Neither stdin's end nor anything short of a kill stops this child.
		for {
			time.Sleep(10 * time.Millisecond)
		}
	case ordinaryChildFail:
		fmt.Fprintln(os.Stderr, ordinaryChildStderr)
		os.Exit(3)
	case ordinaryChildReportEnv:
		fmt.Println(strings.Join(os.Environ(), "\n"))
	case ordinaryChildVersion:
		fmt.Println(ordinaryChildVersionOutput)
	case ordinaryChildSilent:
	case ordinaryChildExit:
	}

	os.Exit(0)
}

// fakeOrdinaryExecutable names an executable image the platform will start.
// This test binary is the one such image every platform is guaranteed to have,
// and the child mode decides what it does once running, so nothing is copied
// or compiled to obtain it.
func fakeOrdinaryExecutable(t *testing.T) string {
	t.Helper()

	executable, err := os.Executable()
	require.NoError(t, err)

	return executable
}

// absTestPath builds a host-absolute path from POSIX-looking segments, so a
// test states "an absolute directory" rather than a spelling only one platform
// accepts.
func absTestPath(segments ...string) string {
	root := "/"
	if runtime.GOOS == "windows" {
		root = `C:\`
	}

	return filepath.Join(append([]string{root}, segments...)...)
}
