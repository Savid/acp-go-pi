//go:build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// nativeBrowserRecordedTrace is the syscall trace a passing canary run produced:
// the probe, the pinned pi release, its PATH search landing in the adapter's
// shim directory first, and the interpreter that search resolved to. It is
// verbatim fixture material, so the decisions below are exercised against the
// grammar strace actually emits rather than an invented one.
const nativeBrowserRecordedTrace = `342   execve("/usr/local/bin/native-browser.test", ["/usr/local/bin/native-browser.te"..., "-test.v"], 0xffffed011138 /* 8 vars */) = 0
348   execve("/usr/local/bin/pi", ["/usr/local/bin/pi", "--version"], 0x3f001eb78be0 /* 8 vars */) = 0
342   --- SIGCHLD {si_signo=SIGCHLD, si_code=CLD_EXITED, si_pid=348, si_uid=0, si_status=0} ---
356   execve("/usr/local/bin/acp-go-pi.test", ["/usr/local/bin/acp-go-pi.test", "-path", "/usr/local/bin/pi"], 0x3f001eb78cd0 /* 8 vars */) = 0
374   execve("/tmp/TestNativeBrowser000/001/acp-go-pi-browser-shim-2006248589/node", ["node", "/usr/local/bin/pi", "--mode", "rpc"], 0xffffdc0f5820 /* 7 vars */) = -1 ENOENT (No such file or directory)
374   execve("/usr/local/bin/node", ["node", "/usr/local/bin/pi", "--mode", "rpc"], 0xffffdc0f5820 /* 7 vars */) = 0
`

// TestNativeBrowserTraceAcceptsAContainedRun is the positive control: the
// evidence a passing canary produces has to satisfy every decision the driver
// makes about it, or the canary is asserting something it never observes.
func TestNativeBrowserTraceAcceptsAContainedRun(t *testing.T) {
	require.True(t, traceExecsBase(nativeBrowserRecordedTrace, "acp-go-pi.test"))
	require.True(t, traceExecsBase(nativeBrowserRecordedTrace, "pi"))
	require.True(t, traceResolvesThroughShim(nativeBrowserRecordedTrace))
	require.Empty(t, unshimmedLauncherExecs(nativeBrowserRecordedTrace))
}

// TestNativeBrowserTraceRejectsAnUnshimmedLauncher is the assertion the canary
// exists for: a launcher reached from anywhere but an adapter shim directory is
// a URL opened on somebody's desktop.
func TestNativeBrowserTraceRejectsAnUnshimmedLauncher(t *testing.T) {
	for name, exec := range map[string]string{
		"system launcher":                       `374   execve("/usr/bin/xdg-open", ["xdg-open", "https://example.invalid/auth"], 0xff /* 7 vars */) = 0`,
		"relative launcher":                     `374   execveat(AT_FDCWD, "open", ["open", "https://example.invalid/auth"], 0xff /* 7 vars */, 0) = 0`,
		"browser binary":                        `374   execve("/opt/google/chrome/google-chrome", ["google-chrome", "https://example.invalid/auth"], 0xff /* 7 vars */) = 0`,
		"shim-named parent, unshimmed launcher": `374   execve("/tmp/acp-go-pi-browser-shim-1/nested/../../firefox", ["firefox"], 0xff /* 7 vars */) = 0`,
	} {
		t.Run(name, func(t *testing.T) {
			trace := nativeBrowserRecordedTrace + exec + "\n"

			require.NotEmpty(t, unshimmedLauncherExecs(trace))
		})
	}
}

// TestNativeBrowserTraceAcceptsTheAdaptersOwnLaunchers keeps the assertion from
// condemning the interception itself: reaching a shim no-op is the contained
// outcome, not the failure.
func TestNativeBrowserTraceAcceptsTheAdaptersOwnLaunchers(t *testing.T) {
	trace := nativeBrowserRecordedTrace +
		`374   execve("/tmp/TestNativeBrowser000/001/acp-go-pi-browser-shim-2006248589/xdg-open", ` +
		`["xdg-open", "https://example.invalid/auth"], 0xff /* 7 vars */) = 0` + "\n"

	require.Empty(t, unshimmedLauncherExecs(trace))
	require.True(t, traceResolvesThroughShim(trace))
}

// TestNativeBrowserTraceRejectsAMissingNativeRun covers the other half of the
// canary: a run where pi never executed proves nothing about pi's boundary, and
// an absent shim makes a clean launcher result meaningless.
func TestNativeBrowserTraceRejectsAMissingNativeRun(t *testing.T) {
	withoutPi := strings.ReplaceAll(nativeBrowserRecordedTrace, `execve("/usr/local/bin/pi"`, `execve("/usr/local/bin/pip"`)
	require.False(t, traceExecsBase(withoutPi, "pi"))

	withoutShim := strings.ReplaceAll(nativeBrowserRecordedTrace, nativeBrowserShimPrefix, "unrelated-scratch-")
	require.False(t, traceResolvesThroughShim(withoutShim))
	require.True(t, traceExecsBase(withoutShim, "pi"))
}

// TestNativeBrowserTraceIgnoresNonExecLines keeps signal names and argument text
// from being read as executions.
func TestNativeBrowserTraceIgnoresNonExecLines(t *testing.T) {
	for name, line := range map[string]string{
		"signal":             `342   --- SIGCHLD {si_signo=SIGCHLD, si_code=CLD_EXITED} ---`,
		"launcher in argv":   `374   execve("/usr/local/bin/node", ["node", "--browser", "xdg-open"], 0xff /* 7 vars */) = 0`,
		"unfinished syscall": `374   execve("/usr/local/bin/node", ["node" <unfinished ...>`,
		"exit":               `374   +++ exited with 0 +++`,
	} {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, unshimmedLauncherExecs(line+"\n"))
		})
	}
}
