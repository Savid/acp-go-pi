//go:build linux

package pi

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// These fixtures drive the real /proc enumeration against real processes. A
// stubbed descendant list would prove only that the stub returned what the test
// wrote into it, and vacancy is the one fact this adapter publishes as
// authoritative.

// requireRealEnumeration refuses to run against an overridden seam, so a fixture
// can never silently degrade into a stubbed proof.
func requireRealEnumeration(t *testing.T) {
	t.Helper()

	seamed, err := turnSupervisorDescendants(os.Getpid())
	require.NoError(t, err)

	walked, err := linuxDescendants(os.Getpid())
	require.NoError(t, err)
	require.Len(t, seamed, len(walked))
}

// awaitDescendants polls the real enumeration until the predicate holds, because
// a freshly forked grandchild appears in /proc a moment after the shell that
// forked it returns.
func awaitDescendants(t *testing.T, root int, holds func([]linuxProcessIdentity) bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for {
		descendants, err := linuxDescendants(root)
		require.NoError(t, err)

		if holds(descendants) {
			return
		}

		require.True(t, time.Now().Before(deadline), "the real enumeration never reached the expected tree")
		time.Sleep(10 * time.Millisecond)
	}
}

func containsPID(descendants []linuxProcessIdentity, pid int) bool {
	for _, descendant := range descendants {
		if descendant.pid == pid {
			return true
		}
	}

	return false
}

// startAnnouncedBackground starts a shell that forks one background sleep and
// announces its pid, so the fixture holds the real pid of a process it never
// started itself.
func startAnnouncedBackground(t *testing.T, script string) (*exec.Cmd, int) {
	t.Helper()

	shell := exec.Command("/bin/sh", "-c", script)

	stdout, err := shell.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, shell.Start())

	background := 0
	_, err = fmt.Fscan(stdout, &background)
	require.NoError(t, err)
	require.Positive(t, background)

	return shell, background
}

// TestDescendantEnumerationWalksARealContainedTree proves the walk reaches a
// grandchild the wrapper never started directly: a boundary that counted only
// direct children would report a tree it cannot see the bottom of.
func TestDescendantEnumerationWalksARealContainedTree(t *testing.T) {
	requireRealEnumeration(t)

	shell, grandchild := startAnnouncedBackground(t, "sleep 30 & echo $! ; wait")

	defer func() {
		_ = shell.Process.Kill()
		_ = unix.Kill(grandchild, unix.SIGKILL)
		_ = shell.Wait()
	}()

	awaitDescendants(t, os.Getpid(), func(found []linuxProcessIdentity) bool {
		return containsPID(found, shell.Process.Pid) && containsPID(found, grandchild)
	})
}

// TestDescendantEnumerationFindsARealEscapee proves the walk still holds a
// descendant whose own parent exited. A subreaper collects such an orphan, so
// the escapee stays inside the boundary and a vacancy claim cannot skip it.
func TestDescendantEnumerationFindsARealEscapee(t *testing.T) {
	requireRealEnumeration(t)
	require.NoError(t, unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0))

	shell, escapee := startAnnouncedBackground(t, "sleep 30 <&- >&- 2>&- & echo $!")
	shellErr := shell.Wait()

	// The escapee has already reparented onto this process, so the disposition
	// is restored immediately rather than held across the assertions.
	require.NoError(t, unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0))
	require.NoError(t, shellErr)

	defer func() {
		_ = unix.Kill(escapee, unix.SIGKILL)
		_, _ = unix.Wait4(escapee, nil, 0, nil)
	}()

	awaitDescendants(t, os.Getpid(), func(found []linuxProcessIdentity) bool {
		return containsPID(found, escapee) && !containsPID(found, shell.Process.Pid)
	})
}

// TestDescendantCountReportsARealVacantBoundary proves the published inventory
// reaches zero only once the real tree is empty, and that a boundary with no
// supervisor publishes no observation at all rather than a floor of zero.
func TestDescendantCountReportsARealVacantBoundary(t *testing.T) {
	requireRealEnumeration(t)

	shell, grandchild := startAnnouncedBackground(t, "sleep 30 & echo $! ; wait")
	supervised := &processTree{supervised: true, process: shell.Process}

	count, available := supervised.descendantCount()
	require.True(t, available)
	require.Positive(t, count)

	require.NoError(t, shell.Process.Kill())
	require.NoError(t, unix.Kill(grandchild, unix.SIGKILL))
	_ = shell.Wait()

	awaitDescendants(t, shell.Process.Pid, func(found []linuxProcessIdentity) bool { return len(found) == 0 })

	count, available = supervised.descendantCount()
	require.True(t, available)
	require.Zero(t, count)

	for _, boundary := range []*processTree{nil, {}, {supervised: true}} {
		count, available = boundary.descendantCount()
		require.False(t, available)
		require.Zero(t, count)
	}
}

// TestDescendantCountReportsNothingWhenEnumerationFails proves a walk that
// cannot read the tree publishes no observation, because an unreadable boundary
// is not an empty one.
func TestDescendantCountReportsNothingWhenEnumerationFails(t *testing.T) {
	restore := turnSupervisorDescendants
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return nil, os.ErrPermission }

	defer func() { turnSupervisorDescendants = restore }()

	count, available := (&processTree{supervised: true, process: &os.Process{Pid: os.Getpid()}}).descendantCount()
	require.False(t, available)
	require.Zero(t, count)
}
