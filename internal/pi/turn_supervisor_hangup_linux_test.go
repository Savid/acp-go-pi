//go:build linux

package pi

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	orphanHangupRoleEnv          = "ACP_GO_PI_TEST_ORPHAN_HANGUP_ROLE"
	orphanHangupRoleIntermediate = "intermediate"
	orphanHangupRoleWorker       = "worker"
	orphanHangupRoleDescendant   = "descendant"

	orphanHangupDeadline = 30 * time.Second
	orphanHangupPoll     = 5 * time.Millisecond
)

// The orphan proof needs three generations of real processes, so the test
// binary re-enters itself in one of the helper roles before any test runs.
func init() {
	role := os.Getenv(orphanHangupRoleEnv)
	if role == "" {
		return
	}

	os.Exit(runOrphanHangupRole(role))
}

func runOrphanHangupRole(role string) int {
	switch role {
	case orphanHangupRoleIntermediate:
		return runOrphanHangupIntermediate()
	case orphanHangupRoleWorker:
		return runOrphanHangupWorker()
	case orphanHangupRoleDescendant:
		// Held open by the test, so the descendant outlives every step until
		// the worker's containment kills it.
		_, _ = io.Copy(io.Discard, os.NewFile(3, "hold"))

		return 0
	default:
		fmt.Fprintln(os.Stderr, "unknown orphan hangup role", role)

		return 2
	}
}

func orphanHangupEnvironment(role string) []string {
	return []string{orphanHangupRoleEnv + "=" + role}
}

// runOrphanHangupIntermediate is the generation whose death orphans the worker.
// It leads its own session, so whichever process adopts the worker afterwards
// is guaranteed to sit outside the worker's session and the worker's process
// group is genuinely orphaned no matter who reaps it.
func runOrphanHangupIntermediate() int {
	topology := os.NewFile(3, "topology")

	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(topology, "error:%v\n", err)

		return 1
	}

	worker := exec.Command(executable)
	worker.Env = orphanHangupEnvironment(orphanHangupRoleWorker)
	worker.ExtraFiles = []*os.File{
		os.NewFile(4, "report"), os.NewFile(5, "control"), os.NewFile(6, "hold"),
	}
	worker.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err = worker.Start(); err != nil {
		fmt.Fprintf(topology, "error:%v\n", err)

		return 1
	}

	fmt.Fprintf(topology, "worker:%d\n", worker.Process.Pid)

	for {
		time.Sleep(time.Minute)
	}
}

// runOrphanHangupWorker stands in for a supervisor role: it installs the
// production hangup guard, owns a descendant in its own process group exactly
// as the guardian owns the liveness supervisor, and quiesces only when its
// control channel closes.
func runOrphanHangupWorker() int {
	report := os.NewFile(3, "report")
	control := os.NewFile(4, "control")
	hold := os.NewFile(5, "hold")

	stopHangupGuard := installTurnSupervisorHangupGuard()
	defer stopHangupGuard()

	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(report, "error:%v\n", err)

		return 1
	}

	descendant := exec.Command(executable)
	descendant.Env = orphanHangupEnvironment(orphanHangupRoleDescendant)
	descendant.ExtraFiles = []*os.File{hold}
	descendant.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err = descendant.Start(); err != nil {
		fmt.Fprintf(report, "error:%v\n", err)

		return 1
	}

	fmt.Fprintf(report, "ready:%d\n", descendant.Process.Pid)

	_, _ = io.Copy(io.Discard, control)

	if err = awaitLinuxSupervisorContainment(os.Getpid(), 0); err != nil {
		fmt.Fprintf(report, "error:%v\n", err)

		return 1
	}

	fmt.Fprint(report, "complete\n")

	return 0
}

type orphanHangupStat struct {
	state   byte
	parent  int
	group   int
	session int
}

func readOrphanHangupStat(pid int) (orphanHangupStat, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return orphanHangupStat{}, err
	}

	line := string(raw)

	closing := strings.LastIndexByte(line, ')')
	if closing < 0 || closing+2 >= len(line) {
		return orphanHangupStat{}, fmt.Errorf("parse /proc/%d/stat: malformed comm field", pid)
	}

	fields := strings.Fields(line[closing+2:])
	if len(fields) < 4 || len(fields[0]) != 1 {
		return orphanHangupStat{}, fmt.Errorf("parse /proc/%d/stat: incomplete fields", pid)
	}

	stat := orphanHangupStat{state: fields[0][0]}

	for index, target := range []*int{&stat.parent, &stat.group, &stat.session} {
		value, convErr := strconv.Atoi(fields[index+1])
		if convErr != nil {
			return orphanHangupStat{}, fmt.Errorf("parse /proc/%d/stat field %d: %w", pid, index+1, convErr)
		}

		*target = value
	}

	return stat, nil
}

func awaitOrphanHangupState(t *testing.T, pid int, accept func(orphanHangupStat) bool, what string) orphanHangupStat {
	t.Helper()

	deadline := time.Now().Add(orphanHangupDeadline)
	for {
		stat, err := readOrphanHangupStat(pid)
		if err == nil && accept(stat) {
			return stat
		}

		if time.Now().After(deadline) {
			t.Fatalf("process %d never reached %s: last stat %+v, err %v", pid, what, stat, err)
		}

		time.Sleep(orphanHangupPoll)
	}
}

func awaitOrphanHangupExit(t *testing.T, pid int) {
	t.Helper()

	deadline := time.Now().Add(orphanHangupDeadline)
	for {
		stat, err := readOrphanHangupStat(pid)
		if errors.Is(err, os.ErrNotExist) || (err == nil && stat.state == 'Z') {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("process %d survived containment: last stat %+v, err %v", pid, stat, err)
		}

		time.Sleep(orphanHangupPoll)
	}
}

func readOrphanHangupLine(t *testing.T, reader *bufio.Reader, prefix string) string {
	t.Helper()

	line, err := reader.ReadString('\n')
	require.NoErrorf(t, err, "read %q report", prefix)

	value, found := strings.CutPrefix(strings.TrimSuffix(line, "\n"), prefix)
	require.Truef(t, found, "report %q is not a %q line", line, prefix)

	return value
}

// TestOrphanedSupervisorGroupSurvivesHangupUntilAuthenticatedQuiescence proves
// the containment property SIGHUP used to break: a stopped supervisor group
// that becomes orphaned — the guardian's ordinary fate, since it deliberately
// outlives the host and carries no Pdeathsig — is signalled SIGHUP by the
// kernel and must survive it, keeping its subreaper role and its descendants
// until its authenticated control channel closes and the whole tree reaches
// ECHILD.
//
// Nothing here is skipped or tolerated. The kernel pairs the SIGHUP it sends to
// a newly orphaned stopped group with a SIGCONT, and the test sends no SIGCONT
// of its own, so the worker leaving the stopped state is positive proof the
// hangup was delivered; an environment that cannot produce a genuine orphan
// leaves the worker stopped and fails the test loudly.
func TestOrphanedSupervisorGroupSurvivesHangupUntilAuthenticatedQuiescence(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)

	topologyRead, topologyWrite, err := os.Pipe()
	require.NoError(t, err)

	reportRead, reportWrite, err := os.Pipe()
	require.NoError(t, err)

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)

	holdRead, holdWrite, err := os.Pipe()
	require.NoError(t, err)

	intermediate := exec.Command(executable)
	intermediate.Env = orphanHangupEnvironment(orphanHangupRoleIntermediate)
	intermediate.ExtraFiles = []*os.File{topologyWrite, reportWrite, controlRead, holdRead}
	intermediate.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	require.NoError(t, intermediate.Start())

	// The test must not retain the child ends, or no reader here ever sees the
	// EOF that drives quiescence and completion.
	for _, file := range []*os.File{topologyWrite, reportWrite, controlRead, holdRead} {
		require.NoError(t, file.Close())
	}

	workerPID, err := strconv.Atoi(readOrphanHangupLine(t, bufio.NewReader(topologyRead), "worker:"))
	require.NoError(t, err)

	reports := bufio.NewReader(reportRead)

	descendantPID, err := strconv.Atoi(readOrphanHangupLine(t, reports, "ready:"))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = holdWrite.Close()
		_ = controlWrite.Close()

		for _, pid := range []int{workerPID, descendantPID} {
			_ = syscall.Kill(pid, syscall.SIGCONT)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}

		_ = intermediate.Process.Kill()
		_ = intermediate.Wait()
	})

	require.NoError(t, syscall.Kill(workerPID, syscall.SIGSTOP))
	awaitOrphanHangupState(t, workerPID, func(stat orphanHangupStat) bool { return stat.state == 'T' }, "stopped")

	require.NoError(t, intermediate.Process.Kill())
	_, _ = intermediate.Process.Wait()

	orphan, err := readOrphanHangupStat(workerPID)
	require.NoError(t, err)
	require.Equal(t, workerPID, orphan.group, "worker must lead its own process group")

	adopter, err := readOrphanHangupStat(orphan.parent)
	require.NoError(t, err)

	if adopter.session == orphan.session {
		t.Fatalf(
			"environment produced no genuine orphan: worker %d in session %d was adopted by %d in the same session",
			workerPID, orphan.session, orphan.parent,
		)
	}

	resumed := awaitOrphanHangupState(
		t, workerPID, func(stat orphanHangupStat) bool { return stat.state != 'T' }, "the kernel's orphan hangup",
	)
	require.NotEqual(t, byte('Z'), resumed.state, "orphaned supervisor was terminated by SIGHUP")

	descendant, err := readOrphanHangupStat(descendantPID)
	require.NoError(t, err)
	require.NotEqual(t, byte('Z'), descendant.state, "supervised descendant did not survive the orphan hangup")

	require.NoError(t, controlWrite.Close())

	require.Equal(t, "", readOrphanHangupLine(t, reports, "complete"))

	awaitOrphanHangupExit(t, descendantPID)
	awaitOrphanHangupExit(t, workerPID)

	require.NoError(t, topologyRead.Close())
	require.NoError(t, reportRead.Close())
	require.NoError(t, holdWrite.Close())
}

// TestTurnSupervisorHangupGuardDrainsWithoutQuiescing proves the guard takes
// SIGHUP off its terminate disposition and then discards it: nothing the
// handler receives reaches the quiescence paths.
func TestTurnSupervisorHangupGuardDrainsWithoutQuiescing(t *testing.T) {
	restoreTurnSupervisorSeams(t)

	var (
		registered []os.Signal
		hangups    chan<- os.Signal
		stopped    bool
	)

	turnSupervisorSignalNotify = func(target chan<- os.Signal, signals ...os.Signal) {
		hangups = target
		registered = append(registered, signals...)
	}
	turnSupervisorSignalStop = func(chan<- os.Signal) { stopped = true }

	stopHangupGuard := installTurnSupervisorHangupGuard()
	require.Equal(t, []os.Signal{syscall.SIGHUP}, registered)

	// The guard's buffer holds one signal, so the second send completing is
	// proof that the drain consumed the first.
	hangups <- syscall.SIGHUP
	hangups <- syscall.SIGHUP

	stopHangupGuard()
	require.True(t, stopped)
}

// TestTurnSupervisorBootstrapGuardsEverySupervisorRoleFromHangup proves both
// supervisor roles install the guard before any inherited input is touched, and
// that an ordinary adapter process installs nothing.
func TestTurnSupervisorBootstrapGuardsEverySupervisorRoleFromHangup(t *testing.T) {
	restoreTurnSupervisorSeams(t)

	var guarded []os.Signal

	turnSupervisorSignalNotify = func(_ chan<- os.Signal, signals ...os.Signal) {
		guarded = append(guarded, signals...)
	}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorExit = func(int) {}
	turnSupervisorInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return nil, nil, nil, errors.New("input")
	}

	for _, mode := range []string{turnSupervisorMode, turnSupervisorLivenessMode} {
		t.Setenv(turnSupervisorModeEnv, mode)

		guarded = nil

		turnSupervisorBootstrap()
		require.Equalf(t, []os.Signal{syscall.SIGHUP}, guarded, "%s bootstrap hangup guard", mode)
	}

	t.Setenv(turnSupervisorModeEnv, "")

	guarded = nil

	turnSupervisorBootstrap()
	require.Empty(t, guarded)
}
