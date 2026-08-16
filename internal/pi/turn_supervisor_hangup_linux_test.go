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
	// orphanHangupSupervisorArg re-enters the test binary as the supervisor
	// generation. The role travels in argv and is consumed before testing parses
	// flags, so the proof claims no name in the product's environment namespace.
	orphanHangupSupervisorArg = "orphan-hangup-supervisor"

	// orphanHangupIntermediateScript is the generation whose death orphans the
	// supervisor. It leads its own session, so whoever adopts the supervisor
	// afterwards is guaranteed to sit outside the supervisor's session and the
	// supervisor's process group is genuinely orphaned no matter who reaps it.
	orphanHangupIntermediateScript = `"$1" "$2" &
wait
`

	// orphanHangupWitnessScript is both the stopped group member the kernel
	// requires before it hangs up a newly orphaned group and the only observer of
	// that hangup. Its trap reports on the supervisor's own report pipe, so the
	// proof reads the kernel's delivery receipt instead of inferring it. Arming is
	// announced on a second descriptor: a witness stopped before its trap exists
	// would meet a real hangup with the default disposition and turn the proof
	// into a silent lie.
	orphanHangupWitnessScript = `trap 'echo hangup >&3' HUP
echo armed >&4
while :; do sleep 30; done
`

	orphanHangupDeadline = 30 * time.Second
	orphanHangupQuiet    = 250 * time.Millisecond
	orphanHangupPoll     = 5 * time.Millisecond
)

func init() {
	if len(os.Args) != 2 || os.Args[1] != orphanHangupSupervisorArg {
		return
	}

	os.Exit(runOrphanHangupSupervisor())
}

// runOrphanHangupSupervisor stands in for a supervisor role: it installs the
// production hangup guard, leads its own process group as the guardian and the
// liveness supervisor do, owns a descendant inside that group, and quiesces only
// when its control channel closes — then exits through the production
// containment wait, so its completion report means the whole tree reached
// ECHILD.
func runOrphanHangupSupervisor() int {
	report := os.NewFile(3, "report")
	control := os.NewFile(4, "control")

	stopHangupGuard := installTurnSupervisorHangupGuard()
	defer stopHangupGuard()

	if err := syscall.Setpgid(0, 0); err != nil {
		fmt.Fprintf(report, "setpgid:%v\n", err)

		return 1
	}

	armedRead, armedWrite, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(report, "pipe:%v\n", err)

		return 1
	}

	witness := exec.Command("/bin/sh", "-c", orphanHangupWitnessScript)
	witness.ExtraFiles = []*os.File{report, armedWrite}
	witness.Stderr = os.Stderr

	if err = witness.Start(); err != nil {
		fmt.Fprintf(report, "witness:%v\n", err)

		return 1
	}

	_ = armedWrite.Close()

	if _, err = bufio.NewReader(armedRead).ReadString('\n'); err != nil {
		fmt.Fprintf(report, "armed:%v\n", err)

		return 1
	}

	_ = armedRead.Close()

	fmt.Fprintf(report, "ready:%d:%d\n", os.Getpid(), witness.Process.Pid)

	_, _ = io.Copy(io.Discard, control)

	if err = awaitLinuxSupervisorContainment(os.Getpid(), 0); err != nil {
		fmt.Fprintf(report, "containment:%v\n", err)

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

// readOrphanHangupReport takes one line off the report pipe under a deadline, so
// a supervisor the kernel killed fails the proof here instead of hanging it.
func readOrphanHangupReport(t *testing.T, pipe *os.File, reports *bufio.Reader) string {
	t.Helper()

	require.NoError(t, pipe.SetReadDeadline(time.Now().Add(orphanHangupDeadline)))

	line, err := reports.ReadString('\n')
	require.NoError(t, err, "read supervisor report")
	require.NoError(t, pipe.SetReadDeadline(time.Time{}))

	return strings.TrimSuffix(line, "\n")
}

// TestOrphanedSupervisorGroupSurvivesHangupUntilAuthenticatedQuiescence proves
// the containment property SIGHUP used to break: a supervisor process group that
// becomes orphaned — the guardian's ordinary fate, since it deliberately
// outlives the host and carries no Pdeathsig — is signalled SIGHUP by the kernel
// and must survive it, keeping its descendants until its authenticated control
// channel closes and the whole tree reaches ECHILD.
//
// Nothing here is skipped or tolerated. A `/bin/sh` witness stopped inside the
// supervisor's group is the stopped job the kernel requires before it hangs up a
// newly orphaned group, and the witness's own SIGHUP trap writes the receipt the
// proof reads: the kernel's delivery is observed, never assumed, and the SIGCONT
// that thawed the witness enough to run the trap is observed with it. An
// environment that cannot produce a genuine orphan — a subreaper inside the
// supervisor's own session — fails the test loudly instead of standing it down.
func TestOrphanedSupervisorGroupSurvivesHangupUntilAuthenticatedQuiescence(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)

	reportRead, reportWrite, err := os.Pipe()
	require.NoError(t, err)

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)

	intermediate := exec.Command(
		"/bin/sh", "-c", orphanHangupIntermediateScript, "sh", executable, orphanHangupSupervisorArg,
	)
	intermediate.ExtraFiles = []*os.File{reportWrite, controlRead}
	intermediate.Stderr = os.Stderr
	intermediate.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	require.NoError(t, intermediate.Start())

	// The test must not retain the child ends, or no reader here ever sees the
	// EOF that drives quiescence and completion.
	for _, file := range []*os.File{reportWrite, controlRead} {
		require.NoError(t, file.Close())
	}

	reports := bufio.NewReader(reportRead)

	readiness := readOrphanHangupReport(t, reportRead, reports)

	pids, found := strings.CutPrefix(readiness, "ready:")
	require.Truef(t, found, "supervisor report %q is not a readiness line", readiness)

	supervisorText, witnessText, found := strings.Cut(pids, ":")
	require.Truef(t, found, "supervisor readiness %q carries no witness pid", readiness)

	supervisorPID, err := strconv.Atoi(supervisorText)
	require.NoError(t, err)

	witnessPID, err := strconv.Atoi(witnessText)
	require.NoError(t, err)

	// A failed proof must leave nothing behind holding this binary's standard
	// error open, so the whole supervisor group goes, not just the pids the
	// report named.
	t.Cleanup(func() {
		_ = controlWrite.Close()
		_ = syscall.Kill(-supervisorPID, syscall.SIGCONT)
		_ = syscall.Kill(-supervisorPID, syscall.SIGKILL)
		_ = intermediate.Process.Kill()
		_ = intermediate.Wait()
		_ = reportRead.Close()
	})

	supervisor, err := readOrphanHangupStat(supervisorPID)
	require.NoError(t, err)
	require.Equal(t, supervisorPID, supervisor.group, "supervisor must lead its own process group")

	witness, err := readOrphanHangupStat(witnessPID)
	require.NoError(t, err)
	require.Equal(t, supervisorPID, witness.group, "witness must share the supervisor's process group")

	require.NoError(t, syscall.Kill(witnessPID, syscall.SIGSTOP))
	awaitOrphanHangupState(t, witnessPID, func(stat orphanHangupStat) bool { return stat.state == 'T' }, "stopped")

	require.NoError(t, intermediate.Process.Kill())
	_, _ = intermediate.Process.Wait()

	orphan, err := readOrphanHangupStat(supervisorPID)
	require.NoError(t, err)

	adopter, err := readOrphanHangupStat(orphan.parent)
	require.NoError(t, err)

	if adopter.session == orphan.session {
		t.Fatalf(
			"environment produced no genuine orphan: supervisor %d in session %d was adopted by %d in the same session",
			supervisorPID, orphan.session, orphan.parent,
		)
	}

	require.Equal(t, "hangup", readOrphanHangupReport(t, reportRead, reports),
		"the kernel never hung up the orphaned supervisor group")

	surviving, err := readOrphanHangupStat(supervisorPID)
	require.NoErrorf(t, err, "orphaned supervisor %d did not survive the kernel hangup", supervisorPID)
	require.NotEqualf(t, byte('Z'), surviving.state,
		"orphaned supervisor %d was terminated by the kernel hangup", supervisorPID)

	// Quiescence belongs to the control channel alone, so the report pipe must
	// stay silent while the hangup is the only thing that has happened.
	require.NoError(t, reportRead.SetReadDeadline(time.Now().Add(orphanHangupQuiet)))

	premature, err := reports.ReadString('\n')
	require.ErrorIsf(t, err, os.ErrDeadlineExceeded,
		"supervisor reported %q on the kernel hangup instead of waiting for its control channel", premature)

	held, err := readOrphanHangupStat(witnessPID)
	require.NoError(t, err, "supervised descendant did not survive the orphan hangup")
	require.NotEqual(t, byte('Z'), held.state, "supervised descendant did not survive the orphan hangup")

	require.NoError(t, controlWrite.Close())

	require.Equal(t, "complete", readOrphanHangupReport(t, reportRead, reports),
		"supervisor never reported containment completion")

	_, err = readOrphanHangupStat(witnessPID)
	require.ErrorIs(t, err, os.ErrNotExist, "completion was reported before the supervised tree was reaped")

	awaitOrphanHangupExit(t, supervisorPID)
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
