//go:build linux

package pi

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	turnSupervisorModeEnv = "ACP_GO_PI_INTERNAL_TURN_SUPERVISOR"
	turnSupervisorMode    = "1"
	turnSupervisorFDName  = "acp-go-pi-turn-supervisor"
	turnSupervisorReady   = "ready\n"
)

type turnSupervisorConfig struct {
	Path      string           `json:"path"`
	Args      []string         `json:"args"`
	Dir       string           `json:"dir"`
	Env       []string         `json:"env"`
	Isolation ProcessIsolation `json:"isolation"`
}

type linuxProcessIdentity struct {
	pid       int
	parentPID int
	state     byte
	startTime string
}

var (
	turnSupervisorExecutable   = os.Executable
	turnSupervisorMemfd        = unix.MemfdCreate
	turnSupervisorPipe         = os.Pipe
	turnSupervisorExit         = os.Exit
	turnSupervisorSignalNotify = signal.Notify
	turnSupervisorSignalStop   = signal.Stop
	turnSupervisorEnable       = enableTurnSupervisor
	turnSupervisorCommand      = exec.Command
	turnSupervisorContain      = awaitLinuxSupervisorContainment
	turnSupervisorProcessID    = os.Getpid
	turnSupervisorSignalGroup  = signalProcessGroupID
	turnSupervisorWriteConfig  = writeTurnSupervisorConfig
	turnSupervisorDescendants  = linuxDescendants
	turnSupervisorIdentity     = readLinuxProcessIdentity
	turnSupervisorSignalPID    = signalLinuxIdentity
	turnSupervisorWait4        = unix.Wait4
	turnSupervisorSleep        = time.Sleep
	turnSupervisorProcRoot     = "/proc"
	turnSupervisorRun          = runTurnSupervisor
	turnSupervisorOpenFile     = os.NewFile
	turnSupervisorCloseOnExec  = unix.CloseOnExec
	turnSupervisorInput        = inheritedTurnSupervisorInput
	turnSupervisorPrctl        = unix.Prctl
	turnSupervisorSetrlimit    = unix.Setrlimit
	turnSupervisorAcquireLock  = acquireAgentIdentityLock
	turnSupervisorSealConfig   = unix.FcntlInt
	turnSupervisorEffectiveUID = os.Geteuid
)

func enableTurnSupervisor() error {
	if err := turnSupervisorSetrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return fmt.Errorf("disable pi native core dumps: %w", err)
	}
	if err := turnSupervisorPrctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return err
	}
	if err := turnSupervisorPrctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}

	return turnSupervisorPrctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
}

func inheritedTurnSupervisorInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	config := turnSupervisorOpenFile(3, "pi-turn-supervisor-config")
	control := turnSupervisorOpenFile(4, "pi-turn-supervisor-control")

	ready := turnSupervisorOpenFile(5, "pi-turn-supervisor-ready")
	if config == nil || control == nil || ready == nil {
		return nil, nil, nil, errors.New("turn supervisor inherited descriptors are unavailable")
	}

	turnSupervisorCloseOnExec(int(config.Fd()))
	turnSupervisorCloseOnExec(int(control.Fd()))
	turnSupervisorCloseOnExec(int(ready.Fd()))

	return config, control, ready, nil
}

func init() {
	turnSupervisorBootstrap()
}

func turnSupervisorBootstrap() {
	if os.Getenv(turnSupervisorModeEnv) != turnSupervisorMode {
		return
	}

	var err error
	var config, control io.ReadCloser
	var ready io.WriteCloser
	config, control, ready, err = turnSupervisorInput()
	if err == nil {
		err = turnSupervisorRun(config, control, ready)
	}

	if config != nil {
		_ = config.Close()
	}

	if control != nil {
		_ = control.Close()
	}

	if ready != nil {
		_ = ready.Close()
	}

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "acp-go-pi turn supervisor:", err)

		exitCode := 1

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() >= 0 {
			exitCode = exitErr.ExitCode()
		}

		turnSupervisorExit(exitCode)

		return
	}

	turnSupervisorExit(0)
}

func prepareProcessTreeCommand(native *exec.Cmd, containment ContainmentSpec) (*processTreeCommand, error) {
	if containment.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin best-effort containment is invalid on linux", ErrProcessContainmentIncomplete)
	}
	if err := validateProcessIsolation(containment.Isolation); err != nil {
		return nil, fmt.Errorf("prepare pi turn supervisor isolation: %w", err)
	}
	if err := validateTurnSupervisorIdentity(containment.Isolation); err != nil {
		return nil, fmt.Errorf("prepare pi turn supervisor identity: %w", err)
	}

	config := turnSupervisorConfig{
		Path:      native.Path,
		Args:      append([]string(nil), native.Args...),
		Dir:       native.Dir,
		Env:       append([]string(nil), native.Env...),
		Isolation: *containment.Isolation,
	}
	if config.Path == "" || len(config.Args) == 0 {
		return nil, errors.New("prepare pi turn supervisor: native command is incomplete")
	}

	configFD, err := turnSupervisorMemfd(turnSupervisorFDName, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("prepare pi turn supervisor config: %w", err)
	}

	configFile := os.NewFile(uintptr(configFD), turnSupervisorFDName)
	if writeErr := turnSupervisorWriteConfig(configFile, config); writeErr != nil {
		_ = configFile.Close()

		return nil, writeErr
	}
	if _, sealErr := turnSupervisorSealConfig(configFile.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); sealErr != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("seal pi turn supervisor config: %w", sealErr)
	}

	controlRead, controlWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("prepare pi turn supervisor control: %w", err)
	}

	readyRead, readyWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()

		return nil, fmt.Errorf("prepare pi turn supervisor readiness: %w", err)
	}

	executable, err := turnSupervisorExecutable()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = readyRead.Close()
		_ = readyWrite.Close()

		return nil, fmt.Errorf("resolve embedded pi turn supervisor: %w", err)
	}

	helper := turnSupervisorCommand(executable) // #nosec G204 -- the current executable hosts the private supervisor mode.
	helper.Dir = "/"
	helper.Env = turnSupervisorEnvironment()
	helper.ExtraFiles = []*os.File{configFile, controlRead, readyWrite}
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	helper.Stdin = native.Stdin
	helper.Stdout = native.Stdout
	helper.Stderr = native.Stderr
	helper.WaitDelay = defaultProcessTreeWait

	return &processTreeCommand{
		cmd:             helper,
		inherited:       []*os.File{configFile, controlRead, readyWrite},
		control:         controlWrite,
		ready:           readyRead,
		nativeIsolation: true,
	}, nil
}

func awaitProcessTreeReady(launch *processTreeCommand) error {
	if launch.ready == nil {
		return nil
	}

	if err := launch.ready.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("arm pi turn supervisor readiness: %w", err)
	}

	launch.status = bufio.NewReader(launch.ready)

	line, err := launch.status.ReadString('\n')
	if err != nil {
		return fmt.Errorf("await pi turn supervisor readiness: %w", err)
	}

	if line != turnSupervisorReady {
		return fmt.Errorf("invalid pi turn supervisor readiness %q", strings.TrimSpace(line))
	}

	return nil
}

func writeTurnSupervisorConfig(file io.WriteSeeker, config turnSupervisorConfig) error {
	if err := json.NewEncoder(file).Encode(config); err != nil {
		return fmt.Errorf("encode pi turn supervisor config: %w", err)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind pi turn supervisor config: %w", err)
	}

	return nil
}

func turnSupervisorNativeEnvironment(configured []string) []string {
	return withoutTurnSupervisorMode(configured)
}

func turnSupervisorEnvironment() []string {
	return []string{
		turnSupervisorModeEnv + "=" + turnSupervisorMode,
	}
}

func withoutTurnSupervisorMode(configured []string) []string {
	env := make([]string, 0, len(configured))
	for _, entry := range configured {
		if strings.HasPrefix(entry, turnSupervisorModeEnv+"=") {
			continue
		}

		env = append(env, entry)
	}

	return env
}

func startTurnSupervisorNative(native *exec.Cmd, isolation *ProcessIsolation) (<-chan error, error, error) {
	var privilegeErr error
	waitDone, startErr := startCommandOnCreatorThread(func() error {
		if err := turnSupervisorEnable(); err != nil {
			privilegeErr = err

			return err
		}
		if err := applyProcessIsolation(native, isolation); err != nil {
			privilegeErr = fmt.Errorf("apply pi native process isolation: %w", err)

			return privilegeErr
		}

		return native.Start()
	}, native.Wait)
	if privilegeErr != nil {
		return nil, privilegeErr, nil
	}

	return waitDone, nil, startErr
}

func runTurnSupervisor(configInput io.Reader, controlInput io.Reader, readyOutput io.Writer) error {
	var config turnSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode pi turn supervisor config: %w", err)
	}

	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("pi turn supervisor config is incomplete")
	}
	if err := validateProcessIsolation(&config.Isolation); err != nil {
		return fmt.Errorf("validate pi turn supervisor isolation: %w", err)
	}
	if err := validateTurnSupervisorIdentity(&config.Isolation); err != nil {
		return fmt.Errorf("validate pi turn supervisor identity: %w", err)
	}

	signals := make(chan os.Signal, 2)

	turnSupervisorSignalNotify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer turnSupervisorSignalStop(signals)

	controlDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, controlInput)
		close(controlDone)
	}()

	identityLock, err := turnSupervisorAcquireLock(config.Isolation.UID, config.Isolation.TestOnlyNoCredential, config.Isolation.TestOnlyIdentityLockRoot, controlDone, signals)
	if err != nil {
		return fmt.Errorf("acquire pi agent identity lock: %w", err)
	}
	defer identityLock.Close()

	native := turnSupervisorCommand(config.Path, config.Args[1:]...) // #nosec G204 -- private config came from the wrapper-built pi command.
	native.Args = append([]string(nil), config.Args...)
	native.Dir = config.Dir
	native.Env = turnSupervisorNativeEnvironment(config.Env)
	native.Stdin = os.Stdin
	native.Stdout = os.Stdout
	native.Stderr = os.Stderr
	native.SysProcAttr = processSysProcAttr()

	waitDone, enableErr, startErr := startTurnSupervisorNative(native, &config.Isolation)
	if enableErr != nil {
		return fmt.Errorf("enable pi native supervisor privileges: %w", enableErr)
	}

	if startErr != nil {
		return fmt.Errorf("start supervised pi native root: %w", startErr)
	}

	if _, err := io.WriteString(readyOutput, turnSupervisorReady); err != nil {
		_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
		waitErr := <-waitDone
		containErr := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid)

		return errors.Join(fmt.Errorf("publish pi turn supervisor readiness: %w", err), containErr, waitErr)
	}

	for {
		select {
		case waitErr := <-waitDone:
			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			if err := publishTurnSupervisorProof(readyOutput); err != nil {
				return err
			}

			return waitErr
		case <-controlDone:
			_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
			waitErr := <-waitDone

			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			if err := publishTurnSupervisorProof(readyOutput); err != nil {
				return err
			}

			return waitErr
		case received := <-signals:
			nativeSignal, ok := received.(syscall.Signal)
			if !ok {
				continue
			}

			_ = turnSupervisorSignalGroup(native.Process.Pid, nativeSignal)
		}
	}
}

func publishTurnSupervisorProof(output io.Writer) error {
	if _, err := io.WriteString(output, turnSupervisorComplete); err != nil {
		return fmt.Errorf("publish pi turn supervisor containment proof: %w", err)
	}

	return nil
}

func validateTurnSupervisorIdentity(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation is required")
	}

	effectiveUID := turnSupervisorEffectiveUID()
	if effectiveUID != 0 {
		return fmt.Errorf("trusted root identity is required, effective uid is %d", effectiveUID)
	}
	if isolation.UID == uint32(effectiveUID) {
		return errors.New("native target identity must differ from the trusted supervisor")
	}

	return nil
}

// awaitLinuxSupervisorContainment keeps the dedicated subreaper alive on an
// unproven tree. A bounded parent wait therefore retains its permit while the
// helper keeps retrying until its exit can truthfully publish quiescence.
func awaitLinuxSupervisorContainment(supervisorPID int, nativePID int) error {
	for {
		if err := containLinuxSupervisorDescendants(supervisorPID, nativePID); err == nil {
			return nil
		}

		turnSupervisorSleep(time.Second)
	}
}

func containLinuxSupervisorDescendants(supervisorPID int, nativePID int) error {
	_ = turnSupervisorSignalGroup(nativePID, syscall.SIGKILL)

	for {
		descendants, err := turnSupervisorDescendants(supervisorPID)
		if err != nil {
			return fmt.Errorf("%w: enumerate supervised pi descendants: %v", ErrProcessContainmentIncomplete, err)
		}

		for _, descendant := range descendants {
			if descendant.state != 'Z' {
				if err := turnSupervisorSignalPID(descendant, syscall.SIGKILL); err != nil {
					return fmt.Errorf("%w: kill supervised pi descendant %d: %v", ErrProcessContainmentIncomplete, descendant.pid, err)
				}
			}
		}

		for {
			reapedPID, waitErr := turnSupervisorWait4(-1, nil, unix.WNOHANG, nil)
			if reapedPID > 0 {
				continue
			}

			if errors.Is(waitErr, syscall.EINTR) {
				continue
			}

			if errors.Is(waitErr, syscall.ECHILD) {
				return nil
			}

			if waitErr != nil {
				return fmt.Errorf("%w: inspect supervised pi child set: %v", ErrProcessContainmentIncomplete, waitErr)
			}

			break
		}

		turnSupervisorSleep(5 * time.Millisecond)
	}
}

func linuxDescendants(rootPID int) ([]linuxProcessIdentity, error) {
	entries, err := os.ReadDir(turnSupervisorProcRoot)
	if err != nil {
		return nil, err
	}

	children := make(map[int][]linuxProcessIdentity)

	for _, entry := range entries {
		pid, parseErr := strconv.Atoi(entry.Name())
		if parseErr != nil {
			continue
		}

		identity, readErr := turnSupervisorIdentity(pid)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}

		if readErr != nil {
			return nil, readErr
		}

		children[identity.parentPID] = append(children[identity.parentPID], identity)
	}

	result := make([]linuxProcessIdentity, 0)

	queue := append([]linuxProcessIdentity(nil), children[rootPID]...)

	for len(queue) > 0 {
		identity := queue[0]
		queue = queue[1:]

		result = append(result, identity)
		queue = append(queue, children[identity.pid]...)
	}

	return result, nil
}

func readLinuxProcessIdentity(pid int) (linuxProcessIdentity, error) {
	raw, err := os.ReadFile(filepath.Join(turnSupervisorProcRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return linuxProcessIdentity{}, err
	}

	line := string(raw)

	closing := strings.LastIndexByte(line, ')')
	if closing < 0 || closing+2 >= len(line) {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat: malformed comm field", pid)
	}

	fields := strings.Fields(line[closing+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat: incomplete fields", pid)
	}

	parentPID, err := strconv.Atoi(fields[1])
	if err != nil {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat parent: %w", pid, err)
	}

	return linuxProcessIdentity{
		pid:       pid,
		parentPID: parentPID,
		state:     fields[0][0],
		startTime: fields[19],
	}, nil
}

func signalLinuxIdentity(identity linuxProcessIdentity, processSignal syscall.Signal) error {
	current, err := turnSupervisorIdentity(identity.pid)
	if errors.Is(err, os.ErrNotExist) || (err == nil && current.startTime != identity.startTime) {
		return nil
	}

	if err != nil {
		return err
	}

	if err := syscallKill(identity.pid, processSignal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}
