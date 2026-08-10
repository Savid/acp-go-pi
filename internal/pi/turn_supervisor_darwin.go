//go:build darwin

package pi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	darwinPipeWait            = 500 * time.Millisecond
	darwinLaunchBootstrapEnv  = "ACP_GO_PI_INTERNAL_DARWIN_LAUNCH"
	darwinLaunchBootstrapMode = "1"
)

type darwinLaunchConfig struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

var (
	darwinLaunchExecutable   = os.Executable
	darwinLaunchCommand      = exec.Command
	darwinLaunchExit         = os.Exit
	darwinLaunchExec         = syscall.Exec
	darwinLaunchInput        = inheritedDarwinLaunchInput
	darwinLaunchOpenFile     = os.NewFile
	darwinLaunchFcntl        = unix.FcntlInt
	darwinLaunchCloseOnExec  = setDarwinLaunchCloseOnExec
	darwinLaunchCreateTemp   = os.CreateTemp
	darwinLaunchFileChmod    = func(file *os.File, mode os.FileMode) error { return file.Chmod(mode) }
	darwinLaunchEncodeConfig = func(file *os.File, config darwinLaunchConfig) error { return json.NewEncoder(file).Encode(config) }
	darwinLaunchFileSeek     = func(file *os.File, offset int64, whence int) (int64, error) { return file.Seek(offset, whence) }
	darwinLaunchRemove       = os.Remove
	darwinLaunchPipe         = os.Pipe
	darwinLaunchStatusWait   = defaultProcessTreeWait
)

func init() {
	darwinLaunchBootstrap()
}

func darwinLaunchBootstrap() {
	if os.Getenv(darwinLaunchBootstrapEnv) != darwinLaunchBootstrapMode {
		return
	}

	configFile, gate, status, err := darwinLaunchInput()
	if err == nil {
		err = runDarwinLaunchBootstrap(configFile, gate)
	}

	if configFile != nil {
		_ = configFile.Close()
	}

	if gate != nil {
		_ = gate.Close()
	}

	if status != nil {
		if err != nil {
			_, _ = fmt.Fprintln(status, err)
		}

		_ = status.Close()
	}

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "acp-go-pi Darwin launch bootstrap:", err)

		darwinLaunchExit(1)

		return
	}

	darwinLaunchExit(0)
}

func inheritedDarwinLaunchInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	configFile := darwinLaunchOpenFile(3, "pi-darwin-launch-config")

	gate := darwinLaunchOpenFile(4, "pi-darwin-launch-gate")

	status := darwinLaunchOpenFile(5, "pi-darwin-launch-status")
	if configFile == nil || gate == nil || status == nil {
		return configFile, gate, status, errors.New("darwin native launch descriptors are unavailable")
	}

	if err := darwinLaunchCloseOnExec(int(status.Fd())); err != nil {
		return configFile, gate, status, err
	}

	return configFile, gate, status, nil
}

func setDarwinLaunchCloseOnExec(fd int) error {
	flags, err := darwinLaunchFcntl(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read inherited Pi Darwin launch descriptor flags: %w", err)
	}

	if _, err = darwinLaunchFcntl(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect inherited Pi Darwin launch descriptor from exec: %w", err)
	}

	return nil
}

func runDarwinLaunchBootstrap(configInput io.ReadCloser, gate io.ReadCloser) error {
	var config darwinLaunchConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode native launch config: %w", err)
	}

	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("native launch config is incomplete")
	}

	var release [1]byte
	if _, err := io.ReadFull(gate, release[:]); err != nil || release[0] != 1 {
		return errors.Join(errors.New("native launch was not released after containment validation"), err)
	}

	if err := errors.Join(configInput.Close(), gate.Close()); err != nil {
		return fmt.Errorf("close Darwin native launch descriptors: %w", err)
	}

	if err := darwinLaunchExec(config.Path, config.Args, config.Env); err != nil {
		return fmt.Errorf("exec native pi command: %w", err)
	}

	return nil
}

func prepareProcessTreeCommand(native *exec.Cmd, containment ContainmentSpec) (*processTreeCommand, error) {
	if containment.Isolation == nil && !containment.DarwinBestEffort {
		configureProcessCommandPlatform(native)

		return &processTreeCommand{cmd: native, ordinary: true}, nil
	}

	if containment.Isolation != nil {
		return nil, errors.New("explicit process isolation is supported only on linux")
	}

	if err := validateContainmentSpec(containment); err != nil {
		return nil, fmt.Errorf("%w: validate Darwin launch containment: %v", ErrProcessContainmentIncomplete, err)
	}

	config := darwinLaunchConfig{
		Path: native.Path,
		Args: append([]string(nil), native.Args...),
		Env:  scrubDarwinInternalEnvironment(native.Env),
	}
	if config.Path == "" || len(config.Args) == 0 {
		return nil, errors.New("prepare Darwin native launch: command is incomplete")
	}

	configFile, createErr := darwinLaunchCreateTemp(containment.GenerationRoot, ".launch-")
	if createErr != nil {
		return nil, fmt.Errorf("create Darwin native launch config: %w", createErr)
	}

	cleanupConfig := func() {
		_ = configFile.Close()
		_ = darwinLaunchRemove(configFile.Name())
	}
	if chmodErr := darwinLaunchFileChmod(configFile, 0o600); chmodErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("secure Darwin native launch config: %w", chmodErr)
	}

	if encodeErr := darwinLaunchEncodeConfig(configFile, config); encodeErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("encode Darwin native launch config: %w", encodeErr)
	}

	if _, seekErr := darwinLaunchFileSeek(configFile, 0, io.SeekStart); seekErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("rewind Darwin native launch config: %w", seekErr)
	}

	if unlinkErr := darwinLaunchRemove(configFile.Name()); unlinkErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("unlink Darwin native launch config: %w", unlinkErr)
	}

	gateRead, gateWrite, pipeErr := darwinLaunchPipe()
	if pipeErr != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("create Darwin native launch gate: %w", pipeErr)
	}

	statusRead, statusWrite, statusErr := darwinLaunchPipe()
	if statusErr != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()

		return nil, fmt.Errorf("create Darwin native launch status: %w", statusErr)
	}

	executable, executableErr := darwinLaunchExecutable()
	if executableErr != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()
		_ = statusRead.Close()
		_ = statusWrite.Close()

		return nil, fmt.Errorf("resolve Darwin native launch bootstrap: %w", executableErr)
	}

	helper := darwinLaunchCommand(executable) // #nosec G204 -- the current executable hosts the private launch bootstrap.
	helper.Dir = native.Dir

	helper.Env = darwinLaunchBootstrapEnvironment()

	helper.Stdin = native.Stdin
	helper.Stdout = native.Stdout
	helper.Stderr = native.Stderr
	helper.WaitDelay = darwinPipeWait
	helper.ExtraFiles = []*os.File{configFile, gateRead, statusWrite}
	configureProcessCommandPlatform(helper)

	return &processTreeCommand{
		cmd:       helper,
		inherited: []*os.File{configFile, gateRead, statusWrite},
		startGate: gateWrite,
		ready:     statusRead,
	}, nil
}

func scrubDarwinInternalEnvironment(environment []string) []string {
	scrubbed := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(name), privateEnvPrefix) {
			continue
		}

		scrubbed = append(scrubbed, entry)
	}

	return scrubbed
}

func darwinLaunchBootstrapEnvironment() []string {
	return []string{darwinLaunchBootstrapEnv + "=" + darwinLaunchBootstrapMode}
}

func awaitProcessTreeReady(launch *processTreeCommand) error {
	if launch.ordinary {
		return nil
	}

	if launch.ready == nil {
		return errors.New("darwin native launch status is unavailable")
	}

	status := launch.ready
	launch.ready = nil

	defer func() { _ = status.Close() }()

	if err := status.SetReadDeadline(time.Now().Add(darwinLaunchStatusWait)); err != nil {
		return fmt.Errorf("arm Darwin native launch status: %w", err)
	}

	const statusLimit = 4096

	contents, err := io.ReadAll(io.LimitReader(status, statusLimit+1))
	if err != nil {
		return fmt.Errorf("await Darwin native launch status: %w", err)
	}

	if len(contents) > statusLimit {
		return errors.New("darwin native launch failure exceeded the status limit")
	}

	if message := strings.TrimSpace(string(contents)); message != "" {
		return fmt.Errorf("exec native pi command: %s", message)
	}

	return nil
}
