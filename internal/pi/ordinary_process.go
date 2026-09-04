package pi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultShutdownStepTimeout = 2 * time.Second
	stderrTailLimit            = 8 << 10
	envPath                    = "PATH"
	envHome                    = "HOME"
	envNodeOptions             = "NODE_OPTIONS"
	envBashEnv                 = "BASH_ENV"
	envShellEnv                = "ENV"
	privateEnvPrefix           = "ACP_" + "GO_PI_INTERNAL_"
)

var (
	ordinaryProcessPipe = os.Pipe
	ordinaryProcessKill = func(process *os.Process) error { return process.Kill() }
)

type LaunchSpec struct {
	ExecutablePath      string
	NativeRoot          string
	AgentDir            string
	SessionDir          string
	SessionPath         string
	SessionID           string
	ExtensionPaths      []string
	SkillPaths          []string
	PromptTemplatePaths []string
	Env                 map[string]string
	BaseEnvironment     map[string]string
	ExtraPathDirs       []string
	BrowserShim         *BrowserShim
	Cwd                 string
	ShutdownStepTimeout time.Duration
}

func (spec LaunchSpec) Args() []string {
	args := []string{"--mode", "rpc", "--no-extensions"}
	for _, path := range spec.ExtensionPaths {
		args = append(args, "-e", path)
	}

	for _, path := range spec.SkillPaths {
		args = append(args, "--skill", path)
	}

	for _, path := range spec.PromptTemplatePaths {
		args = append(args, "--prompt-template", path)
	}

	args = append(args, "--no-skills", "--no-prompt-templates", "--no-themes", "--session-dir", spec.SessionDir, "--no-approve")
	if spec.SessionPath != "" {
		args = append(args, "--session", spec.SessionPath)
	}

	if spec.SessionID != "" {
		args = append(args, "--session-id", spec.SessionID)
	}

	return args
}

func (spec LaunchSpec) Environ() []string {
	base := make(map[string]string, len(spec.BaseEnvironment))
	for key, value := range spec.BaseEnvironment {
		if ordinaryEnvironmentKey(key) {
			base[key] = value
		}
	}

	explicit := make(map[string]string, len(spec.Env))
	for key, value := range spec.Env {
		if safeExplicitEnvKey(key) {
			explicit[key] = value
		}
	}

	env := ComposeEnvironment(base, explicit)
	if search := prependPathDirs(env[envPath], spec.ExtraPathDirs); search != "" {
		env[envPath] = search
	}

	env = ComposeEnvironment(env, map[string]string{"PI_OFFLINE": "1", "PI_CODING_AGENT_DIR": spec.AgentDir})

	environment := spec.BrowserShim.Environ(environmentEntries(env))
	sort.Strings(environment)

	return environment
}

func prependPathDirs(search string, dirs []string) string {
	entries := make([]string, 0, len(dirs)+1)
	for _, dir := range dirs {
		if filepath.IsAbs(dir) && !strings.ContainsRune(dir, os.PathListSeparator) {
			entries = append(entries, dir)
		}
	}

	if len(entries) == 0 {
		return search
	}

	if search != "" {
		entries = append(entries, search)
	}

	return strings.Join(entries, string(os.PathListSeparator))
}

func safeExplicitEnvKey(key string) bool {
	if key == "" {
		return false
	}

	for index, r := range key {
		switch {
		case r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z'):
		case r >= '0' && r <= '9' && index > 0:
		default:
			return false
		}
	}

	upper := strings.ToUpper(key)
	if upper == envNodeOptions || upper == envBashEnv || upper == envShellEnv || strings.HasPrefix(upper, privateEnvPrefix) {
		return false
	}

	return !strings.HasPrefix(upper, "LD_") && !strings.HasPrefix(upper, "DYLD_")
}

type Process struct {
	cmd    *exec.Cmd
	stdin  ioWriteCloser
	stdout ioReadCloser
	stderr *tailBuffer

	shutdownStepTimeout time.Duration
	stdinOnce           sync.Once
	stdinErr            error
	stdoutOnce          sync.Once
	exited              chan struct{}
	waitErr             error
}

type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}
type ioReadCloser interface {
	Read([]byte) (int, error)
	Close() error
}

func StartOrdinaryProcess(ctx context.Context, spec LaunchSpec) (*Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if spec.ExecutablePath == "" {
		return nil, errors.New("pi executable path is required")
	}

	environment := spec.Environ()

	executable, err := lookPathInOrdinaryEnvironment(spec.ExecutablePath, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve pi executable: %w", err)
	}

	cmd := exec.Command(executable, spec.Args()...)
	cmd.Dir = spec.Cwd
	cmd.Env = environment

	stdinRead, stdin, err := ordinaryProcessPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdin: %w", err)
	}

	stdout, stdoutWrite, err := ordinaryProcessPipe()
	if err != nil {
		_ = stdinRead.Close()
		_ = stdin.Close()

		return nil, fmt.Errorf("create native stdout: %w", err)
	}

	cmd.Stdin = stdinRead
	cmd.Stdout = stdoutWrite

	stderr := &tailBuffer{limit: stderrTailLimit}

	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdinRead.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWrite.Close()

		return nil, fmt.Errorf("start pi process: %w", err)
	}

	_ = stdinRead.Close()
	_ = stdoutWrite.Close()

	step := spec.ShutdownStepTimeout
	if step <= 0 {
		step = defaultShutdownStepTimeout
	}

	p := &Process{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, shutdownStepTimeout: step, exited: make(chan struct{})}
	go func() { p.waitErr = cmd.Wait(); close(p.exited) }()

	return p, nil
}

func (p *Process) Stdin() ioWriteCloser { return p.stdin }
func (p *Process) Stdout() ioReadCloser { return p.stdout }
func (p *Process) CloseStdin() error {
	p.stdinOnce.Do(func() { p.stdinErr = p.stdin.Close() })

	return p.stdinErr
}
func (p *Process) Exited() <-chan struct{} { return p.exited }
func (p *Process) WaitErr() error {
	select {
	case <-p.exited:
		return p.waitErr
	default:
		return errors.New("pi process still running")
	}
}
func (p *Process) StderrTail() string { return p.stderr.String() }
func (p *Process) Shutdown(ctx context.Context) error {
	_ = p.CloseStdin()
	if p.waitStep(ctx) {
		return nil
	}

	if err := p.Kill(); err != nil {
		return err
	}

	select {
	case <-p.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *Process) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	err := ordinaryProcessKill(p.cmd.Process)
	if ordinaryProcessAlreadyFinished(err) {
		return nil
	}

	return err
}
func (p *Process) Close() error {
	_ = p.CloseStdin()
	select {
	case <-p.exited:
	default:
		_ = p.Kill()
		<-p.exited
	}

	p.stdoutOnce.Do(func() { _ = p.stdout.Close() })

	return nil
}
func (p *Process) waitStep(ctx context.Context) bool {
	timer := time.NewTimer(p.shutdownStepTimeout)
	defer timer.Stop()

	select {
	case <-p.exited:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)
	if len(b.data) > b.limit {
		b.data = b.data[len(b.data)-b.limit:]
	}

	return len(p), nil
}
func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.data)
}
