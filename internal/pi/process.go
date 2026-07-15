package pi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"sync"
	"time"
)

// defaultShutdownStepTimeout bounds each rung of the shutdown ladder.
const defaultShutdownStepTimeout = 2 * time.Second

// defaultProcessTreeWait bounds the final containment-boundary proof.
const defaultProcessTreeWait = 5 * time.Second

// stderrTailLimit bounds the retained stderr tail used for error reporting.
const stderrTailLimit = 8 << 10

// baseEnvironmentKeys are the only parent environment variables a pi child
// inherits. Everything else is scrubbed: pi treats ambient provider API keys
// as live auth, so credentials must flow through explicit env additions.
var baseEnvironmentKeys = []string{"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "TERM"}
var processPipe = os.Pipe

// LaunchSpec describes one pi RPC-mode process launch.
type LaunchSpec struct {
	// ExecutablePath is the resolved pi binary.
	ExecutablePath string
	// AgentDir is the isolated per-session agent directory (PI_CODING_AGENT_DIR).
	AgentDir string
	// SessionDir is the per-session session storage directory (--session-dir).
	SessionDir string
	// SessionPath selects an existing session file at spawn (--session).
	SessionPath string
	// SessionID selects an exact session id, creating it if missing (--session-id).
	SessionID string
	// ExtensionPaths are wrapper-owned extension files loaded with -e, in order.
	ExtensionPaths []string
	// Env is added to the scrubbed base environment. Wrapper-managed keys
	// (PI_CODING_AGENT_DIR, PI_OFFLINE) always win.
	Env map[string]string
	// Cwd is the child working directory.
	Cwd string
	// ShutdownStepTimeout bounds each rung of the shutdown ladder; zero uses
	// the default.
	ShutdownStepTimeout time.Duration
}

// Args returns the pi CLI argument list for the launch: RPC mode with all
// ambient resource discovery disabled, only wrapper-owned extensions loaded,
// project-local resources ignored, and per-session session storage.
func (spec LaunchSpec) Args() []string {
	args := []string{"--mode", "rpc", "--no-extensions"}

	for _, path := range spec.ExtensionPaths {
		args = append(args, "-e", path)
	}

	args = append(args,
		"--no-skills",
		"--no-prompt-templates",
		"--no-themes",
		"--session-dir", spec.SessionDir,
		"--no-approve",
	)

	if spec.SessionPath != "" {
		args = append(args, "--session", spec.SessionPath)
	}

	if spec.SessionID != "" {
		args = append(args, "--session-id", spec.SessionID)
	}

	return args
}

// Environ returns the scrubbed child environment: explicit basics from the
// parent, then spec.Env, then the wrapper-managed keys, which always win.
func (spec LaunchSpec) Environ() []string {
	env := make(map[string]string, len(baseEnvironmentKeys)+len(spec.Env)+2)

	for _, key := range baseEnvironmentKeys {
		if value, ok := os.LookupEnv(key); ok {
			env[key] = value
		}
	}

	for key, value := range spec.Env {
		env[key] = value
	}

	env["PI_OFFLINE"] = "1"
	env["PI_CODING_AGENT_DIR"] = spec.AgentDir

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	environ := make([]string, 0, len(keys))
	for _, key := range keys {
		environ = append(environ, key+"="+env[key])
	}

	return environ
}

// Process is one running pi RPC-mode child process.
type Process struct {
	cmd    *exec.Cmd
	tree   *processTree
	stdin  *os.File
	stdout *os.File
	stderr *tailBuffer

	shutdownStepTimeout time.Duration

	stdinOnce  sync.Once
	stdinErr   error
	stdoutOnce sync.Once

	exited  chan struct{}
	waitErr error
}

// StartProcess launches pi per spec with a scrubbed environment, its own
// process group, and parent-death enforcement where the platform supports it.
func StartProcess(ctx context.Context, spec LaunchSpec) (*Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if spec.ExecutablePath == "" {
		return nil, fmt.Errorf("pi executable path is required")
	}

	stdinRead, stdinWrite, err := processPipe()
	if err != nil {
		return nil, fmt.Errorf("create stdin pipe: %w", err)
	}

	stdoutRead, stdoutWrite, err := processPipe()
	if err != nil {
		closeQuietly(stdinRead, stdinWrite)

		return nil, fmt.Errorf("create stdout pipe: %w", err)
	}

	stderr := &tailBuffer{limit: stderrTailLimit}

	// The context owns the child: cancellation triggers the platform Cancel
	// (group SIGTERM where supported), backstopping the shutdown ladder.
	cmd := exec.CommandContext(ctx, spec.ExecutablePath, spec.Args()...) // #nosec G204 -- launches the operator-configured pi binary with wrapper-built args.
	cmd.Dir = spec.Cwd
	cmd.Env = spec.Environ()
	cmd.Stdin = stdinRead
	cmd.Stdout = stdoutWrite
	cmd.Stderr = stderr
	configureProcessCommandPlatform(cmd)

	tree, err := startProcessTree(cmd)
	if err != nil {
		closeQuietly(stdinRead, stdinWrite, stdoutRead, stdoutWrite)

		return nil, fmt.Errorf("start pi process: %w", err)
	}

	closeQuietly(stdinRead, stdoutWrite)

	stepTimeout := spec.ShutdownStepTimeout
	if stepTimeout <= 0 {
		stepTimeout = defaultShutdownStepTimeout
	}

	process := &Process{
		cmd:                 cmd,
		tree:                tree,
		stdin:               stdinWrite,
		stdout:              stdoutRead,
		stderr:              stderr,
		shutdownStepTimeout: stepTimeout,
		exited:              make(chan struct{}),
	}
	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)

		_ = tree.kill()
	})

	wait := func() {
		process.waitErr = cmd.Wait()

		if stopCancellation() {
			close(cancellationDone)
		}

		<-cancellationDone

		close(process.exited)
	}

	go wait()

	return process, nil
}

// Stdin returns the child stdin writer.
func (p *Process) Stdin() *os.File {
	return p.stdin
}

// Stdout returns the child stdout reader.
func (p *Process) Stdout() *os.File {
	return p.stdout
}

// CloseStdin signals end of input; pi exits immediately on stdin EOF.
func (p *Process) CloseStdin() error {
	p.stdinOnce.Do(func() {
		p.stdinErr = p.stdin.Close()
	})

	return p.stdinErr
}

// Exited is closed once the child has been reaped.
func (p *Process) Exited() <-chan struct{} {
	return p.exited
}

// WaitErr returns the child exit error after Exited is closed; nil for a
// clean exit.
func (p *Process) WaitErr() error {
	select {
	case <-p.exited:
		return p.waitErr
	default:
		return fmt.Errorf("pi process still running")
	}
}

// StderrTail returns the retained tail of child stderr.
func (p *Process) StderrTail() string {
	return p.stderr.String()
}

// Shutdown runs the shutdown ladder: stdin EOF, then SIGTERM to the process
// group, then SIGKILL, each rung bounded by the per-step timeout and by ctx.
// It returns once the child has been reaped or ctx ends.
func (p *Process) Shutdown(ctx context.Context) error {
	_ = p.CloseStdin()

	if p.waitStep(ctx) {
		return p.tree.terminateAndWait(defaultProcessTreeWait)
	}

	if err := p.tree.terminate(); err != nil {
		return fmt.Errorf("terminate pi process: %w", err)
	}

	if p.waitStep(ctx) {
		return p.tree.terminateAndWait(defaultProcessTreeWait)
	}

	if err := p.tree.kill(); err != nil {
		return fmt.Errorf("kill pi process: %w", err)
	}

	select {
	case <-p.exited:
		return p.tree.terminateAndWait(defaultProcessTreeWait)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Kill forcefully terminates the child process group.
func (p *Process) Kill() error {
	if err := p.tree.kill(); err != nil {
		return fmt.Errorf("kill pi process: %w", err)
	}

	return nil
}

// Close releases the parent-held pipe ends. Call after the reader is done.
func (p *Process) Close() error {
	_ = p.CloseStdin()
	quiescenceErr := p.tree.terminateAndWait(defaultProcessTreeWait)

	p.stdoutOnce.Do(func() {
		_ = p.stdout.Close()
	})

	return quiescenceErr
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

func closeQuietly(files ...*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}

// tailBuffer is a bounded writer retaining only the most recent bytes.
type tailBuffer struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

// Write implements io.Writer, retaining at most limit trailing bytes.
func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)
	if len(b.data) > b.limit {
		b.data = b.data[len(b.data)-b.limit:]
	}

	return len(p), nil
}

// String returns the retained tail.
func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.data)
}
