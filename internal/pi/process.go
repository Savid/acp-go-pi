package pi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
)

// defaultShutdownStepTimeout bounds each rung of the shutdown ladder.
const defaultShutdownStepTimeout = 2 * time.Second

// defaultProcessTreeWait bounds completion of the selected containment boundary.
const defaultProcessTreeWait = 5 * time.Second

// stderrTailLimit bounds the retained stderr tail used for error reporting.
const stderrTailLimit = 8 << 10

const (
	envPath          = "PATH"
	envNodeOptions   = "NODE_OPTIONS"
	envBashEnv       = "BASH_ENV"
	envShellEnv      = "ENV"
	envRuntimeID     = "ACP_GO_PI_RUNTIME_ID"
	envScratchRoot   = "ACP_GO_PI_SCRATCH_ROOT"
	privateEnvPrefix = "ACP_" + "GO_PI_INTERNAL_"
)

// baseEnvironmentKeys are the only parent environment variables a pi child
// inherits. Everything else is scrubbed: pi treats ambient provider API keys
// as live auth, so credentials must flow through explicit env additions.
var baseEnvironmentKeys = []string{envPath, "HOME", "TMPDIR", "LANG", "LC_ALL", "TERM"}
var processPipe = os.Pipe
var processPrepareTreeCommand = prepareProcessTreeCommand
var processPrepareContainmentRecord = prepareContainmentRecord
var processStartTree = startProcessTree
var processAfterPrepare = func() {}
var processDirectChildWait = func(tree *processTree) *directChildWait { return tree.directChildWait() }
var processTreeTerminate = func(tree *processTree) error { return tree.terminate() }
var processTreeKill = func(tree *processTree) error { return tree.kill() }
var processTreeTerminateAndWait = func(tree *processTree, timeout time.Duration) error { return tree.terminateAndWait(timeout) }
var processKillWait = defaultProcessTreeWait

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
	// ExtensionPaths are explicit seed and wrapper-owned extension files loaded with -e, in order.
	ExtensionPaths []string
	// SkillPaths are explicitly seeded skill files loaded despite disabled discovery.
	SkillPaths []string
	// PromptTemplatePaths are explicitly seeded prompt templates loaded despite disabled discovery.
	PromptTemplatePaths []string
	// Env is added to the scrubbed base environment. Wrapper-managed keys
	// (PI_CODING_AGENT_DIR, PI_OFFLINE) always win.
	Env map[string]string
	// Cwd is the child working directory.
	Cwd string
	// ShutdownStepTimeout bounds each rung of the shutdown ladder; zero uses
	// the default.
	ShutdownStepTimeout time.Duration
	Containment         ContainmentSpec
}

// Args returns the pi CLI argument list for the launch: RPC mode with all
// ambient resource discovery disabled, only explicit seed and wrapper-owned
// resources loaded, project-local resources ignored, and per-session storage.
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
		if safeExplicitEnvKey(key) {
			env[key] = value
		}
	}

	env["PI_OFFLINE"] = "1"

	env["PI_CODING_AGENT_DIR"] = spec.AgentDir
	if spec.Containment.DarwinBestEffort {
		env[envRuntimeID] = spec.Containment.RuntimeID
		env[envScratchRoot] = spec.Containment.GenerationRoot
	}

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

// safeExplicitEnvKey is defense in depth for internal LaunchSpec callers.
// Public options reject these keys before launch; this boundary also drops
// malformed names and loader, shell, PATH, and Node injection vectors.
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
	if upper == envPath || upper == envNodeOptions || upper == envBashEnv || upper == envShellEnv {
		return false
	}

	if strings.HasPrefix(upper, privateEnvPrefix) {
		return false
	}

	return !strings.HasPrefix(upper, "LD_") && !strings.HasPrefix(upper, "DYLD_")
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

// ProviderDescendantCount returns the absolute number of processes in the
// native containment boundary when that boundary provides authoritative
// inventory. A false result means no observation may be inferred.
func (p *Process) ProviderDescendantCount() (int, bool) {
	if p == nil || p.tree == nil {
		return 0, false
	}

	return p.tree.descendantCount()
}

// StartProcess launches pi per spec with a scrubbed environment, its own
// process group, and parent-death enforcement where the platform supports it.
func StartProcess(ctx context.Context, spec LaunchSpec) (*Process, error) {
	contextErr := ctx.Err()
	if contextErr != nil {
		return nil, contextErr
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

	// Cancellation is joined below only after the original process boundary has
	// been captured. An exec.Cmd context watcher would be a second, racing
	// signal owner and could rediscover a reused process-group id.
	cmd := exec.Command(spec.ExecutablePath, spec.Args()...) // #nosec G204 -- launches the operator-configured pi binary with wrapper-built args.
	cmd.Dir = spec.Cwd
	cmd.Env = spec.Environ()
	cmd.Stdin = stdinRead
	cmd.Stdout = stdoutWrite
	cmd.Stderr = stderr

	launch, err := processPrepareTreeCommand(cmd, spec.Containment)
	if err != nil {
		closeQuietly(stdinRead, stdinWrite, stdoutRead, stdoutWrite)

		return nil, fmt.Errorf("prepare pi process: %w", err)
	}

	launch.containment, err = processPrepareContainmentRecord(spec.Containment)
	if err != nil {
		launch.close()
		closeQuietly(stdinRead, stdinWrite, stdoutRead, stdoutWrite)

		return nil, fmt.Errorf("prepare pi containment record: %w", err)
	}

	processAfterPrepare()

	contextErr = ctx.Err()
	if contextErr != nil {
		recordErr := completeUnstartedContainment(launch.containment)
		launch.close()
		closeQuietly(stdinRead, stdinWrite, stdoutRead, stdoutWrite)

		return nil, errors.Join(contextErr, recordErr)
	}

	cmd = launch.cmd

	tree, err := processStartTree(launch)
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

		_ = processTreeKill(tree)
	})

	waiter := processDirectChildWait(tree)
	wait := func() {
		if waiter == nil {
			process.waitErr = ErrProcessContainmentIncomplete
		} else {
			<-waiter.done
			process.waitErr = settleDirectProcessExit(tree, waiter.err)
		}

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
		return processTreeTerminateAndWait(p.tree, defaultProcessTreeWait)
	}

	terminateErr := processTreeTerminate(p.tree)
	if terminateErr != nil {
		return fmt.Errorf("terminate pi process: %w", terminateErr)
	}

	if p.waitStep(ctx) {
		return processTreeTerminateAndWait(p.tree, defaultProcessTreeWait)
	}

	killErr := processTreeKill(p.tree)
	if killErr != nil {
		return fmt.Errorf("kill pi process: %w", killErr)
	}

	select {
	case <-p.exited:
		return processTreeTerminateAndWait(p.tree, defaultProcessTreeWait)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Kill forcefully terminates the child process group and returns only after
// the root is reaped and the selected containment boundary is complete.
func (p *Process) Kill() error {
	killErr := processTreeKill(p.tree)
	if killErr != nil {
		return fmt.Errorf("kill pi process: %w", killErr)
	}

	select {
	case <-p.exited:
	case <-time.After(processKillWait):
		return fmt.Errorf("%w: pi process was not reaped after kill", ErrProcessContainmentIncomplete)
	}

	return processTreeTerminateAndWait(p.tree, defaultProcessTreeWait)
}

// Close releases the parent-held pipe ends. Call after the reader is done.
func (p *Process) Close() error {
	_ = p.CloseStdin()

	containmentErr := processTreeTerminateAndWait(p.tree, defaultProcessTreeWait)
	if !p.waitStep(context.Background()) {
		containmentErr = errors.Join(
			containmentErr,
			fmt.Errorf("%w: pi direct child was not reaped", ErrProcessContainmentIncomplete),
		)
	}

	p.stdoutOnce.Do(func() {
		_ = p.stdout.Close()
	})

	return containmentErr
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
