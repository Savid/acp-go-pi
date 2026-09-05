package piacp

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type edgeHostAuthority struct {
	environment func() map[string]string
	prepare     func(context.Context, string) error
	reclaim     func(context.Context, string) error
	start       func(context.Context, NativeRequest) (NativeProcess, error)
}

func (a *edgeHostAuthority) NativeEnvironment() map[string]string {
	if a.environment != nil {
		return a.environment()
	}

	return map[string]string{}
}

func (a *edgeHostAuthority) PrepareNativeTree(ctx context.Context, root string) error {
	if a.prepare != nil {
		return a.prepare(ctx, root)
	}

	return nil
}

func (*edgeHostAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (*edgeHostAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (a *edgeHostAuthority) ReclaimNativeTree(ctx context.Context, root string) error {
	if a.reclaim != nil {
		return a.reclaim(ctx, root)
	}

	return nil
}

func (a *edgeHostAuthority) StartNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if a.start != nil {
		return a.start(ctx, request)
	}

	return &edgeNativeProcess{}, nil
}

type edgeNativeProcess struct {
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	wait         func(context.Context) (NativeResult, error)
	revoke       func(context.Context) error
	panicStreams bool
	nilStreams   bool
}

type valueAuthority struct{}

func (valueAuthority) NativeEnvironment() map[string]string { return map[string]string{} }

func (valueAuthority) PrepareNativeTree(context.Context, string) error { return nil }

func (valueAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (valueAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (valueAuthority) ReclaimNativeTree(context.Context, string) error { return nil }

func (valueAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return valueNativeProcess{}, nil
}

type valueNativeProcess struct{}

func (valueNativeProcess) Stdin() io.WriteCloser { return discardWriteCloser{Writer: io.Discard} }

func (valueNativeProcess) Stdout() io.ReadCloser { return io.NopCloser(&emptyReader{}) }

func (valueNativeProcess) Stderr() io.ReadCloser { return io.NopCloser(&emptyReader{}) }

func (valueNativeProcess) Wait(context.Context) (NativeResult, error) { return NativeResult{}, nil }

func (valueNativeProcess) Revoke(context.Context) error { return nil }

func (p *edgeNativeProcess) Stdin() io.WriteCloser {
	if p.panicStreams {
		panic("stdin")
	}
	if p.nilStreams {
		return nil
	}
	if p.stdin != nil {
		return p.stdin
	}

	return discardWriteCloser{Writer: io.Discard}
}

func (p *edgeNativeProcess) Stdout() io.ReadCloser {
	if p.panicStreams {
		panic("stdout")
	}
	if p.nilStreams {
		return nil
	}
	if p.stdout != nil {
		return p.stdout
	}

	return io.NopCloser(&emptyReader{})
}

func (p *edgeNativeProcess) Stderr() io.ReadCloser {
	if p.panicStreams {
		panic("stderr")
	}
	if p.nilStreams {
		return nil
	}
	if p.stderr != nil {
		return p.stderr
	}

	return io.NopCloser(&emptyReader{})
}

func (p *edgeNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	if p.wait != nil {
		return p.wait(ctx)
	}

	return NativeResult{}, nil
}

func (p *edgeNativeProcess) Revoke(ctx context.Context) error {
	if p.revoke != nil {
		return p.revoke(ctx)
	}

	return nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

type errorWriteCloser struct{ err error }

func (c *errorWriteCloser) Write([]byte) (int, error) { return 0, c.err }

func (c *errorWriteCloser) Close() error { return c.err }

type errorReadCloser struct{ err error }

func (c *errorReadCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (c *errorReadCloser) Close() error { return c.err }

type blockingEdgeReadCloser struct {
	startedOnce sync.Once
	closeOnce   sync.Once
	started     chan struct{}
	closed      chan struct{}
	exited      chan struct{}
}

func newBlockingEdgeReadCloser() *blockingEdgeReadCloser {
	return &blockingEdgeReadCloser{
		started: make(chan struct{}), closed: make(chan struct{}), exited: make(chan struct{}),
	}
}

func (r *blockingEdgeReadCloser) Read([]byte) (int, error) {
	r.startedOnce.Do(func() { close(r.started) })
	<-r.closed
	close(r.exited)

	return 0, io.EOF
}

func (r *blockingEdgeReadCloser) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })

	return nil
}

type signalingWriteCloser struct {
	sync.Once
	closed chan struct{}
}

type emptyWaitMultiError struct{}

func (emptyWaitMultiError) Error() string { return "empty wait error" }

func (emptyWaitMultiError) Unwrap() []error { return nil }

func (w *signalingWriteCloser) Write(data []byte) (int, error) { return len(data), nil }

func (w *signalingWriteCloser) Close() error {
	w.Do(func() { close(w.closed) })

	return nil
}

func TestHostAuthorityRuntimeEdges(t *testing.T) {
	var nilAuthority *edgeHostAuthority
	require.True(t, hostAuthorityNil(nil))
	require.True(t, hostAuthorityNil(nilAuthority))
	require.False(t, hostAuthorityNil(&edgeHostAuthority{}))
	require.False(t, hostAuthorityNil(valueAuthority{}))

	_, err := readHostEnvironment(nil)
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string { panic("environment") }})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string { return nil }})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	environment, err := readHostEnvironment(&edgeHostAuthority{environment: func() map[string]string {
		return map[string]string{"PATH": "/native"}
	}})
	require.NoError(t, err)
	require.Equal(t, "/native", environment["PATH"])

	ordinary := NewAgent()
	require.NoError(t, ordinary.prepareNativeTree(t.Context(), "/unused"))
	require.NoError(t, ordinary.reclaimNativeTree(t.Context(), "/unused"))
	require.NoError(t, ordinary.disposeNativeTree(t.Context(), ""))
	require.NoError(t, ordinary.removeNativeTree(""))

	originalRemoveAll := materializeRemoveAll
	t.Cleanup(func() { materializeRemoveAll = originalRemoveAll })
	removed := ""
	materializeRemoveAll = func(root string) error {
		removed = root

		return nil
	}
	require.NoError(t, ordinary.disposeNativeTree(t.Context(), "/ordinary"))
	require.Equal(t, "/ordinary", removed)
	require.NoError(t, ordinary.removeNativeTree("/removed"))
	require.Equal(t, "/removed", removed)

	wantErr := errors.New("authority fault")
	managed := NewAgent()
	managed.options.hostAuthoritySupplied = true
	managed.options.HostAuthority = &edgeHostAuthority{
		prepare: func(context.Context, string) error { return wantErr },
		reclaim: func(context.Context, string) error { return wantErr },
	}
	require.ErrorIs(t, managed.prepareNativeTree(t.Context(), "/tree"), wantErr)
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/tree"), wantErr)
	require.ErrorIs(t, managed.disposeNativeTree(t.Context(), "/tree"), wantErr)

	managed.options.HostAuthority = &edgeHostAuthority{
		prepare: func(context.Context, string) error { panic("prepare") },
		reclaim: func(context.Context, string) error { panic("reclaim") },
	}
	require.ErrorIs(t, managed.prepareNativeTree(t.Context(), "/panic"), ErrHostAuthorityUnavailable)
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/panic"), ErrHostAuthorityUnavailable)

	managed.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	require.ErrorIs(t, managed.reclaimNativeTree(t.Context(), "/busy"), ErrNativeTreeBusy)
	managed.options.HostAuthority = &edgeHostAuthority{}
	require.NoError(t, managed.reclaimNativeTree(t.Context(), "/busy"))

	var nilProcess *edgeNativeProcess
	require.True(t, nativeProcessNil(nil))
	require.True(t, nativeProcessNil(nilProcess))
	require.False(t, nativeProcessNil(&edgeNativeProcess{}))
	require.False(t, nativeProcessNil(valueNativeProcess{}))
	require.True(t, nativeContainmentComplete(nil))
	require.False(t, nativeContainmentComplete(ErrContainmentIncomplete))
}

func TestRuntimeGenerationFailureEdges(t *testing.T) {
	originalParent := runtimeGenerationEnsureScratchParent
	originalAbs := runtimeGenerationAbs
	originalMkdirTemp := runtimeGenerationMkdirTemp
	originalChmod := runtimeGenerationChmod
	originalRemoveAll := runtimeGenerationRemoveAll
	originalWrite := runtimeGenerationWriteProbeAgentDir
	t.Cleanup(func() {
		runtimeGenerationEnsureScratchParent = originalParent
		runtimeGenerationAbs = originalAbs
		runtimeGenerationMkdirTemp = originalMkdirTemp
		runtimeGenerationChmod = originalChmod
		runtimeGenerationRemoveAll = originalRemoveAll
		runtimeGenerationWriteProbeAgentDir = originalWrite
	})

	_, err := (*runtimeGeneration)(nil).prepareVersionProbeAgentDir(t.Context())
	require.Error(t, err)
	_, err = (&runtimeGeneration{root: "relative"}).prepareVersionProbeAgentDir(t.Context())
	require.Error(t, err)

	wantErr := errors.New("generation fault")
	root := filepath.Join(t.TempDir(), "generation")
	generation := &runtimeGeneration{agent: NewAgent(), root: root}
	runtimeGenerationWriteProbeAgentDir = func(string) error { return wantErr }
	_, err = generation.prepareVersionProbeAgentDir(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationWriteProbeAgentDir = originalWrite
	require.NoError(t, os.MkdirAll(root, 0o700))
	agentDir, err := generation.prepareVersionProbeAgentDir(t.Context())
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "probe-agent"), agentDir)
	prepareAgent := NewAgent()
	prepareAgent.options.hostAuthoritySupplied = true
	prepareAgent.options.HostAuthority = &edgeHostAuthority{prepare: func(context.Context, string) error {
		return wantErr
	}}
	prepareRoot := filepath.Join(t.TempDir(), "prepared")
	require.NoError(t, os.MkdirAll(prepareRoot, 0o700))
	_, err = (&runtimeGeneration{agent: prepareAgent, root: prepareRoot}).prepareVersionProbeAgentDir(t.Context())
	require.ErrorIs(t, err, wantErr)

	closed := NewAgent()
	closed.closed = true
	_, err = closed.createRuntimeGeneration(t.Context())
	require.Error(t, err)

	agent := NewAgent()
	runtimeGenerationEnsureScratchParent = func(string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationEnsureScratchParent = func(string) (string, error) { return "/parent", nil }
	runtimeGenerationAbs = func(string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	runtimeGenerationAbs = func(path string) (string, error) { return path, nil }
	runtimeGenerationMkdirTemp = func(string, string) (string, error) { return "", wantErr }
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)

	removed := ""
	runtimeGenerationMkdirTemp = func(string, string) (string, error) { return "/root", nil }
	runtimeGenerationChmod = func(string, os.FileMode) error { return wantErr }
	runtimeGenerationRemoveAll = func(path string) error {
		removed = path

		return nil
	}
	_, err = agent.createRuntimeGeneration(t.Context())
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, "/root", removed)

	require.ErrorIs(t, (*runtimeGeneration)(nil).finalize(t.Context(), wantErr), wantErr)
	incomplete := &runtimeGeneration{agent: agent, root: "/incomplete"}
	require.ErrorIs(t, incomplete.finalize(t.Context(), ErrContainmentIncomplete), ErrContainmentIncomplete)

	removeErr := errors.New("remove fault")
	runtimeGenerationRemoveAll = func(string) error { return removeErr }
	complete := &runtimeGeneration{agent: agent, root: "/complete"}
	require.ErrorIs(t, complete.finalize(t.Context(), nil), removeErr)
	require.ErrorIs(t, complete.finalize(t.Context(), nil), removeErr)

	busyAgent := NewAgent()
	busyAgent.options.hostAuthoritySupplied = true
	busyAgent.options.HostAuthority = &edgeHostAuthority{reclaim: func(context.Context, string) error {
		return ErrNativeTreeBusy
	}}
	busy := &runtimeGeneration{agent: busyAgent, root: "/busy", prepared: true}
	require.ErrorIs(t, busy.finalize(t.Context(), nil), ErrNativeTreeBusy)

	successAgent := NewAgent()
	successAgent.options.hostAuthoritySupplied = true
	successAgent.options.HostAuthority = &edgeHostAuthority{}
	runtimeGenerationRemoveAll = func(string) error { return nil }
	success := &runtimeGeneration{agent: successAgent, root: "/success", prepared: true}
	require.NoError(t, success.finalize(t.Context(), nil))
	require.False(t, success.prepared)

	runtimeGenerationEnsureScratchParent = originalParent
	runtimeGenerationAbs = originalAbs
	runtimeGenerationMkdirTemp = originalMkdirTemp
	runtimeGenerationChmod = originalChmod
	runtimeGenerationRemoveAll = originalRemoveAll
	created, err := (&Agent{scratchParent: t.TempDir()}).createRuntimeGeneration(t.Context())
	require.NoError(t, err)
	require.NoError(t, created.finalize(t.Context(), nil))
}

func edgeManagedAgent(authority HostAuthority) *Agent {
	agent := NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = authority
	agent.nativeEnvironment = map[string]string{"PATH": "/native"}

	return agent
}

type stagedErrorContext struct{ calls atomic.Int32 }

func (*stagedErrorContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (*stagedErrorContext) Done() <-chan struct{} { return nil }

func (c *stagedErrorContext) Err() error {
	if c.calls.Add(1) > 1 {
		return context.Canceled
	}

	return nil
}

func (*stagedErrorContext) Value(any) any { return nil }

type closeAgentOnErrContext struct {
	context.Context //nolint:containedctx // Test hook closes the agent at the explicit final context gate.
	agent           *Agent
	once            sync.Once
}

type closeAgentOnSpanStart struct {
	agent *Agent
	once  sync.Once
}

func (p *closeAgentOnSpanStart) OnStart(context.Context, sdktrace.ReadWriteSpan) {
	p.once.Do(func() {
		p.agent.mu.Lock()
		p.agent.closed = true
		p.agent.mu.Unlock()
	})
}

func (*closeAgentOnSpanStart) OnEnd(sdktrace.ReadOnlySpan) {}

func (*closeAgentOnSpanStart) Shutdown(context.Context) error { return nil }

func (*closeAgentOnSpanStart) ForceFlush(context.Context) error { return nil }

type contextIgnoringStore struct{ SessionStore }

func (s *contextIgnoringStore) Load(_ context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	return s.SessionStore.Load(context.Background(), key)
}

func (c *closeAgentOnErrContext) Err() error {
	c.once.Do(func() { _, _ = c.agent.beginClose() })

	return nil
}
