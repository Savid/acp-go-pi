package piacp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

type deterministicHostAuthority struct {
	mu                   sync.Mutex
	trace                []string
	hidden               map[string]string
	lastRoot             string
	preparedWasExclusive bool
	reclaimSawTree       bool
	busyReclaims         int
	prepareErr           error
	startErr             error
}

type authorityTracingStore struct {
	SessionStore
	authority *deterministicHostAuthority
}

func (s *authorityTracingStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	err := s.SessionStore.Append(ctx, key, entries)
	if err == nil && key.Subpath == SessionStoreMainSubpath && len(entries) > 0 {
		s.authority.mu.Lock()
		s.authority.trace = append(s.authority.trace, "mirror:"+key.SessionID)
		s.authority.mu.Unlock()
	}

	return err
}

func newDeterministicHostAuthority() *deterministicHostAuthority {
	return &deterministicHostAuthority{hidden: make(map[string]string)}
}

func (a *deterministicHostAuthority) NativeEnvironment() map[string]string {
	return map[string]string{"HOME": "/native/home", "PATH": "/native/bin"}
}

func (a *deterministicHostAuthority) PrepareNativeTree(_ context.Context, root string) error {
	if _, err := os.Stat(root); err != nil {
		return err
	}
	hidden := root + ".host-owned"
	if err := os.Rename(root, hidden); err != nil {
		return err
	}
	_, err := os.Stat(root)
	a.mu.Lock()
	a.trace = append(a.trace, "prepare:"+root)
	a.hidden[root] = hidden
	a.lastRoot = root
	a.preparedWasExclusive = errors.Is(err, fs.ErrNotExist)
	prepareErr := a.prepareErr
	a.mu.Unlock()

	return prepareErr
}

func (*deterministicHostAuthority) WriteNativeAppendLog(context.Context, string, [][]byte) error {
	return ErrHostAuthorityUnavailable
}

func (*deterministicHostAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (a *deterministicHostAuthority) ReclaimNativeTree(_ context.Context, root string) error {
	a.mu.Lock()
	if a.busyReclaims > 0 {
		a.busyReclaims--
		a.trace = append(a.trace, "busy:"+root)
		a.mu.Unlock()

		return ErrNativeTreeBusy
	}
	hidden := a.hidden[root]
	a.mu.Unlock()
	if hidden == "" {
		return ErrHostAuthorityUnavailable
	}
	if err := os.Rename(hidden, root); err != nil {
		return err
	}
	_, err := os.Stat(root)
	a.mu.Lock()
	a.trace = append(a.trace, "reclaim:"+root)
	a.reclaimSawTree = err == nil
	delete(a.hidden, root)
	a.mu.Unlock()

	return nil
}

func (a *deterministicHostAuthority) StartNative(_ context.Context, request NativeRequest) (NativeProcess, error) {
	root := a.rootForRequest(request)
	a.mu.Lock()
	a.trace = append(a.trace, "start:"+root)
	startErr := a.startErr
	a.mu.Unlock()
	if startErr != nil {
		return nil, startErr
	}
	if slicesContains(request.Arguments, "--mode") {
		a.mu.Lock()
		hidden := a.hidden[root]
		a.mu.Unlock()

		return newManagedRPCNativeProcess(a, root, hidden, request), nil
	}

	return &deterministicNativeProcess{authority: a, root: root}, nil
}

func slicesContains(values []string, value string) bool {
	return slices.Contains(values, value)
}

func (a *deterministicHostAuthority) rootForRequest(request NativeRequest) string {
	for _, entry := range request.Environment {
		if after, ok := strings.CutPrefix(entry, "PI_CODING_AGENT_DIR="); ok {
			return filepath.Dir(after)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lastRoot
}

func (a *deterministicHostAuthority) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]string(nil), a.trace...)
}

func (a *deterministicHostAuthority) cleanup() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, hidden := range a.hidden {
		_ = os.RemoveAll(hidden)
	}
}

type deterministicNativeProcess struct {
	authority *deterministicHostAuthority
	root      string
}

type cancellationNativeProcess struct {
	mu            sync.Mutex
	done          chan struct{}
	release       chan struct{}
	waitCalls     int
	revokeCalls   int
	waitDetached  bool
	revoked       bool
	releaseOnce   sync.Once
	doneOnce      sync.Once
	nativeResult  NativeResult
	nativeWaitErr error
}

func newCancellationNativeProcess(result NativeResult) *cancellationNativeProcess {
	return &cancellationNativeProcess{
		done: make(chan struct{}), release: make(chan struct{}), nativeResult: result,
	}
}

func (*cancellationNativeProcess) Stdin() io.WriteCloser {
	return discardWriteCloser{Writer: io.Discard}
}

func (*cancellationNativeProcess) Stdout() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }
func (*cancellationNativeProcess) Stderr() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }

func (p *cancellationNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.mu.Lock()
	p.waitCalls++
	p.waitDetached = ctx.Done() == nil
	p.mu.Unlock()

	select {
	case <-p.done:
		p.mu.Lock()
		result := p.nativeResult
		result.Revoked = p.revoked || result.Revoked
		err := p.nativeWaitErr
		p.mu.Unlock()

		return result, err
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	}
}

func (p *cancellationNativeProcess) Revoke(ctx context.Context) error {
	p.mu.Lock()
	p.revokeCalls++
	p.revoked = true
	p.mu.Unlock()
	go func() {
		<-p.release
		p.doneOnce.Do(func() { close(p.done) })
	}()

	return ctx.Err()
}

func (p *cancellationNativeProcess) finish() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *deterministicNativeProcess) Stdin() io.WriteCloser {
	return discardWriteCloser{Writer: io.Discard}
}

func (p *deterministicNativeProcess) Stdout() io.ReadCloser {
	return io.NopCloser(strings.NewReader("0.80.6\n"))
}

func (p *deterministicNativeProcess) Stderr() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}

func (p *deterministicNativeProcess) Wait(context.Context) (NativeResult, error) {
	p.authority.mu.Lock()
	p.authority.trace = append(p.authority.trace, "wait:"+p.root)
	p.authority.mu.Unlock()

	return NativeResult{}, nil
}

func (p *deterministicNativeProcess) Revoke(context.Context) error {
	p.authority.mu.Lock()
	p.authority.trace = append(p.authority.trace, "revoke:"+p.root)
	p.authority.mu.Unlock()

	return nil
}

type discardWriteCloser struct{ io.Writer }

func (discardWriteCloser) Close() error { return nil }

type managedRPCNativeProcess struct {
	authority   *deterministicHostAuthority
	root        string
	hidden      string
	request     NativeRequest
	stdin       *io.PipeWriter
	stdout      *io.PipeReader
	stderr      *io.PipeReader
	input       *io.PipeReader
	output      *io.PipeWriter
	diagnostic  *io.PipeWriter
	done        chan struct{}
	once        sync.Once
	waitTrace   sync.Once
	mu          sync.Mutex
	revoked     bool
	sessionID   string
	sessionFile string
}

func newManagedRPCNativeProcess(
	authority *deterministicHostAuthority,
	root string,
	hidden string,
	request NativeRequest,
) *managedRPCNativeProcess {
	input, stdin := io.Pipe()
	stdout, output := io.Pipe()
	stderr, diagnostic := io.Pipe()
	process := &managedRPCNativeProcess{
		authority:  authority,
		root:       root,
		hidden:     hidden,
		request:    request,
		stdin:      stdin,
		stdout:     stdout,
		stderr:     stderr,
		input:      input,
		output:     output,
		diagnostic: diagnostic,
		done:       make(chan struct{}),
		sessionID:  "managed-parent",
	}
	process.sessionFile = process.argumentAfter("--session")
	if process.sessionFile == "" {
		process.sessionFile = filepath.Join(process.argumentAfter("--session-dir"), "managed-parent.jsonl")
	}
	go process.serve()

	return process
}

func (p *managedRPCNativeProcess) argumentAfter(name string) string {
	for index := 0; index+1 < len(p.request.Arguments); index++ {
		if p.request.Arguments[index] == name {
			return p.request.Arguments[index+1]
		}
	}

	return ""
}

func (p *managedRPCNativeProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *managedRPCNativeProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *managedRPCNativeProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *managedRPCNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	select {
	case <-p.done:
		p.waitTrace.Do(func() {
			p.authority.mu.Lock()
			p.authority.trace = append(p.authority.trace, "wait:"+p.root)
			p.authority.mu.Unlock()
		})
		p.mu.Lock()
		result := NativeResult{Revoked: p.revoked}
		p.mu.Unlock()

		return result, nil
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	}
}

func (p *managedRPCNativeProcess) Revoke(context.Context) error {
	p.authority.mu.Lock()
	p.authority.trace = append(p.authority.trace, "revoke:"+p.root)
	p.authority.mu.Unlock()
	p.mu.Lock()
	p.revoked = true
	p.mu.Unlock()
	p.finish()

	return nil
}

func (p *managedRPCNativeProcess) finish() {
	p.once.Do(func() {
		_ = p.output.Close()
		_ = p.diagnostic.Close()
		_ = p.input.Close()
		close(p.done)
	})
}

func (p *managedRPCNativeProcess) serve() {
	scanner := bufio.NewScanner(p.input)
	for scanner.Scan() {
		var command map[string]any
		if json.Unmarshal(scanner.Bytes(), &command) != nil {
			continue
		}
		kind, _ := command["type"].(string)
		id := command["id"]
		data := any(nil)
		switch kind {
		case "get_state":
			data = map[string]any{
				"sessionId":     p.sessionID,
				"sessionFile":   p.sessionFile,
				"thinkingLevel": "off",
			}
		case "get_available_models":
			data = map[string]any{"models": []any{}}
		case "get_commands":
			data = map[string]any{"commands": []any{}}
		case "get_session_stats":
			data = map[string]any{"sessionId": p.sessionID}
		case "clone":
			p.sessionID = "managed-child"
			data = map[string]any{"cancelled": false}
		case "prompt":
			p.appendSessionRow()
		}

		response := map[string]any{
			"id":      id,
			"type":    "response",
			"command": kind,
			"success": true,
		}
		if data != nil {
			response["data"] = data
		}
		if !p.writeJSON(response) {
			return
		}
		if kind == "prompt" {
			if !p.writeLine(`{"type":"agent_start"}`) ||
				!p.writeLine(`{"type":"agent_end","messages":[]}`) ||
				!p.writeLine(`{"type":"agent_settled"}`) {
				return
			}
		}
	}
	<-p.done
}

func (p *managedRPCNativeProcess) appendSessionRow() {
	path := filepath.Join(p.hidden, strings.TrimPrefix(filepath.Clean(p.sessionFile), filepath.Clean(p.root)))
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintln(file, `{"type":"message","role":"user"}`)
	_ = file.Close()
}

func (p *managedRPCNativeProcess) writeJSON(value any) bool {
	data, err := json.Marshal(value)
	if err != nil {
		return false
	}

	return p.writeLine(string(data))
}

func (p *managedRPCNativeProcess) writeLine(line string) bool {
	_, err := io.WriteString(p.output, line+"\n")

	return err == nil
}

func TestHostAuthorityManagedLaunchTrace(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
	)
	require.NoError(t, agent.ensureVersion(t.Context()))
	root := authority.lastRoot
	require.Equal(t, []string{
		"prepare:" + root,
		"start:" + root,
		"wait:" + root,
		"reclaim:" + root,
	}, authority.snapshot())
}

func TestHostAuthorityPreparedTreeExclusivity(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
	)
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.True(t, authority.preparedWasExclusive)
	_, err := os.Stat(authority.lastRoot)
	require.Error(t, err)
	require.True(t, errors.Is(err, fs.ErrNotExist))
}

func TestHostAuthorityReclaimPrecedesRemoval(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
	)
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.True(t, authority.reclaimSawTree)
	trace := authority.snapshot()
	require.Less(t, indexTracePrefix(trace, "wait:"), indexTracePrefix(trace, "reclaim:"))
	_, err := os.Stat(authority.lastRoot)
	require.True(t, errors.Is(err, fs.ErrNotExist))
}

func TestHostAuthorityNoOrdinaryFallback(t *testing.T) {
	wantErr := errors.New("managed start refused")
	authority := newDeterministicHostAuthority()
	authority.startErr = wantErr
	t.Cleanup(authority.cleanup)
	marker := filepath.Join(t.TempDir(), "ordinary-launched")
	executable := filepath.Join(t.TempDir(), "pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\ntouch "+marker+"\necho 0.80.6\n"), 0o700))
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath(executable),
		WithScratchDir(t.TempDir()),
	)
	err := agent.ensureVersion(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	_, err = os.Stat(marker)
	require.True(t, errors.Is(err, fs.ErrNotExist))
	require.Equal(t, -1, indexTracePrefix(authority.snapshot(), "wait:"))
}

func indexTracePrefix(trace []string, prefix string) int {
	for index, entry := range trace {
		if strings.HasPrefix(entry, prefix) {
			return index
		}
	}

	return -1
}

func lastTracePrefix(trace []string, prefix string) int {
	for index := len(trace) - 1; index >= 0; index-- {
		if strings.HasPrefix(trace[index], prefix) {
			return index
		}
	}

	return -1
}

func TestHostAuthorityNilFailsBeforeNativeMutation(t *testing.T) {
	scratch := t.TempDir()
	agent := NewAgent(WithHostAuthority(nil), WithScratchDir(scratch))
	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.True(t, errors.Is(err, ErrHostAuthorityUnavailable))
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestHostAuthorityWithHomeFailsBeforeMaterialization(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	scratch := t.TempDir()
	home := filepath.Join(t.TempDir(), "managed-home")
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(scratch),
		WithHome(home),
	)

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	requireRefusedField(t, optionFieldHome, err)
	_, err = os.Stat(home)
	require.ErrorIs(t, err, fs.ErrNotExist)
	entries, err := os.ReadDir(scratch)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Empty(t, authority.snapshot())
}

func TestHostAuthorityPrepareFailureRemainsOpaque(t *testing.T) {
	wantErr := errors.New("prepare ownership uncertain")
	authority := newDeterministicHostAuthority()
	authority.prepareErr = wantErr
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
	)

	err := agent.ensureVersion(t.Context())
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	root := authority.lastRoot
	require.NotEmpty(t, root)
	_, err = os.Stat(root)
	require.ErrorIs(t, err, fs.ErrNotExist)
	trace := authority.snapshot()
	require.NotEqual(t, -1, indexTracePrefix(trace, "prepare:"))
	require.Equal(t, -1, indexTracePrefix(trace, "start:"))
	require.Equal(t, -1, indexTracePrefix(trace, "reclaim:"))
}

func TestHostAuthorityManagedGenerationSettlement(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	store := &authorityTracingStore{SessionStore: NewInMemorySessionStore(), authority: authority}
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	response, err := agent.Prompt(t.Context(), TextPromptRequest(opened.SessionId, "managed-turn", "hello"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	response, err = agent.Prompt(t.Context(), TextPromptRequest(opened.SessionId, "managed-turn-2", "again"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

	trace := authority.snapshot()
	joinedTrace := strings.Join(trace, "\n")
	require.GreaterOrEqual(t, strings.Count(joinedTrace, "revoke:"), 1)
	require.Equal(t, strings.Count(joinedTrace, "start:"), strings.Count(joinedTrace, "wait:"))
	require.Less(t, lastTracePrefix(trace, "wait:"), lastTracePrefix(trace, "reclaim:"))
	require.Less(t, lastTracePrefix(trace, "reclaim:"), lastTracePrefix(trace, "mirror:"))
	entries, err := agent.sessionStore().Load(t.Context(), SessionKey{SessionID: string(opened.SessionId)})
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestHostAuthorityForkMirrorFollowsWaitAndReclaim(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	baseStore := NewInMemorySessionStore()
	require.NoError(t, baseStore.Append(t.Context(), SessionKey{SessionID: string(forkParentID)}, []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"` + string(forkParentID) + `","cwd":` + testCwdJSON + `}`),
		json.RawMessage(`{"type":"message","role":"user"}`),
	}))
	appendLifecycleBoundaryForRows(t, baseStore, string(forkParentID), 2)
	store := &authorityTracingStore{SessionStore: baseStore, authority: authority}
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())
	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)

	_, err = agent.handleForkSession(t.Context(), forkRaw(t, forkParams(t)))
	require.NoError(t, err)
	trace := authority.snapshot()
	joinedTrace := strings.Join(trace, "\n")
	require.Equal(t, strings.Count(joinedTrace, "start:"), strings.Count(joinedTrace, "wait:"))
	require.Less(t, lastTracePrefix(trace, "wait:"), lastTracePrefix(trace, "reclaim:"))
	require.Less(t, lastTracePrefix(trace, "reclaim:"), lastTracePrefix(trace, "mirror:"))
}

func TestHostAuthorityBusyGenerationRetriesBeforeReplacement(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	authority.mu.Lock()
	authority.busyReclaims = 1
	authority.mu.Unlock()

	_, err = agent.Prompt(t.Context(), TextPromptRequest(opened.SessionId, "busy-turn", "first"))
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	startsBeforeAdmission := strings.Count(strings.Join(authority.snapshot(), "\n"), "start:")
	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.Equal(t, startsBeforeAdmission, strings.Count(strings.Join(authority.snapshot(), "\n"), "start:"))
	response, err := agent.Prompt(t.Context(), TextPromptRequest(opened.SessionId, "retry-turn", "second"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

	entries, err := agent.sessionStore().Load(t.Context(), SessionKey{SessionID: string(opened.SessionId)})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	trace := authority.snapshot()
	require.NotEqual(t, -1, indexTracePrefix(trace, "busy:"))
	require.Less(t, indexTracePrefix(trace, "busy:"), lastTracePrefix(trace, "start:"))
}

func TestHostAuthorityAgentCloseRetriesBusyGeneration(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent.setConnection(newDirectAgentClient())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	authority.mu.Lock()
	authority.busyReclaims = 1
	authority.mu.Unlock()

	require.NoError(t, agent.Close())
	trace := authority.snapshot()
	require.NotEqual(t, -1, indexTracePrefix(trace, "busy:"))
	require.Less(t, indexTracePrefix(trace, "busy:"), lastTracePrefix(trace, "reclaim:"))
}

func TestHostAuthorityRepeatedAgentCloseRetriesBusyGeneration(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	agent.setConnection(newDirectAgentClient())

	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	authority.mu.Lock()
	authority.busyReclaims = 3
	authority.mu.Unlock()

	require.ErrorIs(t, agent.Close(), ErrNativeTreeBusy)
	require.NoError(t, agent.Close())
	trace := authority.snapshot()
	require.Equal(t, 3, strings.Count(strings.Join(trace, "\n"), "busy:"))
	require.Less(t, lastTracePrefix(trace, "busy:"), lastTracePrefix(trace, "reclaim:"))
}

func TestHostAuthorityAdmissionRetriesBusyVersionProbe(t *testing.T) {
	authority := newDeterministicHostAuthority()
	authority.busyReclaims = 1
	t.Cleanup(authority.cleanup)
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath("pi"),
		WithScratchDir(t.TempDir()),
		WithLogger(slog.New(slog.DiscardHandler)),
	)
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())

	err := agent.ensureVersion(t.Context())
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.Equal(t, 1, strings.Count(strings.Join(authority.snapshot(), "\n"), "start:"))

	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	trace := authority.snapshot()
	require.Equal(t, 2, strings.Count(strings.Join(trace, "\n"), "start:"))
	require.Less(t, indexTracePrefix(trace, "busy:"), lastTracePrefix(trace, "reclaim:"))
	require.Less(t, lastTracePrefix(trace, "reclaim:"), lastTracePrefix(trace, "start:"))
}

func TestHostAuthorityCanceledControlRejoinsCachedTerminalWait(t *testing.T) {
	native := newCancellationNativeProcess(NativeResult{ExitCode: -1, Signal: 9, Revoked: true})
	wrapped := &authorityPiProcess{
		agent: NewAgent(), process: native,
		stdin: discardWriteCloser{Writer: io.Discard}, stdout: io.NopCloser(strings.NewReader("")),
		stderr: io.NopCloser(strings.NewReader("")), tail: &nativeStderrTail{limit: 8 << 10},
		exited: make(chan struct{}), stderrDone: make(chan struct{}),
	}
	close(wrapped.stderrDone)
	wrapped.startWait()

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	err := wrapped.Shutdown(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	native.finish()
	require.NoError(t, wrapped.Close())
	require.NoError(t, wrapped.WaitErr())
	require.Equal(t, NativeResult{ExitCode: -1, Signal: 9, Revoked: true}, wrapped.result)

	native.mu.Lock()
	require.Equal(t, 2, native.waitCalls)
	require.Equal(t, 2, native.revokeCalls)
	require.False(t, native.waitDetached)
	native.mu.Unlock()
}

func TestHostAuthorityLossFencesEveryLiveSession(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	firstProcess := newStubProcess(false)
	secondProcess := newStubProcess(false)
	first := &agentSession{agent: agent, id: "first", proc: firstProcess, nativeBoundary: newNativeBoundaryTracker()}
	second := &agentSession{agent: agent, id: "second", proc: secondProcess, nativeBoundary: newNativeBoundaryTracker()}
	agent.sessions[first.id] = first
	agent.sessions[second.id] = second

	agent.recordNativeContainment(ErrHostAuthorityUnavailable)
	require.Eventually(t, func() bool {
		return errors.Is(first.nativeContainmentError(), ErrHostAuthorityUnavailable) &&
			errors.Is(second.nativeContainmentError(), ErrHostAuthorityUnavailable)
	}, 2*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, first.Close(t.Context()), ErrHostAuthorityUnavailable)
	require.ErrorIs(t, second.Close(t.Context()), ErrHostAuthorityUnavailable)
	require.Equal(t, 1, firstProcess.shutdownCalls)
	require.Equal(t, 1, secondProcess.shutdownCalls)
	require.ErrorIs(t, agent.sessionStartConfigurationError(), ErrHostAuthorityUnavailable)
}
