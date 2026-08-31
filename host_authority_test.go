package piacp

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type deterministicHostAuthority struct {
	mu                   sync.Mutex
	trace                []string
	hidden               map[string]string
	lastRoot             string
	preparedWasExclusive bool
	reclaimSawTree       bool
	startErr             error
}

func newDeterministicHostAuthority() *deterministicHostAuthority {
	return &deterministicHostAuthority{hidden: make(map[string]string)}
}

func (a *deterministicHostAuthority) NativeEnvironment() map[string]string {
	return map[string]string{"HOME": "/native/home", "PATH": "/native/bin"}
}

func (a *deterministicHostAuthority) PrepareNativeTree(_ context.Context, root string) error {
	if _, err := os.Stat(filepath.Join(root, "probe-agent")); err != nil {
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
	a.mu.Unlock()

	return nil
}

func (a *deterministicHostAuthority) ReclaimNativeTree(_ context.Context, root string) error {
	a.mu.Lock()
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

	return &deterministicNativeProcess{authority: a, root: root}, nil
}

func (a *deterministicHostAuthority) rootForRequest(request NativeRequest) string {
	for _, entry := range request.Environment {
		if strings.HasPrefix(entry, "PI_CODING_AGENT_DIR=") {
			return filepath.Dir(strings.TrimPrefix(entry, "PI_CODING_AGENT_DIR="))
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

func TestHostAuthorityNilFailsBeforeNativeMutation(t *testing.T) {
	scratch := t.TempDir()
	agent := NewAgent(WithHostAuthority(nil), WithScratchDir(scratch))
	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.True(t, errors.Is(err, ErrHostAuthorityUnavailable))
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}
