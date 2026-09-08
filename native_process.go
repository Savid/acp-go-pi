package piacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

var authorityProcessKillTimeout = 30 * time.Second

type authorityPiProcess struct {
	agent   *Agent
	process NativeProcess
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	tail    *nativeStderrTail

	stdinOnce   sync.Once
	stdinErr    error
	waitMu      sync.Mutex
	waitFlight  *authorityWaitFlight
	terminal    bool
	exitOnce    sync.Once
	exited      chan struct{}
	stderrDone  chan struct{}
	waitErr     error
	result      NativeResult
	closeMu     sync.Mutex
	streamsOnce sync.Once
	streamsErr  error
}

type authorityWaitFlight struct {
	cancel   context.CancelFunc
	done     chan struct{}
	result   NativeResult
	waitErr  error
	detached bool
	terminal bool
}

func (*authorityPiProcess) managedByHostAuthority() {}

func (a *Agent) startAuthorityPiProcess(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
	if boundary := a.nativeContainmentError(); boundary != nil {
		return nil, nil, errors.Join(boundary, ErrContainmentIncomplete)
	}

	a.managedHandoff.freeze()

	request := NativeRequest{
		Executable: spec.ExecutablePath, Arguments: spec.Args(), Environment: spec.Environ(), WorkingDirectory: spec.Cwd,
	}

	var process NativeProcess

	var err error

	func() {
		defer func() {
			if recover() != nil {
				err = ErrHostAuthorityUnavailable
			}
		}()

		process, err = a.options.HostAuthority.StartNative(ctx, request)
	}()

	if err != nil {
		return nil, nil, err
	}

	if nativeProcessNil(process) {
		err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		a.recordNativeContainment(err)

		return nil, nil, err
	}

	wrapped := &authorityPiProcess{
		agent: a, process: process, exited: make(chan struct{}), stderrDone: make(chan struct{}),
		tail: &nativeStderrTail{limit: 8 << 10},
	}

	func() {
		defer func() {
			if recover() != nil {
				err = ErrHostAuthorityUnavailable
			}
		}()

		wrapped.stdin, wrapped.stdout, wrapped.stderr = process.Stdin(), process.Stdout(), process.Stderr()
	}()

	if err != nil {
		settleErr := settleStartedNativeProcess(process)
		err = errors.Join(err, settleErr)

		a.recordNativeContainment(err)

		return nil, nil, err
	}

	if wrapped.stdin == nil || wrapped.stdout == nil || wrapped.stderr == nil {
		err = errors.Join(ErrHostAuthorityUnavailable, settleStartedNativeProcess(process))

		a.recordNativeContainment(err)

		return nil, nil, err
	}

	go func() {
		defer close(wrapped.stderrDone)

		_, _ = io.Copy(wrapped.tail, wrapped.stderr)
	}()

	wrapped.startWait()

	return wrapped, pi.NewClient(wrapped.stdin, wrapped.stdout), nil
}

func (p *authorityPiProcess) startWait() *authorityWaitFlight {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()

	if p.terminal {
		return nil
	}

	if p.waitFlight != nil {
		return p.waitFlight
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	flight := &authorityWaitFlight{cancel: cancelWait, done: make(chan struct{})}
	p.waitFlight = flight

	go p.runWait(waitCtx, flight)

	return flight
}

func (p *authorityPiProcess) runWait(ctx context.Context, flight *authorityWaitFlight) {
	defer flight.cancel()

	flight.result, flight.waitErr = waitNativeProcess(ctx, p.process)
	flight.terminal = flight.waitErr == nil
	flight.detached = ctx.Err() != nil && detachedWaitError(flight.waitErr)

	if flight.terminal && flight.result.ExitCode != 0 && !flight.result.Revoked {
		flight.waitErr = fmt.Errorf("exit status %d", flight.result.ExitCode)
	} else if !flight.terminal && !flight.detached {
		flight.waitErr = errors.Join(flight.waitErr, ErrContainmentIncomplete)
	}

	var containmentErr error

	p.waitMu.Lock()
	if p.waitFlight == flight {
		switch {
		case flight.terminal:
			p.result = flight.result
			p.waitErr = flight.waitErr
			p.terminal = true
			p.exitOnce.Do(func() { close(p.exited) })
		case !flight.detached:
			p.waitErr = errors.Join(p.waitErr, flight.waitErr)
			containmentErr = flight.waitErr

			p.exitOnce.Do(func() { close(p.exited) })
		}

		if !flight.terminal {
			p.waitFlight = nil
		}
	}

	close(flight.done)
	p.waitMu.Unlock()

	if containmentErr != nil && p.agent != nil {
		p.agent.recordNativeContainment(containmentErr)
	}
}

func (p *authorityPiProcess) CloseStdin() error {
	p.stdinOnce.Do(func() { p.stdinErr = p.stdin.Close() })

	return p.stdinErr
}
func (p *authorityPiProcess) Exited() <-chan struct{} { return p.exited }
func (p *authorityPiProcess) WaitErr() error {
	select {
	case <-p.exited:
		p.waitMu.Lock()
		defer p.waitMu.Unlock()

		return p.waitErr
	default:
		return errors.New("pi process still running")
	}
}
func (p *authorityPiProcess) StderrTail() string { return p.tail.String() }
func (p *authorityPiProcess) Shutdown(ctx context.Context) error {
	_ = p.CloseStdin()
	flight := p.startWait()
	revokeErr := p.revoke(ctx)

	terminal, waitErr := p.awaitWait(ctx, flight)
	if terminal {
		return errors.Join(authorityTerminalRevokeError(revokeErr), waitErr)
	}

	return errors.Join(revokeErr, ctx.Err(), waitErr, ErrContainmentIncomplete)
}
func (p *authorityPiProcess) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), authorityProcessKillTimeout)
	defer cancel()

	flight := p.startWait()
	revokeErr := p.revoke(ctx)

	terminal, waitErr := p.awaitWait(ctx, flight)
	if terminal {
		return errors.Join(authorityTerminalRevokeError(revokeErr), waitErr)
	}

	return errors.Join(revokeErr, ctx.Err(), waitErr, ErrContainmentIncomplete)
}

func (p *authorityPiProcess) awaitWait(ctx context.Context, flight *authorityWaitFlight) (bool, error) {
	if flight == nil {
		p.waitMu.Lock()
		defer p.waitMu.Unlock()

		return p.terminal, p.waitErr
	}

	select {
	case <-flight.done:
	case <-ctx.Done():
		flight.cancel()
		<-flight.done
	}

	return flight.terminal, flight.waitErr
}
func (p *authorityPiProcess) revoke(ctx context.Context) (err error) {
	defer func() {
		if errors.Is(err, ErrHostAuthorityUnavailable) {
			p.agent.recordNativeContainment(err)
		}
	}()

	return revokeNativeProcess(ctx, p.process)
}
func (p *authorityPiProcess) Close() error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	_ = p.CloseStdin()

	p.waitMu.Lock()
	terminal := p.terminal
	p.waitMu.Unlock()

	var killErr error
	if !terminal {
		killErr = p.Kill()
	}

	p.streamsOnce.Do(func() {
		if p.stdout != nil {
			p.streamsErr = errors.Join(p.streamsErr, p.stdout.Close())
		}

		if p.stderr != nil {
			p.streamsErr = errors.Join(p.streamsErr, p.stderr.Close())
		}

		if p.stderrDone != nil {
			<-p.stderrDone
		}
	})

	p.waitMu.Lock()
	terminal = p.terminal
	waitErr := p.waitErr
	p.waitMu.Unlock()

	if !terminal {
		return errors.Join(killErr, waitErr, p.streamsErr, ErrContainmentIncomplete)
	}

	return errors.Join(killErr, waitErr, p.streamsErr)
}

type nativeStderrTail struct {
	mu    sync.Mutex
	limit int
	data  []byte
}

func (b *nativeStderrTail) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, data...)
	if len(b.data) > b.limit {
		b.data = b.data[len(b.data)-b.limit:]
	}

	return len(data), nil
}
func (b *nativeStderrTail) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return string(b.data)
}

func waitNativeProcess(ctx context.Context, process NativeProcess) (result NativeResult, err error) {
	defer func() {
		if recover() != nil {
			result = NativeResult{}
			err = ErrHostAuthorityUnavailable
		}
	}()

	return process.Wait(ctx)
}

func detachedWaitError(err error) bool {
	if err == nil || errors.Is(err, ErrHostAuthorityUnavailable) || errors.Is(err, ErrContainmentIncomplete) {
		return false
	}

	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		children := value.Unwrap()
		if len(children) == 0 {
			return false
		}

		for _, child := range children {
			if !detachedWaitError(child) {
				return false
			}
		}

		return true
	case interface{ Unwrap() error }:
		return detachedWaitError(value.Unwrap())
	default:
		return err == context.Canceled || err == context.DeadlineExceeded
	}
}

func revokeNativeProcess(ctx context.Context, process NativeProcess) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrHostAuthorityUnavailable
		}
	}()

	return process.Revoke(ctx)
}

func terminalNativeClose(prior error, closeErr error) error {
	if nativeContainmentComplete(closeErr) {
		return closeErr
	}

	return errors.Join(prior, closeErr)
}

func authorityTerminalRevokeError(err error) error {
	if errors.Is(err, ErrHostAuthorityUnavailable) {
		return err
	}

	return nil
}

func settleStartedNativeProcess(process NativeProcess) error {
	ctx, cancel := context.WithTimeout(context.Background(), sessionShutdownTimeout)
	defer cancel()

	revokeErr := revokeNativeProcess(ctx, process)

	_, waitErr := waitNativeProcess(ctx, process)
	if waitErr != nil {
		return errors.Join(revokeErr, waitErr, ErrContainmentIncomplete)
	}

	return authorityTerminalRevokeError(revokeErr)
}
