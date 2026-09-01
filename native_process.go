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
	waitOnce    sync.Once
	exited      chan struct{}
	stderrDone  chan struct{}
	waitErr     error
	result      NativeResult
	closeMu     sync.Mutex
	streamsOnce sync.Once
	streamsErr  error
}

func (*authorityPiProcess) managedByHostAuthority() {}

func (a *Agent) startAuthorityPiProcess(ctx context.Context, spec pi.LaunchSpec) (piProcess, piClient, error) {
	if boundary := a.nativeContainmentError(); boundary != nil {
		return nil, nil, errors.Join(boundary, ErrContainmentIncomplete)
	}

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

func (p *authorityPiProcess) startWait() {
	p.waitOnce.Do(func() {
		go func() {
			result, err := waitNativeProcess(context.Background(), p.process)
			p.result = result

			if err != nil {
				p.waitErr = errors.Join(err, ErrContainmentIncomplete)
				p.agent.recordNativeContainment(p.waitErr)
			} else if result.ExitCode != 0 && !result.Revoked {
				p.waitErr = fmt.Errorf("exit status %d", result.ExitCode)
			}

			close(p.exited)
		}()
	})
}

func (p *authorityPiProcess) CloseStdin() error {
	p.stdinOnce.Do(func() { p.stdinErr = p.stdin.Close() })

	return p.stdinErr
}
func (p *authorityPiProcess) Exited() <-chan struct{} { return p.exited }
func (p *authorityPiProcess) WaitErr() error {
	select {
	case <-p.exited:
		return p.waitErr
	default:
		return errors.New("pi process still running")
	}
}
func (p *authorityPiProcess) StderrTail() string { return p.tail.String() }
func (p *authorityPiProcess) Shutdown(ctx context.Context) error {
	_ = p.CloseStdin()
	revokeErr := p.revoke(ctx)

	select {
	case <-p.exited:
		return errors.Join(authorityTerminalRevokeError(revokeErr), p.waitErr)
	default:
	}

	select {
	case <-p.exited:
		return errors.Join(authorityTerminalRevokeError(revokeErr), p.waitErr)
	case <-ctx.Done():
		return errors.Join(revokeErr, ctx.Err(), ErrContainmentIncomplete)
	}
}
func (p *authorityPiProcess) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), authorityProcessKillTimeout)
	defer cancel()

	revokeErr := p.revoke(ctx)

	select {
	case <-p.exited:
		return errors.Join(authorityTerminalRevokeError(revokeErr), p.waitErr)
	default:
	}

	select {
	case <-p.exited:
		return errors.Join(authorityTerminalRevokeError(revokeErr), p.waitErr)
	case <-ctx.Done():
		return errors.Join(revokeErr, ctx.Err(), ErrContainmentIncomplete)
	}
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
	select {
	case <-p.exited:
	default:
		_ = p.Kill()
	}

	select {
	case <-p.exited:
	default:
		return ErrContainmentIncomplete
	}

	p.streamsOnce.Do(func() {
		p.streamsErr = errors.Join(p.stdout.Close(), p.stderr.Close())
		<-p.stderrDone
	})

	return errors.Join(p.waitErr, p.streamsErr)
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
