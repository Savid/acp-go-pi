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

type authorityPiProcess struct {
	agent   *Agent
	process NativeProcess
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	tail    *nativeStderrTail

	stdinOnce sync.Once
	stdinErr  error
	waitOnce  sync.Once
	exited    chan struct{}
	waitErr   error
	result    NativeResult
	closeOnce sync.Once
	closeErr  error
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
		a.recordNativeContainment(err)

		return nil, nil, err
	}

	if nativeProcessNil(process) {
		err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		a.recordNativeContainment(err)

		return nil, nil, err
	}

	wrapped := &authorityPiProcess{agent: a, process: process, exited: make(chan struct{}), tail: &nativeStderrTail{limit: 8 << 10}}

	func() {
		defer func() {
			if recover() != nil {
				err = ErrHostAuthorityUnavailable
			}
		}()

		wrapped.stdin, wrapped.stdout, wrapped.stderr = process.Stdin(), process.Stdout(), process.Stderr()
	}()

	if err != nil {
		err = errors.Join(err, ErrContainmentIncomplete)
		_ = revokeNativeProcess(ctx, process)
		_, _ = waitNativeProcess(context.Background(), process)

		a.recordNativeContainment(err)

		return nil, nil, err
	}

	if wrapped.stdin == nil || wrapped.stdout == nil || wrapped.stderr == nil {
		err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		_ = revokeNativeProcess(ctx, process)
		_, _ = waitNativeProcess(context.Background(), process)

		a.recordNativeContainment(err)

		return nil, nil, err
	}
	go func() { _, _ = io.Copy(wrapped.tail, wrapped.stderr) }()

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
			} else if result.ExitCode != 0 {
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
	if err := p.revoke(ctx); err != nil {
		return err
	}

	select {
	case <-p.exited:
		return p.waitErr
	case <-ctx.Done():
		return errors.Join(ctx.Err(), ErrContainmentIncomplete)
	}
}
func (p *authorityPiProcess) Kill() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := p.revoke(ctx); err != nil {
		return err
	}

	select {
	case <-p.exited:
		return p.waitErr
	case <-ctx.Done():
		return errors.Join(ctx.Err(), ErrContainmentIncomplete)
	}
}
func (p *authorityPiProcess) revoke(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrHostAuthorityUnavailable
		}

		if err != nil {
			err = errors.Join(err, ErrContainmentIncomplete)
			p.agent.recordNativeContainment(err)
		}
	}()

	return revokeNativeProcess(ctx, p.process)
}
func (p *authorityPiProcess) Close() error {
	p.closeOnce.Do(func() {
		_ = p.CloseStdin()
		select {
		case <-p.exited:
		default:
			p.closeErr = p.Kill()
		}

		p.closeErr = errors.Join(p.closeErr, p.stdout.Close(), p.stderr.Close())
	})

	return p.closeErr
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
