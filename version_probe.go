package piacp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/savid/acp-go-pi/internal/pi"
)

func (a *Agent) probeNativeVersion(ctx context.Context, executable, agentDir string) (string, error) {
	environment := (pi.LaunchSpec{AgentDir: agentDir, BaseEnvironment: a.nativeBaseEnvironment()}).Environ()
	if !a.options.hostAuthoritySupplied {
		return pi.ProbeOrdinaryVersion(ctx, executable, environment)
	}

	var process NativeProcess

	var err error

	func() {
		defer func() {
			if recover() != nil {
				err = ErrHostAuthorityUnavailable
			}
		}()

		process, err = a.options.HostAuthority.StartNative(ctx, NativeRequest{
			Executable: executable, Arguments: []string{"--version"}, Environment: environment,
		})
	}()

	if err != nil {
		return "", err
	}

	if nativeProcessNil(process) {
		err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		a.recordNativeContainment(err)

		return "", err
	}

	var stdin io.WriteCloser

	var stdoutPipe, stderrPipe io.ReadCloser

	func() {
		defer func() {
			if recover() != nil {
				err = ErrHostAuthorityUnavailable
			}
		}()

		stdin, stdoutPipe, stderrPipe = process.Stdin(), process.Stdout(), process.Stderr()
	}()

	if err != nil || stdin == nil || stdoutPipe == nil || stderrPipe == nil {
		err = errors.Join(ErrHostAuthorityUnavailable, err, settleStartedNativeProcess(process))

		a.recordNativeContainment(err)

		return "", err
	}

	_ = stdin.Close()
	stdout := make(chan []byte, 1)
	stderr := make(chan []byte, 1)

	go func() { data, _ := io.ReadAll(stdoutPipe); stdout <- data }()
	go func() { data, _ := io.ReadAll(stderrPipe); stderr <- data }()

	result, waitErr := waitNativeProcess(ctx, process)
	if waitErr != nil && ctx.Err() != nil && detachedWaitError(waitErr) {
		revokeCtx, cancelRevoke := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		revokeErr := revokeNativeProcess(revokeCtx, process)

		cancelRevoke()

		waitCtx, cancelWait := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)
		terminalResult, terminalWaitErr := waitNativeProcess(waitCtx, process)

		cancelWait()

		if terminalWaitErr != nil {
			waitErr = errors.Join(ctx.Err(), revokeErr, terminalWaitErr, ErrContainmentIncomplete)
		} else {
			result = terminalResult
			waitErr = errors.Join(ctx.Err(), authorityTerminalRevokeError(revokeErr))
		}
	} else if waitErr != nil {
		waitErr = errors.Join(waitErr, ErrContainmentIncomplete)
	}

	_ = stdoutPipe.Close()
	_ = stderrPipe.Close()
	output := <-stdout
	diagnostic := <-stderr

	if waitErr != nil {
		a.recordNativeContainment(waitErr)

		return "", waitErr
	}

	if result.ExitCode != 0 {
		return "", fmt.Errorf("probe pi version: exit status %d: %s", result.ExitCode, strings.TrimSpace(string(diagnostic)))
	}

	version := strings.TrimSpace(string(output))
	if version == "" {
		return "", errors.New("probe pi version: empty output")
	}

	return version, nil
}

func (a *Agent) nativeBaseEnvironment() map[string]string {
	if a.options.hostAuthoritySupplied {
		return cloneStringMap(a.nativeEnvironment)
	}

	return cloneStringMap(a.ordinaryEnvironment)
}
