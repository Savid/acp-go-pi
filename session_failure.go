package piacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Native failures share one uniform wire shape so hosts can classify them
// without vendor-specific parsing. A native turn failure terminates
// session/prompt with a JSON-RPC error and no PromptResponse; it is never
// reported as a stop reason. A native session-start failure carries the same
// shape, because it has the same causes and the same recovery.
const (
	turnFailedError = "pi_turn_failed"

	failureFieldCause = "cause"

	failureCauseProcessExit = "process_exit"
	failureCauseTransport   = "transport"
	failureCauseProvider    = "provider"
	failureCauseTimeout     = "timeout"
	failureCauseExtension   = "extension"

	// extensionFailureMessage is the whole of what a client is told about an
	// extension that threw. The wrapper-owned extensions are the permission
	// bridge and the MCP client: their filesystem paths are adapter-internal
	// and their thrown text can carry tool input, so the cause is named and the
	// prose is not.
	extensionFailureMessage = "a pi extension failed"

	// nativeCauseMaxBytes bounds the native cause text the client is told. The
	// uniform shape carries the cause, not a transcript: a dying pi can print
	// anything at all to stderr. The bounded final cause stays on the wire;
	// native stderr is never copied into the adapter log.
	nativeCauseMaxBytes = 2048
)

// turnFailureError builds the uniform -32603 pi_turn_failed error with the
// given machine-readable cause and the real native cause text, bounded.
func turnFailureError(cause string, message string) *acp.RequestError {
	return acp.NewInternalError(map[string]any{
		jsonFieldError:    turnFailedError,
		failureFieldCause: cause,
		jsonFieldMessage:  boundNativeCause(message),
	})
}

// extensionTurnFailure is the uniform failure a cycle reports when pi's
// extension surface threw. It fails closed with a fixed message, because the
// permission bridge is the session's authorization boundary and a cycle that
// continued past its failure would be running on an admission nobody granted.
func extensionTurnFailure() *acp.RequestError {
	return turnFailureError(failureCauseExtension, extensionFailureMessage)
}

// boundNativeCause is the single gate every native cause text passes through
// before it reaches a client.
func boundNativeCause(message string) string {
	// The cap applies to the original byte prefix. Trimming first would let an
	// arbitrarily long run of leading spaces expose a suffix beyond the wire
	// budget, and repairing before slicing could allocate or scan unbounded
	// attacker-controlled input.
	bounded := message
	if len(bounded) > nativeCauseMaxBytes {
		bounded = bounded[:nativeCauseMaxBytes]
	}

	return strings.TrimSpace(strings.ToValidUTF8(bounded, ""))
}

// processExitClassifyGrace bounds how long failure classification waits for
// the child's exit to be reaped: a dying pi closes stdout before its exit
// status is observed, so classifying at the instant of stream EOF would race
// the reaper and report a genuine child death as transport.
var processExitClassifyGrace = 2 * time.Second

// nativeTurnFailure classifies an error observed while driving a pi turn into
// the uniform failure shape. A native command rejection (pi answered
// success:false with a reason, for example a missing provider API key) maps
// to provider with the native text — an answered command proves the process
// was alive, so it skips the exit-classification grace. A dead child maps to
// process_exit with the recovered exit cause; everything else (stream close,
// write failure) maps to transport. It never surfaces a fixed placeholder or
// a bare EOF.
func (s *agentSession) nativeTurnFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	var commandErr *pi.CommandError
	if errors.As(err, &commandErr) {
		return turnFailureError(failureCauseProvider, commandErr.Message)
	}

	if exitMessage, exited := s.processExitCause(ctx, "pi process exited"); exited {
		return turnFailureError(failureCauseProcessExit, exitMessage)
	}

	s.agent.log.ErrorContext(ctx, "pi turn transport failed",
		slog.String(acpFieldSessionID, string(s.id)),
		slog.String(failureFieldCause, failureCauseTransport),
	)

	return turnFailureError(failureCauseTransport, err.Error())
}

// processExitCause recovers the real native cause of a dead pi child. It waits
// up to the classify grace for the exit to be reaped, so a death whose stream
// EOF arrives first still classifies as process_exit.
func (s *agentSession) processExitCause(ctx context.Context, subject string) (string, bool) {
	proc := s.process()
	if proc == nil {
		return "", false
	}

	select {
	case <-proc.Exited():
	case <-time.After(processExitClassifyGrace):
		return "", false
	}

	return nativeExitCause(ctx, s.agent.log, subject, proc), true
}

// nativeExitCause composes the exit status with the final retained stderr line
// that names the bounded wire cause. No native stderr is logged.
func nativeExitCause(ctx context.Context, log *slog.Logger, subject string, proc piProcess) string {
	message := subject

	waitErr := proc.WaitErr()
	if waitErr != nil {
		message = fmt.Sprintf("%s: %v", subject, waitErr)
	}

	tail := strings.TrimSpace(proc.StderrTail())

	log.ErrorContext(ctx, "pi native process exited",
		slog.String("stage", subject),
	)

	if cause := nativeCauseLine(tail); cause != "" {
		message += ": " + cause
	}

	return message
}

// nativeCauseLine is the line of a captured stderr tail that names the cause:
// a dying harness prints its fatal reason last, and everything ahead of it is
// transcript the client has no contract to receive.
func nativeCauseLine(tail string) string {
	if index := strings.LastIndexByte(tail, '\n'); index >= 0 {
		return strings.TrimSpace(tail[index+1:])
	}

	return tail
}

func (s *agentSession) process() piProcess {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.proc
}

// providerTurnFailure maps an assistant message that ended with a native
// error into the uniform failure shape. pi is BYO-key and this adapter
// advertises no ACP auth methods, so every provider error — including an
// authentication failure — is -32603 with cause "provider", never -32000.
func providerTurnFailure(state *promptTurnState) error {
	if state.stopReason != stopReasonError {
		return nil
	}

	message := strings.TrimSpace(state.errorMessage)
	if message == "" {
		message = "pi reported a turn error"
	}

	return turnFailureError(failureCauseProvider, message)
}

// nativeStartFailure maps a failed native session start onto the same uniform
// shape a failed turn uses: the causes are the same, and a host that can
// classify one can classify the other. A pi that already exited — an MCP
// server that failed to connect makes pi exit at startup — carries the
// recovered exit cause; otherwise the stage's own cause stands. Native
// evidence goes to the adapter log; the client receives the bounded cause.
func (a *Agent) nativeStartFailure(ctx context.Context, cause string, err error, proc piProcess) error {
	if proc != nil {
		select {
		case <-proc.Exited():
			exit := turnFailureError(failureCauseProcessExit, nativeExitCause(ctx, a.log, "pi exited during session start", proc))

			return errors.Join(exit, err)
		default:
		}
	}

	a.log.ErrorContext(ctx, "pi session start failed", slog.String(failureFieldCause, cause))

	// The driving error is joined rather than discarded: the wire answer is
	// the RequestError the mapper produces, while adapter-internal callers
	// still match containment and cancellation identity on the same value.
	return errors.Join(turnFailureError(cause, err.Error()), err)
}

// emptyCloneError reports whether a native command failure names a missing
// session entry, which pi returns when cloning an empty session.
func emptyCloneError(err error) bool {
	var commandErr *pi.CommandError

	return errors.As(err, &commandErr) && strings.Contains(commandErr.Message, "not found")
}
