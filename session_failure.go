package piacp

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

// Native turn failures share one uniform wire shape so hosts can classify a
// failed turn without vendor-specific parsing. A native turn failure
// terminates session/prompt with a JSON-RPC error and no PromptResponse; it
// is never reported as a stop reason.
const (
	turnFailedError = "pi_turn_failed"

	failureFieldCause = "cause"

	failureCauseProcessExit = "process_exit"
	failureCauseTransport   = "transport"
	failureCauseProvider    = "provider"
	failureCauseTimeout     = "timeout"
)

// turnFailureError builds the uniform -32603 pi_turn_failed error with the
// given machine-readable cause and the real native cause text.
func turnFailureError(cause string, message string) *acp.RequestError {
	return acp.NewInternalError(map[string]any{
		jsonFieldError:    turnFailedError,
		failureFieldCause: cause,
		jsonFieldMessage:  message,
	})
}

// processExitClassifyGrace bounds how long failure classification waits for
// the child's exit to be reaped: a dying pi closes stdout before its exit
// status is observed, so classifying at the instant of stream EOF would race
// the reaper and report a genuine child death as transport.
var processExitClassifyGrace = 2 * time.Second

// nativeTurnFailure classifies an error observed while driving a pi turn into
// the uniform failure shape. A native command rejection (pi answered
// success:false with a reason, for example a missing provider API key) maps
// to provider with the native text verbatim — an answered command proves the
// process was alive, so it skips the exit-classification grace. A dead child
// maps to process_exit with the real exit status and stderr tail; everything
// else (stream close, write failure) maps to transport. It never surfaces a
// fixed placeholder or a bare EOF.
func (s *agentSession) nativeTurnFailure(err error) error {
	if err == nil {
		return nil
	}

	var commandErr *pi.CommandError
	if errors.As(err, &commandErr) {
		return turnFailureError(failureCauseProvider, commandErr.Message)
	}

	if exitMessage, exited := s.processExitMessage(); exited {
		return turnFailureError(failureCauseProcessExit, exitMessage)
	}

	return turnFailureError(failureCauseTransport, err.Error())
}

// processExitMessage recovers the real native cause of a dead pi child: the
// exit status plus the retained stderr tail. It waits up to the classify
// grace for the exit to be reaped, so a death whose stream EOF arrives first
// still classifies as process_exit.
func (s *agentSession) processExitMessage() (string, bool) {
	proc := s.process()
	if proc == nil {
		return "", false
	}

	select {
	case <-proc.Exited():
	case <-time.After(processExitClassifyGrace):
		return "", false
	}

	message := "pi process exited"
	if waitErr := proc.WaitErr(); waitErr != nil {
		message = fmt.Sprintf("pi process exited: %v", waitErr)
	}

	if tail := strings.TrimSpace(proc.StderrTail()); tail != "" {
		message += ": " + tail
	}

	return message, true
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

// spawnFailureError maps a pi process that died during session start (for
// example an MCP server connect failure, which makes pi exit at startup) into
// a structured session-start error naming the real native cause.
func spawnFailureError(err error, proc piProcess) error {
	message := err.Error()

	if proc != nil {
		select {
		case <-proc.Exited():
			message = "pi exited during session start"
			if waitErr := proc.WaitErr(); waitErr != nil {
				message = fmt.Sprintf("pi exited during session start: %v", waitErr)
			}

			if tail := strings.TrimSpace(proc.StderrTail()); tail != "" {
				message += ": " + tail
			}
		default:
		}
	}

	return acp.NewInternalError(map[string]any{
		jsonFieldError:   "pi_session_start_failed",
		jsonFieldMessage: message,
	})
}

// emptyCloneError reports whether a native command failure names a missing
// session entry, which pi returns when cloning an empty session.
func emptyCloneError(err error) bool {
	var commandErr *pi.CommandError

	return errors.As(err, &commandErr) && strings.Contains(commandErr.Message, "not found")
}
