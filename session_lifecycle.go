package piacp

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// sessionInterruptTimeout bounds the native abort. The abort runs under a
// background-derived context so a cancelled caller context cannot abort it.
var sessionInterruptTimeout = 5 * time.Second

// cancelDrainTimeout bounds how long a cancelled or timed-out turn waits for
// pi's terminal events after abort before the turn context is cut.
var cancelDrainTimeout = 5 * time.Second

// sessionShutdownTimeout bounds the process shutdown ladder on close.
var sessionShutdownTimeout = 10 * time.Second

func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	turn := s.turnQueue()

	select {
	case turn <- struct{}{}:
		return func() { <-turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("session_prompt")
	}
}

func (s *agentSession) turnQueue() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.turn == nil {
		s.turn = make(chan struct{}, sessionTurnCapacity)
	}

	return s.turn
}

func (s *agentSession) currentClient() piClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.client
}

// ensureProcessAlive relaunches the pi process when it died on a previous
// turn. The session is never removed on a native failure, so a follow-up
// prompt lands here and brings the process back up on the same native session
// file rather than returning the unknown-session error.
func (s *agentSession) ensureProcessAlive(ctx context.Context) error {
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()

	if proc == nil {
		return errors.New("pi session has no process")
	}

	select {
	case <-proc.Exited():
	default:
		return nil
	}

	s.stopPump()

	_ = proc.Close()

	s.mu.Lock()
	lastSessionFile := s.sessionFilePath
	s.mu.Unlock()

	spec := s.launch
	if s.sessionFileExists() {
		spec.SessionPath = lastSessionFile
		spec.SessionID = ""
	} else {
		spec.SessionPath = ""
		spec.SessionID = string(s.id)
	}

	// Detached like the initial launch: the relaunched child must survive
	// past the prompt request that triggered it.
	startCtx, finishStart := s.agent.observe.StartPiProcess(context.WithoutCancel(ctx), "relaunch")
	relaunched, client, err := s.agent.startPiProcess(startCtx, spec)

	finishStart(err)

	if err != nil {
		return err
	}

	if startErr := client.Start(context.WithoutCancel(ctx)); startErr != nil {
		_ = relaunched.Kill()
		_ = relaunched.Close()

		return startErr
	}

	s.mu.Lock()
	s.proc = relaunched
	s.client = client
	s.mu.Unlock()

	s.startPump(client)

	if retryErr := client.SetAutoRetry(ctx, false); retryErr != nil {
		return retryErr
	}

	// pi computes a fresh session file path per process even for the same
	// --session-id, so the mirror source must be re-captured after every
	// relaunch or commitMirror would silently read a path that never exists.
	state, stateErr := client.GetState(ctx)
	if stateErr != nil {
		return stateErr
	}

	if state.SessionID != string(s.id) {
		return fmt.Errorf("native session id drift on relaunch: expected %s, got %s", s.id, state.SessionID)
	}

	s.mu.Lock()
	s.sessionFilePath = state.SessionFile

	if spec.SessionID != "" {
		s.mirroredRows = 0
	}
	s.mu.Unlock()

	return nil
}

// Cancel cancels the active pi turn. Pending dialogs are resolved cancelled
// first, then the native abort runs on a bounded background context; the
// prompt loop keeps draining until pi settles the aborted turn so the mirror
// commit still lands, with a bounded backstop that cuts the turn context if
// the settle never arrives.
func (s *agentSession) Cancel(ctx context.Context) (err error) {
	s.cancelPendingInteractions()

	s.mu.Lock()
	turnCancel := s.cancel
	client := s.client
	s.mu.Unlock()

	if client == nil {
		return nil
	}

	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "abort")
		defer func() { finish(err) }()
	}

	interruptCtx, cancelInterrupt := context.WithTimeout(context.WithoutCancel(ctx), sessionInterruptTimeout)
	defer cancelInterrupt()

	err = client.Abort(interruptCtx)

	if turnCancel != nil {
		time.AfterFunc(cancelDrainTimeout, turnCancel)
	}

	return err
}

// cancelPendingInteractions marks the turn cancelled and resolves any pending
// extension UI dialogs as cancelled. Callers invoke this before the native
// abort so outstanding client requests are answered cancelled first.
func (s *agentSession) cancelPendingInteractions() {
	s.mu.Lock()
	if s.cancel != nil || len(s.pendingDialogs) > 0 {
		s.turnCancelled = true
	}

	dialogCancels := make([]context.CancelFunc, 0, len(s.pendingDialogs))
	for id, entry := range s.pendingDialogs {
		dialogCancels = append(dialogCancels, entry.cancel)

		delete(s.pendingDialogs, id)
	}
	s.mu.Unlock()

	for _, cancel := range dialogCancels {
		cancel()
	}
}

func (s *agentSession) wasTurnCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.turnCancelled
}

// Close shuts the pi process down and releases the session's resources.
func (s *agentSession) Close(ctx context.Context) (err error) {
	if s.agent != nil {
		var finish func(error)

		ctx, finish = s.agent.observe.StartPiProcess(ctx, "close")
		defer func() { finish(err) }()
	}

	s.cancelPendingInteractions()

	s.mu.Lock()
	cancel := s.cancel
	proc := s.proc
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if proc != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), sessionShutdownTimeout)

		if shutdownErr := proc.Shutdown(shutdownCtx); shutdownErr != nil {
			err = errors.Join(err, shutdownErr)
		}

		cancelShutdown()
	}

	s.stopPump()

	if proc != nil {
		err = errors.Join(err, proc.Close())
	}

	waitCtx, stopWaiting := context.WithTimeout(context.WithoutCancel(ctx), s.closeTurnTimeout())
	defer stopWaiting()

	if releaseTurn, waitErr := s.acquireTurn(waitCtx); waitErr != nil {
		err = errors.Join(err, waitErr)
	} else {
		releaseTurn()
	}

	err = errors.Join(err, s.removeSessionRoot())

	if s.agent != nil {
		s.agent.observe.RecordPiProcessExit(ctx, "closed", err)
	}

	return err
}

func (s *agentSession) closeTurnTimeout() time.Duration {
	if s.closeTurnWait > 0 {
		return s.closeTurnWait
	}

	return defaultSessionCloseTurnWait
}
