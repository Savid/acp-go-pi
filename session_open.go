package piacp

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type generationDoneContext struct {
	done <-chan struct{}
}

func (c generationDoneContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c generationDoneContext) Done() <-chan struct{}       { return c.done }
func (c generationDoneContext) Value(any) any               { return nil }

func (c generationDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// admitPostResponseHook claims the exact native generation before the hook is
// launched. Its context is cancelled by that generation's containment and is
// independently bounded, while its producer token is joined after native
// Close has made any blocked transport write interruptible.
func (s *agentSession) admitPostResponseHook() (context.Context, func(), bool) {
	s.mu.Lock()
	outbox := s.outbox
	closing := s.closing
	s.mu.Unlock()

	if outbox == nil || closing {
		return nil, func() {}, false
	}

	outbox.mu.Lock()
	if outbox.closing || outbox.fenced || outbox.ended || outbox.interactionsClosed {
		outbox.mu.Unlock()

		return nil, func() {}, false
	}

	releaseProducer, admitted := outbox.producers.acquire(1)
	generationDone := outbox.generationDone
	outbox.mu.Unlock()

	if !admitted {
		return nil, func() {}, false
	}

	producerCtx := context.WithoutCancel(context.Background())
	if generationDone != nil {
		producerCtx = generationDoneContext{done: generationDone}
	}

	hookCtx, cancelHook := context.WithTimeout(producerCtx, sessionSettleTimeout)

	var once sync.Once

	return hookCtx, func() {
		once.Do(func() {
			cancelHook()
			releaseProducer()
		})
	}, true
}

// publishSessionOpen emits the establishing snapshot for one session, exactly
// once: the explicit command catalog — including the empty case, which is an
// answer rather than a silence a host has to guess at — and the opening
// lifecycle stream for the native generation the session is running on.
//
// A lifecycle stream this session cannot open is not opened: the failure is
// recorded and the incarnation is fenced, so the next one states the truthful
// state rather than continuing from a first event that never landed.
func (s *agentSession) publishSessionOpen(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	if s.opened {
		s.mu.Unlock()

		return s.admissionFenceError(ctx)
	}

	s.opened = true
	generation := s.pumpGeneration
	outbox := s.outbox
	s.mu.Unlock()

	if err := s.emitRequiredAvailableCommandsUpdate(ctx); err != nil {
		s.agent.log.ErrorContext(ctx, "publish initial pi command catalog failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

		poisonErr := s.poison(
			context.WithoutCancel(ctx), "the required session-open command catalog was not delivered",
		)
		containmentErr := s.containGenerationSync(
			context.WithoutCancel(ctx), outbox, "the required session-open command catalog was not delivered",
		)
		s.fenceLifecycleGeneration(generation)

		return errors.Join(err, poisonErr, containmentErr)
	}

	if err := context.Cause(ctx); err != nil {
		// A bounded producer whose host write returned after generation
		// containment owns no continuation. In particular it may not open or
		// terminalize lifecycle state after Close retained that producer.
		return err
	}

	if err := s.openLifecycleStream(ctx, generation); err != nil {
		s.agent.log.ErrorContext(ctx, "open pi session lifecycle stream failed",
			slog.String(acpFieldSessionID, string(s.id)),
		)

		poisonErr := s.poison(
			context.WithoutCancel(ctx), "the required session-open lifecycle snapshot was not delivered",
		)
		containmentErr := s.containGenerationSync(
			context.WithoutCancel(ctx), outbox, "the required session-open lifecycle snapshot was not delivered",
		)
		s.fenceLifecycleGeneration(generation)

		return errors.Join(err, poisonErr, containmentErr)
	}

	if err := outbox.acceptEstablishment(); err != nil {
		containmentErr := s.containGenerationSync(
			context.WithoutCancel(ctx), outbox, "the session-open generation gate could not be released",
		)

		return errors.Join(err, containmentErr)
	}

	// Replay the startup prefix after the response and opening snapshot, but
	// before prompt admission observes establishment complete.
	s.drainOutbox(ctx, outbox)
	outbox.wake()

	return nil
}

// publishSessionOpenInline emits the establishing snapshot for a host whose
// connection has no post-response hook to defer it behind. The stdio connection
// this package builds writes the establishing response first and then runs the
// hook; an embedded host holding the Agent directly has no transport write to
// order against, so its snapshot is published as the establishing call returns.
func (a *Agent) publishSessionOpenInline(ctx context.Context, session *agentSession) error {
	if _, deferred := a.connection().(*localAgentConnection); deferred {
		return nil
	}

	return session.publishSessionOpen(ctx)
}
