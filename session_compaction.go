package piacp

import (
	"context"
	"crypto/rand"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

func (s *session) projectCompaction(ctx context.Context, rt *runtime, state *cycleState, event pi.Event) (bool, error) {
	value := wire.Compaction{}
	reason := ""
	key := rt.compactionKey

	switch typed := event.(type) {
	case pi.CompactionStartEvent:
		if rt.compactionKey == "" {
			rt.compactionKey = rand.Text()
		}

		key = rt.compactionKey

		value.Status = wire.CompactionInProgress
		reason = typed.Reason
	case pi.CompactionEndEvent:
		rt.compactionKey = ""

		reason = typed.Reason
		switch {
		case typed.Aborted:
			value.Status = wire.CompactionCancelled
		case typed.Result != nil:
			value.Status = wire.CompactionCompleted
			value.ContextBefore = typed.Result.TokensBefore
			value.ContextAfter = typed.Result.EstimatedTokensAfter

			if state != nil {
				state.context = 0
			}
		case typed.ErrorMessage != "":
			value.Status = wire.CompactionFailed
		default:
			return true, nil
		}
	default:
		return false, nil
	}

	switch reason {
	case "manual":
		value.Trigger = wire.CompactionTriggerManual
	case "threshold", "overflow":
		value.Trigger = wire.CompactionTriggerAuto
	}

	return true, rt.compactions.Publish(ctx, s.agent.connection(), s.id, key, value)
}
