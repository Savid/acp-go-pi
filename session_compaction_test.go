package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/savid/acp-go-pi/internal/pi"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func compactionReports(t *testing.T, notifications []acp.SessionNotification) []wire.Compaction {
	t.Helper()
	var reports []wire.Compaction
	for _, notification := range notifications {
		value, exists := notification.Meta[wire.CompactionKey]
		if !exists {
			continue
		}
		carrier, err := json.Marshal(notification.Update)
		require.NoError(t, err)
		require.JSONEq(t, `{"sessionUpdate":"session_info_update"}`, string(carrier))
		require.Len(t, notification.Meta, 1)
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		var report wire.Compaction
		require.NoError(t, json.Unmarshal(encoded, &report))
		require.NotEmpty(t, report.CompactionID)
		reports = append(reports, report)
	}

	return reports
}

func TestCompactionTransport(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	created := h.newSession()
	for range 2 {
		_, err := h.prompt(created.SessionId, "COMPACT", nil)
		require.NoError(t, err)
	}
	reports := compactionReports(t, h.rec.snapshot())
	require.Len(t, reports, 4)
	require.NotEqual(t, reports[0].CompactionID, reports[2].CompactionID)
	for i := 0; i < len(reports); i += 2 {
		require.Equal(t, wire.CompactionCompleted, reports[i+1].Status)
		require.Equal(t, wire.CompactionInProgress, reports[i].Status)
		require.Equal(t, reports[i].CompactionID, reports[i+1].CompactionID)
		require.Equal(t, "auto", reports[i+1].Trigger)
		require.Equal(t, new(950), reports[i+1].ContextBefore)
		require.Equal(t, new(120), reports[i+1].ContextAfter)
	}
}

func TestCompactionRetriesAndOutcomes(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	state := &cycleState{}
	rt := &runtime{}
	for _, event := range []pi.Event{
		pi.CompactionStartEvent{Reason: "overflow"},
		pi.CompactionStartEvent{Reason: "overflow"},
		pi.CompactionEndEvent{ErrorMessage: "summary failed"},
		pi.CompactionStartEvent{Reason: "manual"},
		pi.CompactionEndEvent{Aborted: true},
		pi.CompactionStartEvent{Reason: "threshold"},
		pi.CompactionEndEvent{Result: &pi.CompactionResult{TokensBefore: new(120), EstimatedTokensAfter: new(0)}},
		pi.CompactionStartEvent{},
		pi.CompactionEndEvent{},
	} {
		handled, err := s.projectCompaction(t.Context(), rt, state, event)
		require.True(t, handled)
		require.NoError(t, err)
	}
	reports := compactionReports(t, rec.snapshot())
	require.Len(t, reports, 7)
	for i, status := range []string{wire.CompactionFailed, wire.CompactionCancelled, wire.CompactionCompleted} {
		require.Equal(t, reports[2*i].CompactionID, reports[2*i+1].CompactionID)
		require.Equal(t, status, reports[2*i+1].Status)
		if i > 0 {
			require.NotEqual(t, reports[2*i-1].CompactionID, reports[2*i].CompactionID)
		}
	}
	require.Equal(t, "auto", reports[1].Trigger)
	require.Equal(t, "manual", reports[3].Trigger)
	require.Equal(t, new(0), reports[5].ContextAfter)
	require.Empty(t, reports[6].Trigger)
	handled, err := s.projectCompaction(t.Context(), &runtime{}, state, pi.CompactionStartEvent{})
	require.True(t, handled)
	require.NoError(t, err)
	restarted := compactionReports(t, rec.snapshot())
	require.Len(t, restarted, 8)
	require.NotEqual(t, reports[6].CompactionID, restarted[7].CompactionID)
}

func TestCompactionOverflowRetryExhaustion(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	rt := &runtime{}
	for _, raw := range []string{
		`{"type":"compaction_start","reason":"overflow"}`,
		`{"type":"compaction_end","reason":"overflow","result":{"tokensBefore":950,"estimatedTokensAfter":120},"aborted":false,"willRetry":true}`,
		`{"type":"compaction_end","reason":"overflow","aborted":false,"willRetry":false,"errorMessage":"Context overflow recovery failed after one compact-and-retry attempt."}`,
		`{"type":"compaction_start","reason":"manual"}`,
		`{"type":"compaction_end","reason":"manual","aborted":true,"willRetry":false}`,
	} {
		message, err := pi.DecodeMessage([]byte(raw))
		require.NoError(t, err)
		handled, err := s.projectCompaction(t.Context(), rt, nil, message.Event)
		require.True(t, handled)
		require.NoError(t, err)
	}
	reports := compactionReports(t, rec.snapshot())
	require.Len(t, reports, 5)
	require.Equal(t, reports[0].CompactionID, reports[1].CompactionID)
	require.Equal(t, wire.CompactionCompleted, reports[1].Status)
	require.NotEqual(t, reports[1].CompactionID, reports[2].CompactionID)
	require.Equal(t, wire.CompactionFailed, reports[2].Status)
	require.Equal(t, "auto", reports[2].Trigger)
	require.Nil(t, reports[2].ContextBefore)
	require.Nil(t, reports[2].ContextAfter)
	require.NotEqual(t, reports[2].CompactionID, reports[3].CompactionID)
	require.Equal(t, reports[3].CompactionID, reports[4].CompactionID)
	require.Equal(t, wire.CompactionCancelled, reports[4].Status)
}

type compactionFailureClient struct {
	*recorder
	failed bool
}

func (c *compactionFailureClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if notification.Meta[wire.CompactionKey] != nil && !c.failed {
		c.failed = true

		return errors.New("compaction delivery unavailable")
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestCompactionSendFailureKeepsRuntime(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := &compactionFailureClient{recorder: newRecorder()}
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rt := &runtime{cancel: cancel}
	s.runtime = rt
	for range 2 {
		s.handleEvent(ctx, rt, pi.CompactionEndEvent{Reason: "overflow", ErrorMessage: "summary failed"})
	}
	require.NoError(t, ctx.Err())
	require.True(t, rec.failed)
	require.Len(t, compactionReports(t, rec.snapshot()), 1)
}
