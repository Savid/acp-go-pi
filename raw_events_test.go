package piacp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawMessageConfigAndMarkers(t *testing.T) {
	require.False(t, rawMessageConfigFromMeta(nil).Enabled())
	require.False(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: "bad"}).Enabled())
	require.False(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: map[string]any{metaRawEventKey: true}}).Enabled())
	require.True(t, rawMessageConfigFromMeta(map[string]any{piMetaKey: map[string]any{
		metaRawEventKey: map[string]any{metaRawEventEnabledKey: true},
	}}).Enabled())

	marker, marked := rawEventMarker(map[string]any{"value": "small"})
	require.False(t, marked)
	require.Nil(t, marker)

	marker, marked = rawEventMarker(map[string]any{"value": make(chan int)})
	require.True(t, marked)
	require.Equal(t, rawEventReasonUnserializable, marker[rawEventFieldReason])
	require.NotContains(t, marker, rawEventFieldSizeBytes)

	marker, marked = rawEventMarker(map[string]any{"value": string(make([]byte, rawEventMaxBytes))})
	require.True(t, marked)
	require.Equal(t, rawEventReasonOversize, marker[rawEventFieldReason])
	require.Greater(t, marker[rawEventFieldSizeBytes], rawEventMaxBytes)
}

func TestRawEventFinalPayloadBoundaryIncludesRouteMeta(t *testing.T) {
	payload := map[string]any{
		acpFieldSessionID:     "session-1",
		rawEventFieldSequence: int64(1),
		rawEventFieldSource:   rawEventSourceValue,
		rawEventFieldEvent:    map[string]any{"data": ""},
		"_meta":               turnRouteMeta(strings.Repeat("n", routeTurnNonceMaxBytes)),
	}
	empty, err := json.Marshal(payload)
	require.NoError(t, err)
	padding := rawEventMaxBytes - len(empty)
	require.Positive(t, padding)
	payload[rawEventFieldEvent] = map[string]any{"data": strings.Repeat("x", padding)}

	capped, err := capRawEventPayload(payload)
	require.NoError(t, err)
	encoded, err := json.Marshal(capped)
	require.NoError(t, err)
	require.Len(t, encoded, rawEventMaxBytes)
	require.NotContains(t, anyMap(t, capped[rawEventFieldEvent]), rawEventFieldTruncated)

	payload[rawEventFieldEvent] = map[string]any{"data": strings.Repeat("x", padding+1)}
	capped, err = capRawEventPayload(payload)
	require.NoError(t, err)
	encoded, err = json.Marshal(capped)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), rawEventMaxBytes)
	marker := anyMap(t, capped[rawEventFieldEvent])
	require.Equal(t, rawEventReasonOversize, marker[rawEventFieldReason])
	require.Equal(t, rawEventMaxBytes+1, marker[rawEventFieldSizeBytes])
}

func TestRawEventFinalPayloadRejectsUnboundedInternalRoute(t *testing.T) {
	payload := map[string]any{
		acpFieldSessionID:     "session-1",
		rawEventFieldSequence: int64(1),
		rawEventFieldSource:   rawEventSourceValue,
		rawEventFieldEvent:    map[string]any{"type": "event"},
		"_meta":               turnRouteMeta(strings.Repeat("n", rawEventMaxBytes)),
	}

	_, err := capRawEventPayload(payload)
	require.ErrorContains(t, err, "exceeds")

	payload["_meta"] = make(chan int)
	_, err = capRawEventPayload(payload)
	require.ErrorContains(t, err, "marshal capped raw event payload")
}
