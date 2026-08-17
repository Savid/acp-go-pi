package piacp

import (
	"encoding/json"
	"errors"
	"log/slog"
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

func TestRedactRawEventImages(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	raw := []byte(`{"type":"tool_execution_end","toolCallId":"call","result":{"content":[` +
		`{"type":"text","text":"made an image"},` +
		`{"type":"image","data":"` + png + `","mimeType":"image/png"}]}}`)

	redacted := redactRawEventImages(raw)
	require.NotContains(t, string(redacted), png)
	require.Contains(t, string(redacted), `"sha256"`)
	require.Contains(t, string(redacted), `"sizeBytes"`)
	require.Contains(t, string(redacted), `"image/png"`)
	require.Contains(t, string(redacted), "made an image")

	// Undecodable payloads are still stripped, marked truncated instead of
	// carrying metadata.
	undecodable := redactRawEventImages([]byte(`{"images":[{"type":"image","data":"not base64!"}]}`))
	require.NotContains(t, string(undecodable), "not base64!")
	require.Contains(t, string(undecodable), `"truncated":true`)

	// Events without image blocks pass through byte-identical.
	plain := []byte(`{"type":"message_update","text":"an image of a cat"}`)
	require.Equal(t, plain, redactRawEventImages(plain))
	typed := []byte(`{"type":"message_update","content":[{"type":"image"},{"type":"text","text":"x"}]}`)
	require.Equal(t, typed, redactRawEventImages(typed))

	// A line that names image content but does not parse is replaced whole.
	require.JSONEq(t, redactedImageMarker, string(redactRawEventImages([]byte(`{"type":"image","data":`))))

	// A re-encode failure after redaction falls back to the whole-line marker.
	previous := marshalRedactedEvent
	marshalRedactedEvent = func(any) ([]byte, error) { return nil, errors.New("marshal") }

	t.Cleanup(func() { marshalRedactedEvent = previous })

	require.JSONEq(t, redactedImageMarker, string(redactRawEventImages(raw)))
}

func TestEmitRawPiEventRedactsImageData(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)
	session := &agentSession{agent: agent, id: "id", rawMessages: rawMessageConfig{All: true}}

	png := fixtureBase64(t, "valid.png")
	session.emitRawPiEvent(t.Context(), []byte(`{"type":"tool_execution_end","result":{"content":[{"type":"image","data":"`+png+`","mimeType":"image/png"}]}}`))

	require.Len(t, connection.notified, 1)
	encoded, err := json.Marshal(connection.notified[0])
	require.NoError(t, err)
	require.NotContains(t, string(encoded), png)
	require.Contains(t, string(encoded), `"sha256"`)
}
