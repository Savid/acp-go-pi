package piacp

import (
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
