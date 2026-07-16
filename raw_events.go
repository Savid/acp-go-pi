package piacp

import (
	"encoding/json"
	"fmt"
)

const (
	rawEventMaxBytes = 64 * 1024

	rawEventSourceValue = "pi"

	rawEventFieldEvent     = "event"
	rawEventFieldSequence  = "sequence"
	rawEventFieldSource    = "source"
	rawEventFieldTruncated = "truncated"
	rawEventFieldReason    = "reason"
	rawEventFieldMaxBytes  = "maxBytes"
	rawEventFieldSizeBytes = "sizeBytes"

	rawEventReasonOversize       = "oversize"
	rawEventReasonUnserializable = "unserializable"
)

type rawMessageConfig struct {
	All bool
}

func rawMessageConfigFromMeta(meta map[string]any) rawMessageConfig {
	piMeta, _ := meta[piMetaKey].(map[string]any)
	if piMeta == nil {
		return rawMessageConfig{}
	}

	raw, _ := piMeta[metaRawEventKey].(map[string]any)

	enabled, _ := raw[metaRawEventEnabledKey].(bool)
	if enabled {
		return rawMessageConfig{All: true}
	}

	return rawMessageConfig{}
}

func (c rawMessageConfig) Enabled() bool {
	return c.All
}

// rawEventMarker inspects the fully-marshalled notification payload and
// returns a fixed truncation marker when the event cannot be delivered
// verbatim: oversize when it exceeds the 64 KiB limit, unserializable when it
// fails to marshal. It returns (nil, false) when the event fits and can be
// sent as-is. Oversized events are never dropped; the marker consumes the
// sequence so the per-session sequence stays contiguous and self-describing.
func rawEventMarker(payload map[string]any) (map[string]any, bool) {
	data, err := json.Marshal(payload)
	if err != nil {
		return map[string]any{
			rawEventFieldTruncated: true,
			rawEventFieldReason:    rawEventReasonUnserializable,
			rawEventFieldMaxBytes:  rawEventMaxBytes,
		}, true
	}

	if len(data) > rawEventMaxBytes {
		return map[string]any{
			rawEventFieldTruncated: true,
			rawEventFieldReason:    rawEventReasonOversize,
			rawEventFieldMaxBytes:  rawEventMaxBytes,
			rawEventFieldSizeBytes: len(data),
		}, true
	}

	return nil, false
}

// capRawEventPayload replaces only the native event when the complete routed
// payload is oversized, then proves the marker payload itself fits. Valid
// inbound routes are bounded so this final check is defensive rather than a
// reason to discard a native event.
func capRawEventPayload(payload map[string]any) (map[string]any, error) {
	if marker, replaced := rawEventMarker(payload); replaced {
		payload[rawEventFieldEvent] = marker
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal capped raw event payload: %w", err)
	}

	if len(data) > rawEventMaxBytes {
		return nil, fmt.Errorf("capped raw event payload is %d bytes, exceeds %d", len(data), rawEventMaxBytes)
	}

	return payload, nil
}
