package lifecycle

import (
	"bytes"
	"encoding/json"
	"iter"
	"strings"
)

type requestJSON struct{ json.RawMessage }

// PreserveRequestMeta retains the owned wire value after the SDK has decoded a
// valid request. Validation stays at the negotiation/correlation boundary, so
// construction and route errors keep their precedence. Other namespaces retain
// the SDK's decoding behavior.
func PreserveRequestMeta(params json.RawMessage, meta map[string]any) map[string]any {
	var raw json.RawMessage

	envelopes, occurrences := 0, 0

	for name, value := range requestObjectMembers(params) {
		if !strings.EqualFold(name, "_meta") {
			continue
		}

		envelopes++

		for key, member := range requestObjectMembers(value) {
			if key == MetaKey {
				raw = member
				occurrences++
			}
		}
	}

	if occurrences == 0 {
		return meta
	}

	if meta == nil {
		meta = make(map[string]any)
	}

	if envelopes > 1 || occurrences > 1 {
		meta[MetaKey] = paramError()
	} else {
		meta[MetaKey] = requestJSON{raw}
	}

	return meta
}

// requestObjectMembers visits members without merging repeated keys. Callers
// supply valid JSON from a successfully decoded request or one of its values;
// token and value reads therefore cannot fail inside an object.
func requestObjectMembers(raw json.RawMessage) iter.Seq2[string, json.RawMessage] {
	return func(yield func(string, json.RawMessage) bool) {
		decoder := json.NewDecoder(bytes.NewReader(raw))

		opening, _ := decoder.Token()
		if opening != json.Delim('{') {
			return
		}

		for decoder.More() {
			token, _ := decoder.Token()
			name, _ := token.(string)

			var value json.RawMessage

			_ = decoder.Decode(&value)
			if !yield(name, value) {
				return
			}
		}
	}
}

func requestFields(raw any, members ...string) (map[string]any, *ParamError) {
	if refusal, ok := raw.(*ParamError); ok {
		return nil, refusal
	}

	if data, ok := raw.(requestJSON); ok {
		if bytes.TrimSpace(data.RawMessage)[0] != '{' {
			return nil, paramError(members...)
		}

		fields := make(map[string]any)
		for name, value := range requestObjectMembers(data.RawMessage) {
			if _, duplicate := fields[name]; duplicate {
				return nil, paramError(append(members, name)...)
			}

			if name == fieldSubmission && len(members) == 0 {
				fields[name] = requestJSON{value}

				continue
			}

			decoder := json.NewDecoder(bytes.NewReader(value))
			decoder.UseNumber()

			var decoded any

			_ = decoder.Decode(&decoded)
			fields[name] = decoded
		}

		return fields, nil
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return nil, paramError(members...)
	}

	return fields, nil
}
