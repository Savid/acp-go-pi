package piacp

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/coder/acp-go-sdk"
)

// maskOwnedRequestParams keeps the SDK from coercing owned metadata numbers or
// rejecting their magnitude before semantic validation. Only owned values are
// masked; member order, repeated envelopes, and foreign values remain intact.
// The original values are restored immediately after typed decoding.
func maskOwnedRequestParams(params json.RawMessage, value any) json.RawMessage {
	if !json.Valid(params) {
		return params
	}

	_, prompt := value.(*acp.PromptRequest)
	_, cancel := value.(*acp.CancelNotification)

	return mapWireObject(params, func(name string, raw json.RawMessage) json.RawMessage {
		switch {
		case strings.EqualFold(name, "_meta"):
			return maskWireMeta(raw, lifecycleMetaKey, prompt || cancel)
		case prompt && strings.EqualFold(name, "prompt"):
			return maskWireImages(raw)
		default:
			return raw
		}
	})
}

func maskWireMeta(raw json.RawMessage, owned string, route bool) json.RawMessage {
	return mapWireObject(raw, func(name string, value json.RawMessage) json.RawMessage {
		if name == owned || (route && name == routeMetaKey) {
			return json.RawMessage("null")
		}

		return value
	})
}

func maskWireImages(raw json.RawMessage) json.RawMessage {
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return raw
	}

	for index, block := range blocks {
		var kind struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(block, &kind); err != nil || kind.Type != contentBlockTypeImage {
			continue
		}

		blocks[index] = mapWireObject(block, func(name string, value json.RawMessage) json.RawMessage {
			if strings.EqualFold(name, "_meta") {
				return maskWireMeta(value, handoffMetaKey, false)
			}

			return value
		})
	}

	encoded, _ := json.Marshal(blocks)

	return encoded
}

// mapWireObject only visits a validated JSON value. Raw member values preserve
// duplicate members and numbers while the callback replaces selected values.
func mapWireObject(raw json.RawMessage, replace func(string, json.RawMessage) json.RawMessage) json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(raw))

	opening, _ := decoder.Token()
	if opening != json.Delim('{') {
		return raw
	}

	var output bytes.Buffer
	output.WriteByte('{')

	for decoder.More() {
		token, _ := decoder.Token()
		name, _ := token.(string)

		var value json.RawMessage

		_ = decoder.Decode(&value)

		if output.Len() > 1 {
			output.WriteByte(',')
		}

		key, _ := json.Marshal(name)
		output.Write(key)
		output.WriteByte(':')
		output.Write(replace(name, value))
	}

	output.WriteByte('}')

	return output.Bytes()
}

func restoreWireNamespace(params json.RawMessage, meta map[string]any, key string) {
	var envelope struct {
		Meta map[string]json.RawMessage `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	_ = json.Unmarshal(params, &envelope)
	if raw, present := envelope.Meta[key]; present {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()

		var value any

		_ = decoder.Decode(&value)
		meta[key] = value
	}
}

func restoreWireImages(params json.RawMessage, prompt []acp.ContentBlock) {
	var envelope struct {
		Prompt []json.RawMessage `json:"prompt"`
	}

	_ = json.Unmarshal(params, &envelope)

	for index, block := range prompt {
		if block.Image != nil {
			restoreWireNamespace(envelope.Prompt[index], block.Image.Meta, handoffMetaKey)
		}
	}
}

// wireIntegerValue accepts an exact integral JSON number, including decimal and
// exponent spellings. It never rounds a fraction or underflow to an integer and
// bounds exponent work by the int64 result, rather than allocating its magnitude.
func wireIntegerValue(number json.Number) (int64, bool) {
	raw := string(number)
	if !json.Valid([]byte(raw)) {
		return 0, false
	}

	mantissa, exponentText, scientific := strings.Cut(strings.ToLower(raw), "e")
	negative := strings.HasPrefix(mantissa, "-")
	whole, fraction, _ := strings.Cut(strings.TrimPrefix(mantissa, "-"), ".")

	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return 0, true
	}

	var exponent int64

	if scientific {
		parsed, err := strconv.ParseInt(exponentText, 10, 64)
		if err != nil {
			return 0, false
		}

		exponent = parsed
	}

	if exponent > int64(len(fraction))+19 || exponent < -int64(len(digits)) {
		return 0, false
	}

	shift := exponent - int64(len(fraction))
	if shift < 0 {
		remove := -shift
		if remove >= int64(len(digits)) || strings.Trim(digits[int64(len(digits))-remove:], "0") != "" {
			return 0, false
		}

		digits = digits[:int64(len(digits))-remove]
	} else {
		if int64(len(digits))+shift > 19 {
			return 0, false
		}

		digits += strings.Repeat("0", int(shift))
	}

	if negative {
		digits = "-" + digits
	}

	value, err := strconv.ParseInt(digits, 10, 64)

	return value, err == nil
}
