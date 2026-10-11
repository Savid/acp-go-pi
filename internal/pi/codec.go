// Package pi implements the native boundary to the pi coding agent CLI: the
// launch arguments of one `pi --mode rpc` process, its LF-delimited JSONL RPC,
// the wrapper-owned extensions it loads, and the session-file layout pi keeps
// in its own home.
package pi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrJSONLStructural reports a native record that is not a JSON object of a
// shape this package decodes. It deliberately carries no record bytes or
// decoder detail.
var ErrJSONLStructural = errors.New("pi jsonl stream structural error")

// MessageKind classifies one stdout JSONL record.
type MessageKind int

const (
	// MessageKindEvent is an agent event record, or the bridge extension's
	// response-id relay.
	MessageKindEvent MessageKind = iota
	// MessageKindResponse is a command response record.
	MessageKindResponse
	// MessageKindUIRequest is an extension UI request record.
	MessageKindUIRequest
)

// Response is a pi RPC command response.
type Response struct {
	ID      string          `json:"id,omitempty"`
	Command string          `json:"command"`
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// CommandError is a pi RPC command failure carrying the real native cause.
type CommandError struct {
	Command string
	Message string
}

// Error implements the error interface.
func (e *CommandError) Error() string {
	return fmt.Sprintf("pi command %s failed: %s", e.Command, e.Message)
}

// Err converts a failed response into a *CommandError; nil when successful.
func (r Response) Err() error {
	if r.Success {
		return nil
	}

	return &CommandError{Command: r.Command, Message: r.Error}
}

// UIRequest is one extension UI request emitted by a pi extension. Dialog
// methods (select, confirm, input, editor) block the extension until the
// client answers with a UIResponse carrying the same ID; fire-and-forget
// methods expect no response.
type UIRequest struct {
	ID          string   `json:"id"`
	Method      string   `json:"method"`
	Title       string   `json:"title,omitempty"`
	Options     []string `json:"options,omitempty"`
	Message     string   `json:"message,omitempty"`
	Placeholder string   `json:"placeholder,omitempty"`
	Prefill     string   `json:"prefill,omitempty"`
	StatusKey   string   `json:"statusKey,omitempty"`
	StatusText  string   `json:"statusText,omitempty"`
}

// uiMethodSetStatus is the fire-and-forget method that sets a keyed status.
const uiMethodSetStatus = "setStatus"

// IsDialog reports whether the request blocks the extension until answered.
func (r UIRequest) IsDialog() bool {
	switch r.Method {
	case "select", "confirm", "input", "editor":
		return true
	default:
		return false
	}
}

// UIResponse answers one extension UI dialog request on pi's stdin.
type UIResponse struct {
	Type      string  `json:"type"`
	ID        string  `json:"id"`
	Value     *string `json:"value,omitempty"`
	Confirmed *bool   `json:"confirmed,omitempty"`
	Cancelled bool    `json:"cancelled,omitempty"`
}

const uiResponseType = "extension_ui_response"

// UIValueResponse answers a select, input, or editor dialog with a value.
func UIValueResponse(id string, value string) UIResponse {
	return UIResponse{Type: uiResponseType, ID: id, Value: &value}
}

// UIConfirmResponse answers a confirm dialog.
func UIConfirmResponse(id string, confirmed bool) UIResponse {
	return UIResponse{Type: uiResponseType, ID: id, Confirmed: &confirmed}
}

// UICancelResponse dismisses any dialog request.
func UICancelResponse(id string) UIResponse {
	return UIResponse{Type: uiResponseType, ID: id, Cancelled: true}
}

// Message is one classified stdout JSONL record.
type Message struct {
	Kind      MessageKind
	Event     Event
	Response  Response
	UIRequest UIRequest
}

type envelope struct {
	Type string `json:"type"`
}

// DecodeMessage classifies and decodes one stdout JSONL record, parsing it
// once. The record is retained as an event's raw bytes, so the caller hands
// over a line it no longer writes to.
func DecodeMessage(line []byte) (Message, error) {
	recordType, err := messageType(line)
	if err != nil {
		return Message{}, err
	}

	switch recordType {
	case "response":
		var response Response
		if err := json.Unmarshal(line, &response); err != nil {
			return Message{}, fmt.Errorf("decode response record: %w", err)
		}

		return Message{Kind: MessageKindResponse, Response: response}, nil
	case "extension_ui_request":
		var request UIRequest
		if err := json.Unmarshal(line, &request); err != nil {
			return Message{}, fmt.Errorf("decode extension ui request: %w", err)
		}

		if request.Method == uiMethodSetStatus && request.StatusKey == ResponseStatusKey {
			return Message{Kind: MessageKindEvent, Event: ResponseIDEvent{baseEvent: baseEvent{raw: line}, ResponseID: request.StatusText}}, nil
		}

		return Message{Kind: MessageKindUIRequest, UIRequest: request}, nil
	default:
		event, err := decodeEvent(recordType, line)
		if err != nil {
			return Message{}, err
		}

		return Message{Kind: MessageKindEvent, Event: event}, nil
	}
}

// messageType reads a record's type member. pi writes it among the leading
// scalar members, so only the record's head is read; a record whose type
// follows a compound member is decoded whole.
func messageType(line []byte) (string, error) {
	if recordType, ok := leadingType(line); ok {
		return recordType, nil
	}

	var env envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return "", fmt.Errorf("decode jsonl record: %w", err)
	}

	return env.Type, nil
}

// leadingType returns the string type member of a JSON object when only
// scalar members precede it.
func leadingType(line []byte) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(line))

	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return "", false
	}

	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return "", false
		}

		value, err := decoder.Token()
		if err != nil {
			return "", false
		}

		if _, compound := value.(json.Delim); compound {
			return "", false
		}

		if key == "type" {
			recordType, ok := value.(string)

			return recordType, ok
		}
	}

	return "", false
}
