// Package pi implements the native boundary to the pi coding agent CLI:
// launching `pi --mode rpc` processes with isolated per-session agent
// directories, speaking pi's LF-delimited JSONL RPC protocol, and authoring
// the wrapper-owned extension and configuration files each session needs.
package pi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// maxLineBytes bounds one JSONL record. pi lines can be large (base64 image
// payloads inside events), so the cap is generous; beyond it the stream is
// considered broken.
const maxLineBytes = 64 << 20

// LineReader reads strict JSONL records: LF is the only record delimiter and
// an optional trailing CR is stripped. It never splits on Unicode separators
// (U+2028/U+2029 are valid inside JSON strings), per pi's RPC framing rules.
type LineReader struct {
	reader *bufio.Reader
}

// NewLineReader wraps r in a strict-LF JSONL record reader.
func NewLineReader(r io.Reader) *LineReader {
	return &LineReader{reader: bufio.NewReaderSize(r, 64<<10)}
}

// Next returns the next record without its delimiter. A final unterminated
// record is returned with a nil error; the subsequent call returns io.EOF.
func (l *LineReader) Next() ([]byte, error) {
	var line []byte

	for {
		chunk, err := l.reader.ReadSlice('\n')
		line = append(line, chunk...)

		if len(line) > maxLineBytes {
			return nil, fmt.Errorf("jsonl record exceeds %d bytes", maxLineBytes)
		}

		switch {
		case err == nil:
			return trimRecord(line), nil
		case err == bufio.ErrBufferFull:
			continue
		case err == io.EOF && len(line) > 0:
			return trimRecord(line), nil
		default:
			return nil, err
		}
	}
}

func trimRecord(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))

	return line
}

// MessageKind classifies one stdout JSONL record.
type MessageKind int

const (
	// MessageKindEvent is an agent event record.
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

	raw json.RawMessage
}

// RawJSON returns the raw response line bytes.
func (r Response) RawJSON() json.RawMessage {
	return r.raw
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
	NotifyType  string   `json:"notifyType,omitempty"`
	TimeoutMs   *float64 `json:"timeout,omitempty"`

	raw json.RawMessage
}

// RawJSON returns the raw request line bytes.
func (r UIRequest) RawJSON() json.RawMessage {
	return r.raw
}

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

// DecodeMessage classifies and decodes one stdout JSONL record.
func DecodeMessage(line []byte) (Message, error) {
	var env envelope
	if err := json.Unmarshal(line, &env); err != nil {
		return Message{}, fmt.Errorf("decode jsonl record: %w", err)
	}

	raw := json.RawMessage(append([]byte(nil), line...))

	switch env.Type {
	case "response":
		var response Response
		if err := json.Unmarshal(line, &response); err != nil {
			return Message{}, fmt.Errorf("decode response record: %w", err)
		}

		response.raw = raw

		return Message{Kind: MessageKindResponse, Response: response}, nil
	case "extension_ui_request":
		var request UIRequest
		if err := json.Unmarshal(line, &request); err != nil {
			return Message{}, fmt.Errorf("decode extension ui request: %w", err)
		}

		request.raw = raw

		return Message{Kind: MessageKindUIRequest, UIRequest: request}, nil
	default:
		event, err := decodeEvent(env.Type, line, raw)
		if err != nil {
			return Message{}, err
		}

		return Message{Kind: MessageKindEvent, Event: event}, nil
	}
}
