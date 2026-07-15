//go:build integration

package integration

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// runTurn streams one accepted prompt. Terminal ordering is fixed:
// message_end → turn_end → agent_end, the session file is durable, and only
// then agent_settled fires — exactly once per accepted prompt, including
// after abort and provider errors.
func (s *fakePiServer) runTurn(turn *fakeTurn, message string) {
	finished := false

	defer func() {
		s.mu.Lock()
		s.turn = nil
		s.mu.Unlock()

		if finished {
			s.out.writeJSON(fakeTypeOnlyEvent{Type: "agent_settled"})
		}

		close(turn.done)
	}()

	s.out.writeJSON(fakeTypeOnlyEvent{Type: "agent_start"})
	s.appendMessageEntry(mustJSON(fakeUserMessage{
		Role:      "user",
		Content:   message,
		Timestamp: time.Now().UnixMilli(),
	}), fakeEntryMeta{typ: "message", role: "user"})

	switch s.scenario.PromptBehavior {
	case fakeBehaviorHang:
		block := make(chan struct{})
		<-block
	case fakeBehaviorDie:
		s.out.writeJSON(fakeTypeOnlyEvent{Type: "turn_start"})
		s.out.writeJSON(fakeMessageEvent{Type: "message_start", Message: s.assistantJSON(nil, "", "")})
		s.emitTextDelta("par", s.assistantJSON([]string{"par"}, "", ""))
		s.out.flushAndExit(1)
	case fakeBehaviorProviderError:
		s.runProviderErrorTurn()

		finished = true
	default:
		finished = s.runReplyTurn(turn)
	}
}

func (s *fakePiServer) runProviderErrorTurn() {
	errorMessage := s.scenario.ProviderError
	if errorMessage == "" {
		errorMessage = "500 {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Internal server error\"}}"
	}

	s.out.writeJSON(fakeTypeOnlyEvent{Type: "turn_start"})
	s.out.writeJSON(fakeMessageEvent{Type: "message_start", Message: s.assistantJSON(nil, "", "")})

	failed := s.assistantJSON(nil, "error", errorMessage)
	s.appendMessageEntry(failed, fakeEntryMeta{typ: "message", role: "assistant", usage: fakeTurnUsage()})
	s.out.writeJSON(fakeMessageEvent{Type: "message_end", Message: failed})
	s.out.writeJSON(fakeTurnEndEvent{Type: "turn_end", Message: failed, ToolResults: []json.RawMessage{}})
	s.out.writeJSON(fakeAgentEndEvent{Type: "agent_end", Messages: []json.RawMessage{failed}, WillRetry: false})
	s.persist()
}

func (s *fakePiServer) runReplyTurn(turn *fakeTurn) bool {
	var toolMessages []json.RawMessage

	if s.scenario.ToolName != "" {
		toolMessages = s.runToolTurn(turn)
	}

	// A plain (non-permission) extension dialog blocks the turn until the
	// client answers; the answer becomes the reply so tests can pin the
	// round trip.
	var elicitReply string
	if s.scenario.ElicitMethod != "" {
		answer := s.raiseDialog(turn, s.scenario.ElicitMethod, s.scenario.ElicitTitle, nil)

		elicitReply = fakeElicitCancelledReply
		if !answer.cancelled {
			elicitReply = answer.value
		}
	}

	s.out.writeJSON(fakeTypeOnlyEvent{Type: "turn_start"})
	s.out.writeJSON(fakeMessageEvent{Type: "message_start", Message: s.assistantJSON(nil, "", "")})

	deltas := s.scenario.DeltaTexts
	if len(deltas) == 0 {
		reply := s.scenario.ReplyText
		if reply == "" {
			reply = fakeDefaultReply
		}
		deltas = []string{reply}
	}

	if elicitReply != "" {
		deltas = []string{elicitReply}
	}

	s.emitStartDelta()

	streamed := make([]string, 0, len(deltas))
	for i, delta := range deltas {
		if s.scenario.StreamDelayMs > 0 {
			time.Sleep(time.Duration(s.scenario.StreamDelayMs) * time.Millisecond)
		}

		if turn.aborted() {
			s.finishAborted(streamed, toolMessages)

			return true
		}

		streamed = append(streamed, delta)
		s.emitTextDelta(delta, s.assistantJSON(streamed, "", ""))

		if s.scenario.PromptBehavior == fakeBehaviorGarbageBurst && i == 0 {
			for range max(s.scenario.GarbageLines, 3) {
				s.out.writeRaw("{{{ this is not a json record")
			}
		}
	}

	if turn.aborted() {
		s.finishAborted(streamed, toolMessages)

		return true
	}

	full := strings.Join(streamed, "")
	s.out.writeJSON(fakeMessageUpdateEvent{
		Type:    "message_update",
		Message: s.assistantJSON(streamed, "", ""),
		AssistantMessageEvent: fakeDelta{
			Type:         "text_end",
			ContentIndex: fakeIntPtr(0),
			Content:      full,
		},
	})
	s.out.writeJSON(fakeMessageUpdateEvent{
		Type:                  "message_update",
		Message:               s.assistantJSON(streamed, "", ""),
		AssistantMessageEvent: fakeDelta{Type: "done", Reason: "stop"},
	})

	final := s.assistantJSON(streamed, "stop", "")
	s.appendMessageEntry(final, fakeEntryMeta{typ: "message", role: "assistant", usage: fakeTurnUsage()})
	s.out.writeJSON(fakeMessageEvent{Type: "message_end", Message: final})
	s.out.writeJSON(fakeTurnEndEvent{Type: "turn_end", Message: final, ToolResults: []json.RawMessage{}})

	messages := append(toolMessages, final)
	s.out.writeJSON(fakeAgentEndEvent{Type: "agent_end", Messages: messages, WillRetry: false})
	s.persist()

	return true
}

func (s *fakePiServer) finishAborted(streamed []string, toolMessages []json.RawMessage) {
	aborted := s.assistantJSON(streamed, "aborted", fakeAbortedErrorMessage)

	// Real pi retains the aborted partial assistant message in the session.
	s.appendMessageEntry(aborted, fakeEntryMeta{typ: "message", role: "assistant", usage: fakeTurnUsage()})
	s.out.writeJSON(fakeMessageEvent{Type: "message_end", Message: aborted})
	s.out.writeJSON(fakeTurnEndEvent{Type: "turn_end", Message: aborted, ToolResults: []json.RawMessage{}})
	s.out.writeJSON(fakeAgentEndEvent{
		Type:      "agent_end",
		Messages:  append(toolMessages, aborted),
		WillRetry: false,
	})
	s.persist()
}

// runToolTurn emits one assistant tool-call turn gated by the bridge
// permission dialog, returning the messages it produced.
func (s *fakePiServer) runToolTurn(turn *fakeTurn) []json.RawMessage {
	const toolCallID = "call_1"

	args := s.scenario.ToolArgs
	if args == nil {
		args = map[string]any{}
	}
	argsJSON := mustJSON(args)

	toolCall := mustJSON(fakeToolCallBlock{
		Type:      "toolCall",
		ID:        toolCallID,
		Name:      s.scenario.ToolName,
		Arguments: args,
	})

	s.out.writeJSON(fakeTypeOnlyEvent{Type: "turn_start"})
	s.out.writeJSON(fakeMessageEvent{Type: "message_start", Message: s.assistantJSON(nil, "", "")})

	callMessage := s.assistantToolCallJSON(toolCall)
	s.out.writeJSON(fakeMessageUpdateEvent{
		Type:    "message_update",
		Message: callMessage,
		AssistantMessageEvent: fakeDelta{
			Type:         "toolcall_end",
			ContentIndex: fakeIntPtr(0),
			ToolCall:     toolCall,
		},
	})
	s.appendMessageEntry(callMessage, fakeEntryMeta{
		typ:       "message",
		role:      "assistant",
		toolCalls: 1,
		usage:     fakeTurnUsage(),
	})
	s.out.writeJSON(fakeMessageEvent{Type: "message_end", Message: callMessage})

	allowed := true
	if len(s.extensionPaths) > 0 && s.permissionAsk {
		allowed = s.askPermission(turn, argsJSON)
	}

	s.out.writeJSON(fakeToolExecutionEvent{
		Type:       "tool_execution_start",
		ToolCallID: toolCallID,
		ToolName:   s.scenario.ToolName,
		Args:       argsJSON,
	})

	output := s.scenario.ToolOutput
	if output == "" {
		output = "fake tool output"
	}

	isError := !allowed
	if isError {
		output = "Denied by ACP client"
	} else {
		s.out.writeJSON(fakeToolExecutionEvent{
			Type:          "tool_execution_update",
			ToolCallID:    toolCallID,
			ToolName:      s.scenario.ToolName,
			Args:          argsJSON,
			PartialResult: fakeToolResultPayload(output),
		})
	}

	s.out.writeJSON(fakeToolExecutionEvent{
		Type:       "tool_execution_end",
		ToolCallID: toolCallID,
		ToolName:   s.scenario.ToolName,
		Result:     fakeToolResultPayload(output),
		IsError:    &isError,
	})

	toolResult := mustJSON(fakeToolResultMessage{
		Role:       "toolResult",
		ToolCallID: toolCallID,
		ToolName:   s.scenario.ToolName,
		Content:    []any{fakeTextBlock{Type: "text", Text: output}},
		IsError:    isError,
		Timestamp:  time.Now().UnixMilli(),
	})
	s.appendMessageEntry(toolResult, fakeEntryMeta{typ: "message", role: "toolResult", isError: isError})
	s.out.writeJSON(fakeTurnEndEvent{
		Type:        "turn_end",
		Message:     callMessage,
		ToolResults: []json.RawMessage{toolResult},
	})

	return []json.RawMessage{callMessage, toolResult}
}

// raiseDialog emits one extension_ui_request dialog and blocks until its
// extension_ui_response arrives; an aborted turn resolves it cancelled.
func (s *fakePiServer) raiseDialog(turn *fakeTurn, method string, title string, options []string) fakeUIAnswer {
	s.mu.Lock()
	s.uiSeq++
	id := fmt.Sprintf("ui-%d", s.uiSeq)
	waiter := make(chan fakeUIAnswer, 1)
	s.ui[id] = waiter
	s.mu.Unlock()

	s.out.writeJSON(fakeUIRequestEvent{
		Type:    "extension_ui_request",
		ID:      id,
		Method:  method,
		Title:   title,
		Options: options,
	})

	select {
	case answer := <-waiter:
		return answer
	case <-turn.abort:
		return fakeUIAnswer{cancelled: true}
	}
}

// askPermission raises the bridge extension's tool-approval select dialog:
// the payload travels JSON-encoded in the dialog title behind the marker
// prefix, and any non-allow answer (deny or a cancelled dialog) fails
// closed.
func (s *fakePiServer) askPermission(turn *fakeTurn, input json.RawMessage) bool {
	title := pi.PermissionTitleMarker + string(mustJSON(pi.PermissionPrompt{
		ToolName: s.scenario.ToolName,
		Input:    input,
	}))

	answer := s.raiseDialog(turn, "select", title,
		[]string{pi.PermissionOptionAllow, pi.PermissionOptionDeny})

	return !answer.cancelled && answer.value == pi.PermissionOptionAllow
}

func (s *fakePiServer) emitStartDelta() {
	s.out.writeJSON(fakeMessageUpdateEvent{
		Type:    "message_update",
		Message: s.assistantJSON(nil, "", ""),
		AssistantMessageEvent: fakeDelta{
			Type:         "text_start",
			ContentIndex: fakeIntPtr(0),
		},
	})
}

func (s *fakePiServer) emitTextDelta(delta string, partial json.RawMessage) {
	s.out.writeJSON(fakeMessageUpdateEvent{
		Type:    "message_update",
		Message: partial,
		AssistantMessageEvent: fakeDelta{
			Type:         "text_delta",
			ContentIndex: fakeIntPtr(0),
			Delta:        delta,
		},
	})
}

func (s *fakePiServer) assistantJSON(textParts []string, stopReason string, errorMessage string) json.RawMessage {
	content := make([]any, 0, 1)
	if len(textParts) > 0 {
		content = append(content, fakeTextBlock{Type: "text", Text: strings.Join(textParts, "")})
	}

	return s.assistantMessageJSON(content, stopReason, errorMessage)
}

func (s *fakePiServer) assistantToolCallJSON(toolCall json.RawMessage) json.RawMessage {
	return s.assistantMessageJSON([]any{toolCall}, "toolUse", "")
}

func (s *fakePiServer) assistantMessageJSON(content []any, stopReason string, errorMessage string) json.RawMessage {
	provider, modelID := "unknown", "unknown"

	s.mu.Lock()
	if s.model != nil {
		provider, modelID = s.model.Provider, s.model.ID
	}
	s.mu.Unlock()

	return mustJSON(fakeAssistantMessage{
		ACPMessageID: fakeUUID(),
		Role:         "assistant",
		Content:      content,
		API:          "fake-api",
		Provider:     provider,
		Model:        modelID,
		Usage:        *fakeTurnUsage(),
		StopReason:   stopReason,
		ErrorMessage: errorMessage,
		Timestamp:    time.Now().UnixMilli(),
	})
}

func fakeTurnUsage() *fakeUsage {
	return &fakeUsage{
		Input:       100,
		Output:      25,
		CacheRead:   0,
		CacheWrite:  0,
		TotalTokens: 125,
		Cost: fakeUsageCost{
			Input:  0.001,
			Output: 0.002,
			Total:  0.003,
		},
	}
}

func (s *fakePiServer) appendMessageEntry(message json.RawMessage, meta fakeEntryMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	row := fakeEntryRow{
		Type:      "message",
		Timestamp: fakeEntryTimestamp(),
		Message:   message,
	}
	s.appendEntryLocked(row, meta)
}

func (s *fakePiServer) appendEntryLocked(row fakeEntryRow, meta fakeEntryMeta) {
	s.entrySeq++
	row.ID = fmt.Sprintf("%08x", s.entrySeq)

	if len(s.session.entries) > 0 {
		row.ParentID = s.session.entries[len(s.session.entries)-1].id
	}

	meta.id = row.ID
	meta.row = mustJSON(row)
	s.session.entries = append(s.session.entries, meta)
}

// persist writes the full session JSONL (header first, then every entry
// row); the write completes before the caller emits agent_settled, matching
// real pi's file-durable-at-settle guarantee. The file stays absent until
// the first message entry exists, matching real pi's lazy creation.
func (s *fakePiServer) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.persistLocked()
}

func (s *fakePiServer) persistIfNeededLocked() {
	if s.session.persisted {
		s.persistLocked()
	}
}

func (s *fakePiServer) persistLocked() {
	var builder strings.Builder

	builder.Write(mustJSON(fakeSessionHeader{
		Type:          "session",
		Version:       3,
		ID:            s.session.id,
		Timestamp:     fakeEntryTimestamp(),
		Cwd:           s.cwd,
		ParentSession: s.session.parentSession,
	}))
	builder.WriteByte('\n')

	for _, entry := range s.session.entries {
		builder.Write(entry.row)
		builder.WriteByte('\n')
	}

	if err := os.MkdirAll(filepath.Dir(s.session.file), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "fake pi: create session dir: %v\n", err)

		return
	}

	if err := os.WriteFile(s.session.file, []byte(builder.String()), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "fake pi: write session file: %v\n", err)

		return
	}

	s.session.persisted = true
}

func fakeEntryTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000") + "Z"
}

func fakeUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		fmt.Fprintf(os.Stderr, "fake pi: read random bytes: %v\n", err)
		os.Exit(1)
	}

	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func fakeIntPtr(v int) *int {
	return &v
}

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fake pi: encode %T: %v\n", v, err)
		os.Exit(1)
	}

	return data
}

type fakeLineWriter struct {
	mu     sync.Mutex
	writer *bufio.Writer
}

func (w *fakeLineWriter) writeJSON(v any) {
	w.writeRaw(string(mustJSON(v)))
}

func (w *fakeLineWriter) writeRaw(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.writer.WriteString(line + "\n"); err != nil {
		os.Exit(1)
	}
	if err := w.writer.Flush(); err != nil {
		os.Exit(1)
	}
}

func (w *fakeLineWriter) flushAndExit(code int) {
	w.mu.Lock()
	_ = w.writer.Flush()
	os.Exit(code)
}
