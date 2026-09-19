package piacp

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// The test binary doubles as a fake pi: TestMain runs fakePi when this
// variable is set in the environment the adapter launched it with.
const (
	fakePiEnv              = "ACP_GO_PI_TEST_FAKE"
	fakePiEnvDump          = "ACP_GO_PI_TEST_ENV_DUMP"
	fakePiEnvNoModel       = "ACP_GO_PI_TEST_NO_MODEL"
	fakePiEnvUsageHold     = "ACP_GO_PI_TEST_USAGE_HOLD"
	fakePiEnvStartupDialog = "ACP_GO_PI_TEST_STARTUP_DIALOG"
	fakePiEnvStartupDeath  = "ACP_GO_PI_TEST_STARTUP_DEATH"
	// fakePiEnvResumeHold names a file a resumed fake pi creates before it
	// stops answering, so a test can act while the adapter is still relaunching.
	fakePiEnvResumeHold = "ACP_GO_PI_TEST_RESUME_HOLD"

	// fakePiResumeHold is how long a held resume refuses to serve. It outlasts
	// the shutdown the adapter sends when it gives up on the relaunch.
	fakePiResumeHold = 30 * time.Second

	// tinyPNG is a valid 1x1 PNG.
	tinyPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
)

var fakeModels = []pi.Model{
	{ID: "vision", Name: "Fake Vision", Provider: "fake", Input: []string{"text", "image"}, ContextWindow: 1000, MaxTokens: 100},
	{ID: "text-only", Name: "Fake Text", Provider: "fake", Input: []string{"text"}, ContextWindow: 500, MaxTokens: 50},
}

type fakePi struct {
	agentDir      string
	cwd           string
	sessionID     string
	sessionFile   string
	bridgePath    string
	model         *pi.Model
	thinkingLevel string
	autoRetry     bool
	statsID       string
	entries       int

	writeMu sync.Mutex
	out     *bufio.Writer

	dialogMu sync.Mutex
	dialogs  map[string]chan pi.UIResponse

	turnMu   sync.Mutex
	abort    chan struct{}
	turnDone chan struct{}
}

func runFakePi(args []string) int {
	if os.Getenv(fakePiEnvStartupDialog) != "" {
		request := map[string]any{"type": "extension_ui_request", "id": "startup", "method": "input", "title": "Startup input"}
		if err := json.NewEncoder(os.Stdout).Encode(request); err != nil {
			return 1
		}
		var response pi.UIResponse
		if err := json.NewDecoder(os.Stdin).Decode(&response); err != nil || !response.Cancelled {
			return 1
		}
	}

	if reason := os.Getenv(fakePiEnvStartupDeath); reason != "" {
		fmt.Fprintln(os.Stderr, reason)

		return 1
	}

	if marker := os.Getenv(fakePiEnvUsageHold); marker != "" {
		if err := os.WriteFile(marker, nil, 0o600); err != nil {
			return 1
		}
		time.Sleep(fakePiResumeHold)

		return 1
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 1
	}
	usageServer := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+os.Getenv(pi.EnvUsageToken) || r.URL.Path != "/access" {
			w.WriteHeader(http.StatusForbidden)

			return
		}
		if path := os.Getenv("ACP_GO_PI_TEST_USAGE_ACCESS"); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)

				return
			}
			_, _ = w.Write(data)

			return
		}
		_, _ = io.WriteString(w, `{"configured":false,"custom":false,"routes":[]}`)
	})}
	go func() { _ = usageServer.Serve(listener) }()
	defer func() { _ = usageServer.Close() }()
	endpointFile := os.Getenv(pi.EnvUsageFile)
	if err := os.WriteFile(endpointFile+".tmp", []byte("http://"+listener.Addr().String()), 0o600); err != nil {
		return 1
	}
	if err := os.Rename(endpointFile+".tmp", endpointFile); err != nil {
		return 1
	}
	if dump := os.Getenv(fakePiEnvDump); dump != "" {
		_ = os.WriteFile(dump, []byte(strings.Join(os.Environ(), "\n")+"\n"), 0o600)
	}

	cwd, _ := os.Getwd()

	f := &fakePi{
		agentDir:      os.Getenv(pi.EnvAgentDir),
		cwd:           cwd,
		thinkingLevel: "medium",
		out:           bufio.NewWriter(os.Stdout),
		dialogs:       make(map[string]chan pi.UIResponse),
	}

	if f.agentDir == "" {
		f.agentDir = filepath.Join(os.Getenv("HOME"), ".pi", "agent")
	}

	if os.Getenv(fakePiEnvNoModel) == "" {
		model := fakeModels[0]
		f.model = &model
	}

	for index := range args {
		switch args[index] {
		case "-e":
			if index+1 < len(args) && strings.HasSuffix(args[index+1], pi.BridgeExtensionFileName) {
				f.bridgePath = args[index+1]
			}
		case "--session":
			if index+1 < len(args) {
				f.sessionFile = args[index+1]
			}
		}
	}

	resumed := f.sessionFile != ""

	if !resumed {
		f.sessionID = fakeUUID()
		f.sessionFile = pi.SessionFile(f.agentDir, cwd, f.sessionID, time.Now())
		_ = os.MkdirAll(filepath.Dir(f.sessionFile), 0o700)
		// pi's header row carries members the adapter does not model; the fake
		// writes the whole row so restore sees what pi writes.
		header, _ := json.Marshal(map[string]any{
			"type": pi.HeaderRowType, "version": 3, "id": f.sessionID,
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "cwd": cwd,
		})
		_ = os.WriteFile(f.sessionFile, append(header, '\n'), 0o600)
		f.entries = 1
	} else {
		rows, err := pi.ReadRows(f.sessionFile)
		if err != nil || len(rows) == 0 {
			fmt.Fprintln(os.Stderr, "fake pi: session file unreadable")

			return 1
		}

		header, ok := pi.ParseHeader(rows[0])
		if !ok {
			fmt.Fprintln(os.Stderr, "fake pi: session file has no header")

			return 1
		}

		f.sessionID = header.ID
		f.entries = len(rows)
	}

	f.statsID = f.sessionID

	if hold := os.Getenv(fakePiEnvResumeHold); hold != "" && resumed {
		_ = os.WriteFile(hold, []byte("held\n"), 0o600)
		time.Sleep(fakePiResumeHold)
	}

	return f.serve(os.Stdin)
}

func fakeUUID() string {
	raw := make([]byte, 16)
	_, _ = rand.Read(raw)

	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

func (f *fakePi) serve(input io.Reader) int {
	lines := pi.NewLineReader(input)

	for {
		line, err := lines.Next()
		if len(line) > 0 {
			f.dispatch(line)
		}

		if err != nil {
			return 0
		}
	}
}

func (f *fakePi) write(value any) {
	encoded, _ := json.Marshal(value)

	f.writeMu.Lock()
	defer f.writeMu.Unlock()

	_, _ = f.out.Write(encoded)
	_ = f.out.WriteByte('\n')
	_ = f.out.Flush()
}

func (f *fakePi) respond(id string, command string, data any) {
	response := map[string]any{"type": "response", "id": id, "command": command, "success": true}
	if data != nil {
		response["data"] = data
	}

	f.write(response)
}

func (f *fakePi) fail(id string, command string, message string) {
	f.write(map[string]any{"type": "response", "id": id, "command": command, "success": false, "error": message})
}

func (f *fakePi) event(fields map[string]any) {
	f.write(fields)
}

func (f *fakePi) dispatch(line []byte) {
	var command struct {
		Type      string `json:"type"`
		ID        string `json:"id"`
		Message   string `json:"message"`
		Images    []pi.ImageContent
		Provider  string          `json:"provider"`
		ModelID   string          `json:"modelId"`
		Level     string          `json:"level"`
		Enabled   bool            `json:"enabled"`
		Value     *string         `json:"value"`
		Confirmed *bool           `json:"confirmed"`
		Cancelled bool            `json:"cancelled"`
		Raw       json.RawMessage `json:"-"`
	}

	if err := json.Unmarshal(line, &command); err != nil {
		f.fail("", "parse", "bad command")

		return
	}

	switch command.Type {
	case "extension_ui_response":
		f.dialogMu.Lock()
		waiter := f.dialogs[command.ID]
		delete(f.dialogs, command.ID)
		f.dialogMu.Unlock()

		if waiter != nil {
			waiter <- pi.UIResponse{ID: command.ID, Value: command.Value, Confirmed: command.Confirmed, Cancelled: command.Cancelled}
		}
	case "get_state":
		f.respond(command.ID, command.Type, pi.SessionState{
			Model: f.model, ThinkingLevel: f.thinkingLevel, SessionFile: f.sessionFile, SessionID: f.sessionID,
		})
	case "get_available_models":
		f.respond(command.ID, command.Type, map[string]any{"models": fakeModels})
	case "set_model":
		for _, model := range fakeModels {
			if model.Provider == command.Provider && model.ID == command.ModelID {
				chosen := model
				f.model = &chosen
				f.thinkingLevel = "medium"
				if chosen.ID == "text-only" {
					f.thinkingLevel = "off"
				}
				f.respond(command.ID, command.Type, chosen)

				return
			}
		}

		f.fail(command.ID, command.Type, "Model not found: "+command.Provider+"/"+command.ModelID)
	case "set_thinking_level":
		if slices.Contains(pi.ThinkingLevels(), command.Level) {
			f.thinkingLevel = command.Level
			if f.model != nil && f.model.ID == "text-only" {
				f.thinkingLevel = "off"
			}
		}

		f.respond(command.ID, command.Type, nil)
	case "set_auto_retry":
		f.autoRetry = command.Enabled
		f.respond(command.ID, command.Type, nil)
	case "get_session_stats":
		tokens := int64(42)
		f.respond(command.ID, command.Type, pi.SessionStats{
			SessionID:    f.statsID,
			ContextUsage: &pi.ContextUsage{Tokens: &tokens, ContextWindow: 1000},
		})

	case "get_commands":
		f.respond(command.ID, command.Type, map[string]any{"commands": []pi.SlashCommand{
			{Name: "help", Description: "Show help"},
			{Name: "bad name"},
		}})
	case "prompt":
		if strings.HasPrefix(command.Message, "REJECT") {
			f.fail(command.ID, command.Type, "no provider key")

			return
		}

		f.turnMu.Lock()
		f.abort = make(chan struct{})
		f.turnDone = make(chan struct{})
		abort, done := f.abort, f.turnDone
		f.turnMu.Unlock()

		f.respond(command.ID, command.Type, nil)

		go f.runTurn(command.Message, len(command.Images), abort, done)
	case "abort":
		f.turnMu.Lock()
		abort, done := f.abort, f.turnDone
		f.turnMu.Unlock()

		if abort != nil {
			select {
			case <-abort:
			default:
				close(abort)
			}

			<-done
		}

		f.respond(command.ID, command.Type, nil)
	default:
		f.fail(command.ID, command.Type, "unknown command")
	}
}

func (f *fakePi) dialog(request map[string]any) pi.UIResponse {
	id := fakeUUID()
	waiter := make(chan pi.UIResponse, 1)

	f.dialogMu.Lock()
	f.dialogs[id] = waiter
	f.dialogMu.Unlock()

	request["type"] = "extension_ui_request"
	request["id"] = id
	f.write(request)

	select {
	case response := <-waiter:
		return response
	case <-time.After(30 * time.Second):
		return pi.UIResponse{ID: id, Cancelled: true}
	}
}

func (f *fakePi) appendRow(message map[string]any) {
	f.entries++
	row := map[string]any{
		"type": "message", "id": fmt.Sprintf("e%d", f.entries), "parentId": fmt.Sprintf("e%d", f.entries-1),
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": message,
	}
	encoded, _ := json.Marshal(row)

	file, err := os.OpenFile(f.sessionFile, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}

	_, _ = file.Write(append(encoded, '\n'))
	_ = file.Close()
}

func (f *fakePi) assistantMessage(content []map[string]any, stopReason string, errorMessage string) map[string]any {
	message := map[string]any{
		"role": "assistant", "content": content, "api": "fake", "provider": "fake", "model": "vision",
		"usage": map[string]any{"input": 10, "output": 5, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 15,
			"cost": map[string]any{"input": 0.001, "output": 0.009, "cacheRead": 0, "cacheWrite": 0, "total": 0.01}},
		"stopReason": stopReason, "timestamp": time.Now().UnixMilli(), "acpMessageId": fakeUUID(),
	}
	if errorMessage != "" {
		message["errorMessage"] = errorMessage
	}

	return message
}

func textBlock(text string) map[string]any { return map[string]any{"type": "text", "text": text} }

// runTurn drives one scripted run: the keyword at the start of the message
// selects the script.
func (f *fakePi) runTurn(message string, imageCount int, abort <-chan struct{}, done chan struct{}) {
	defer close(done)

	f.appendRow(map[string]any{"role": "user", "content": []map[string]any{textBlock(message)}, "timestamp": time.Now().UnixMilli()})
	f.run(message, imageCount, abort)

	if strings.HasPrefix(message, "AGENTWORK") {
		f.run("HELLO background", 0, make(chan struct{}))
	}

	if strings.HasPrefix(message, "AGENTHANG") {
		f.run("SLOW background", 0, abort)
	}
}

func (f *fakePi) run(message string, imageCount int, abort <-chan struct{}) {
	f.event(map[string]any{"type": "agent_start"})

	if strings.HasPrefix(message, "DIE") {
		fmt.Fprintln(os.Stderr, "fatal: dead")
		os.Exit(3)
	}

	f.event(map[string]any{"type": "turn_start"})
	f.event(map[string]any{"type": "message_start", "message": map[string]any{"role": "assistant", "content": []any{}}})

	content := []map[string]any{}
	stopReason := "stop"
	errorMessage := ""

	delta := func(kind string, text string) {
		f.event(map[string]any{"type": "message_update", "assistantMessageEvent": map[string]any{"type": kind, "contentIndex": 0, "delta": text}})
	}

	switch {
	case strings.HasPrefix(message, "SUFFIX"):
		delta("text_delta", "Hel")
		content = append(content, textBlock("Hello"))
	case strings.HasPrefix(message, "THINK"):
		delta("thinking_delta", "hmm")
		delta("text_delta", "ok")
		content = append(content, map[string]any{"type": "thinking", "thinking": "hmm"}, textBlock("ok"))
	case strings.HasPrefix(message, "TOOLIMAGE"):
		f.toolCall("call-1", "read", map[string]any{"path": "x.png"}, []map[string]any{{"type": "image", "data": tinyPNG, "mimeType": "image/png"}})
		content = append(content, textBlock("done"))
	case strings.HasPrefix(message, "TOOL"):
		f.toolCall("call-1", "bash", map[string]any{"command": "ls"}, []map[string]any{textBlock("out\n")})
		content = append(content, textBlock("done"))
	case strings.HasPrefix(message, "ASK"):
		answer := f.dialog(map[string]any{"method": "input", "title": "Your name?", "placeholder": "type"})
		if answer.Cancelled || answer.Value == nil {
			content = append(content, textBlock("declined"))
		} else {
			content = append(content, textBlock("hi "+*answer.Value))
		}
	case strings.HasPrefix(message, "IMAGE"):
		content = append(content, textBlock("here"), map[string]any{"type": "image", "data": tinyPNG, "mimeType": "image/png"})
	case strings.HasPrefix(message, "ERROR"):
		stopReason = "error"
		errorMessage = "boom"
	case strings.HasPrefix(message, "SLOW"):
		select {
		case <-abort:
			stopReason = "aborted"
		case <-time.After(30 * time.Second):
		}
	case strings.HasPrefix(message, "WAIT"):
		select {
		case <-abort:
			stopReason = "aborted"
		case <-time.After(2 * time.Second):
			content = append(content, textBlock("waited"))
		}
	case strings.HasPrefix(message, "EXTERR"):
		f.event(map[string]any{"type": "extension_error", "extensionPath": f.bridgePath, "event": "tool_call", "error": "boom"})

		select {
		case <-abort:
			stopReason = "aborted"
		case <-time.After(5 * time.Second):
		}
	case strings.HasPrefix(message, "BADSESSION"):
		f.statsID = "00000000-0000-4000-8000-000000000000"
		content = append(content, textBlock("drifted"))
	case strings.HasPrefix(message, "ECHO"):
		content = append(content, textBlock(message))
	default:
		delta("text_delta", "Hello")
		delta("text_delta", " world")
		content = append(content, textBlock("Hello world"))
	}

	if imageCount > 0 {
		content = append(content, textBlock(fmt.Sprintf("images:%d", imageCount)))
	}

	assistant := f.assistantMessage(content, stopReason, errorMessage)
	f.event(map[string]any{"type": "message_end", "message": assistant})
	f.appendRow(assistant)
	f.event(map[string]any{"type": "turn_end", "message": assistant, "toolResults": []any{}})
	f.event(map[string]any{"type": "agent_end", "messages": []any{assistant}, "willRetry": false})
	f.event(map[string]any{"type": "agent_settled"})

	if strings.HasPrefix(message, "QUIT") {
		// pi exits cleanly once its run has settled. Its stdout closes first so
		// nothing can be answered by a process on its way out.
		f.writeMu.Lock()
		_ = f.out.Flush()
		_ = os.Stdout.Close()
		os.Exit(0)
	}

	if strings.HasPrefix(message, "NOISE") {
		fmt.Fprintln(os.Stderr, "fatal: chatter on stderr")
		f.writeMu.Lock()
		_, _ = f.out.WriteString("not a json record at all\n")
		_ = f.out.Flush()
		f.writeMu.Unlock()
	}
}

func (f *fakePi) toolCall(id string, name string, args map[string]any, result []map[string]any) {
	allowed := true

	if os.Getenv(pi.EnvPermissionMode) != pi.PermissionModeAllow {
		payload, _ := json.Marshal(pi.PermissionPrompt{ToolCallID: id, ToolName: name, Input: mustJSON(args)})
		answer := f.dialog(map[string]any{"method": "select", "title": pi.PermissionTitleMarker + string(payload), "options": []string{"allow", "deny"}})
		allowed = !answer.Cancelled && answer.Value != nil && *answer.Value == pi.PermissionOptionAllow
	}

	f.event(map[string]any{"type": "tool_execution_start", "toolCallId": id, "toolName": name, "args": args})

	if !allowed {
		result = []map[string]any{textBlock("Denied by ACP client")}
	} else if len(result) > 0 && result[0]["type"] == "text" {
		f.event(map[string]any{"type": "tool_execution_update", "toolCallId": id, "toolName": name, "args": args,
			"partialResult": map[string]any{"content": []map[string]any{textBlock("out")}}})
	}

	f.event(map[string]any{"type": "tool_execution_end", "toolCallId": id, "toolName": name, "isError": !allowed,
		"result": map[string]any{"content": result}})
	f.appendRow(map[string]any{"role": "toolResult", "toolCallId": id, "toolName": name, "content": result, "isError": !allowed, "timestamp": time.Now().UnixMilli()})
}

func mustJSON(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)

	return encoded
}
