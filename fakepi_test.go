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
	fakePiEnvUsageGateways = "ACP_GO_PI_TEST_USAGE_GATEWAYS"
	// fakePiEnvZeroEstimate makes get_session_stats estimate a context of
	// zero tokens.
	fakePiEnvZeroEstimate = "ACP_GO_PI_TEST_ZERO_ESTIMATE"
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
	thinkingLevel string
	autoRetry     bool
	entries       int

	// statsMu guards the model and the statistics a run changes while
	// commands read them. context is the last usable response's context,
	// trailing the estimate of the user messages after it, and cost the
	// session's summed cost; compacted holds from a compaction until a usable
	// response follows it.
	statsMu   sync.Mutex
	model     *pi.Model
	statsID   string
	context   int64
	trailing  int64
	cost      float64
	compacted bool
	// statsRead signals each answered get_session_stats.
	statsRead chan struct{}

	writeMu sync.Mutex
	out     *bufio.Writer

	dialogMu sync.Mutex
	dialogs  map[string]chan pi.UIResponse

	turnMu   sync.Mutex
	abort    chan struct{}
	turnDone chan struct{}

	// response is the id the gateway returned for the open assistant
	// response, empty when it exposes none, and relayed whether the bridge
	// extension relayed it yet. Only the run goroutine touches them.
	response string
	relayed  bool
}

// fakeUsageAccess answers the adapter's account-access and gateway queries
// from the files the test names, or with nothing configured.
func fakeUsageAccess(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+os.Getenv(pi.EnvUsageToken) || (r.URL.Path != "/access" && r.URL.Path != "/gateways") {
		w.WriteHeader(http.StatusForbidden)

		return
	}
	if r.URL.Path == "/gateways" {
		if path := os.Getenv(fakePiEnvUsageGateways); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)

				return
			}
			_, _ = w.Write(data)

			return
		}
		_, _ = io.WriteString(w, `{"routes":[]}`)

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
	usageServer := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(fakeUsageAccess)}
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
		statsRead:     make(chan struct{}, 1),
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
		f.context = lastUsableContext(rows)
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
		f.statsMu.Lock()
		model := f.model
		f.statsMu.Unlock()

		f.respond(command.ID, command.Type, pi.SessionState{
			Model: model, ThinkingLevel: f.thinkingLevel, SessionFile: f.sessionFile, SessionID: f.sessionID,
		})
	case "get_available_models":
		f.respond(command.ID, command.Type, map[string]any{"models": fakeModels})
	case "set_model":
		for _, model := range fakeModels {
			if model.Provider == command.Provider && model.ID == command.ModelID {
				chosen := model
				f.statsMu.Lock()
				f.model = &chosen
				f.statsMu.Unlock()
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
			f.statsMu.Lock()
			if f.model != nil && f.model.ID == "text-only" {
				f.thinkingLevel = "off"
			}
			f.statsMu.Unlock()
		}

		f.respond(command.ID, command.Type, nil)
	case "set_auto_retry":
		f.autoRetry = command.Enabled
		f.respond(command.ID, command.Type, nil)
	case "get_session_stats":
		f.respond(command.ID, command.Type, f.sessionStats())

		select {
		case f.statsRead <- struct{}{}:
		default:
		}

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

// sessionStats answers as pi does: the summed cost of every response, and a
// context estimate only for a model with a window, unknown after a compaction
// until a usable response follows it.
func (f *fakePi) sessionStats() pi.SessionStats {
	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	stats := pi.SessionStats{SessionID: f.statsID, Cost: f.cost}
	if f.model == nil || f.model.ContextWindow <= 0 {
		return stats
	}

	stats.ContextUsage = &pi.ContextUsage{ContextWindow: f.model.ContextWindow}

	switch {
	case f.compacted:
	case os.Getenv(fakePiEnvZeroEstimate) != "":
		stats.ContextUsage.Tokens = new(int64(0))
	default:
		stats.ContextUsage.Tokens = new(f.context + f.trailing)
	}

	return stats
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

func (f *fakePi) assistantMessage(content []map[string]any, stopReason string, errorMessage string, usage map[string]any) map[string]any {
	message := map[string]any{
		"role": "assistant", "content": content, "api": "openai-completions", "provider": "fake", "model": "vision",
		"usage": usage, "stopReason": stopReason, "timestamp": time.Now().UnixMilli(),
	}
	if f.response != "" {
		message["responseId"] = f.response
	}
	if errorMessage != "" {
		message["errorMessage"] = errorMessage
	}

	return message
}

// fakeResponseID is an id shaped as OpenRouter returns for a completion.
func fakeResponseID() string {
	raw := make([]byte, 10)
	_, _ = rand.Read(raw)

	return fmt.Sprintf("gen-%d-%x", time.Now().Unix(), raw)
}

// startAssistant opens a model call as pi's OpenAI completions path does:
// message_start precedes the gateway's first chunk, so it carries no
// responseId. gateway reports whether the gateway answers with an id.
func (f *fakePi) startAssistant(gateway bool) {
	f.response, f.relayed = "", false
	if gateway {
		f.response = fakeResponseID()
	}

	f.event(map[string]any{"type": "message_start", "message": map[string]any{
		"role": "assistant", "content": []any{}, "api": "openai-completions", "provider": "fake", "model": "vision",
		"usage": zeroUsage(), "stopReason": "pending", "timestamp": time.Now().UnixMilli(),
	}})
}

// update streams one message_update holding usage. The first update of a
// response whose id the gateway returned is preceded by the bridge
// extension's relay of that id.
func (f *fakePi) update(assistantEvent map[string]any, usage map[string]any) {
	if f.response != "" && !f.relayed && f.bridgePath != "" {
		f.relayed = true
		f.event(map[string]any{"type": "extension_ui_request", "id": fakeUUID(), "method": "setStatus",
			"statusKey": pi.ResponseStatusKey, "statusText": f.response})
	}

	f.event(map[string]any{"type": "message_update", "usage": usage, "assistantMessageEvent": assistantEvent})
}

// fakeUsage is one model call's native usage: the new input, the cached
// prefix the call resent, and the output.
func fakeUsage(input int, cacheRead int, output int) map[string]any {
	return map[string]any{"input": input, "output": output, "cacheRead": cacheRead, "cacheWrite": 0, "totalTokens": input + cacheRead + output,
		"cost": map[string]any{"input": 0.125, "output": 0.125, "cacheRead": 0, "cacheWrite": 0, "total": fakeCallCost}}
}

// zeroUsage is the usage of a model call a gateway answered from its response
// cache: every figure and the cost zero.
func zeroUsage() map[string]any {
	return map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
		"cost": map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
}

// fakeCallCost is every model call's cost, exact in binary so sums compare. A
// call reporting no tokens costs nothing.
const fakeCallCost = 0.25

// endAssistant finishes one model call: its message_end, its session row,
// and its usage in the session's statistics, counted as pi counts it.
func (f *fakePi) endAssistant(message map[string]any) {
	f.event(map[string]any{"type": "message_end", "message": message})
	f.appendRow(message)

	usage, _ := message["usage"].(map[string]any)
	tokens, _ := usage["totalTokens"].(int)
	stopReason, _ := message["stopReason"].(string)

	f.statsMu.Lock()
	defer f.statsMu.Unlock()

	if tokens > 0 {
		f.cost += fakeCallCost
	}

	if tokens > 0 && stopReason != "aborted" && stopReason != "error" {
		f.context, f.trailing, f.compacted = int64(tokens), 0, false
	}
}

// lastUsableContext is the context the last usable response in a session's
// rows left, from which pi estimates a resumed session's context.
func lastUsableContext(rows [][]byte) int64 {
	var context int64

	for _, raw := range rows {
		var row struct {
			Message pi.AgentMessage `json:"message"`
		}

		if json.Unmarshal(raw, &row) != nil || row.Message.Role != "assistant" || row.Message.Usage == nil {
			continue
		}

		if stop := row.Message.StopReason; stop != "aborted" && stop != "error" && row.Message.Usage.TotalTokens > 0 {
			context = row.Message.Usage.TotalTokens
		}
	}

	return context
}

// awaitStats waits for the adapter to read the statistics of the run that
// just settled, so later background work cannot change what it reads.
func (f *fakePi) awaitStats() {
	select {
	case <-f.statsRead:
	case <-time.After(5 * time.Second):
	}
}

func textBlock(text string) map[string]any { return map[string]any{"type": "text", "text": text} }

// aborted waits up to wait for the adapter to abort the run.
func aborted(abort <-chan struct{}, wait time.Duration) bool {
	select {
	case <-abort:
		return true
	case <-time.After(wait):
		return false
	}
}

// multiCall runs every model call of a multi-call script but the last and
// returns the last call's usage.
func (f *fakePi) multiCall(message string) map[string]any {
	if strings.HasPrefix(message, "EARLY") {
		// Two model calls from a provider that reports each call's input when
		// the response starts, before its first delta.
		f.partial(map[string]any{"input": 100, "output": 1, "cacheRead": 1000, "cacheWrite": 50, "totalTokens": 1151})
		f.step(map[string]any{"input": 100, "output": 30, "cacheRead": 1000, "cacheWrite": 50, "totalTokens": 1180})
		f.partial(map[string]any{"input": 60, "output": 1, "cacheRead": 1180, "cacheWrite": 0, "totalTokens": 1241})

		return map[string]any{"input": 60, "output": 20, "cacheRead": 1180, "cacheWrite": 0, "totalTokens": 1260}
	}

	// Three model calls, each resending the context the previous one left.
	for _, step := range []map[string]any{fakeUsage(100, 1000, 20), fakeUsage(50, 1120, 30)} {
		f.step(step)
	}

	return fakeUsage(40, 1200, 10)
}

// gatewayCalls runs every model call of a script whose usage arrives only at
// the end of the stream but the last, and returns the last call's usage. A
// call a gateway answered from its response cache reports every figure zero.
func (f *fakePi) gatewayCalls(message string) map[string]any {
	f.partial(zeroUsage())

	switch {
	case strings.HasPrefix(message, "LATEUSAGE"):
		return fakeUsage(100, 1000, 20)
	case strings.HasPrefix(message, "REPLAY"):
		// A real call, then a replayed one.
		f.step(fakeUsage(100, 1000, 20))
		f.partial(zeroUsage())
	}

	return zeroUsage()
}

// partial streams the first delta of a response whose streaming message
// already holds the given usage.
func (f *fakePi) partial(usage map[string]any) {
	f.update(map[string]any{"type": "text_start", "contentIndex": 0}, usage)
}

// step finishes one tool-using model call and opens the next.
func (f *fakePi) step(usage map[string]any) {
	assistant := f.assistantMessage([]map[string]any{textBlock("step")}, "toolUse", "", usage)
	f.endAssistant(assistant)
	f.event(map[string]any{"type": "turn_end", "message": assistant, "toolResults": []any{}})
	f.event(map[string]any{"type": "turn_start"})
	f.startAssistant(true)
}

// runTurn drives one scripted run: the keyword at the start of the message
// selects the script.
func (f *fakePi) runTurn(message string, imageCount int, abort <-chan struct{}, done chan struct{}) {
	defer close(done)

	// Only a read after this run settles releases its background work.
	select {
	case <-f.statsRead:
	default:
	}

	f.appendRow(map[string]any{"role": "user", "content": []map[string]any{textBlock(message)}, "timestamp": time.Now().UnixMilli()})

	// pi estimates a message after the last usable response at four
	// characters a token.
	f.statsMu.Lock()
	f.trailing += int64((len(message) + 3) / 4)
	f.statsMu.Unlock()

	f.run(message, imageCount, abort)

	if strings.HasPrefix(message, "AGENTWORK") {
		f.awaitStats()
		f.run("HELLO background", 0, make(chan struct{}))
	}

	if strings.HasPrefix(message, "AGENTHANG") {
		f.run("SLOW background", 0, abort)
	}
}

// gatewayAnswers reports whether a script's first call gets a response id: a
// call that failed before the gateway answered, and one through a provider
// whose stream exposes no response id, carry none.
func gatewayAnswers(message string) bool {
	return !strings.HasPrefix(message, "ERROR") && !strings.HasPrefix(message, "NOID")
}

func (f *fakePi) run(message string, imageCount int, abort <-chan struct{}) {
	f.event(map[string]any{"type": "agent_start"})

	if strings.HasPrefix(message, "DIE") {
		fmt.Fprintln(os.Stderr, "fatal: dead")
		os.Exit(3)
	}

	f.event(map[string]any{"type": "turn_start"})
	f.startAssistant(gatewayAnswers(message))

	content := []map[string]any{}
	stopReason := "stop"
	errorMessage := ""
	usage := fakeUsage(10, 0, 5)

	delta := func(kind string, text string) {
		f.update(map[string]any{"type": kind, "contentIndex": 0, "delta": text}, zeroUsage())
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
		if aborted(abort, 30*time.Second) {
			stopReason = "aborted"
		}
	case strings.HasPrefix(message, "WAIT"):
		if aborted(abort, 2*time.Second) {
			stopReason = "aborted"
		} else {
			content = append(content, textBlock("waited"))
		}
	case strings.HasPrefix(message, "EXTERR"):
		f.event(map[string]any{"type": "extension_error", "extensionPath": f.bridgePath, "event": "tool_call", "error": "boom"})

		if aborted(abort, 5*time.Second) {
			stopReason = "aborted"
		}
	case strings.HasPrefix(message, "BADSESSION"):
		f.statsMu.Lock()
		f.statsID = "00000000-0000-4000-8000-000000000000"
		f.statsMu.Unlock()

		content = append(content, textBlock("drifted"))
	case strings.HasPrefix(message, "SWITCH"):
		// An extension moves the session to another model mid-run.
		f.statsMu.Lock()
		model := fakeModels[1]
		f.model = &model
		f.statsMu.Unlock()

		content = append(content, textBlock("switched"))
	case strings.HasPrefix(message, "ECHO"):
		content = append(content, textBlock(message))
	case strings.HasPrefix(message, "MULTI"), strings.HasPrefix(message, "EARLY"):
		content = append(content, textBlock("done"))
		usage = f.multiCall(message)
	case strings.HasPrefix(message, "LATEUSAGE"), strings.HasPrefix(message, "REPLAY"), strings.HasPrefix(message, "CACHED"):
		content = append(content, textBlock("done"))
		usage = f.gatewayCalls(message)
	case strings.HasPrefix(message, "COMPACT"):
		content = append(content, textBlock("long"))
		usage = fakeUsage(100, 800, 50)
	case strings.HasPrefix(message, "STEPSLOW"):
		f.step(fakeUsage(100, 1000, 20))

		if aborted(abort, 30*time.Second) {
			stopReason = "aborted"
		}
	case strings.HasPrefix(message, "FLAKY"):
		// The provider fails one call and pi retries it automatically.
		failed := f.assistantMessage([]map[string]any{}, "error", "overloaded", fakeUsage(100, 1000, 0))
		f.endAssistant(failed)
		f.event(map[string]any{"type": "turn_end", "message": failed, "toolResults": []any{}})
		f.event(map[string]any{"type": "agent_end", "messages": []any{failed}, "willRetry": true})
		f.event(map[string]any{"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 0, "errorMessage": "overloaded"})
		f.event(map[string]any{"type": "agent_start"})
		f.event(map[string]any{"type": "turn_start"})
		f.startAssistant(true)

		content = append(content, textBlock("recovered"))
		usage = fakeUsage(100, 1000, 20)
	default:
		delta("text_delta", "Hello")
		delta("text_delta", " world")
		content = append(content, textBlock("Hello world"))
	}

	if imageCount > 0 {
		content = append(content, textBlock(fmt.Sprintf("images:%d", imageCount)))
	}

	assistant := f.assistantMessage(content, stopReason, errorMessage, usage)
	f.endAssistant(assistant)
	f.event(map[string]any{"type": "turn_end", "message": assistant, "toolResults": []any{}})
	f.event(map[string]any{"type": "agent_end", "messages": []any{assistant}, "willRetry": false})

	if strings.HasPrefix(message, "COMPACT") {
		// The response crossed the threshold, so pi compacts before settling.
		f.event(map[string]any{"type": "compaction_start", "reason": "threshold"})
		f.statsMu.Lock()
		f.compacted = true
		f.statsMu.Unlock()
		f.event(map[string]any{"type": "compaction_end", "reason": "threshold", "aborted": false, "willRetry": false,
			"result": map[string]any{"summary": "summary", "firstKeptEntryId": "e2", "tokensBefore": 950, "estimatedTokensAfter": 120}})
	}

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
