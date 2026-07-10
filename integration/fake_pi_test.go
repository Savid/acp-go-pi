//go:build integration

package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// The fake pi harness below speaks the pi RPC protocol as observed against
// the real binary (v0.80.6): strict-LF JSONL framing, silent startup, id
// echo on responses, the response-after-events barrier, agent_settled
// exactly once per accepted prompt, and the exact rejection/error strings.
// It emits only native-shaped frames and implements none of the wrapper's
// uniform behavior.
const (
	envFakePiHelper = "ACP_GO_PI_FAKE_HELPER"
	// envFakePiMode carries the absolute path of a scenario JSON file; empty
	// selects the default scenario, which mimics a credential-less real pi
	// (unknown model, empty catalog, prompts rejected).
	envFakePiMode = "ACP_GO_PI_FAKE_MODE"
)

const fakePiVersion = "0.80.6"

// Exact native strings observed live from pi 0.80.6.
const (
	fakeBusyError = "Agent is already processing. " +
		"Specify streamingBehavior ('steer' or 'followUp') to queue the message."
	fakeNoAPIKeyError = "No API key found for the selected model.\n\n" +
		"Use /login to log into a provider via OAuth or API key."
	fakeAbortedErrorMessage = "This operation was aborted"
	// pi's prompt handler dereferences message before validating it.
	fakeMissingMessageError = "Cannot read properties of undefined (reading 'startsWith')"
	fakeUnknownCommandError = "Unknown command: undefined"
	fakeParseErrorPrefix    = "Failed to parse command: "
	fakeParseEmptyError     = fakeParseErrorPrefix + "Unexpected end of JSON input"
)

const (
	fakeBehaviorReply         = "reply"
	fakeBehaviorProviderError = "providerError"
	fakeBehaviorDie           = "die"
	fakeBehaviorHang          = "hang"
	fakeBehaviorGarbageBurst  = "garbageBurst"
)

const (
	fakeDefaultReply         = "FAKE_PI_REPLY"
	fakeElicitCancelledReply = "FAKE_PI_ELICIT_CANCELLED"
)

type fakeModelSpec struct {
	Provider      string `json:"provider"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	ContextWindow int64  `json:"contextWindow"`
	MaxTokens     int64  `json:"maxTokens"`
}

type fakeCommandSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Source      string `json:"source"`
	Path        string `json:"path,omitempty"`
}

type fakeScenario struct {
	Models            []fakeModelSpec   `json:"models,omitempty"`
	ReplyText         string            `json:"replyText,omitempty"`
	DeltaTexts        []string          `json:"deltaTexts,omitempty"`
	StreamDelayMs     int               `json:"streamDelayMs,omitempty"`
	PromptBehavior    string            `json:"promptBehavior,omitempty"`
	ProviderError     string            `json:"providerError,omitempty"`
	GarbageLines      int               `json:"garbageLines,omitempty"`
	ToolName          string            `json:"toolName,omitempty"`
	ToolArgs          map[string]any    `json:"toolArgs,omitempty"`
	ToolOutput        string            `json:"toolOutput,omitempty"`
	ElicitMethod      string            `json:"elicitMethod,omitempty"`
	ElicitTitle       string            `json:"elicitTitle,omitempty"`
	Commands          []fakeCommandSpec `json:"commands,omitempty"`
	MCPStartupFailure string            `json:"mcpStartupFailure,omitempty"`
}

// fakeTurnScenario returns a scenario whose prompt turns succeed: a
// one-model catalog is pre-selected so the no-API-key rejection does not
// apply.
func fakeTurnScenario() fakeScenario {
	return fakeScenario{
		Models: []fakeModelSpec{{
			Provider:      "fake",
			ID:            "fake-model",
			Name:          "Fake Model",
			ContextWindow: 200000,
			MaxTokens:     16384,
		}},
		ReplyText: fakeDefaultReply,
	}
}

// TestFakePiExecutable is the fake harness entry point. It is inert in a
// normal test run; the generated wrapper script re-executes this test binary
// with the helper environment set, turning this test into a standalone
// `pi --mode rpc` replacement.
func TestFakePiExecutable(t *testing.T) {
	if os.Getenv(envFakePiHelper) != "1" {
		return
	}

	os.Exit(runFakePi(os.Args))
}

// fakePiExecutable writes an executable shim that re-runs this test binary
// as the fake pi harness with the scenario baked in. The scenario travels in
// the script itself because the wrapper launches pi with a scrubbed
// environment.
func fakePiExecutable(t *testing.T, scenario fakeScenario) string {
	t.Helper()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}

	dir := t.TempDir()

	scenarioPath := filepath.Join(dir, "scenario.json")
	data, err := json.Marshal(scenario)
	if err != nil {
		t.Fatalf("encode fake scenario: %v", err)
	}
	if err := os.WriteFile(scenarioPath, data, 0o600); err != nil {
		t.Fatalf("write fake scenario: %v", err)
	}

	path := filepath.Join(dir, "pi")
	script := fmt.Sprintf("#!/bin/sh\n%s=1 %s=%q exec %q -test.run '^TestFakePiExecutable$' -- \"$@\"\n",
		envFakePiHelper, envFakePiMode, scenarioPath, testBinary)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { // #nosec G306 -- private per-test executable shim.
		t.Fatalf("write fake pi executable: %v", err)
	}

	return path
}

func runFakePi(args []string) int {
	rest := args[1:]
	for i, arg := range args {
		if arg == "--" {
			rest = args[i+1:]

			break
		}
	}

	var sessionDir, sessionPath, sessionID string
	var extensionPaths []string

	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "--version":
			fmt.Println(fakePiVersion)

			return 0
		case "--session-dir":
			if i+1 < len(rest) {
				i++
				sessionDir = rest[i]
			}
		case "--session":
			if i+1 < len(rest) {
				i++
				sessionPath = rest[i]
			}
		case "--session-id":
			if i+1 < len(rest) {
				i++
				sessionID = rest[i]
			}
		case "-e":
			if i+1 < len(rest) {
				i++
				extensionPaths = append(extensionPaths, rest[i])
			}
		}
	}

	scenario, err := loadFakeScenario()
	if err != nil {
		fmt.Fprintf(os.Stderr, "fake pi: %v\n", err)

		return 1
	}

	server := newFakePiServer(scenario, sessionDir, extensionPaths)

	// Real pi loads extensions before serving; an MCP extension whose server
	// connection fails throws in its factory and pi exits loudly at startup.
	if err := server.checkMCPStartup(); err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 1
	}

	server.startSession(sessionPath, sessionID)

	return server.serve()
}

func loadFakeScenario() (fakeScenario, error) {
	var scenario fakeScenario

	path := os.Getenv(envFakePiMode)
	if path == "" {
		return scenario, nil
	}

	data, err := os.ReadFile(path) // #nosec G304 -- path is baked into the test-generated shim.
	if err != nil {
		return scenario, fmt.Errorf("read scenario: %w", err)
	}

	if err := json.Unmarshal(data, &scenario); err != nil {
		return scenario, fmt.Errorf("decode scenario: %w", err)
	}

	return scenario, nil
}

type fakeUIAnswer struct {
	value     string
	cancelled bool
}

type fakeTurn struct {
	abortOnce sync.Once
	abort     chan struct{}
	done      chan struct{}
}

func newFakeTurn() *fakeTurn {
	return &fakeTurn{abort: make(chan struct{}), done: make(chan struct{})}
}

func (t *fakeTurn) requestAbort() {
	t.abortOnce.Do(func() { close(t.abort) })
}

func (t *fakeTurn) aborted() bool {
	select {
	case <-t.abort:
		return true
	default:
		return false
	}
}

type fakeEntryMeta struct {
	id        string
	typ       string
	role      string
	toolCalls int
	isError   bool
	usage     *fakeUsage
	row       json.RawMessage
}

type fakeSessionState struct {
	id            string
	file          string
	name          string
	parentSession string
	thinkingLevel string
	entries       []fakeEntryMeta
	persisted     bool
}

type fakePiServer struct {
	scenario       fakeScenario
	sessionDir     string
	cwd            string
	extensionPaths []string
	permissionAsk  bool
	out            *fakeLineWriter

	mu        sync.Mutex
	session   fakeSessionState
	model     *fakeModelSpec
	autoRetry bool
	steering  []string
	followUp  []string
	turn      *fakeTurn
	ui        map[string]chan fakeUIAnswer
	uiSeq     int
	entrySeq  int
}

func newFakePiServer(scenario fakeScenario, sessionDir string, extensionPaths []string) *fakePiServer {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "/"
	}

	server := &fakePiServer{
		scenario:       scenario,
		sessionDir:     sessionDir,
		cwd:            cwd,
		extensionPaths: extensionPaths,
		permissionAsk:  os.Getenv(pi.EnvPermissionMode) != pi.PermissionModeAllow,
		out:            &fakeLineWriter{writer: bufio.NewWriter(os.Stdout)},
		autoRetry:      true,
		ui:             make(map[string]chan fakeUIAnswer, 2),
	}

	if len(scenario.Models) > 0 {
		server.model = &scenario.Models[0]
	}

	return server
}

func (s *fakePiServer) checkMCPStartup() error {
	configPath := os.Getenv(pi.EnvMCPConfig)
	if configPath == "" {
		return nil
	}

	extension := pi.MCPExtensionFileName
	if len(s.extensionPaths) > 0 {
		extension = s.extensionPaths[len(s.extensionPaths)-1]
	}

	data, err := os.ReadFile(configPath) // #nosec G304 -- path supplied by the launching wrapper under test.
	if err != nil {
		return fmt.Errorf("Failed to load extension %s: %v", extension, err)
	}

	var config pi.MCPConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("Failed to load extension %s: %v", extension, err)
	}

	if s.scenario.MCPStartupFailure != "" {
		return fmt.Errorf("Failed to load extension %s: %s", extension, s.scenario.MCPStartupFailure)
	}

	return nil
}

func (s *fakePiServer) startSession(sessionPath string, sessionID string) {
	if sessionPath != "" {
		if s.loadSessionFile(sessionPath) {
			return
		}

		s.resetSession(fakeUUID(), sessionPath, "")

		return
	}

	id := sessionID
	if id == "" {
		id = fakeUUID()
	}

	s.resetSession(id, s.defaultSessionFile(id), "")
}

func (s *fakePiServer) defaultSessionFile(id string) string {
	now := time.Now().UTC()
	stamp := now.Format("2006-01-02T15-04-05") + fmt.Sprintf("-%03dZ", now.Nanosecond()/1e6)

	return filepath.Join(s.sessionDir, stamp+"_"+id+".jsonl")
}

// resetSession seeds the entry tree with the thinking_level_change row a
// real pi RPC process appends at startup; the session file itself stays
// absent until the first message entry commits.
func (s *fakePiServer) resetSession(id string, file string, parentSession string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.session = fakeSessionState{
		id:            id,
		file:          file,
		parentSession: parentSession,
		thinkingLevel: "off",
	}
	s.steering = nil
	s.followUp = nil

	s.appendEntryLocked(fakeEntryRow{
		Type:          "thinking_level_change",
		Timestamp:     fakeEntryTimestamp(),
		ThinkingLevel: s.session.thinkingLevel,
	}, fakeEntryMeta{typ: "thinking_level_change"})
}

func (s *fakePiServer) loadSessionFile(path string) bool {
	data, err := os.ReadFile(path) // #nosec G304 -- path supplied by the launching wrapper under test.
	if err != nil {
		return false
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) == 0 {
		return false
	}

	var header fakeSessionHeader
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil || header.Type != "session" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.session = fakeSessionState{
		id:            header.ID,
		file:          path,
		parentSession: header.ParentSession,
		thinkingLevel: "off",
		persisted:     true,
	}

	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var row struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Message struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
				Usage   *fakeUsage      `json:"usage"`
				IsError bool            `json:"isError"`
			} `json:"message"`
			ThinkingLevel string `json:"thinkingLevel"`
			Name          string `json:"name"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}

		meta := fakeEntryMeta{
			id:      row.ID,
			typ:     row.Type,
			role:    row.Message.Role,
			isError: row.Message.IsError,
			usage:   row.Message.Usage,
			row:     json.RawMessage(line),
		}
		meta.toolCalls = strings.Count(string(row.Message.Content), `"toolCall"`)

		if row.Type == "thinking_level_change" && row.ThinkingLevel != "" {
			s.session.thinkingLevel = row.ThinkingLevel
		}
		if row.Type == "session_info" {
			s.session.name = row.Name
		}

		// Hydrated rows keep their original ids; the id counter must resume
		// past them so freshly appended entries cannot collide.
		if seq, err := strconv.ParseUint(row.ID, 16, 32); err == nil && int(seq) > s.entrySeq {
			s.entrySeq = int(seq)
		}

		s.session.entries = append(s.session.entries, meta)
	}

	s.steering = nil
	s.followUp = nil

	return true
}

func (s *fakePiServer) serve() int {
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r")

		if line != "" || err == nil {
			if exit, code := s.handleLine(line); exit {
				return code
			}
		}

		if err != nil {
			// Real pi exits immediately on stdin EOF, even mid-stream.
			return 0
		}
	}
}

func (s *fakePiServer) handleLine(line string) (bool, int) {
	if strings.TrimSpace(line) == "" {
		s.respondError("", "parse", fakeParseEmptyError)

		return false, 0
	}

	var decoded any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		s.respondError("", "parse", fakeParseErrorPrefix+err.Error())

		return false, 0
	}

	switch command := decoded.(type) {
	case nil:
		// A bare `null` record crashes real pi with an uncaught TypeError.
		fmt.Fprintln(os.Stderr, "TypeError: Cannot read properties of null (reading 'id')")

		return true, 1
	case map[string]any:
		s.handleCommand(command)

		return false, 0
	default:
		// Non-object JSON records draw the "undefined" rejection with no
		// command field.
		s.out.writeJSON(fakeResponse{Type: "response", Success: false, Error: fakeUnknownCommandError})

		return false, 0
	}
}

func (s *fakePiServer) handleCommand(command map[string]any) {
	id, _ := command["id"].(string)

	commandType, _ := command["type"].(string)
	if commandType == "" {
		s.out.writeJSON(fakeResponse{ID: id, Type: "response", Success: false, Error: fakeUnknownCommandError})

		return
	}

	switch commandType {
	case "prompt":
		s.handlePrompt(id, command)
	case "steer":
		s.handleQueued(id, commandType, command, true)
	case "follow_up":
		s.handleQueued(id, commandType, command, false)
	case "abort":
		s.handleAbort(id)
	case "new_session":
		s.handleNewSession(id, command)
	case "switch_session":
		s.handleSwitchSession(id, command)
	case "clone":
		s.handleClone(id)
	case "get_state":
		s.respondData(id, commandType, s.stateData())
	case "get_commands":
		s.respondData(id, commandType, s.commandsData())
	case "get_available_models":
		s.respondData(id, commandType, s.modelsData())
	case "set_model":
		s.handleSetModel(id, command)
	case "set_thinking_level":
		s.handleSetThinkingLevel(id, command)
	case "set_auto_retry":
		s.handleSetAutoRetry(id, command)
	case "set_session_name":
		s.handleSetSessionName(id, command)
	case "get_session_stats":
		s.respondData(id, commandType, s.statsData())
	case "get_entries":
		s.handleGetEntries(id, command)
	case "extension_ui_response":
		s.handleUIResponse(command)
	default:
		s.respondError(id, commandType, "Unknown command: "+commandType)
	}
}

func (s *fakePiServer) handlePrompt(id string, command map[string]any) {
	message, hasMessage := command["message"].(string)

	s.mu.Lock()

	if s.turn != nil {
		behavior, _ := command["streamingBehavior"].(string)

		switch behavior {
		case "steer":
			s.steering = append(s.steering, message)
		case "followUp":
			s.followUp = append(s.followUp, message)
		default:
			s.mu.Unlock()
			s.respondError(id, "prompt", fakeBusyError)

			return
		}

		update := s.queueUpdateLocked()
		s.mu.Unlock()

		// Queue acceptance is acked only after its queue_update event.
		s.out.writeJSON(update)
		s.respondOK(id, "prompt")

		return
	}

	if !hasMessage {
		s.mu.Unlock()
		s.respondError(id, "prompt", fakeMissingMessageError)

		return
	}

	if s.model == nil {
		s.mu.Unlock()
		s.respondError(id, "prompt", fakeNoAPIKeyError)

		return
	}

	turn := newFakeTurn()
	s.turn = turn
	s.mu.Unlock()

	// The prompt ack precedes the events it causes; everything after streams
	// asynchronously.
	s.respondOK(id, "prompt")

	go s.runTurn(turn, message)
}

func (s *fakePiServer) handleQueued(id string, commandType string, command map[string]any, steer bool) {
	message, _ := command["message"].(string)

	s.mu.Lock()
	if steer {
		s.steering = append(s.steering, message)
	} else {
		s.followUp = append(s.followUp, message)
	}
	update := s.queueUpdateLocked()
	s.mu.Unlock()

	s.out.writeJSON(update)
	s.respondOK(id, commandType)
}

func (s *fakePiServer) handleAbort(id string) {
	s.mu.Lock()
	turn := s.turn
	s.mu.Unlock()

	if turn == nil {
		s.respondOK(id, "abort")

		return
	}

	// The abort ack must trail the aborted turn's terminal events; a hung
	// turn therefore never acks (deliberate, for timeout coverage).
	go func() {
		turn.requestAbort()
		<-turn.done
		s.respondOK(id, "abort")
	}()
}

func (s *fakePiServer) handleNewSession(id string, command map[string]any) {
	parentSession, _ := command["parentSession"].(string)

	newID := fakeUUID()
	s.resetSession(newID, s.defaultSessionFile(newID), parentSession)
	s.respondData(id, "new_session", fakeCancelledData{Cancelled: false})
}

func (s *fakePiServer) handleSwitchSession(id string, command map[string]any) {
	sessionPath, _ := command["sessionPath"].(string)

	if !s.loadSessionFile(sessionPath) {
		// Real pi accepts a missing target path and starts a fresh session
		// there.
		s.resetSession(fakeUUID(), sessionPath, "")
	}

	s.respondData(id, "switch_session", fakeCancelledData{Cancelled: false})
}

func (s *fakePiServer) handleClone(id string) {
	s.mu.Lock()

	hasMessage := false
	for _, entry := range s.session.entries {
		if entry.typ == "message" {
			hasMessage = true

			break
		}
	}

	if !hasMessage {
		leaf := "null"
		if len(s.session.entries) > 0 {
			leaf = s.session.entries[len(s.session.entries)-1].id
		}
		s.mu.Unlock()

		s.respondError(id, "clone", fmt.Sprintf("Entry %s not found", leaf))

		return
	}

	cloneID := fakeUUID()
	parentPath := s.session.file
	s.session.id = cloneID
	s.session.file = s.defaultSessionFile(cloneID)
	s.session.parentSession = parentPath
	s.persistLocked()
	s.mu.Unlock()

	s.respondData(id, "clone", fakeCancelledData{Cancelled: false})
}

func (s *fakePiServer) handleSetModel(id string, command map[string]any) {
	provider, _ := command["provider"].(string)
	modelID, _ := command["modelId"].(string)

	for i := range s.scenario.Models {
		spec := &s.scenario.Models[i]
		if spec.Provider != provider || spec.ID != modelID {
			continue
		}

		s.mu.Lock()
		s.model = spec
		s.appendEntryLocked(fakeEntryRow{
			Type:      "model_change",
			Timestamp: fakeEntryTimestamp(),
			Provider:  provider,
			ModelID:   modelID,
		}, fakeEntryMeta{typ: "model_change"})
		s.persistIfNeededLocked()
		s.mu.Unlock()

		s.respondData(id, "set_model", fakeModelValueFor(*spec))

		return
	}

	s.respondError(id, "set_model", fmt.Sprintf("Model not found: %s/%s", provider, modelID))
}

func (s *fakePiServer) handleSetThinkingLevel(id string, command map[string]any) {
	level, _ := command["level"].(string)

	// Real pi accepts any level string with success and silently coerces
	// invalid values; only valid levels change state.
	if pi.IsValidThinkingLevel(level) {
		s.mu.Lock()
		s.session.thinkingLevel = level
		s.appendEntryLocked(fakeEntryRow{
			Type:          "thinking_level_change",
			Timestamp:     fakeEntryTimestamp(),
			ThinkingLevel: level,
		}, fakeEntryMeta{typ: "thinking_level_change"})
		s.persistIfNeededLocked()
		s.mu.Unlock()
	}

	s.respondOK(id, "set_thinking_level")
}

func (s *fakePiServer) handleSetAutoRetry(id string, command map[string]any) {
	enabled, _ := command["enabled"].(bool)

	s.mu.Lock()
	s.autoRetry = enabled
	s.mu.Unlock()

	s.respondOK(id, "set_auto_retry")
}

func (s *fakePiServer) handleSetSessionName(id string, command map[string]any) {
	name, _ := command["name"].(string)

	s.mu.Lock()
	s.session.name = name
	s.appendEntryLocked(fakeEntryRow{
		Type:      "session_info",
		Timestamp: fakeEntryTimestamp(),
		Name:      name,
	}, fakeEntryMeta{typ: "session_info"})
	s.persistIfNeededLocked()
	s.mu.Unlock()

	// The session_info_changed event precedes the ack.
	s.out.writeJSON(fakeSessionInfoChangedEvent{Type: "session_info_changed", Name: name})
	s.respondOK(id, "set_session_name")
}

func (s *fakePiServer) handleGetEntries(id string, command map[string]any) {
	since, _ := command["since"].(string)

	s.mu.Lock()
	entries := s.session.entries
	s.mu.Unlock()

	start := 0
	if since != "" {
		start = -1
		for i, entry := range entries {
			if entry.id == since {
				start = i + 1

				break
			}
		}
		if start < 0 {
			s.respondError(id, "get_entries", "Entry not found: "+since)

			return
		}
	}

	rows := make([]json.RawMessage, 0, len(entries)-start)
	for _, entry := range entries[start:] {
		rows = append(rows, entry.row)
	}

	var leaf any
	if len(entries) > 0 {
		leaf = entries[len(entries)-1].id
	}

	s.respondData(id, "get_entries", fakeEntriesData{Entries: rows, LeafID: leaf})
}

func (s *fakePiServer) handleUIResponse(command map[string]any) {
	id, _ := command["id"].(string)

	s.mu.Lock()
	waiter, ok := s.ui[id]
	if ok {
		delete(s.ui, id)
	}
	s.mu.Unlock()

	if !ok {
		return
	}

	answer := fakeUIAnswer{}
	if cancelled, _ := command["cancelled"].(bool); cancelled {
		answer.cancelled = true
	} else if value, ok := command["value"].(string); ok {
		answer.value = value
	} else {
		answer.cancelled = true
	}

	waiter <- answer
}
