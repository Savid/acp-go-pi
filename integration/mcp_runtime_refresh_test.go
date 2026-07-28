//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

const (
	runtimeMCPExecuteTool = "mcp__runtime__execute"
	runtimeMCPSearchTool  = "mcp__runtime__search"
	runtimeMCPMarker      = "PI_RUNTIME_MCP_REFRESH_OK"
)

// TestPiACPRuntimeMCPRefresh drives the real pi binary without provider
// credentials. The MCP server first behaves like a disarmed runtime route and
// exposes only a readiness tool. After session/new returns, it exposes its real
// execute/search surface. A deterministic local Chat Completions provider then
// requests execute by its exact registered name and verifies the returned
// marker. This reproduces the worker-proxy lifecycle without model tokens.
func TestPiACPRuntimeMCPRefresh(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	mcp := newRuntimeMCPHarness(t)
	model := newToolCallingModelHarness(t)

	modelsJSON, err := json.Marshal(map[string]any{
		"providers": map[string]any{
			"runtime-test": map[string]any{
				"baseUrl": model.server.URL + "/v1",
				"api":     "openai-completions",
				"apiKey":  "local-test-key",
				"models": []any{map[string]any{
					"id": "tool-model", "name": "Runtime MCP Tool Model",
					"reasoning": false, "contextWindow": 16_384, "maxTokens": 1_024,
				}},
			},
		},
	})
	require.NoError(t, err)

	client := &recordingClient{}
	store := piacp.NewInMemorySessionStore()
	options := []piacp.Option{
		piacp.WithExecutablePath(smokePiPath(t)),
		piacp.WithScratchDir(t.TempDir()),
		piacp.WithDefaultModel("runtime-test/tool-model"),
		piacp.WithSeedFiles(map[string]string{"models.json": string(modelsJSON)}),
		piacp.WithSessionStore(store),
	}
	conn, stopFirst := connectControlledAgent(t, ctx, client, options...)

	cwd := t.TempDir()
	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(cwd,
		piacp.WithSessionMCPServers(
			piacp.HTTPMCPServer("runtime", mcp.server.URL, nil),
		),
		piacp.WithSessionPiOptions(piacp.NewPiOptions(
			piacp.WithPiPermission("allow"),
		)),
	))
	require.NoError(t, err)
	require.Equal(t, int64(1), mcp.readinessLists.Load(),
		"session establishment must observe the provisional readiness surface")

	mcp.armed.Store(true)
	resp, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"runtime-mcp-turn", "Call the runtime execute tool exactly once."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
	require.Contains(t, client.text(), runtimeMCPMarker)
	require.Equal(t, int64(1), mcp.executeCalls.Load())
	require.Positive(t, mcp.activeLists.Load(), "the authorized process must refresh tools/list")
	require.Equal(t, []string{runtimeMCPExecuteTool, runtimeMCPSearchTool}, model.firstRequestTools())

	liveMessageID := piMessageID(resp.Meta)
	require.NotEmpty(t, liveMessageID)
	storedMessageID := lastStoredAssistantMessageID(t, store, string(session.SessionId))
	require.Equal(t, liveMessageID, storedMessageID,
		"the terminal live UUID must be the final mirrored assistant UUID")

	// Closing the adapter process leaves its external store intact. A new
	// adapter instance must resume the exact transcript, publish the same
	// terminal identity without replaying content, refresh its provisional MCP
	// snapshot on the next authorized turn, and continue the native session.
	stopFirst()
	mcp.armed.Store(false)
	loadedClient := &recordingClient{}
	loadedConn, stopLoaded := connectControlledAgent(t, ctx, loadedClient, options...)
	t.Cleanup(stopLoaded)
	_, err = loadedConn.ResumeSession(ctx, piacp.ResumeSessionRequest(session.SessionId, cwd,
		piacp.WithSessionMCPServers(
			piacp.HTTPMCPServer("runtime", mcp.server.URL, nil),
		),
		piacp.WithSessionPiOptions(piacp.NewPiOptions(
			piacp.WithPiPermission("allow"),
		)),
	))
	require.NoError(t, err)
	loadedNotifications := loadedClient.notificationSnapshot()
	require.Equal(t, liveMessageID, lastNotificationMessageID(loadedNotifications))
	require.Empty(t, loadedClient.text(), "session/resume must publish identity without replaying transcript text")
	require.Equal(t, int64(2), mcp.readinessLists.Load(),
		"session/resume must also establish against the provisional surface")

	mcp.armed.Store(true)
	continued, err := loadedConn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"runtime-mcp-turn-2", "Reply with the deterministic marker."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, continued.StopReason)
	require.NotEmpty(t, piMessageID(continued.Meta))
	require.GreaterOrEqual(t, mcp.activeLists.Load(), int64(2),
		"the resumed process must refresh its fixed registry before continuing")
}

type runtimeMCPHarness struct {
	server *httptest.Server

	armed          atomic.Bool
	readinessLists atomic.Int64
	activeLists    atomic.Int64
	executeCalls   atomic.Int64
}

func newRuntimeMCPHarness(t *testing.T) *runtimeMCPHarness {
	t.Helper()

	h := &runtimeMCPHarness{}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		var request struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		if request.Method == "notifications/initialized" {
			w.WriteHeader(http.StatusAccepted)

			return
		}

		var result any
		switch request.Method {
		case "initialize":
			name := "runtime-readiness"
			if h.armed.Load() {
				name = "runtime-tools"
			}
			result = map[string]any{
				"protocolVersion": "2025-06-18",
				"capabilities": map[string]any{
					"tools": map[string]any{"listChanged": false},
				},
				"serverInfo": map[string]any{"name": name, "version": "1"},
			}
		case "tools/list":
			if !h.armed.Load() {
				h.readinessLists.Add(1)
				result = map[string]any{"tools": []any{mcpToolDefinition(
					"runtime_ready", "Proves that the provisional route is reachable.",
					map[string]any{"nonce": map[string]any{"type": "string"}}, []string{"nonce"},
				)}}
			} else {
				h.activeLists.Add(1)
				result = map[string]any{"tools": []any{
					mcpToolDefinition("execute", "Execute a deterministic runtime action.",
						map[string]any{"marker": map[string]any{"type": "string"}}, []string{"marker"}),
					mcpToolDefinition("search", "Search deterministic runtime data.",
						map[string]any{"query": map[string]any{"type": "string"}}, []string{"query"}),
				}}
			}
		case "tools/call":
			if !h.armed.Load() || request.Params.Name != "execute" {
				writeJSONRPCError(w, request.ID, -32602, "unexpected tool")

				return
			}
			h.executeCalls.Add(1)
			result = map[string]any{"content": []any{map[string]any{
				"type": "text", "text": runtimeMCPMarker,
			}}}
		default:
			writeJSONRPCError(w, request.ID, -32601, "method not found")

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0", "id": request.ID, "result": result,
		})
	}))
	t.Cleanup(h.server.Close)

	return h
}

func mcpToolDefinition(name, description string, properties map[string]any, required []string) map[string]any {
	return map[string]any{
		"name": name, "description": description,
		"inputSchema": map[string]any{
			"type": "object", "properties": properties, "required": required,
			"additionalProperties": false,
		},
	}
}

func writeJSONRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": message},
	})
}

type toolCallingModelHarness struct {
	server *httptest.Server

	mu        sync.Mutex
	requests  int
	toolNames []string
}

func newToolCallingModelHarness(t *testing.T) *toolCallingModelHarness {
	t.Helper()

	h := &toolCallingModelHarness{}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		h.mu.Lock()
		requestNumber := h.requests
		h.requests++
		if requestNumber == 0 {
			for _, tool := range request.Tools {
				if len(tool.Function.Name) >= len("mcp__") && tool.Function.Name[:len("mcp__")] == "mcp__" {
					h.toolNames = append(h.toolNames, tool.Function.Name)
				}
			}
			slices.Sort(h.toolNames)
		}
		h.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		if requestNumber == 0 {
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-tool", "object": "chat.completion.chunk", "created": 1,
				"model": "tool-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{
						"role": "assistant",
						"tool_calls": []any{map[string]any{
							"index": 0, "id": "runtime-call-1", "type": "function",
							"function": map[string]any{
								"name":      runtimeMCPExecuteTool,
								"arguments": `{"marker":"` + runtimeMCPMarker + `"}`,
							},
						}},
					},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-tool", "object": "chat.completion.chunk", "created": 1,
				"model": "tool-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls",
				}},
			})
		} else {
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-final", "object": "chat.completion.chunk", "created": 2,
				"model": "tool-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{"role": "assistant", "content": runtimeMCPMarker},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-final", "object": "chat.completion.chunk", "created": 2,
				"model": "tool-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "stop",
				}},
			})
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(h.server.Close)

	return h
}

func (h *toolCallingModelHarness) firstRequestTools() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.toolNames)
}

func writeSSE(t *testing.T, w io.Writer, payload any) {
	t.Helper()

	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	_, err = fmt.Fprintf(w, "data: %s\n\n", encoded)
	require.NoError(t, err)
}

func connectControlledAgent(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	options ...piacp.Option,
) (*acp.ClientSideConnection, func()) {
	t.Helper()

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, cancelServe := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() {
		opts := append([]piacp.Option{
			piacp.WithLogger(integrationLogger), integrationContainmentOption(),
		}, options...)
		serveErr <- piacp.Serve(serveCtx, c2aR, a2cW, opts...)
	}()

	conn := acp.NewClientSideConnection(client, c2aW, a2cR)
	_, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancelServe()
			_ = c2aW.Close()
			_ = a2cR.Close()
			select {
			case err := <-serveErr:
				if err != nil && ctx.Err() == nil {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("controlled agent did not stop")
			}
			_ = c2aR.Close()
			_ = a2cW.Close()
		})
	}

	return conn, stop
}

func piMessageID(meta map[string]any) string {
	piMeta, _ := meta["pi"].(map[string]any)
	messageID, _ := piMeta["messageId"].(string)

	return messageID
}

func lastStoredAssistantMessageID(
	t *testing.T,
	store *piacp.InMemorySessionStore,
	sessionID string,
) string {
	t.Helper()

	entries, err := store.Load(t.Context(), piacp.SessionKey{SessionID: sessionID})
	require.NoError(t, err)
	for index := len(entries) - 1; index >= 0; index-- {
		var row struct {
			Type    string `json:"type"`
			Message struct {
				Role         string `json:"role"`
				ACPMessageID string `json:"acpMessageId"`
			} `json:"message"`
		}
		if json.Unmarshal(entries[index], &row) == nil &&
			row.Type == "message" && row.Message.Role == "assistant" {
			return row.Message.ACPMessageID
		}
	}

	return ""
}

func lastNotificationMessageID(notifications []acp.SessionNotification) string {
	for index := len(notifications) - 1; index >= 0; index-- {
		notification := notifications[index]
		if notification.Update.AgentMessageChunk != nil &&
			notification.Update.AgentMessageChunk.MessageId != nil {
			return *notification.Update.AgentMessageChunk.MessageId
		}
		if notification.Update.AgentThoughtChunk != nil &&
			notification.Update.AgentThoughtChunk.MessageId != nil {
			return *notification.Update.AgentThoughtChunk.MessageId
		}
		if messageID := piMessageID(notification.Meta); messageID != "" {
			return messageID
		}
	}

	return ""
}
