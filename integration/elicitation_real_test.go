//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

const realElicitationMarker = "PI_REAL_ELICITATION_OK"

// TestPiRealQuestionToolElicitation proves the wrapper's shipped question
// tool is loaded by the real pi CLI, offered to the model, routed through the
// real extension UI protocol and ACP elicitation, then resumed with the
// client's answer. The local provider is deterministic and spends no tokens.
func TestPiRealQuestionToolElicitation(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	model := newQuestionModelHarness(t)
	modelsJSON, err := json.Marshal(map[string]any{
		"providers": map[string]any{
			"question-test": map[string]any{
				"baseUrl": model.server.URL + "/v1",
				"api":     "openai-completions",
				"apiKey":  "local-test-key",
				"models": []any{map[string]any{
					"id": "question-model", "name": "Question Tool Model",
					"reasoning": false, "contextWindow": 16_384, "maxTokens": 1_024,
				}},
			},
		},
	})
	require.NoError(t, err)

	client := &recordingClient{elicitationValue: realElicitationMarker}
	conn := connectAgentWithInitForTest(t, ctx, client, formElicitationInit(),
		piacp.WithExecutablePath(smokePiPath(t)),
		piacp.WithScratchDir(t.TempDir()),
		piacp.WithDefaultModel("question-test/question-model"),
		piacp.WithSeedFiles(map[string]string{"models.json": string(modelsJSON)}),
	)
	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir(),
		piacp.WithSessionPiOptions(piacp.NewPiOptions(piacp.WithPiPermission("ask"))),
	))
	require.NoError(t, err)

	response, err := conn.Prompt(ctx, piacp.TextPromptRequest(session.SessionId,
		"real-question-turn", "Ask me for the deterministic marker."))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, client.text(), realElicitationMarker)
	require.Equal(t, []string{"question"}, model.firstRequestTools())

	elicitations := client.elicitationSnapshot()
	require.Len(t, elicitations, 1)
	require.Contains(t, elicitations[0].Form.Message, "What is the marker?")
	require.Equal(t, []string{"value"}, elicitations[0].Form.RequestedSchema.Required)
}

type questionModelHarness struct {
	server *httptest.Server

	mu        sync.Mutex
	requests  int
	toolNames []string
}

func newQuestionModelHarness(t *testing.T) *questionModelHarness {
	t.Helper()

	harness := &questionModelHarness{}
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		harness.mu.Lock()
		requestNumber := harness.requests
		harness.requests++
		if requestNumber == 0 {
			for _, tool := range request.Tools {
				if tool.Function.Name == "question" {
					harness.toolNames = append(harness.toolNames, tool.Function.Name)
				}
			}
			slices.Sort(harness.toolNames)
		}
		harness.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		if requestNumber == 0 {
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-question", "object": "chat.completion.chunk", "created": 1,
				"model": "question-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{
						"role": "assistant",
						"tool_calls": []any{map[string]any{
							"index": 0, "id": "question-call-1", "type": "function",
							"function": map[string]any{
								"name": "question", "arguments": `{"question":"What is the marker?"}`,
							},
						}},
					},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-question", "object": "chat.completion.chunk", "created": 1,
				"model": "question-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls",
				}},
			})
		} else {
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-answer", "object": "chat.completion.chunk", "created": 2,
				"model": "question-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{"role": "assistant", "content": realElicitationMarker},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-answer", "object": "chat.completion.chunk", "created": 2,
				"model": "question-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "stop",
				}},
			})
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(harness.server.Close)

	return harness
}

func (h *questionModelHarness) firstRequestTools() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.toolNames)
}
