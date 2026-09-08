//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

// TestPiRealExtraPathDirsSurviveNativeBashRewrite proves the real pi CLI's
// bash-tool boundary keeps each session's validated ExtraPathDirs ahead of
// PI_CODING_AGENT_DIR/bin. Two live sessions carry different directories;
// loading the first session with a third directory actively replaces its
// native process and must drop both stale carriers.
func TestPiRealExtraPathDirsSurviveNativeBashRewrite(t *testing.T) {
	requireRunIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	model := newBashPathModelHarness(t)
	modelsJSON, err := json.Marshal(map[string]any{
		"providers": map[string]any{
			"path-test": map[string]any{
				"baseUrl": model.server.URL + "/v1",
				"api":     "openai-completions",
				"apiKey":  "local-test-key",
				"models": []any{map[string]any{
					"id": "bash-model", "name": "Bash PATH Model",
					"reasoning": false, "contextWindow": 16_384, "maxTokens": 1_024,
				}},
			},
		},
	})
	require.NoError(t, err)

	store := piacp.NewInMemorySessionStore()
	client := &recordingClient{}
	conn := connectEmbeddedAgentForTest(t, ctx, client,
		piacp.WithExecutablePath(smokePiPath(t)),
		piacp.WithScratchDir(integrationScratchDir(t)),
		piacp.WithDefaultModel("path-test/bash-model"),
		piacp.WithSeedFiles(map[string]string{"models.json": string(modelsJSON)}),
		piacp.WithSessionStore(store),
	)
	cwd := t.TempDir()
	firstDir := t.TempDir()
	peerDir := t.TempDir()
	reboundDir := t.TempDir()
	writePathCarrier(t, firstDir, "PI_PATH_FIRST", peerDir, reboundDir)
	writePathCarrier(t, peerDir, "PI_PATH_PEER", firstDir, reboundDir)
	writePathCarrier(t, reboundDir, "PI_PATH_REBOUND", firstDir, peerDir)

	newSession := func(dir string) acp.SessionId {
		session, sessionErr := conn.NewSession(ctx, piacp.NewSessionRequest(cwd,
			piacp.WithSessionPiOptions(piacp.NewPiOptions(
				piacp.WithPiExtraPathDirs(dir),
				piacp.WithPiPermission("allow"),
			)),
		))
		require.NoError(t, sessionErr)

		return session.SessionId
	}
	prompt := func(sessionID acp.SessionId, turnID string) string {
		before := client.text()
		response, promptErr := conn.Prompt(ctx, piacp.TextPromptRequest(
			sessionID, turnID, "Call the bash tool exactly once.",
		))
		require.NoError(t, promptErr)
		require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

		return strings.TrimPrefix(client.text(), before)
	}

	firstSession := newSession(firstDir)
	require.Contains(t, prompt(firstSession, "path-first"), "PI_PATH_FIRST")

	peerSession := newSession(peerDir)
	require.Contains(t, prompt(peerSession, "path-peer"), "PI_PATH_PEER")

	_, err = conn.LoadSession(ctx, piacp.LoadSessionRequest(firstSession, cwd,
		piacp.WithSessionPiOptions(piacp.NewPiOptions(
			piacp.WithPiExtraPathDirs(reboundDir),
			piacp.WithPiPermission("allow"),
		)),
	))
	require.NoError(t, err)
	require.Contains(t, prompt(firstSession, "path-rebound"), "PI_PATH_REBOUND")
	require.Equal(t, int64(3), model.toolCalls.Load())
}

func writePathCarrier(t *testing.T, dir string, marker string, forbidden ...string) {
	t.Helper()
	var command strings.Builder
	command.WriteString("#!/bin/sh\n" +
		"test \"${PATH%%:*}\" = " + shellSingleQuote(dir) + " || exit 91\n" +
		"path_list=\":${PATH}:\"\n")
	for _, forbiddenDir := range forbidden {
		command.WriteString("case \"${path_list}\" in *" + shellSingleQuote(":"+forbiddenDir+":") + "*) exit 92 ;; esac\n")
	}
	command.WriteString("printf '%s' " + shellSingleQuote(marker) + "\n")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "carrier-probe"), []byte(command.String()), 0o700))
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

type bashPathModelHarness struct {
	server    *httptest.Server
	toolCalls atomic.Int64
}

func newBashPathModelHarness(t *testing.T) *bashPathModelHarness {
	t.Helper()
	h := &bashPathModelHarness{}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		last := request.Messages[len(request.Messages)-1]
		if last.Role != "tool" {
			call := h.toolCalls.Add(1)
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-path", "object": "chat.completion.chunk", "created": call,
				"model": "bash-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{
						"role": "assistant",
						"tool_calls": []any{map[string]any{
							"index": 0, "id": fmt.Sprintf("bash-call-%d", call), "type": "function",
							"function": map[string]any{
								"name": "bash", "arguments": `{"command":"carrier-probe"}`,
							},
						}},
					},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-path", "object": "chat.completion.chunk", "created": call,
				"model": "bash-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls",
				}},
			})
		} else {
			var output string
			require.NoError(t, json.Unmarshal(last.Content, &output))
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-path-final", "object": "chat.completion.chunk", "created": 1,
				"model": "bash-model", "choices": []any{map[string]any{
					"index": 0, "finish_reason": nil,
					"delta": map[string]any{"role": "assistant", "content": strings.TrimSpace(output)},
				}},
			})
			writeSSE(t, w, map[string]any{
				"id": "chatcmpl-path-final", "object": "chat.completion.chunk", "created": 1,
				"model": "bash-model", "choices": []any{map[string]any{
					"index": 0, "delta": map[string]any{}, "finish_reason": "stop",
				}},
			})
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(h.server.Close)

	return h
}
