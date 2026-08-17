package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// call runs one typed client call against scripted command handling.
func scriptCall(t *testing.T, handle func(t *testing.T, harness *testHarness), invoke func(client *Client) error) error {
	t.Helper()

	harness := newTestHarness(t)
	done := make(chan error, 1)

	go func() {
		done <- invoke(harness.client)
	}()

	handle(t, harness)

	return <-done
}

func commandID(t *testing.T, fields map[string]any) string {
	t.Helper()

	id, ok := fields["id"].(string)
	require.True(t, ok)

	return id
}

func TestClientPrompt(t *testing.T) {
	t.Parallel()

	err := scriptCall(t, func(t *testing.T, harness *testHarness) {
		t.Helper()

		command := harness.nextCommand(t)
		require.Equal(t, "prompt", command["type"])
		require.Equal(t, "hello", command["message"])

		images, ok := command["images"].([]any)
		require.True(t, ok)
		require.Len(t, images, 1)

		image, ok := images[0].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "image", image["type"])
		require.Equal(t, "aWNvbg==", image["data"])
		require.Equal(t, "image/png", image["mimeType"])

		harness.respond(t, commandID(t, command), "prompt", "")
	}, func(client *Client) error {
		return client.Prompt(t.Context(), "hello", []ImageContent{NewImageContent("aWNvbg==", "image/png")})
	})
	require.NoError(t, err)
}

func TestClientPromptOmitsEmptyImages(t *testing.T) {
	t.Parallel()

	err := scriptCall(t, func(t *testing.T, harness *testHarness) {
		t.Helper()

		command := harness.nextCommand(t)
		require.NotContains(t, command, "images")
		harness.respond(t, commandID(t, command), "prompt", "")
	}, func(client *Client) error {
		return client.Prompt(t.Context(), "hello", nil)
	})
	require.NoError(t, err)
}

func TestClientSimpleCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		invoke     func(client *Client) error
		wantType   string
		wantFields map[string]any
	}{
		{
			name:     "abort",
			invoke:   func(client *Client) error { return client.Abort(t.Context()) },
			wantType: "abort",
		},
		{
			name: "set thinking level",
			invoke: func(client *Client) error {
				return client.SetThinkingLevel(t.Context(), "high")
			},
			wantType:   "set_thinking_level",
			wantFields: map[string]any{"level": "high"},
		},
		{
			name: "set auto retry",
			invoke: func(client *Client) error {
				return client.SetAutoRetry(t.Context(), false)
			},
			wantType:   "set_auto_retry",
			wantFields: map[string]any{"enabled": false},
		},
		{
			name: "set session name",
			invoke: func(client *Client) error {
				return client.SetSessionName(t.Context(), "acp")
			},
			wantType:   "set_session_name",
			wantFields: map[string]any{"name": "acp"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := scriptCall(t, func(t *testing.T, harness *testHarness) {
				t.Helper()

				command := harness.nextCommand(t)
				require.Equal(t, test.wantType, command["type"])

				for key, want := range test.wantFields {
					require.Equal(t, want, command[key])
				}

				harness.respond(t, commandID(t, command), test.wantType, "")
			}, test.invoke)
			require.NoError(t, err)
		})
	}
}

func TestClientCancellableCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		invoke     func(client *Client) (bool, error)
		wantType   string
		wantFields map[string]any
	}{
		{
			name: "new session with parent",
			invoke: func(client *Client) (bool, error) {
				return client.NewSession(t.Context(), "/tmp/parent.jsonl")
			},
			wantType:   "new_session",
			wantFields: map[string]any{"parentSession": "/tmp/parent.jsonl"},
		},
		{
			name: "switch session",
			invoke: func(client *Client) (bool, error) {
				return client.SwitchSession(t.Context(), "/tmp/session.jsonl")
			},
			wantType:   "switch_session",
			wantFields: map[string]any{"sessionPath": "/tmp/session.jsonl"},
		},
		{
			name: "clone",
			invoke: func(client *Client) (bool, error) {
				return client.Clone(t.Context())
			},
			wantType: "clone",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var (
				cancelled bool
				invokeErr error
			)

			err := scriptCall(t, func(t *testing.T, harness *testHarness) {
				t.Helper()

				command := harness.nextCommand(t)
				require.Equal(t, test.wantType, command["type"])

				for key, want := range test.wantFields {
					require.Equal(t, want, command[key])
				}

				harness.respond(t, commandID(t, command), test.wantType, `,"data":{"cancelled":true}`)
			}, func(client *Client) error {
				cancelled, invokeErr = test.invoke(client)

				return invokeErr
			})
			require.NoError(t, err)
			require.True(t, cancelled)
		})
	}
}

func TestClientNewSessionWithoutParentOmitsField(t *testing.T) {
	t.Parallel()

	err := scriptCall(t, func(t *testing.T, harness *testHarness) {
		t.Helper()

		command := harness.nextCommand(t)
		require.NotContains(t, command, "parentSession")
		harness.respond(t, commandID(t, command), "new_session", `,"data":{"cancelled":false}`)
	}, func(client *Client) error {
		cancelled, err := client.NewSession(t.Context(), "")
		require.False(t, cancelled)

		return err
	})
	require.NoError(t, err)
}

func TestClientDataCommands(t *testing.T) {
	t.Parallel()

	t.Run("get state", func(t *testing.T) {
		t.Parallel()

		var state SessionState

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_state",
				`,"data":{"model":{"id":"m1","provider":"anthropic"},"thinkingLevel":"medium",`+
					`"isStreaming":false,"isCompacting":false,"steeringMode":"all","followUpMode":"one-at-a-time",`+
					`"sessionFile":"/tmp/s.jsonl","sessionId":"abc","sessionName":"n",`+
					`"autoCompactionEnabled":true,"messageCount":5,"pendingMessageCount":0}`)
		}, func(client *Client) error {
			var err error
			state, err = client.GetState(t.Context())

			return err
		})
		require.NoError(t, err)
		require.Equal(t, "abc", state.SessionID)
		require.NotNil(t, state.Model)
		require.Equal(t, "anthropic", state.Model.Provider)
		require.Equal(t, "medium", state.ThinkingLevel)
		require.Equal(t, 5, state.MessageCount)
	})

	t.Run("set model", func(t *testing.T) {
		t.Parallel()

		var model Model

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			require.Equal(t, "set_model", command["type"])
			require.Equal(t, "openai", command["provider"])
			require.Equal(t, "gpt-4o", command["modelId"])
			harness.respond(t, commandID(t, command), "set_model",
				`,"data":{"id":"gpt-4o","name":"GPT-4o","api":"openai-responses",`+
					`"provider":"openai","reasoning":true,"input":["text","image"],`+
					`"contextWindow":200000,"maxTokens":16384,"cost":{"input":3.0}}`)
		}, func(client *Client) error {
			var err error
			model, err = client.SetModel(t.Context(), "openai", "gpt-4o")

			return err
		})
		require.NoError(t, err)
		require.Equal(t, "gpt-4o", model.ID)
		require.True(t, model.Reasoning)
		require.Equal(t, []string{"text", "image"}, model.Input)
		require.Equal(t, int64(200000), model.ContextWindow)
	})

	t.Run("get available models", func(t *testing.T) {
		t.Parallel()

		var models []Model

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_available_models",
				`,"data":{"models":[{"id":"a","provider":"p1"},{"id":"b","provider":"p2"}]}`)
		}, func(client *Client) error {
			var err error
			models, err = client.GetAvailableModels(t.Context())

			return err
		})
		require.NoError(t, err)
		require.Len(t, models, 2)
		require.Equal(t, "p2", models[1].Provider)
	})

	t.Run("get session stats", func(t *testing.T) {
		t.Parallel()

		var stats SessionStats

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_session_stats",
				`,"data":{"sessionId":"abc","userMessages":5,"assistantMessages":5,"toolCalls":12,`+
					`"toolResults":12,"totalMessages":22,`+
					`"tokens":{"input":50000,"output":10000,"cacheRead":40000,"cacheWrite":5000,"total":105000},`+
					`"cost":0.45,"contextUsage":{"tokens":60000,"contextWindow":200000,"percent":30}}`)
		}, func(client *Client) error {
			var err error
			stats, err = client.GetSessionStats(t.Context())

			return err
		})
		require.NoError(t, err)
		require.Equal(t, int64(105000), stats.Tokens.Total)
		require.NotNil(t, stats.ContextUsage)
		require.Equal(t, int64(200000), stats.ContextUsage.ContextWindow)
		require.NotNil(t, stats.ContextUsage.Tokens)
		require.Equal(t, int64(60000), *stats.ContextUsage.Tokens)
	})

	t.Run("get entries with cursor", func(t *testing.T) {
		t.Parallel()

		var entries Entries

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			require.Equal(t, "get_entries", command["type"])
			require.Equal(t, "abc123", command["since"])
			harness.respond(t, commandID(t, command), "get_entries",
				`,"data":{"entries":[{"type":"message","id":"def456"}],"leafId":"def456"}`)
		}, func(client *Client) error {
			var err error
			entries, err = client.GetEntries(t.Context(), "abc123")

			return err
		})
		require.NoError(t, err)
		require.Len(t, entries.Entries, 1)
		require.NotNil(t, entries.LeafID)
		require.Equal(t, "def456", *entries.LeafID)
	})

	t.Run("get entries empty session", func(t *testing.T) {
		t.Parallel()

		var entries Entries

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			require.NotContains(t, command, "since")
			harness.respond(t, commandID(t, command), "get_entries", `,"data":{"entries":[],"leafId":null}`)
		}, func(client *Client) error {
			var err error
			entries, err = client.GetEntries(t.Context(), "")

			return err
		})
		require.NoError(t, err)
		require.Empty(t, entries.Entries)
		require.Nil(t, entries.LeafID)
	})

	t.Run("get commands", func(t *testing.T) {
		t.Parallel()

		var commands []SlashCommand

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_commands",
				`,"data":{"commands":[{"name":"acp-ping","description":"liveness","source":"extension","path":"/x.ts"}]}`)
		}, func(client *Client) error {
			var err error
			commands, err = client.GetCommands(t.Context())

			return err
		})
		require.NoError(t, err)
		require.Len(t, commands, 1)
		require.Equal(t, "acp-ping", commands[0].Name)
		require.Equal(t, "extension", commands[0].Source)
	})
}

func TestClientDataCommandFailures(t *testing.T) {
	t.Parallel()

	t.Run("native error surfaces as command error", func(t *testing.T) {
		t.Parallel()

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.emit(t, `{"id":"`+commandID(t, command)+`","type":"response","command":"set_model",`+
				`"success":false,"error":"Model not found: nope/missing"}`)
		}, func(client *Client) error {
			_, err := client.SetModel(t.Context(), "nope", "missing")

			return err
		})

		var commandErr *CommandError

		require.ErrorAs(t, err, &commandErr)
		require.Equal(t, "Model not found: nope/missing", commandErr.Message)
	})

	t.Run("missing data fails", func(t *testing.T) {
		t.Parallel()

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_state", "")
		}, func(client *Client) error {
			_, err := client.GetState(t.Context())

			return err
		})
		require.ErrorContains(t, err, "returned no data")
	})

	t.Run("malformed data fails", func(t *testing.T) {
		t.Parallel()

		err := scriptCall(t, func(t *testing.T, harness *testHarness) {
			t.Helper()

			command := harness.nextCommand(t)
			harness.respond(t, commandID(t, command), "get_state", `,"data":{"sessionId":42}`)
		}, func(client *Client) error {
			_, err := client.GetState(t.Context())

			return err
		})
		require.ErrorContains(t, err, "decode get_state response data")
	})
}
