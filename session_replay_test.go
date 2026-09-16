package piacp

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

// replayMessage decodes one message the way replay does, so a case states only
// its native content.
func replayMessage(t *testing.T, role string, content string, opts ...func(*pi.AgentMessage)) ([]acp.SessionUpdate, *image.OutputError) {
	t.Helper()

	message := pi.AgentMessage{Role: role}
	if content != "" {
		message.Content = json.RawMessage(content)
	}

	for _, opt := range opts {
		opt(&message)
	}

	blocks, err := message.ContentBlocks()
	if err != nil {
		blocks = nil
	}

	return replayUpdates(message, blocks, image.DefaultLimits())
}

func TestReplayUpdates(t *testing.T) {
	t.Parallel()

	updates, failure := replayMessage(t, messageRoleUser, `[{"type":"text","text":"hi"},{"type":"image","data":"`+tinyPNG+`","mimeType":"image/png"}]`)
	require.Nil(t, failure)
	require.Len(t, updates, 2)
	require.NotNil(t, updates[1].UserMessageChunk.Content.Image)

	updates, failure = replayMessage(t, messageRoleAssistant,
		`[{"type":"thinking","thinking":"t"},{"type":"text","text":"a"},{"type":"toolCall","id":"c1","name":"bash","arguments":{"command":"ls"}},{"type":"image","data":"`+tinyPNG+`","mimeType":"image/png"}]`,
		func(m *pi.AgentMessage) { m.ACPMessageID = "m1" })
	require.Nil(t, failure)
	require.Len(t, updates, 4)
	require.Equal(t, "m1", *updates[1].AgentMessageChunk.MessageId)
	require.NotNil(t, updates[2].ToolCall)
	require.NotNil(t, updates[3].AgentMessageChunk.Content.Image)

	_, failure = replayMessage(t, messageRoleAssistant, `[{"type":"image","data":"!!"}]`)
	require.NotNil(t, failure)

	updates, failure = replayMessage(t, messageRoleToolResult, `[{"type":"text","text":"no"}]`,
		func(m *pi.AgentMessage) { m.ToolCallID = "c1"; m.IsError = true })
	require.Nil(t, failure)
	require.Len(t, updates, 1)
	require.Len(t, updates[0].ToolCallUpdate.Content, 1)

	updates, failure = replayMessage(t, messageRoleToolResult, "")
	require.Nil(t, failure)
	require.Empty(t, updates)

	updates, failure = replayMessage(t, "custom", "")
	require.Nil(t, failure)
	require.Empty(t, updates)
}

// appendStoredRow republishes a session's generation with one more native row.
func appendStoredRow(t *testing.T, store acpcore.SessionStore, sessionID acp.SessionId, row string) {
	t.Helper()

	main := acpcore.SessionKey{SessionID: string(sessionID)}

	generation, err := store.Load(context.Background(), main.SessionID)
	require.NoError(t, err)

	replacements := make([]acpcore.SessionStoreReplacement, 0, len(generation))

	for subpath, entries := range generation {
		if subpath == acpcore.SessionStoreMainSubpath {
			entries = append(slices.Clone(entries), acpcore.SessionStoreEntry(row))
		}

		replacements = append(replacements, acpcore.SessionStoreReplacement{
			Key: acpcore.SessionKey{SessionID: main.SessionID, Subpath: subpath}, Entries: entries,
		})
	}

	require.NoError(t, store.Replace(context.Background(), main, replacements))
}

func TestRestoreRejectsMalformedNativeRow(t *testing.T) {
	t.Parallel()

	rows := map[string]string{
		"malformed_type":      `{"type":7}`,
		"undecodable_content": `{"type":"message","message":{"role":"user","content":5}}`,
		"undecodable_message": `{"type":"message","message":7}`,
	}

	restores := map[string]func(h *harness, id acp.SessionId, cwd string) error{
		"load": func(h *harness, id acp.SessionId, cwd string) error {
			_, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(id, cwd))

			return err
		},
		"resume": func(h *harness, id acp.SessionId, cwd string) error {
			_, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(id, cwd))

			return err
		},
	}

	for rowName, row := range rows {
		for restoreName, restore := range restores {
			t.Run(rowName+"/"+restoreName, func(t *testing.T) {
				t.Parallel()

				store := acpcore.NewInMemorySessionStore()
				h := newHarness(t, WithSessionStore(store))
				h.initialize()
				cwd := t.TempDir()
				created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
				require.NoError(t, err)
				_, err = h.prompt(created.SessionId, "HELLO", nil)
				require.NoError(t, err)
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
				require.NoError(t, err)

				appendStoredRow(t, store, created.SessionId, row)

				err = restore(h, created.SessionId, cwd)
				require.Error(t, err, "restore succeeded after silently forwarding a row it cannot decode")
				require.Equal(t, vendor+"_restore_failed", requestErrorData(t, err)["error"])
			})
		}
	}
}

func TestLoadRejectsCorruptStoredImage(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)

	appendStoredRow(t, store, created.SessionId,
		`{"type":"message","message":{"role":"user","content":[{"type":"image","data":"!!!","mimeType":"image/png"}]}}`)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.Error(t, err)
	require.Equal(t, vendor+"_restore_failed", requestErrorData(t, err)["error"])
}
