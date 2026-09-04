package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// replaceControlledStore injects Replace failures over the in-memory store.
type replaceControlledStore struct {
	SessionStore

	replaceErr error
}

func (s *replaceControlledStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if s.replaceErr != nil {
		return s.replaceErr
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func freezeImageArtifactClock(t *testing.T, now time.Time) {
	t.Helper()

	previous := imageArtifactNow
	imageArtifactNow = func() time.Time { return now }

	t.Cleanup(func() { imageArtifactNow = previous })
}

func imageArtifactRows(t *testing.T, timestamp time.Time) []SessionStoreEntry {
	t.Helper()

	png := fixtureBase64(t, "valid.png")

	return []SessionStoreEntry{
		json.RawMessage(`{"type":"session","cwd":` + testCwdJSON + `}`),
		messageRow(t, pi.AgentMessage{
			Role:      messageRoleUser,
			Timestamp: timestamp.UnixMilli(),
			Content:   json.RawMessage(`[{"type":"text","text":"draw"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
		messageRow(t, pi.AgentMessage{
			Role:       messageRoleToolResult,
			ToolCallID: "call",
			Timestamp:  timestamp.UnixMilli(),
			Content:    json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
	}
}

func TestLoadCurrentStoreEntriesFreshArtifacts(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	store := NewInMemorySessionStore()
	rows := imageArtifactRows(t, now.Add(-time.Hour))
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, rows))
	appendLifecycleBoundaryForRows(t, store, validSessionUUID, len(rows))

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	entries, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	require.NoError(t, err)
	require.Len(t, entries, len(rows))
}

func TestLoadCurrentStoreEntriesRequiresTheLastDurableBoundary(t *testing.T) {
	t.Parallel()

	t.Run("rejects an unproven suffix", func(t *testing.T) {
		t.Parallel()

		store := NewInMemorySessionStore()
		rows := []SessionStoreEntry{
			json.RawMessage(`{"row":1}`),
			json.RawMessage(`{"row":2}`),
			json.RawMessage(`{"row":3}`),
		}
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, rows))
		appendLifecycleBoundaryForRows(t, store, validSessionUUID, 2)

		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
		_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
		require.ErrorContains(t, err, "has 3 native rows but lifecycle boundary records 2")
	})

	t.Run("rejects a boundary ahead of native storage", func(t *testing.T) {
		t.Parallel()

		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, []SessionStoreEntry{
			json.RawMessage(`{"row":1}`),
		}))
		appendLifecycleBoundaryForRows(t, store, validSessionUUID, 2)

		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
		_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
		require.ErrorContains(t, err, "has 1 native rows but lifecycle boundary records 2")
	})

	t.Run("rejects a native log without a boundary", func(t *testing.T) {
		t.Parallel()

		store := NewInMemorySessionStore()
		require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, []SessionStoreEntry{
			json.RawMessage(`{"row":1}`),
		}))

		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
		_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
		require.ErrorContains(t, err, "no lifecycle boundary")
	})
}

func TestLoadCurrentStoreEntriesReclaimsExpiredArtifacts(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	png := fixtureBase64(t, "valid.png")
	store := NewInMemorySessionStore()
	rows := imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute))
	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: validSessionUUID}, rows))
	appendLifecycleBoundaryForRows(t, store, validSessionUUID, len(rows))

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	requireImageOutputFailure(t, err, imageReasonStorageFailed)

	swept, loadErr := store.Load(t.Context(), SessionKey{SessionID: validSessionUUID})
	require.NoError(t, loadErr)
	require.Len(t, swept, len(rows))

	encoded, marshalErr := json.Marshal(swept)
	require.NoError(t, marshalErr)
	require.Equal(t, 1, strings.Count(string(encoded), png),
		"the tool image bytes are reclaimed while the user prompt image stays session input")

	// A later restore finds the reclaimed artifact and stays truthfully
	// unloadable rather than replaying with a hole.
	_, _, err = agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	data := requireImageOutputFailure(t, err, imageReasonStorageFailed)
	require.Contains(t, data[jsonFieldMessage], "no longer available")
}

func TestLoadCurrentStoreEntriesExpiryEdges(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	store := NewInMemorySessionStore()
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	key := SessionKey{SessionID: validSessionUUID}
	png := fixtureBase64(t, "valid.png")

	// A row exactly at the TTL boundary has not expired.
	exactRows := imageArtifactRows(t, now.Add(-imageArtifactTTL))
	require.NoError(t, store.Append(t.Context(), key, exactRows))
	appendLifecycleBoundaryForRows(t, store, key.SessionID, len(exactRows))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	require.NoError(t, err)

	// Output image rows without a timestamp cannot age.
	require.NoError(t, store.Delete(t.Context(), key))
	agelessKey := SessionKey{SessionID: "01234567-89ab-cdef-0123-456789abcdff"}
	require.NoError(t, store.Append(t.Context(), agelessKey, []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role:       messageRoleToolResult,
			ToolCallID: "call",
			Content:    json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
	}))
	appendLifecycleBoundaryForRows(t, store, agelessKey.SessionID, 1)
	_, _, err = agent.loadCurrentStoreEntries(t.Context(), agelessKey.SessionID)
	require.NoError(t, err)

	// Expired user prompt images are session input, never reclaimed.
	userKey := SessionKey{SessionID: "01234567-89ab-cdef-0123-456789abcd00"}
	require.NoError(t, store.Append(t.Context(), userKey, []SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role:      messageRoleUser,
			Timestamp: now.Add(-imageArtifactTTL - time.Hour).UnixMilli(),
			Content:   json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
	}))
	appendLifecycleBoundaryForRows(t, store, userKey.SessionID, 1)
	_, _, err = agent.loadCurrentStoreEntries(t.Context(), userKey.SessionID)
	require.NoError(t, err)
}

func TestLoadCurrentStoreEntriesErrorBranches(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	loadFailure := errors.New("load")
	agent := NewAgent(
		WithLogger(slog.New(slog.DiscardHandler)),
		WithSessionStore(&errorSessionStore{loadErr: loadFailure}),
	)
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	require.ErrorIs(t, err, loadFailure)

	store := &replaceControlledStore{SessionStore: NewInMemorySessionStore(), replaceErr: errors.New("replace")}
	require.NoError(t, store.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
		imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute)),
	))
	appendLifecycleBoundaryForRows(t, store, validSessionUUID, 3)

	agent = NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	_, _, err = agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	data := requireImageOutputFailure(t, err, imageReasonStorageFailed)
	require.Contains(t, data[jsonFieldMessage], "reclaim expired image artifacts")
}

func TestScanImageArtifactRowsSkipsUndecodableContent(t *testing.T) {
	freezeImageArtifactClock(t, time.Now())

	swept, expired := scanImageArtifactRows([]SessionStoreEntry{
		json.RawMessage(`bad`),
		json.RawMessage(`{"type":"session"}`),
		json.RawMessage(`{"type":"message","message":"bad"}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleAssistant, Content: json.RawMessage(`"plain text"`)}),
		// Content that is neither a string nor a block array cannot decode.
		json.RawMessage(`{"type":"message","message":{"role":"assistant","content":{"unexpected":true}}}`),
		messageRow(t, pi.AgentMessage{Role: messageRoleToolResult, ToolCallID: "call", Content: json.RawMessage(`[{"type":"text","text":"no images"}]`)}),
	})
	require.Zero(t, swept)
	require.Zero(t, expired)
}

func TestScanImageArtifactRowsMixedContentAndExpiry(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	png := fixtureBase64(t, "valid.png")
	// One expired tool result interleaving a text block with an image block,
	// so the non-image block is walked past and the image is counted expired.
	swept, expired := scanImageArtifactRows([]SessionStoreEntry{
		messageRow(t, pi.AgentMessage{
			Role:       messageRoleToolResult,
			ToolCallID: "call",
			Timestamp:  now.Add(-imageArtifactTTL - time.Minute).UnixMilli(),
			Content:    json.RawMessage(`[{"type":"text","text":"here"},{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
		}),
	})
	require.Zero(t, swept)
	require.Equal(t, 1, expired)
}

func TestStripRowImageData(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	row := messageRow(t, pi.AgentMessage{
		Role:       messageRoleToolResult,
		ToolCallID: "call",
		Timestamp:  42,
		Content:    json.RawMessage(`[{"type":"text","text":"kept"},{"type":"image","data":"` + png + `","mimeType":"image/png"},"odd"]`),
	})

	stripped, err := stripRowImageData(row)
	require.NoError(t, err)
	require.NotContains(t, string(stripped), png)
	require.Contains(t, string(stripped), `"kept"`)
	require.Contains(t, string(stripped), `"image/png"`)
	require.Contains(t, string(stripped), `"timestamp":42`)

	_, err = stripRowImageData(json.RawMessage(`bad`))
	require.ErrorContains(t, err, "decode stored row")
	_, err = stripRowImageData(json.RawMessage(`{"type":"message","message":"text"}`))
	require.ErrorContains(t, err, "no message object")
	_, err = stripRowImageData(json.RawMessage(`{"type":"message","message":{"content":"text"}}`))
	require.ErrorContains(t, err, "no content array")

	previous := encodeStoreRow
	encodeStoreRow = func(any) ([]byte, error) { return nil, errors.New("encode") }

	t.Cleanup(func() { encodeStoreRow = previous })

	_, err = stripRowImageData(row)
	require.ErrorContains(t, err, "encode reclaimed row")
}

func TestReclaimExpiredImageRowsEncodeFailure(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	previous := encodeStoreRow
	encodeStoreRow = func(any) ([]byte, error) { return nil, errors.New("encode") }

	t.Cleanup(func() { encodeStoreRow = previous })

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(NewInMemorySessionStore()))
	err := agent.reclaimExpiredImageRows(
		t.Context(),
		validSessionUUID,
		imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute)),
	)
	require.ErrorContains(t, err, "encode reclaimed row")
}

func TestLoadSessionFailsOnExpiredImageArtifacts(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	store := NewInMemorySessionStore()
	require.NoError(t, store.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
		imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute)),
	))
	appendLifecycleBoundaryForRows(t, store, validSessionUUID, 3)

	client := newStubPiClient()
	agent := newStubClientAgent(t, client, WithSessionStore(store))

	_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{
		SessionId: acp.SessionId(validSessionUUID),
		Cwd:       t.TempDir(),
	})
	requireImageOutputFailure(t, err, imageReasonStorageFailed)
}

func TestForkSessionFailsOnExpiredImageArtifacts(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	store := newFaultySessionStore()
	appendForkParentRows(t, store, imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute))...)

	client := newStubPiClient()
	agent := newStubClientAgent(t, client, WithSessionStore(store))

	_, err := agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, forkRaw(t, forkParams(t)))
	requireImageOutputFailure(t, err, imageReasonStorageFailed)
}

// lifecycleLoadFailingStore fails loads of the adapter-owned lifecycle
// boundary subpath while every other key loads normally.
type lifecycleLoadFailingStore struct {
	SessionStore
	err       error
	failAfter int
	loads     int
}

func (s *lifecycleLoadFailingStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if key.Subpath == SessionStoreLifecycleSubpath {
		s.loads++
		if s.loads > s.failAfter {
			return nil, s.err
		}
	}

	return s.SessionStore.Load(ctx, key)
}

// TestReclaimExpiredImageRowsCarriesTheLifecycleBoundaryLog pins that the
// reclaim's wholesale replacement keeps the adapter-owned boundary record: it
// is the only record of how the last incarnation ended, and the next
// incarnation opens its snapshot from it.
func TestReclaimExpiredImageRowsCarriesTheLifecycleBoundaryLog(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	store := NewInMemorySessionStore()
	lifecycleKey := SessionKey{SessionID: validSessionUUID, Subpath: SessionStoreLifecycleSubpath}
	rows := imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute))
	require.NoError(t, store.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
		rows,
	))
	boundary := appendLifecycleBoundaryForRows(t, store, validSessionUUID, len(rows))

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	requireImageOutputFailure(t, err, imageReasonStorageFailed)

	carried, loadErr := store.Load(t.Context(), lifecycleKey)
	require.NoError(t, loadErr)
	require.Len(t, carried, 1)
	require.JSONEq(t, string(boundary), string(carried[0]))
}

// TestReclaimExpiredImageRowsBoundaryLoadFailure pins that a store that cannot
// read the boundary log fails the reclaim closed rather than replacing the
// generation without it.
func TestReclaimExpiredImageRowsBoundaryLoadFailure(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	freezeImageArtifactClock(t, now)

	base := NewInMemorySessionStore()
	store := &lifecycleLoadFailingStore{
		SessionStore: base,
		err:          errors.New("boundary log unreadable"),
		failAfter:    1,
	}
	require.NoError(t, store.Append(
		t.Context(),
		SessionKey{SessionID: validSessionUUID},
		imageArtifactRows(t, now.Add(-imageArtifactTTL-time.Minute)),
	))
	appendLifecycleBoundaryForRows(t, store, validSessionUUID, 3)

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithSessionStore(store))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), validSessionUUID)
	data := requireImageOutputFailure(t, err, imageReasonStorageFailed)
	require.Contains(t, data[jsonFieldMessage], "boundary log unreadable")
}
