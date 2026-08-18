package piacp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

// imageArtifactTTL bounds how long emitted image output bytes stay
// replayable. The mirrored native rows are the canonical owner of those
// bytes; rows older than the TTL have their image data reclaimed at the next
// restore and the session becomes truthfully unloadable rather than
// replaying with holes.
const imageArtifactTTL = 24 * time.Hour

var (
	imageArtifactNow = time.Now
	encodeStoreRow   = json.Marshal
)

// loadCurrentStoreEntries loads a session's mirrored rows for a restore and
// enforces the bounded image-artifact window: expired output image bytes are
// reclaimed from the store and the restore fails as storage_failed, as does
// a restore that finds already-reclaimed artifacts.
func (a *Agent) loadCurrentStoreEntries(ctx context.Context, sessionID string) ([]SessionStoreEntry, error) {
	entries, err := a.loadStoreEntries(ctx, a.sessionStore(), SessionKey{SessionID: sessionID})
	if err != nil {
		return nil, err
	}

	swept, expired := scanImageArtifactRows(entries)
	if expired > 0 {
		if err := a.reclaimExpiredImageRows(ctx, sessionID, entries); err != nil {
			return nil, storageFailure(fmt.Sprintf("reclaim expired image artifacts: %v", err))
		}

		return nil, storageFailure("stored image artifacts outlived the artifact window and were reclaimed")
	}

	if swept > 0 {
		return nil, storageFailure("stored image artifact bytes are no longer available")
	}

	return entries, nil
}

// scanImageArtifactRows counts output-provenance image artifacts that were
// already reclaimed (swept) and those whose row timestamp has outlived the
// TTL (expired). Rows without a timestamp cannot age. User prompt images are
// session input, not emitted artifacts, and never expire here.
func scanImageArtifactRows(entries []SessionStoreEntry) (swept int, expired int) {
	now := imageArtifactNow()

	for _, entry := range entries {
		message, blocks, ok := outputImageMessage(entry)
		if !ok {
			continue
		}

		aged := message.Timestamp > 0 && now.Sub(time.UnixMilli(message.Timestamp)) > imageArtifactTTL

		for index := range blocks {
			if blocks[index].Type != contentBlockTypeImage {
				continue
			}

			switch {
			case blocks[index].Data == "":
				swept++
			case aged:
				expired++
			}
		}
	}

	return swept, expired
}

// reclaimExpiredImageRows rewrites the session's stored rows with expired
// output image data removed, so reclaimed bytes are gone from the store
// rather than merely ignored.
func (a *Agent) reclaimExpiredImageRows(ctx context.Context, sessionID string, entries []SessionStoreEntry) error {
	rewritten := make([]SessionStoreEntry, 0, len(entries))
	now := imageArtifactNow()

	for _, entry := range entries {
		message, _, ok := outputImageMessage(entry)
		if !ok || message.Timestamp <= 0 || now.Sub(time.UnixMilli(message.Timestamp)) <= imageArtifactTTL {
			rewritten = append(rewritten, entry)

			continue
		}

		stripped, err := stripRowImageData(entry)
		if err != nil {
			return err
		}

		rewritten = append(rewritten, stripped)
	}

	main := SessionKey{SessionID: sessionID}
	replacements := []SessionStoreReplacement{{Key: main, Entries: rewritten}}

	// Replace installs the session's whole committed generation, so the
	// adapter-owned lifecycle boundary log is carried through it. Dropping it
	// would erase the record of how the last incarnation ended, which is what
	// the next one opens its snapshot from.
	boundaries, err := a.sessionStore().Load(ctx, SessionKey{SessionID: sessionID, Subpath: SessionStoreLifecycleSubpath})
	if err != nil {
		return err
	}

	if len(boundaries) > 0 {
		replacements = append(replacements, SessionStoreReplacement{
			Key:     SessionKey{SessionID: sessionID, Subpath: SessionStoreLifecycleSubpath},
			Entries: boundaries,
		})
	}

	return a.sessionStore().Replace(ctx, main, replacements)
}

// outputImageMessage decodes one stored row when it is an output-provenance
// message (tool result or assistant) that carries at least one image block,
// returning the message and its decoded content blocks.
func outputImageMessage(entry SessionStoreEntry) (pi.AgentMessage, []pi.ContentBlock, bool) {
	row, ok := decodeStoreRow(entry)
	if !ok || row.Type != storeRowTypeMessage {
		return pi.AgentMessage{}, nil, false
	}

	var message pi.AgentMessage
	if err := json.Unmarshal(row.Message, &message); err != nil {
		return pi.AgentMessage{}, nil, false
	}

	if message.Role != messageRoleToolResult && message.Role != messageRoleAssistant {
		return pi.AgentMessage{}, nil, false
	}

	blocks, err := message.ContentBlocks()
	if err != nil {
		return pi.AgentMessage{}, nil, false
	}

	for index := range blocks {
		if blocks[index].Type == contentBlockTypeImage {
			return message, blocks, true
		}
	}

	return pi.AgentMessage{}, nil, false
}

// stripRowImageData removes image data payloads from one stored row while
// preserving every other field verbatim.
func stripRowImageData(entry SessionStoreEntry) (SessionStoreEntry, error) {
	var row map[string]any
	if err := json.Unmarshal(entry, &row); err != nil {
		return nil, fmt.Errorf("decode stored row: %w", err)
	}

	message, ok := row["message"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("stored row carries no message object")
	}

	content, ok := message["content"].([]any)
	if !ok {
		return nil, fmt.Errorf("stored message carries no content array")
	}

	for _, item := range content {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}

		if blockType, _ := block["type"].(string); blockType != contentBlockTypeImage {
			continue
		}

		delete(block, "data")
	}

	rewritten, err := encodeStoreRow(row)
	if err != nil {
		return nil, fmt.Errorf("encode reclaimed row: %w", err)
	}

	return rewritten, nil
}
