package piacp

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// bmpBytes is a minimal BMP header: a raster the input allowlist excludes
// but output sniff-and-emit carries truthfully.
func bmpBytes() []byte {
	return []byte{'B', 'M', 0x1E, 0, 0, 0, 0, 0, 0, 0, 0x1A, 0, 0, 0, 0x0C, 0, 0, 0, 1, 0, 1, 0, 1, 0, 24, 0, 0xFF, 0xFF, 0xFF, 0}
}

func requireImageOutputFailure(t *testing.T, err error, reason string) map[string]any {
	t.Helper()

	data := requirePiTurnFailure(t, err, failureCauseTransport)
	require.Equal(t, failureStageImageOutput, data[failureFieldStage])
	require.Equal(t, reason, data[failureFieldReason])

	return data
}

func TestNormalizeOutputImage(t *testing.T) {
	png := fixtureBase64(t, "valid.png")

	image, failure := normalizeOutputImage(png, "image/png", maxImageFrameBytes)
	require.Nil(t, failure)
	require.Equal(t, png, image.data)
	require.Equal(t, "image/png", image.mime)
	require.Len(t, image.fingerprint, 64)
	require.EqualValues(t, len(fixtureBytes(t, "valid.png")), image.sizeBytes)

	tests := []struct {
		name     string
		data     string
		declared string
		reason   string
	}{
		{name: "empty data", data: "", declared: "image/png", reason: imageReasonNotARaster},
		{name: "invalid base64", data: "not base64!", declared: "image/png", reason: imageErrorInvalidBase64},
		{name: "unsniffable", data: base64.StdEncoding.EncodeToString([]byte("plain text")), declared: "", reason: imageReasonNotARaster},
		{name: "declared conflict", data: fixtureBase64(t, "valid.gif"), declared: "image/png", reason: imageErrorMediaTypeMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, failure := normalizeOutputImage(test.data, test.declared, maxImageFrameBytes)
			require.NotNil(t, failure)
			require.Equal(t, test.reason, failure.reason)
			require.NotEmpty(t, failure.Error())
		})
	}
}

func TestNormalizeOutputImageSniffAndEmit(t *testing.T) {
	bmp := base64.StdEncoding.EncodeToString(bmpBytes())

	image, failure := normalizeOutputImage(bmp, "", maxImageFrameBytes)
	require.Nil(t, failure)
	require.Equal(t, "image/bmp", image.mime, "output is not format-allowlisted; the sniffed MIME is emitted")

	image, failure = normalizeOutputImage(bmp, "application/octet-stream", maxImageFrameBytes)
	require.Nil(t, failure)
	require.Equal(t, "image/bmp", image.mime, "an unknown declared MIME defers to the sniff")

	animated, failure := normalizeOutputImage(fixtureBase64(t, "animated.webp"), "image/webp", maxImageFrameBytes)
	require.Nil(t, failure)
	require.Equal(t, "image/webp", animated.mime, "animation is not rejected on output")
}

func TestNormalizeOutputImagePerImageBoundary(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	data := base64.StdEncoding.EncodeToString(png)
	size := int64(len(png))

	_, failure := normalizeOutputImage(data, "image/png", size)
	require.Nil(t, failure)

	_, failure = normalizeOutputImage(data, "image/png", size-1)
	require.NotNil(t, failure)
	require.Equal(t, imageErrorTooLarge, failure.reason)
	require.Equal(t, size, failure.sizeBytes)
	require.Equal(t, size-1, failure.maxBytes)
}

func TestDeclaredMIMEConflicts(t *testing.T) {
	require.False(t, declaredMIMEConflicts("", "image/png"))
	require.False(t, declaredMIMEConflicts("image/png", "image/png"))
	require.False(t, declaredMIMEConflicts("application/octet-stream", "image/png"))
	require.True(t, declaredMIMEConflicts("image/jpeg", "image/png"))
	require.True(t, declaredMIMEConflicts("image/bmp", "image/png"))
}

func TestImageOutputTurnFailureEnvelope(t *testing.T) {
	data := requireImageOutputFailure(t, imageOutputTurnFailure(&imageOutputError{
		reason:    imageErrorTooLarge,
		message:   "too big",
		sizeBytes: 10,
		maxBytes:  9,
	}), imageErrorTooLarge)
	require.Equal(t, "too big", data[jsonFieldMessage])
	require.InDelta(t, 10, data[jsonFieldSizeBytes], 0)
	require.InDelta(t, 9, data[jsonFieldMaxBytes], 0)

	data = requireImageOutputFailure(t, storageFailure("gone"), imageReasonStorageFailed)
	require.NotContains(t, data, jsonFieldSizeBytes)
	require.NotContains(t, data, jsonFieldMaxBytes)
}

func TestReplayImageFailureMapping(t *testing.T) {
	tooLarge := &imageOutputError{reason: imageErrorTooLarge, message: "big", sizeBytes: 2, maxBytes: 1}
	require.Same(t, tooLarge, replayImageFailure(tooLarge))

	corrupt := replayImageFailure(&imageOutputError{reason: imageErrorInvalidBase64, message: "bad"})
	require.Equal(t, imageReasonStorageFailed, corrupt.reason)
	require.Contains(t, corrupt.message, "bad")
}

func TestMapToolContentBlocks(t *testing.T) {
	png := fixtureBase64(t, "valid.png")

	items, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "hello"},
		{Type: contentBlockTypeText},
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
		{Type: "other"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)
	require.Len(t, items, 2)
	require.Equal(t, "hello", items[0].content.Content.Content.Text.Text)
	require.Equal(t, png, items[1].content.Content.Content.Image.Data)
	require.Positive(t, items[1].imageBytes)

	_, failure = mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: "not base64!", MimeType: "image/png"},
	}, maxImageFrameBytes)
	require.NotNil(t, failure)
	require.Equal(t, imageErrorInvalidBase64, failure.reason)
}

func TestMergeToolContentReplaceSemantics(t *testing.T) {
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")

	first, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "one"},
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)

	// A cumulative native snapshot merges to itself plus new items.
	second, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "one"},
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
		{Type: contentBlockTypeImage, Data: gif, MimeType: "image/gif"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)

	merged := mergeToolContent(first, second)
	require.Len(t, merged, 3)

	// A native snapshot that dropped an earlier item still keeps it: a
	// delivered item never disappears from a later array.
	shrunk, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "two"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)

	merged = mergeToolContent(merged, shrunk)
	require.Len(t, merged, 4)
	require.Equal(t, "one", merged[0].content.Content.Content.Text.Text)
	require.Equal(t, "two", merged[3].content.Content.Content.Text.Text)

	// Genuinely distinct duplicates are kept by occurrence counting.
	twice, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)
	require.Len(t, mergeToolContent(nil, twice), 2)
	require.Len(t, mergeToolContent(twice, twice), 2)
}

func TestCheckToolContentBudgetBoundary(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	gif := fixtureBytes(t, "valid.gif")
	total := int64(len(png) + len(gif))

	items, failure := mapToolContentBlocks([]pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "free"},
		{Type: contentBlockTypeImage, Data: base64.StdEncoding.EncodeToString(png), MimeType: "image/png"},
		{Type: contentBlockTypeImage, Data: base64.StdEncoding.EncodeToString(gif), MimeType: "image/gif"},
	}, maxImageFrameBytes)
	require.Nil(t, failure)

	require.Nil(t, checkToolContentBudget(items, total))

	failure = checkToolContentBudget(items, total-1)
	require.NotNil(t, failure)
	require.Equal(t, imageErrorTooLarge, failure.reason)
	require.Equal(t, total, failure.sizeBytes)
	require.Equal(t, total-1, failure.maxBytes)
}

func TestBuildToolContent(t *testing.T) {
	png := fixtureBytes(t, "valid.png")
	blocks := []pi.ContentBlock{{Type: contentBlockTypeImage, Data: base64.StdEncoding.EncodeToString(png), MimeType: "image/png"}}

	items, failure := buildToolContent(nil, blocks, defaultImageLimits())
	require.Nil(t, failure)
	require.Len(t, toolContentSnapshot(items), 1)

	_, failure = buildToolContent(nil, blocks, ImageLimits{
		MaxOutputBytesPerImage:    defaultImageLimitBytes,
		MaxOutputBytesPerToolCall: int64(len(png)) - 1,
	})
	require.NotNil(t, failure)
	require.Equal(t, imageErrorTooLarge, failure.reason)

	_, failure = buildToolContent(nil, []pi.ContentBlock{{Type: contentBlockTypeImage, Data: "!", MimeType: "image/png"}}, defaultImageLimits())
	require.NotNil(t, failure)
}

func newImageOutputSession(t *testing.T) (*agentSession, *directAgentClient) {
	t.Helper()

	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	connection := newDirectAgentClient()
	agent.setConnection(connection)

	return &agentSession{agent: agent, id: "id", client: newStubPiClient(), proc: newStubProcess(false)}, connection
}

func TestPublishNativeToolUpdateSnapshots(t *testing.T) {
	session, connection := newImageOutputSession(t)
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")

	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
	}))
	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
		{Type: contentBlockTypeImage, Data: gif, MimeType: "image/gif"},
	}))
	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", nil),
		"an empty snapshot is skipped rather than erasing delivered content")

	require.Len(t, connection.updates, 2)
	first := connection.updates[0].ToolCallUpdate
	require.Len(t, first.Content, 1)
	second := connection.updates[1].ToolCallUpdate
	require.Len(t, second.Content, 2, "every later array contains every earlier item")
	require.Equal(t, png, second.Content[0].Content.Content.Image.Data)
	require.Equal(t, gif, second.Content[1].Content.Content.Image.Data)
	require.True(t, session.turnEmittedImages())
}

func TestPublishNativeToolTerminalSnapshotAndStatusOnly(t *testing.T) {
	session, connection := newImageOutputSession(t)
	png := fixtureBase64(t, "valid.png")

	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
	}))
	require.NoError(t, session.publishNativeToolTerminal(t.Context(), "call", acp.ToolCallStatusCompleted, &pi.ToolResult{
		Content: []pi.ContentBlock{
			{Type: contentBlockTypeImage, Data: png, MimeType: "image/png"},
			{Type: contentBlockTypeText, Text: "done"},
		},
	}))
	require.NoError(t, session.publishNativeToolTerminal(t.Context(), "call", acp.ToolCallStatusCompleted, nil),
		"a terminal after the terminal is dropped")

	require.Len(t, connection.updates, 2)
	terminal := connection.updates[1].ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusCompleted, *terminal.Status)
	require.Len(t, terminal.Content, 2)

	statusOnly, connection := newImageOutputSession(t)
	require.NoError(t, statusOnly.publishNativeToolTerminal(t.Context(), "other", acp.ToolCallStatusFailed, nil))
	require.Len(t, connection.updates, 1)
	require.Nil(t, connection.updates[0].ToolCallUpdate.Content, "a status-only update omits content entirely")
}

func TestPublishNativeToolImageRefusalKeepsTheTurnWithAttribution(t *testing.T) {
	session, connection := newImageOutputSession(t)

	// A verdict the model can act on fails the tool call, carries the guidance
	// as that call's own content, and leaves the turn running.
	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: "not base64!", MimeType: "image/png"},
	}))

	require.Len(t, connection.updates, 1)
	failed := connection.updates[0].ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusFailed, *failed.Status)
	require.Len(t, failed.Content, 1)
	require.Equal(t, imageGuidanceInvalidBase64, failed.Content[0].Content.Content.Text.Text)

	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "late"},
	}), "the failed call accepts no further content")
	require.Len(t, connection.updates, 1)

	// The turn keeps making progress after the refusal.
	require.NoError(t, session.publishNativeToolUpdate(t.Context(), "other", []pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "still here"},
	}))
	require.Len(t, connection.updates, 2)

	terminalSession, terminalConnection := newImageOutputSession(t)
	require.NoError(t, terminalSession.publishNativeToolTerminal(
		t.Context(), "call", acp.ToolCallStatusCompleted, &pi.ToolResult{
			Content: []pi.ContentBlock{{Type: contentBlockTypeImage, Data: "!", MimeType: "image/png"}},
		},
	))
	require.Len(t, terminalConnection.updates, 1)
	require.Equal(t, acp.ToolCallStatusFailed, *terminalConnection.updates[0].ToolCallUpdate.Status)
	require.Len(t, terminalConnection.updates[0].ToolCallUpdate.Content, 1)

	// The artifact window breaking is not something the model can act on, so
	// the tool call reports failed without guidance and the turn ends.
	storageSession, storageConnection := newImageOutputSession(t)
	storageState := storageSession.lockToolCallState("call")
	storageErr := storageSession.failToolImageOutputLocked(t.Context(), "call", storageState, sweptImageFailure())
	storageState.mu.Unlock()

	requireImageOutputFailure(t, storageErr, imageReasonStorageFailed)
	require.Len(t, storageConnection.updates, 1)
	require.Nil(t, storageConnection.updates[0].ToolCallUpdate.Content,
		"the failed tool state for a storage failure is status-only")

	emitFail, emitConnection := newImageOutputSession(t)
	emitConnection.updateErr = errors.New("emit")
	err := emitFail.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeImage, Data: "!", MimeType: "image/png"},
	})
	require.ErrorContains(t, err, "emit")

	emitFatal, emitFatalConnection := newImageOutputSession(t)
	emitFatalConnection.updateErr = errors.New("emit")
	fatalState := emitFatal.lockToolCallState("call")
	fatalErr := emitFatal.failToolImageOutputLocked(t.Context(), "call", fatalState, sweptImageFailure())
	fatalState.mu.Unlock()

	requireImageOutputFailure(t, fatalErr, imageReasonStorageFailed)
	require.ErrorContains(t, fatalErr, "emit")
}

// TestImageOutputGuidanceSplitsRecoverableFromFatal pins the blast radius of
// every image-output verdict: an ordinary mistake that can be retried carries
// guidance and keeps the turn, the adapter's own artifact window breaking does
// not.
func TestImageOutputGuidanceSplitsRecoverableFromFatal(t *testing.T) {
	recoverable := map[string]string{
		imageErrorTooLarge:          imageGuidanceTooLarge,
		imageReasonNotARaster:       imageGuidanceNotRaster,
		imageErrorInvalidBase64:     imageGuidanceInvalidBase64,
		imageErrorMediaTypeMismatch: imageGuidanceMIMEMismatched,
	}

	for reason, guidance := range recoverable {
		message, ok := imageOutputGuidance(&imageOutputError{reason: reason})
		require.True(t, ok, reason)
		require.Equal(t, guidance, message)

		// The guidance says what to do next and never quotes a size, a media
		// type, or the bytes it refused.
		require.NotRegexp(t, `[0-9]`, message)
		require.NotContains(t, message, "limit")
	}

	_, ok := imageOutputGuidance(&imageOutputError{reason: imageReasonStorageFailed})
	require.False(t, ok)
}

func TestPublishNativeToolUpdateEmitFailure(t *testing.T) {
	session, connection := newImageOutputSession(t)
	connection.updateErr = errors.New("emit")

	err := session.publishNativeToolUpdate(t.Context(), "call", []pi.ContentBlock{
		{Type: contentBlockTypeText, Text: "text"},
	})
	require.ErrorContains(t, err, "emit")

	err = session.publishNativeToolTerminal(t.Context(), "call", acp.ToolCallStatusCompleted, &pi.ToolResult{
		Content: []pi.ContentBlock{{Type: contentBlockTypeText, Text: "text"}},
	})
	require.ErrorContains(t, err, "emit")
}

func TestEmitAssistantImages(t *testing.T) {
	session, connection := newImageOutputSession(t)
	png := fixtureBase64(t, "valid.png")
	gif := fixtureBase64(t, "valid.gif")
	state := &promptTurnState{}
	messageID := "018f47ad-839d-7f70-b7f7-c01d6d97b675"
	message := pi.AgentMessage{
		Role:         messageRoleAssistant,
		ACPMessageID: messageID,
		Content: json.RawMessage(`[{"type":"text","text":"look"},` +
			`{"type":"image","data":"` + png + `","mimeType":"image/png"},` +
			`{"type":"image","data":"` + gif + `","mimeType":"image/gif"}]`),
	}

	require.NoError(t, session.emitAssistantImages(t.Context(), message, state))
	require.Len(t, connection.updates, 2, "each agent image travels alone in its own chunk")
	require.Equal(t, png, connection.updates[0].AgentMessageChunk.Content.Image.Data)
	require.Equal(t, messageID, *connection.updates[0].AgentMessageChunk.MessageId)
	require.Equal(t, gif, connection.updates[1].AgentMessageChunk.Content.Image.Data)
	require.True(t, session.turnEmittedImages())

	require.NoError(t, session.emitAssistantImages(t.Context(), message, state))
	require.Len(t, connection.updates, 2, "repeated native delivery does not duplicate an artifact")

	require.NoError(t, session.emitAssistantImages(t.Context(), pi.AgentMessage{
		Role: messageRoleAssistant, Content: json.RawMessage(`bad`),
	}, state))
	require.Len(t, connection.updates, 2)

	// An assistant image has no tool call to attribute to, so the guidance
	// takes the image's place as agent text and the turn runs on.
	require.NoError(t, session.emitAssistantImages(t.Context(), pi.AgentMessage{
		Role:    messageRoleAssistant,
		Content: json.RawMessage(`[{"type":"image","data":"!","mimeType":"image/png"}]`),
	}, state))
	require.Len(t, connection.updates, 3)
	require.Equal(t, imageGuidanceInvalidBase64, connection.updates[2].AgentMessageChunk.Content.Text.Text)

	connection.updateErr = errors.New("emit")
	err := session.emitAssistantImages(t.Context(), pi.AgentMessage{
		Role:    messageRoleAssistant,
		Content: json.RawMessage(`[{"type":"image","data":"!","mimeType":"image/png"}]`),
	}, state)
	require.ErrorContains(t, err, "emit")

	err = session.emitAssistantImages(t.Context(), pi.AgentMessage{
		Role:    messageRoleAssistant,
		Content: json.RawMessage(`[{"type":"image","data":"` + fixtureBase64(t, "valid.webp") + `","mimeType":"image/webp"}]`),
	}, state)
	require.ErrorContains(t, err, "emit")
}

func TestHandleTurnEventEmitsAssistantImages(t *testing.T) {
	session, connection := newImageOutputSession(t)
	png := fixtureBase64(t, "valid.png")
	state := &promptTurnState{}

	settled, err := session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role:         messageRoleAssistant,
		ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b675",
		Content:      json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
	}}, state)
	require.NoError(t, err)
	require.False(t, settled)
	require.Len(t, connection.updates, 2)
	require.NotNil(t, connection.updates[0].AgentMessageChunk)
	require.NotNil(t, connection.updates[1].SessionInfoUpdate)

	_, err = session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role:         messageRoleAssistant,
		ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b676",
		Content:      json.RawMessage(`[{"type":"image","data":"!","mimeType":"image/png"}]`),
	}}, state)
	require.NoError(t, err, "a refused assistant image does not end the turn")
	require.Equal(t, imageGuidanceInvalidBase64, connection.updates[2].AgentMessageChunk.Content.Text.Text)

	connection.updateErr = errors.New("emit")
	_, err = session.handleTurnEvent(t.Context(), pi.MessageEndEvent{Message: pi.AgentMessage{
		Role:         messageRoleAssistant,
		ACPMessageID: "018f47ad-839d-7f70-b7f7-c01d6d97b677",
		Content:      json.RawMessage(`[{"type":"image","data":"` + png + `","mimeType":"image/png"}]`),
	}}, state)
	require.ErrorContains(t, err, "emit")
}

func TestImageAwareMirrorFailure(t *testing.T) {
	session, _ := newImageOutputSession(t)
	cause := errors.New("append failed")

	require.ErrorIs(t, session.imageAwareMirrorFailure(cause), cause,
		"a mirror failure without image output keeps its existing shape")

	session.markTurnImageEmission()
	requireImageOutputFailure(t, session.imageAwareMirrorFailure(cause), imageReasonStorageFailed)
}
