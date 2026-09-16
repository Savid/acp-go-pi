package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

func toolUpdates(updates []acp.SessionNotification) []acp.SessionNotification {
	tools := make([]acp.SessionNotification, 0)

	for _, update := range updates {
		if update.Update.ToolCall != nil || update.Update.ToolCallUpdate != nil {
			tools = append(tools, update)
		}
	}

	return tools
}

func TestPermissionAllowRunsTool(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)

	require.Len(t, h.rec.permissions, 1)
	require.Equal(t, acp.ToolCallId("call-1"), h.rec.permissions[0].ToolCall.ToolCallId)
	require.Equal(t, "bash", *h.rec.permissions[0].ToolCall.Title)

	tools := toolUpdates(h.rec.snapshot())
	require.NotNil(t, tools[0].Update.ToolCall)
	require.Equal(t, acp.ToolCallStatusPending, tools[0].Update.ToolCall.Status)

	last := tools[len(tools)-1].Update.ToolCallUpdate
	require.NotNil(t, last)
	require.Equal(t, acp.ToolCallStatusCompleted, *last.Status)
	require.Len(t, last.Content, 1)
	require.Equal(t, "out\n", last.Content[0].Content.Content.Text.Text)
}

func TestPermissionDenyBlocksTool(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.rec.answer = func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionDeny)}
	}
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)

	tools := toolUpdates(h.rec.snapshot())
	last := tools[len(tools)-1].Update.ToolCallUpdate
	require.Equal(t, acp.ToolCallStatusFailed, *last.Status)
}

func TestPermissionCancelledOutcomeDenies(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.rec.answer = func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}
	}
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)

	tools := toolUpdates(h.rec.snapshot())
	require.Equal(t, acp.ToolCallStatusFailed, *tools[len(tools)-1].Update.ToolCallUpdate.Status)
}

func TestPermissionModeAllowSkipsRequests(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionPiOptions(NewPiOptions(WithPiPermission(pi.PermissionModeAllow))))

	_, err := h.prompt(session.SessionId, "TOOL", nil)
	require.NoError(t, err)
	require.Empty(t, h.rec.permissions)

	tools := toolUpdates(h.rec.snapshot())
	require.Equal(t, acp.ToolCallStatusInProgress, tools[0].Update.ToolCall.Status)
}

func TestElicitationWithFormCapability(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.rec.elicit = func(request acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		require.NotNil(t, request.Form)
		require.Equal(t, "form", request.Form.Mode)
		require.Contains(t, request.Form.Message, "Your name?")

		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"value": "Bob"}}}, nil
	}
	h.initialize(withFormElicitation())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "ASK", nil)
	require.NoError(t, err)
	require.Equal(t, "hi Bob", agentText(h.rec.snapshot()))
}

// Form elicitation is relayed only when the client advertises form support,
// in every shape the capability can take.
func TestElicitationCapabilityGating(t *testing.T) {
	t.Parallel()

	refusing := map[string]func(*acp.InitializeRequest){
		"omitted": func(*acp.InitializeRequest) {},
		"empty":   func(r *acp.InitializeRequest) { r.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{} },
		"url only": func(r *acp.InitializeRequest) {
			r.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{Url: &acp.ElicitationUrlCapabilities{}}
		},
		"both null": func(r *acp.InitializeRequest) {
			r.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: nil, Url: nil}
		},
	}

	for name, option := range refusing {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize(option)
			session := h.newSession()

			_, err := h.prompt(session.SessionId, "ASK", nil)
			require.NoError(t, err)
			require.Equal(t, "declined", agentText(h.rec.snapshot()))
		})
	}

	accepting := map[string]func(*acp.InitializeRequest){
		"form only": withFormElicitation(),
		"form and url": func(r *acp.InitializeRequest) {
			r.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{
				Form: &acp.ElicitationFormCapabilities{},
				Url:  &acp.ElicitationUrlCapabilities{},
			}
		},
	}

	for name, option := range accepting {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
				return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{
					Content: map[string]any{elicitationFieldValue: "Bob"},
				}}, nil
			}
			h.initialize(option)
			session := h.newSession()

			_, err := h.prompt(session.SessionId, "ASK", nil)
			require.NoError(t, err)
			require.Equal(t, "hi Bob", agentText(h.rec.snapshot()))
		})
	}
}

func TestElicitationDeclined(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		return acp.UnstableCreateElicitationResponse{Decline: &acp.UnstableCreateElicitationDecline{}}, nil
	}
	h.initialize(withFormElicitation())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "ASK", nil)
	require.NoError(t, err)
	require.Equal(t, "declined", agentText(h.rec.snapshot()))
}

func TestCancelResolvesPendingPermission(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	release := make(chan struct{})
	h.rec.answer = func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		<-release

		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow)}
	}
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	errs := make(chan error, 1)

	go func() {
		resp, err := h.prompt(session.SessionId, "TOOL", promptMeta(1))
		done <- resp
		errs <- err
	}()

	h.rec.waitFor(t, func([]acp.SessionNotification) bool {
		h.rec.mu.Lock()
		defer h.rec.mu.Unlock()

		return len(h.rec.permissions) == 1
	})

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	resp := <-done
	err := <-errs
	t.Logf("events: %v", eventTypes(lifecycleEvents(h.rec.snapshot())))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, resp.StopReason)
	close(release)

	events := lifecycleEvents(h.rec.snapshot())
	types := eventTypes(events)
	require.Contains(t, types, "action_update:cancelled")
	require.Equal(t, "state_update:idle", types[len(types)-1])
}

func TestDialogAnswerShapes(t *testing.T) {
	t.Parallel()

	value := "b"
	confirmed := true

	require.Equal(t, pi.UIValueResponse("1", "b"), dialogAnswer(pi.UIRequest{ID: "1", Method: uiMethodSelect, Options: []string{"a", "b"}}, map[string]any{"choice": "b"}))
	require.Equal(t, pi.UICancelResponse("1"), dialogAnswer(pi.UIRequest{ID: "1", Method: uiMethodSelect, Options: []string{"a"}}, map[string]any{"choice": "z"}))
	require.Equal(t, pi.UIConfirmResponse("1", true), dialogAnswer(pi.UIRequest{ID: "1", Method: uiMethodConfirm}, map[string]any{"confirmed": confirmed}))
	require.Equal(t, pi.UIValueResponse("1", value), dialogAnswer(pi.UIRequest{ID: "1", Method: uiMethodEditor}, map[string]any{"value": value}))
	require.Equal(t, pi.UICancelResponse("1"), dialogAnswer(pi.UIRequest{ID: "1", Method: "notify"}, nil))

	schema := elicitationSchema(pi.UIRequest{Method: uiMethodSelect, Options: []string{"a"}})
	require.Equal(t, []string{"choice"}, schema.Required)
	schema = elicitationSchema(pi.UIRequest{Method: uiMethodInput, Placeholder: "p", Prefill: "d"})
	require.Equal(t, []string{"value"}, schema.Required)
	require.Equal(t, "pi needs more input.", elicitationMessage(pi.UIRequest{}))
	require.Equal(t, acp.ToolKindExecute, toolKindForName("bash"))
	require.Equal(t, acp.ToolKindOther, toolKindForName("question"))
}
