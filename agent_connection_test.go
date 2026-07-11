package piacp

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestAgentConnectionAndErrorMapping(t *testing.T) {
	_, err := scopedElicitationParams(acp.UnstableCreateElicitationRequest{}, elicitationScope{})
	require.Error(t, err)
	params := acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{
		Message: "message", Mode: elicitationModeForm,
		RequestedSchema: acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject},
		Meta:            map[string]any{"x": true},
	}}
	raw, err := scopedElicitationParams(params, elicitationScope{SessionID: "session", ToolCallID: "tool"})
	require.NoError(t, err)
	require.Contains(t, string(raw), "toolCallId")

	require.Nil(t, requestError(nil))
	requestErr := acp.NewInvalidParams(nil)
	require.Same(t, requestErr, requestError(requestErr))
	require.Equal(t, -32800, requestError(context.Canceled).Code)
	require.Equal(t, -32603, requestError(errors.New("failure")).Code)
	require.Same(t, requestErr, lifecycleMetaError(requestErr))
	var lifecycleRequestError *acp.RequestError
	require.ErrorAs(t, lifecycleMetaError(errors.New("bad meta")), &lifecycleRequestError)
	require.Equal(t, -32602, lifecycleRequestError.Code)
}
