package piacp

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestProviderTurnFailureAndEmptyCloneError(t *testing.T) {
	err := providerTurnFailure(&promptTurnState{stopReason: stopReasonError, errorMessage: "message"})
	requirePiTurnFailure(t, err, failureCauseProvider)
	require.NoError(t, providerTurnFailure(&promptTurnState{}))
	require.False(t, emptyCloneError(errors.New("other")))
	require.True(t, emptyCloneError(&pi.CommandError{Message: "Entry abcd not found"}))
}
