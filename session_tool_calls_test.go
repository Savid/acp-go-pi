package piacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestTurnToolStateFenceAndReset(t *testing.T) {
	session := &agentSession{}
	state := session.lockToolCallState("tool")
	state.status = acp.ToolCallStatusInProgress
	state.mu.Unlock()

	session.resetTurnTools()
	if session.turnTools != nil {
		t.Fatal("turn tool index was not detached")
	}

	fenceTurnTool(&turnToolCall{status: acp.ToolCallStatusCompleted})
}
