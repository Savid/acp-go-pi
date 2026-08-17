package piacp

import (
	"testing"
	"time"
)

func TestSessionCoreDefaults(t *testing.T) {
	if sessionCloseTurnWait != 5*time.Second {
		t.Fatalf("close turn wait = %v", sessionCloseTurnWait)
	}
	if configThoughtLevel != "thought_level" || configTypeSelect != "select" {
		t.Fatalf("config constants = %q, %q", configThoughtLevel, configTypeSelect)
	}

	var session agentSession
	if session.turnTools != nil || session.pendingDialogs != nil || session.turnSink != nil {
		t.Fatal("zero-value session contains live turn state")
	}
}
