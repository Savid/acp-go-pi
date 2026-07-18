package piacp

import (
	"testing"
	"time"
)

func TestSessionCoreDefaults(t *testing.T) {
	if defaultSessionCloseTurnWait != 5*time.Second {
		t.Fatalf("default close wait = %v", defaultSessionCloseTurnWait)
	}
	if configThoughtLevel != "thought_level" || configTypeSelect != "select" {
		t.Fatalf("config constants = %q, %q", configThoughtLevel, configTypeSelect)
	}

	var session agentSession
	if session.turnTools != nil || session.pendingDialogs != nil || session.turnSink != nil {
		t.Fatal("zero-value session contains live turn state")
	}
}
