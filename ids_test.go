package piacp

import "testing"

func TestExtensionMethodIDs(t *testing.T) {
	if ForkSessionMethod != "_pi/session/fork" {
		t.Fatalf("ForkSessionMethod = %q", ForkSessionMethod)
	}
	if RawEventMethod != "_pi/rawEvent" {
		t.Fatalf("RawEventMethod = %q", RawEventMethod)
	}
}
