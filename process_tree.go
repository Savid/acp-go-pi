package piacp

import internalpi "github.com/savid/acp-go-pi/internal/pi"

// ErrProcessContainmentIncomplete reports that the selected native process
// boundary did not complete. Callers must retain resources that may still be
// reachable by the native runtime.
var ErrProcessContainmentIncomplete = internalpi.ErrProcessContainmentIncomplete
