package piacp

import internalpi "github.com/savid/acp-go-pi/internal/pi"

// ErrProcessTreeUnproven reports that the adapter could not prove every
// native descendant exited. Callers must retain resources that may still be
// reachable by the process tree.
var ErrProcessTreeUnproven = internalpi.ErrProcessTreeNotQuiescent
