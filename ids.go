package piacp

const (
	// ForkSessionMethod is the pi extension method used to fork a session.
	// Fork duplicates the parent session's active branch into a new session
	// with a new session id (pi's native clone).
	ForkSessionMethod = "_pi/session/fork"

	// RawEventMethod is the pi extension notification method carrying raw
	// native pi RPC event payloads when a session opts in to raw events.
	RawEventMethod = "_pi/rawEvent"
)
