package pi

import (
	"context"
	"encoding/json"
)

const sessionInitializationMarker = "acp-go-pi:session-initialization:"

// EnvSessionInitialization requests the fresh session's native entry prefix.
const EnvSessionInitialization = "ACP_GO_PI_INTERNAL_SESSION_INITIALIZATION"

// InitialSession returns the native header and entry prefix captured by the
// bridge at session_start, before pi's lazy transcript file exists.
func (c *Client) InitialSession(ctx context.Context) ([]json.RawMessage, error) {
	select {
	case entries := <-c.initialSession:
		return entries, nil
	case <-c.done:
		return nil, ErrTransportClosed
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
