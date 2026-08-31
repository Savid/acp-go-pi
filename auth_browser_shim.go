package piacp

import (
	"github.com/savid/acp-go-pi/internal/pi"
)

// newSessionBrowserShim builds the launcher shim one session's pi child runs
// behind. A shim the platform cannot give is not a session failure: pi runs its
// provider-auth login inside that same process, so the login leg is the only
// thing there with a browser to open, and that leg refuses on a nil shim. The
// nil carries the whole outcome, so there is nothing further in the error.
func (a *Agent) newSessionBrowserShim(parent string) *pi.BrowserShim {
	shim, err := pi.NewBrowserShim(parent)
	if err != nil {
		return nil
	}

	return shim
}

func (a *Agent) newOwnedSessionBrowserShim(parent string) (*pi.BrowserShim, error) {
	shim := a.newSessionBrowserShim(parent)
	if shim == nil {
		return nil, nil //nolint:nilnil // Absence is the fail-closed auth result; the session itself may proceed.
	}

	return shim, nil
}
