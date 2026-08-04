package piacp

import (
	"errors"

	"github.com/savid/acp-go-pi/internal/pi"
)

// newSessionBrowserShim builds the launcher shim one session's pi child runs
// behind. A shim the platform cannot give is not a session failure: pi runs its
// provider-auth login inside that same process, so the login leg is the only
// thing there with a browser to open, and that leg refuses on a nil shim. The
// nil carries the whole outcome, so there is nothing further in the error.
func (a *Agent) newSessionBrowserShim() *pi.BrowserShim {
	shim, err := pi.NewBrowserShim(scratchParent(a.options.ScratchDir))
	if err != nil {
		return nil
	}

	return shim
}

func (a *Agent) newOwnedSessionBrowserShim() (*pi.BrowserShim, error) {
	shim := a.newSessionBrowserShim()
	if shim == nil {
		return nil, nil
	}
	if err := shim.Handoff(internalProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential, a.options.testOnlyIdentityLockRoot)); err != nil {
		return nil, errors.Join(err, shim.Remove())
	}

	return shim, nil
}
