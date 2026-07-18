//go:build !linux && !darwin && !freebsd && !openbsd

package pi

import (
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(native *exec.Cmd, containment ContainmentSpec) (*processTreeCommand, error) {
	if containment.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin best-effort containment is invalid on this platform", ErrProcessContainmentIncomplete)
	}

	configureProcessCommandPlatform(native)

	return &processTreeCommand{cmd: native}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
