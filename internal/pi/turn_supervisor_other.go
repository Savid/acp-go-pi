//go:build !linux && !darwin && !freebsd && !openbsd

package pi

import (
	"errors"
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(native *exec.Cmd, containment ContainmentSpec) (*processTreeCommand, error) {
	if containment.Isolation != nil {
		return nil, errors.New("explicit process isolation is supported only on linux")
	}

	if containment.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin best-effort containment is invalid on this platform", ErrProcessContainmentIncomplete)
	}

	configureProcessCommandPlatform(native)

	return &processTreeCommand{cmd: native, ordinary: true}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
