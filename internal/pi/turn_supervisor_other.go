//go:build !linux && !darwin && !freebsd && !openbsd

package pi

import "os/exec"

func prepareProcessTreeCommand(native *exec.Cmd) (*processTreeCommand, error) {
	configureProcessCommandPlatform(native)

	return &processTreeCommand{cmd: native}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
