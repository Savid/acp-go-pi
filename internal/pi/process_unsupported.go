//go:build !linux && !darwin && !freebsd && !openbsd && !windows

package pi

import (
	"fmt"
	"os/exec"
	"time"
)

type processTree struct{}

func (*processTree) directChildWait() *directChildWait { return nil }

func configureProcessCommandPlatform(*exec.Cmd) {}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	launch.close()

	return nil, fmt.Errorf("%w: platform containment backend unavailable", ErrProcessContainmentIncomplete)
}

func (*processTree) terminate() error { return ErrProcessContainmentIncomplete }
func (*processTree) kill() error      { return ErrProcessContainmentIncomplete }
func (*processTree) terminateAndWait(time.Duration) error {
	return ErrProcessContainmentIncomplete
}
