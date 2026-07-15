//go:build !linux && !darwin && !freebsd && !openbsd && !windows

package pi

import (
	"fmt"
	"os/exec"
	"time"
)

type processTree struct{}

func configureProcessCommandPlatform(*exec.Cmd) {}

func startProcessTree(*exec.Cmd) (*processTree, error) {
	return nil, fmt.Errorf("%w: platform containment backend unavailable", ErrProcessTreeNotQuiescent)
}

func (*processTree) terminate() error { return ErrProcessTreeNotQuiescent }
func (*processTree) kill() error      { return ErrProcessTreeNotQuiescent }
func (*processTree) terminateAndWait(time.Duration) error {
	return ErrProcessTreeNotQuiescent
}
