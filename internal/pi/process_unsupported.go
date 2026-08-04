//go:build !linux && !darwin && !freebsd && !openbsd

package pi

import (
	"fmt"
	"os/exec"
	"time"
)

type processTree struct {
	direct *directChildWait
}

func (t *processTree) directChildWait() *directChildWait { return t.direct }

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

func (*processTree) descendantCount() (int, bool) { return 0, false }
