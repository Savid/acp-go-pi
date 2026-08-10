//go:build !linux && !darwin && !freebsd && !openbsd

package pi

import (
	"errors"
	"os"
	"os/exec"
	"time"
)

type processTree struct {
	process *os.Process
	direct  *directChildWait
}

func (t *processTree) directChildWait() *directChildWait { return t.direct }

func configureProcessCommandPlatform(*exec.Cmd) {}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	if err := launch.cmd.Start(); err != nil {
		launch.close()

		return nil, err
	}

	launch.releaseInherited()
	direct := installPausedDirectChildWait(launch.cmd)
	direct.begin()

	return &processTree{process: launch.cmd.Process, direct: direct}, nil
}

func (t *processTree) terminate() error { return t.kill() }
func (t *processTree) kill() error {
	if t == nil || t.process == nil {
		return nil
	}

	err := t.process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}

	return err
}
func (t *processTree) terminateAndWait(timeout time.Duration) error {
	if t == nil {
		return nil
	}

	return t.direct.awaitReaped(timeout)
}

func (*processTree) descendantCount() (int, bool) { return 0, false }
