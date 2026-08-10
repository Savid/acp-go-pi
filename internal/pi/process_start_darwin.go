//go:build darwin

package pi

import (
	"errors"
	"syscall"
	"time"
)

func handleVanishedProcessGroupLeader(
	launch *processTreeCommand,
	direct *directChildWait,
) (*processTree, bool, error) {
	pid := launch.cmd.Process.Pid
	if launch.ordinary {
		tree := &processTree{
			containment: launch.containment, pgid: pid, process: launch.cmd.Process,
			direct: direct, ordinary: true,
		}
		launch.abortStartGate()
		direct.begin()
		launch.close()

		return tree, true, nil
	}

	probeErr := syscallKill(-pid, 0)
	if errors.Is(probeErr, syscall.ESRCH) {
		tree := &processTree{containment: launch.containment, process: launch.cmd.Process, direct: direct}
		launch.abortStartGate()
		direct.begin()

		if cleanupErr := tree.finishGroupAbsent(time.Now().Add(defaultProcessTreeWait)); cleanupErr != nil {
			launch.close()

			return nil, true, cleanupErr
		}

		launch.close()

		return nil, true, errors.New("darwin native root exited before containment identity capture")
	}

	if probeErr != nil && !errors.Is(probeErr, syscall.EPERM) {
		return nil, false, nil
	}

	tree := &processTree{containment: launch.containment, pgid: pid, process: launch.cmd.Process, direct: direct}
	launch.abortStartGate()

	cleanupErr := cleanupVanishedLeaderGroup(tree)

	launch.close()

	return nil, true, errors.Join(
		errors.New("native group remained observable after its leader vanished before containment identity capture"),
		cleanupErr,
	)
}
