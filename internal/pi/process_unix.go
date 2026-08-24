//go:build linux || darwin || freebsd || openbsd

package pi

import (
	"bufio"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

var activateProcessContainmentRecord = activateContainmentRecord
var processHandleVanishedLeader = handleVanishedProcessGroupLeader

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	if err := launch.cmd.Start(); err != nil {
		recordErr := completeUnstartedContainment(launch.containment)
		launch.close()

		return nil, errors.Join(err, recordErr)
	}

	launch.releaseInherited()
	direct := installPausedDirectChildWait(launch.cmd)

	pgid, err := syscallGetpgid(launch.cmd.Process.Pid)
	if errors.Is(err, syscall.ESRCH) {
		if tree, handled, handleErr := processHandleVanishedLeader(launch, direct); handled {
			return tree, handleErr
		}
	}

	if err != nil || pgid != launch.cmd.Process.Pid {
		launch.abortStartGate()
		_ = launch.cmd.Process.Signal(syscall.SIGKILL)

		direct.begin()
		waitErr := direct.await(defaultProcessTreeWait)
		_ = completeContainmentRecord(launch.containment, containmentStateFailed)
		launch.close()

		return nil, errors.Join(
			fmt.Errorf("%w: validate native process-group leader", ErrProcessContainmentIncomplete),
			err,
			waitErr,
		)
	}

	tree := &processTree{
		containment: launch.containment,
		pgid:        pgid,
		process:     launch.cmd.Process,
		direct:      direct,
		ordinary:    launch.ordinary,
	}
	tree.control = launch.control
	tree.supervised = launch.control != nil

	if err := activateProcessContainmentRecord(launch.containment, launch.cmd.Process.Pid, pgid); err != nil {
		launch.abortStartGate()
		tree.direct.begin()

		cleanupErr := tree.kill()
		waitErr := tree.direct.await(defaultProcessTreeWait)
		_ = completeContainmentRecord(launch.containment, containmentStateFailed)
		launch.close()

		return nil, errors.Join(fmt.Errorf("%w: activate containment record: %v", ErrProcessContainmentIncomplete, err), cleanupErr, waitErr)
	}

	if err := launch.releaseStartGate(); err != nil {
		tree.direct.begin()
		cleanupErr := tree.kill()
		waitErr := tree.direct.await(defaultProcessTreeWait)

		launch.close()

		return nil, errors.Join(
			fmt.Errorf("%w: release validated native launch: %v", ErrProcessContainmentIncomplete, err),
			cleanupErr,
			waitErr,
		)
	}

	direct.begin()

	if err := awaitProcessTreeReady(launch); err != nil {
		cleanupErr := tree.kill()
		transferProcessTreeCompletion(tree, launch)

		launch.close()

		waitErr := tree.direct.await(defaultProcessTreeWait)
		boundaryErr := tree.completeBoundary()

		return nil, errors.Join(err, cleanupErr, waitErr, boundaryErr)
	}

	transferProcessTreeCompletion(tree, launch)

	launch.control = nil

	return tree, nil
}

func transferProcessTreeCompletion(tree *processTree, launch *processTreeCommand) {
	if tree == nil || launch == nil || launch.completion == nil {
		return
	}

	tree.boundary = launch.completion
	tree.status = bufio.NewReader(launch.completion)
	launch.completion = nil
}

func (t *processTree) terminate() error {
	return terminateProcessTree(t)
}

func (t *processTree) kill() error {
	return killProcessTree(t)
}

func (t *processTree) directChildWait() *directChildWait { return t.direct }

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	return awaitProcessGroupBoundary(t, timeout)
}

func (t *processTree) completeBoundary() error {
	if !t.supervised {
		return nil
	}

	t.boundaryOnce.Do(func() {
		defer func() {
			if t.boundary != nil {
				_ = t.boundary.Close()
				t.boundary = nil
			}
		}()

		if t.boundary == nil || t.status == nil {
			t.boundaryErr = fmt.Errorf("%w: pi turn supervisor boundary channel is unavailable", ErrProcessContainmentIncomplete)

			return
		}

		if err := t.boundary.SetReadDeadline(time.Now().Add(defaultProcessTreeWait)); err != nil {
			t.boundaryErr = fmt.Errorf("%w: arm pi turn supervisor boundary: %v", ErrProcessContainmentIncomplete, err)

			return
		}

		line, err := t.status.ReadString('\n')
		if err != nil {
			t.boundaryErr = fmt.Errorf("%w: await pi turn supervisor boundary: %v", ErrProcessContainmentIncomplete, err)

			return
		}

		if line != turnSupervisorComplete {
			t.boundaryErr = fmt.Errorf("%w: invalid pi turn supervisor boundary %q", ErrProcessContainmentIncomplete, strings.TrimSpace(line))
		}
	})

	return t.boundaryErr
}

var syscallGetpgid = syscall.Getpgid
var syscallKill = syscall.Kill

func configureProcessCommandPlatform(cmd *exec.Cmd) {
	cmd.SysProcAttr = processSysProcAttr()
}
