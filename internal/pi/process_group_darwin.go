//go:build darwin

package pi

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

const darwinTermGrace = 500 * time.Millisecond

var darwinProcessGroupSignal = syscallKill
var darwinDirectProcessKill = func(process *os.Process) error { return process.Kill() }

func awaitProcessGroupBoundary(tree *processTree, _ time.Duration) error {
	if tree != nil && tree.ordinary {
		return tree.direct.awaitReaped(defaultProcessTreeWait)
	}

	if tree == nil || tree.pgid <= 0 {
		return nil
	}

	tree.cleanupOnce.Do(func() {
		tree.cleanupErr = runDarwinProcessGroupCleanup(tree)
	})

	return tree.cleanupErr
}

func terminateProcessTree(tree *processTree) error {
	if tree != nil && tree.ordinary {
		_, err := signalOriginalProcessGroup(tree.pgid, syscall.SIGTERM)

		return err
	}

	return awaitProcessGroupBoundary(tree, defaultProcessTreeWait)
}

func killProcessTree(tree *processTree) error {
	if tree != nil && tree.ordinary {
		_, err := signalOriginalProcessGroup(tree.pgid, syscall.SIGKILL)

		return err
	}

	return awaitProcessGroupBoundary(tree, defaultProcessTreeWait)
}

func runDarwinProcessGroupCleanup(tree *processTree) error {
	deadline := time.Now().Add(defaultProcessTreeWait)
	absent, err := signalOriginalProcessGroup(tree.pgid, syscall.SIGTERM)
	tree.direct.begin()

	return finishDarwinProcessGroupCleanup(tree, deadline, absent, err)
}

// cleanupVanishedLeaderGroup delivers the first signal while the direct child
// remains unreaped. That preserves the PID-protected observation made after
// Getpgid failed until cleanup has acted on the expected process group.
func cleanupVanishedLeaderGroup(tree *processTree) error {
	tree.cleanupOnce.Do(func() {
		deadline := time.Now().Add(defaultProcessTreeWait)
		absent, err := signalOriginalProcessGroup(tree.pgid, syscall.SIGTERM)
		tree.direct.begin()
		tree.cleanupErr = finishDarwinProcessGroupCleanup(tree, deadline, absent, err)
	})

	return tree.cleanupErr
}

func finishDarwinProcessGroupCleanup(tree *processTree, deadline time.Time, absent bool, err error) error {
	if err != nil {
		return tree.failCleanup(deadline, fmt.Errorf("signal original process group %d with SIGTERM: %w", tree.pgid, err))
	}

	if absent {
		return tree.finishGroupAbsent(deadline)
	}

	absent, err = pollOriginalProcessGroup(tree.pgid, minTime(deadline, time.Now().Add(darwinTermGrace)))
	if err != nil {
		return tree.failCleanup(deadline, err)
	}

	if absent {
		return tree.finishGroupAbsent(deadline)
	}

	absent, err = signalOriginalProcessGroup(tree.pgid, syscall.SIGKILL)
	if err != nil {
		return tree.failCleanup(deadline, fmt.Errorf("signal original process group %d with SIGKILL: %w", tree.pgid, err))
	}

	if absent {
		return tree.finishGroupAbsent(deadline)
	}

	absent, err = pollOriginalProcessGroup(tree.pgid, deadline)
	if err != nil {
		return tree.failCleanup(deadline, err)
	}

	if !absent {
		return tree.failCleanup(deadline, fmt.Errorf("original process group %d remained observable", tree.pgid))
	}

	return tree.finishGroupAbsent(deadline)
}

func (tree *processTree) failCleanup(deadline time.Time, cause error) error {
	directKillErr := forceKillDarwinDirectChild(tree)
	remaining := time.Until(deadline)

	if remaining <= 0 {
		return tree.incomplete(errors.Join(cause, directKillErr, errors.New("direct child was not reaped before the containment deadline")))
	}

	return tree.incomplete(errors.Join(cause, directKillErr, tree.direct.awaitReaped(remaining)))
}

func forceKillDarwinDirectChild(tree *processTree) error {
	if tree == nil || tree.direct == nil {
		return ErrProcessContainmentIncomplete
	}

	select {
	case <-tree.direct.done:
		return nil
	default:
	}

	if tree.process == nil {
		return ErrProcessContainmentIncomplete
	}

	err := darwinDirectProcessKill(tree.process)
	if err == nil || errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return nil
	}

	return fmt.Errorf("signal direct child %d with SIGKILL: %w", tree.process.Pid, err)
}

// signalOriginalProcessGroup preserves ESRCH as a terminal observation. Once
// the captured group is absent, cleanup must never signal that numeric PGID
// again because it can be reused by an unrelated process group.
func signalOriginalProcessGroup(pgid int, signal syscall.Signal) (bool, error) {
	err := darwinProcessGroupSignal(-pgid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}

	if errors.Is(err, syscall.EPERM) {
		return false, nil
	}

	return false, err
}

func pollOriginalProcessGroup(pgid int, deadline time.Time) (bool, error) {
	for {
		err := darwinProcessGroupSignal(-pgid, 0)
		switch {
		case errors.Is(err, syscall.ESRCH):
			return true, nil
		case err == nil, errors.Is(err, syscall.EPERM):
		case err != nil:
			return false, fmt.Errorf("inspect original process group %d: %w", pgid, err)
		}

		if !time.Now().Before(deadline) {
			return false, nil
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (tree *processTree) finishGroupAbsent(deadline time.Time) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return tree.incomplete(errors.New("direct child was not reaped before the containment deadline"))
	}

	if err := tree.direct.awaitReaped(remaining); err != nil {
		return tree.incomplete(err)
	}

	if err := completeContainmentRecord(tree.containment, containmentStateAbsent); err != nil {
		return tree.incomplete(err)
	}

	return nil
}

func (tree *processTree) incomplete(cause error) error {
	recordErr := completeContainmentRecord(tree.containment, containmentStateFailed)

	return errors.Join(ErrProcessContainmentIncomplete, cause, recordErr)
}

func minTime(left time.Time, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}

	return right
}
