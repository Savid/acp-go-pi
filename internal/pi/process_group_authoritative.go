//go:build linux || freebsd || openbsd

package pi

import (
	"errors"
	"fmt"
	"syscall"
	"time"
)

func awaitProcessGroupBoundary(tree *processTree, timeout time.Duration) error {
	if tree == nil || tree.pgid <= 0 {
		return nil
	}

	if err := tree.kill(); err != nil {
		return fmt.Errorf("%w: terminate process group %d: %w", ErrProcessContainmentIncomplete, tree.pgid, err)
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := syscallKill(-tree.pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return tree.completeBoundary()
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("%w: inspect process group %d: %w", ErrProcessContainmentIncomplete, tree.pgid, err)
		}

		select {
		case <-deadline.C:
			return fmt.Errorf("%w: process group %d remained live", ErrProcessContainmentIncomplete, tree.pgid)
		case <-ticker.C:
		}
	}
}

func terminateProcessTree(tree *processTree) error {
	return signalProcessGroupID(tree.pgid, syscall.SIGTERM)
}

func killProcessTree(tree *processTree) error {
	tree.mu.Lock()
	if tree.supervised {
		var err error
		if tree.control != nil {
			err = tree.control.Close()
			tree.control = nil
		}
		tree.mu.Unlock()

		return err
	}
	tree.mu.Unlock()

	return signalProcessGroupID(tree.pgid, syscall.SIGKILL)
}

func signalProcessGroupID(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}

	if err := syscallKill(-pgid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	return nil
}
