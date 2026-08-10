//go:build darwin

package pi

import (
	"errors"
	"os/exec"
)

func settleDirectProcessExit(tree *processTree, waitErr error) error {
	if tree != nil && tree.ordinary {
		return waitErr
	}

	containmentErr := tree.terminateAndWait(defaultProcessTreeWait)
	if containmentErr == nil && errors.Is(waitErr, exec.ErrWaitDelay) {
		waitErr = nil
	}

	return errors.Join(waitErr, containmentErr)
}
