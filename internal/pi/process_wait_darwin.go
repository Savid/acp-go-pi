//go:build darwin

package pi

import (
	"errors"
	"os/exec"
)

func settleDirectProcessExit(tree *processTree, waitErr error) error {
	containmentErr := tree.terminateAndWait(defaultProcessTreeWait)
	if containmentErr == nil && errors.Is(waitErr, exec.ErrWaitDelay) {
		waitErr = nil
	}

	return errors.Join(waitErr, containmentErr)
}
