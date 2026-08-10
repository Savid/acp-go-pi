//go:build linux

package pi

import "errors"

func settleDirectProcessExit(tree *processTree, waitErr error) error {
	if tree != nil && tree.ordinary {
		return waitErr
	}

	return errors.Join(waitErr, tree.completeBoundary())
}
