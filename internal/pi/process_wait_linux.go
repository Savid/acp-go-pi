//go:build linux

package pi

import "errors"

func settleDirectProcessExit(tree *processTree, waitErr error) error {
	return errors.Join(waitErr, tree.completeBoundary())
}
