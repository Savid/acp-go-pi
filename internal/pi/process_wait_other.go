//go:build !darwin

package pi

func settleDirectProcessExit(_ *processTree, waitErr error) error { return waitErr }
