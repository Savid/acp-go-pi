//go:build !darwin && !linux

package pi

func settleDirectProcessExit(_ *processTree, waitErr error) error { return waitErr }
