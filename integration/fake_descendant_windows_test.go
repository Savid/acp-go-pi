//go:build integration && windows

package integration

import "os"

func currentFakeProcessIdentity() fakeProcessIdentity {
	return fakeProcessIdentity{PID: os.Getpid()}
}

func isolateFakeDescendant() (fakeProcessIdentity, error) {
	return currentFakeProcessIdentity(), nil
}
