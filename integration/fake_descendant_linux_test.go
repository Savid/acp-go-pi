//go:build integration && linux

package integration

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

func currentFakeProcessIdentity() fakeProcessIdentity {
	pid := os.Getpid()
	pgid, _ := unix.Getpgid(pid)
	sid, _ := unix.Getsid(pid)

	return fakeProcessIdentity{PID: pid, PGID: pgid, SID: sid}
}

func isolateFakeDescendant() (fakeProcessIdentity, error) {
	if _, err := unix.Setsid(); err != nil {
		return fakeProcessIdentity{}, err
	}

	signal.Ignore(os.Interrupt, unix.SIGTERM)

	return currentFakeProcessIdentity(), nil
}
