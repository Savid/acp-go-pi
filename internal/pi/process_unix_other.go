//go:build unix && !linux && !freebsd && !darwin

package pi

import "syscall"

func processSysProcAttr() *syscall.SysProcAttr {
	// The remaining unix platforms have no Pdeathsig equivalent; parent-death
	// cleanup is best-effort via process-group signalling.
	return &syscall.SysProcAttr{Setpgid: true}
}
