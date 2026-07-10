//go:build darwin

package pi

import "syscall"

func processSysProcAttr() *syscall.SysProcAttr {
	// Darwin has no Pdeathsig equivalent; parent-death cleanup is best-effort
	// via process-group signalling.
	return &syscall.SysProcAttr{Setpgid: true}
}
