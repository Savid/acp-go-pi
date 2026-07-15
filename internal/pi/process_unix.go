//go:build linux || darwin || freebsd || openbsd

package pi

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type processTree struct {
	pgid int
}

func startProcessTree(cmd *exec.Cmd) (*processTree, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &processTree{pgid: cmd.Process.Pid}, nil
}

func (t *processTree) terminate() error {
	return signalProcessGroupID(t.pgid, syscall.SIGTERM)
}

func (t *processTree) kill() error {
	return signalProcessGroupID(t.pgid, syscall.SIGKILL)
}

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	if t == nil || t.pgid <= 0 {
		return nil
	}

	if err := t.kill(); err != nil {
		return fmt.Errorf("%w: terminate process group %d: %w", ErrProcessTreeNotQuiescent, t.pgid, err)
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := syscallKill(-t.pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("%w: inspect process group %d: %w", ErrProcessTreeNotQuiescent, t.pgid, err)
		}

		select {
		case <-deadline.C:
			return fmt.Errorf("%w: process group %d remained live", ErrProcessTreeNotQuiescent, t.pgid)
		case <-ticker.C:
		}
	}
}

var (
	signalOSProcess = func(process *os.Process, signal os.Signal) error {
		return process.Signal(signal)
	}
	syscallGetpgid = syscall.Getpgid
	syscallKill    = syscall.Kill
)

func configureProcessCommandPlatform(cmd *exec.Cmd) {
	cmd.SysProcAttr = processSysProcAttr()
	cmd.Cancel = func() error {
		_, err := signalProcess(cmd, syscall.SIGTERM)

		return err
	}
}

func signalProcess(cmd *exec.Cmd, signal syscall.Signal) (bool, error) {
	if cmd == nil || cmd.Process == nil {
		return false, nil
	}

	if usesProcessGroup(cmd) {
		return signalProcessGroup(cmd, signal)
	}

	if err := signalOSProcess(cmd.Process, signal); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func signalProcessGroup(cmd *exec.Cmd, signal syscall.Signal) (bool, error) {
	pgid, err := syscallGetpgid(cmd.Process.Pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil
		}

		return false, err
	}

	if err := syscallKill(-pgid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func signalProcessGroupID(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}

	if err := syscallKill(-pgid, signal); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	return nil
}

func usesProcessGroup(cmd *exec.Cmd) bool {
	return cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid
}
