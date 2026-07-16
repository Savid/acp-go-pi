//go:build linux || darwin || freebsd || openbsd

package pi

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type processTree struct {
	mu         sync.Mutex
	pgid       int
	control    *os.File
	supervised bool
	proof      *os.File
	status     *bufio.Reader
	proofOnce  sync.Once
	proofErr   error
}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	if err := launch.cmd.Start(); err != nil {
		launch.close()

		return nil, err
	}

	launch.releaseInherited()

	if err := awaitProcessTreeReady(launch); err != nil {
		launch.close()
		waitErr := launch.cmd.Wait()

		return nil, errors.Join(err, waitErr)
	}

	tree := &processTree{
		pgid:       launch.cmd.Process.Pid,
		control:    launch.control,
		supervised: launch.control != nil,
		proof:      launch.ready,
		status:     launch.status,
	}
	launch.control = nil
	launch.ready = nil
	launch.status = nil

	return tree, nil
}

func (t *processTree) terminate() error {
	return signalProcessGroupID(t.pgid, syscall.SIGTERM)
}

func (t *processTree) kill() error {
	t.mu.Lock()
	if t.supervised {
		var err error
		if t.control != nil {
			err = t.control.Close()
			t.control = nil
		}
		t.mu.Unlock()

		return err
	}
	t.mu.Unlock()

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
			return t.proveQuiescence()
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

func (t *processTree) proveQuiescence() error {
	if !t.supervised {
		return nil
	}

	t.proofOnce.Do(func() {
		defer func() {
			if t.proof != nil {
				_ = t.proof.Close()
				t.proof = nil
			}
		}()

		if t.proof == nil || t.status == nil {
			t.proofErr = fmt.Errorf("%w: pi turn supervisor proof channel is unavailable", ErrProcessTreeNotQuiescent)

			return
		}

		if err := t.proof.SetReadDeadline(time.Now().Add(defaultProcessTreeWait)); err != nil {
			t.proofErr = fmt.Errorf("%w: arm pi turn supervisor proof: %v", ErrProcessTreeNotQuiescent, err)

			return
		}

		line, err := t.status.ReadString('\n')
		if err != nil {
			t.proofErr = fmt.Errorf("%w: await pi turn supervisor proof: %v", ErrProcessTreeNotQuiescent, err)

			return
		}

		if line != turnSupervisorProven {
			t.proofErr = fmt.Errorf("%w: invalid pi turn supervisor proof %q", ErrProcessTreeNotQuiescent, strings.TrimSpace(line))
		}
	})

	return t.proofErr
}

func (*processTree) descendantCount() (int, bool) {
	// A process-group existence probe proves quiescence, but it cannot
	// enumerate an authoritative nonzero membership count.
	return 0, false
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
