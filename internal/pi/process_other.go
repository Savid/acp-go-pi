//go:build !unix

package pi

import (
	"errors"
	"os"
	"os/exec"
)

func configureProcessCommandPlatform(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		_, err := killProcess(cmd)

		return err
	}
}

func terminateProcess(cmd *exec.Cmd) (bool, error) {
	return killProcess(cmd)
}

func killProcess(cmd *exec.Cmd) (bool, error) {
	if cmd == nil || cmd.Process == nil {
		return false, nil
	}

	if err := cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}
