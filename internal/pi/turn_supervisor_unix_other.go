//go:build freebsd || openbsd

package pi

import (
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(*exec.Cmd, ContainmentSpec) (*processTreeCommand, error) {
	return nil, fmt.Errorf(
		"%w: platform cannot prove pi descendants that escape a process group",
		ErrProcessContainmentIncomplete,
	)
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
