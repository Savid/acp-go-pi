//go:build darwin || freebsd || openbsd

package pi

import (
	"errors"
	"os/exec"
	"testing"
)

func TestUnsupportedUnixContainmentFailsClosed(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("pi"))
	if launch != nil || !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("prepareProcessTreeCommand() = %#v, %v", launch, err)
	}
}
