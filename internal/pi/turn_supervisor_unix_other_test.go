//go:build darwin || freebsd || openbsd

package pi

import (
	"errors"
	"os/exec"
	"testing"
)

func TestUnsupportedUnixContainmentFailsClosed(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("pi"), ContainmentSpec{})
	if launch != nil || !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("prepareProcessTreeCommand() = %#v, %v", launch, err)
	}
}
