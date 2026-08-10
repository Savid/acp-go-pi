//go:build darwin || freebsd || openbsd

package pi

import (
	"os/exec"
	"testing"
)

func TestUnsupportedUnixOrdinaryLaunchAndExplicitRefusal(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("pi"), ContainmentSpec{})
	if launch == nil || err != nil || !launch.ordinary {
		t.Fatalf("ordinary prepareProcessTreeCommand() = %#v, %v", launch, err)
	}

	launch, err = prepareProcessTreeCommand(exec.Command("pi"), ContainmentSpec{
		Isolation: &ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{}},
	})
	if launch != nil || err == nil {
		t.Fatalf("explicit prepareProcessTreeCommand() = %#v, %v", launch, err)
	}
}
