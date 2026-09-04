//go:build !windows

package pi

import (
	"errors"
	"os"
)

// ordinaryProcessAlreadyFinished reports the answer a kill gets when the child
// it names has already exited. Killing a finished child is how every close and
// relaunch path ends, so this outcome is success rather than a failure to
// contain anything.
func ordinaryProcessAlreadyFinished(err error) bool {
	return errors.Is(err, os.ErrProcessDone)
}
