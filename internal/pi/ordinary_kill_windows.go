//go:build windows

package pi

import (
	"errors"
	"os"
	"syscall"
)

// ordinaryProcessAlreadyFinished reports the same outcome, which Windows states
// differently. Waiting on a child here releases the handle the kill would have
// terminated, so a kill that follows a completed wait answers EINVAL rather
// than ErrProcessDone. Read as a failure it turned every close of a session
// whose turn had already ended into a containment refusal carrying "invalid
// argument"; both answers mean the child is gone, which is what the caller
// asked for.
func ordinaryProcessAlreadyFinished(err error) bool {
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.EINVAL)
}
