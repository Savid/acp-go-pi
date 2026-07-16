package pi

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
)

// ErrProcessTreeNotQuiescent means the adapter could not prove that every
// descendant in a native command's containment boundary exited. Managed-root
// callers must retain their permit while this error remains unresolved.
var ErrProcessTreeNotQuiescent = errors.New("pi process tree quiescence not proven")

const turnSupervisorProven = "quiescent\n"

// processTreeCommand owns the platform launch wrapper and every parent-side
// descriptor that establishes its containment boundary. Linux launches an
// embedded subreaper, Windows launches directly into a Job Object, and
// platforms without an unescapable boundary reject the launch.
type processTreeCommand struct {
	cmd       *exec.Cmd
	inherited []*os.File
	control   *os.File
	ready     *os.File
	status    *bufio.Reader
}

func (c *processTreeCommand) releaseInherited() {
	for _, file := range c.inherited {
		_ = file.Close()
	}

	c.inherited = nil
}

func (c *processTreeCommand) close() {
	if c == nil {
		return
	}

	c.releaseInherited()

	if c.control != nil {
		_ = c.control.Close()
		c.control = nil
	}

	if c.ready != nil {
		_ = c.ready.Close()
		c.ready = nil
	}
}

// ProcessTreeQuiescent reports whether err proves that no native descendant
// remains. Native command failures do not retain a permit unless they include
// the containment sentinel.
func ProcessTreeQuiescent(err error) bool {
	return !errors.Is(err, ErrProcessTreeNotQuiescent)
}
