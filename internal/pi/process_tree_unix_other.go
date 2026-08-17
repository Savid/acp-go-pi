//go:build linux || freebsd || openbsd

package pi

import (
	"bufio"
	"os"
	"sync"
)

// processTree holds the authoritative process-group boundary established
// around one native launch. Its mutex guards the supervisor control
// descriptor that kill closes.
type processTree struct {
	mu           sync.Mutex
	pgid         int
	process      *os.Process
	containment  containmentRecord
	control      *os.File
	supervised   bool
	boundary     *os.File
	status       *bufio.Reader
	boundaryOnce sync.Once
	boundaryErr  error
	direct       *directChildWait
	ordinary     bool
}
