//go:build darwin

package pi

import (
	"bufio"
	"os"
	"sync"
)

// processTree holds the best-effort boundary Darwin establishes around one
// native process group. The memoized cleanup outcome belongs to that backend
// alone.
type processTree struct {
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
	cleanupOnce  sync.Once
	cleanupErr   error
}
