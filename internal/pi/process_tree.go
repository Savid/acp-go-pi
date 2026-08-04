package pi

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"
)

type directChildWait struct {
	done      chan struct{}
	start     chan struct{}
	startOnce sync.Once
	err       error
}

func installPausedDirectChildWait(cmd *exec.Cmd) *directChildWait {
	wait := &directChildWait{done: make(chan struct{}), start: make(chan struct{})}
	go func() {
		<-wait.start
		wait.err = cmd.Wait()
		close(wait.done)
	}()

	return wait
}

func (w *directChildWait) begin() {
	if w != nil {
		w.startOnce.Do(func() { close(w.start) })
	}
}

func (w *directChildWait) await(timeout time.Duration) error {
	if w == nil {
		return ErrProcessContainmentIncomplete
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.done:
		return w.err
	case <-timer.C:
		return fmt.Errorf("%w: direct child was not reaped", ErrProcessContainmentIncomplete)
	}
}

func (w *directChildWait) awaitReaped(timeout time.Duration) error {
	if w == nil {
		return ErrProcessContainmentIncomplete
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-w.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("%w: direct child was not reaped", ErrProcessContainmentIncomplete)
	}
}

// ContainmentSpec carries wrapper-owned identity and scratch metadata for one
// native launch.
type ContainmentSpec struct {
	DarwinBestEffort bool
	ScratchParent    string
	GenerationRoot   string
	RuntimeID        string
	LifecycleKind    string
	Isolation        *ProcessIsolation
}

// ErrProcessContainmentIncomplete means the selected native boundary did not
// complete. Managed-root callers must retain its resources.
var ErrProcessContainmentIncomplete = errors.New("pi process containment incomplete")

var completeProcessContainmentRecord = completeContainmentRecord

const (
	containmentStateRunning = "running"
	containmentStateAbsent  = "group_absent"
	containmentStateFailed  = "cleanup_incomplete"
)

const turnSupervisorComplete = "complete\n"

// processTreeCommand owns the platform launch wrapper and every parent-side
// descriptor that establishes its containment boundary. Linux launches an
// embedded subreaper, opted-in Darwin uses its best-effort process group, and
// unsupported platforms, including Windows, reject the launch.
type processTreeCommand struct {
	cmd             *exec.Cmd
	inherited       []*os.File
	startGate       *os.File
	control         *os.File
	ready           *os.File
	status          *bufio.Reader
	containment     containmentRecord
	nativeIsolation bool
}

func (c *processTreeCommand) releaseInherited() {
	for _, file := range c.inherited {
		_ = file.Close()
	}

	c.inherited = nil
}

func (c *processTreeCommand) releaseStartGate() error {
	if c == nil || c.startGate == nil {
		return nil
	}

	gate := c.startGate
	c.startGate = nil
	_, writeErr := gate.Write([]byte{1})
	closeErr := gate.Close()

	return errors.Join(writeErr, closeErr)
}

func (c *processTreeCommand) abortStartGate() {
	if c == nil || c.startGate == nil {
		return
	}

	_ = c.startGate.Close()
	c.startGate = nil
}

func (c *processTreeCommand) close() {
	if c == nil {
		return
	}

	c.releaseInherited()
	c.abortStartGate()

	if c.control != nil {
		_ = c.control.Close()
		c.control = nil
	}

	if c.ready != nil {
		_ = c.ready.Close()
		c.ready = nil
	}
}

// ProcessContainmentComplete reports whether the selected native boundary
// completed. Ordinary native command failures do not retain its resources.
func ProcessContainmentComplete(err error) bool {
	return !errors.Is(err, ErrProcessContainmentIncomplete)
}

func completeUnstartedContainment(record containmentRecord) error {
	if err := completeProcessContainmentRecord(record, containmentStateAbsent); err != nil {
		return fmt.Errorf("%w: finalize unstarted containment record: %v", ErrProcessContainmentIncomplete, err)
	}

	return nil
}
