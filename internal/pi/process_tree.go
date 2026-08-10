package pi

import (
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
	// OrdinaryEnvironment is the once-captured sanitized base used only when
	// Isolation is nil. It is never interpreted as an isolation policy.
	OrdinaryEnvironment map[string]string
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

// processTreeCommand owns the selected launch command and any parent-side
// descriptors that establish an explicit containment boundary. Ordinary mode
// launches the direct child portably; Linux explicit mode launches an embedded
// subreaper, and opted-in Darwin uses its best-effort process group.
type processTreeCommand struct {
	cmd             *exec.Cmd
	inherited       []*os.File
	startGate       *os.File
	control         *os.File
	ready           *os.File
	completion      *os.File
	containment     containmentRecord
	nativeIsolation bool
	ordinary        bool
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

	if c.completion != nil {
		_ = c.completion.Close()
		c.completion = nil
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
