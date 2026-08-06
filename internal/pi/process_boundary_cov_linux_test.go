//go:build linux

package pi

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestProcessGroupBoundaryCovRefusesCompletionWhileTheGroupSurvives proves
// that awaitProcessGroupBoundary never declares containment for a process
// group that is still answering signal 0 when the wait budget expires. It
// must return ErrProcessContainmentIncomplete naming the group, and it must
// not consult the supervisor completion channel: a live group that never
// published completion has to keep the managed-root resources retained.
func TestProcessGroupBoundaryCovRefusesCompletionWhileTheGroupSurvives(t *testing.T) {
	original := syscallKill
	t.Cleanup(func() { syscallKill = original })

	var probes atomic.Int64
	syscallKill = func(pid int, signal syscall.Signal) error {
		if signal == 0 {
			probes.Add(1)
		}

		return nil
	}

	tree := &processTree{pgid: 424242, supervised: true}
	err := tree.terminateAndWait(120 * time.Millisecond)
	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("surviving process group = %v, want incomplete containment", err)
	}
	if !strings.Contains(err.Error(), "process group 424242 remained live") {
		t.Fatalf("surviving process group refusal = %v", err)
	}
	if strings.Contains(err.Error(), "boundary") {
		t.Fatalf("surviving process group consulted the supervisor boundary: %v", err)
	}
	if probes.Load() < 2 {
		t.Fatalf("process group liveness probes = %d, want repeated polling", probes.Load())
	}
	if tree.boundaryErr != nil {
		t.Fatalf("supervisor boundary was consulted for a live group: %v", tree.boundaryErr)
	}
}

// TestProcessTreeCovCompletionTransferNeverFabricatesABoundary proves that
// the completion handoff only ever installs a boundary the launch actually
// owns. A launch without a completion descriptor must leave the tree with no
// boundary and no status reader, so a later completeBoundary call refuses
// instead of reporting a containment it never observed.
func TestProcessTreeCovCompletionTransferNeverFabricatesABoundary(t *testing.T) {
	transferProcessTreeCompletion(nil, &processTreeCommand{})
	transferProcessTreeCompletion(&processTree{}, nil)

	tree := &processTree{supervised: true}
	transferProcessTreeCompletion(tree, &processTreeCommand{})
	if tree.boundary != nil || tree.status != nil {
		t.Fatalf("completion-less launch installed a boundary: %#v", tree)
	}
	if err := tree.completeBoundary(); !errors.Is(err, ErrProcessContainmentIncomplete) ||
		!strings.Contains(err.Error(), "boundary channel is unavailable") {
		t.Fatalf("completion-less boundary = %v", err)
	}

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	owned := &processTree{supervised: true}
	launch := &processTreeCommand{completion: read}
	transferProcessTreeCompletion(owned, launch)
	if owned.boundary != read || owned.status == nil {
		t.Fatalf("owned completion was not transferred: %#v", owned)
	}
	if launch.completion != nil {
		t.Fatal("launch retained the completion descriptor after transfer")
	}
	if _, err = write.Write([]byte(turnSupervisorComplete)); err != nil {
		t.Fatal(err)
	}
	if err = owned.completeBoundary(); err != nil {
		t.Fatalf("transferred boundary = %v", err)
	}
}
