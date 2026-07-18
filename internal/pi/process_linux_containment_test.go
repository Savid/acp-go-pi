//go:build linux

package pi

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLinuxProcessTreeGateAndContainmentHelpers(t *testing.T) {
	if err := (*processTreeCommand)(nil).releaseStartGate(); err != nil {
		t.Fatalf("nil gate = %v", err)
	}
	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	gate := &processTreeCommand{startGate: writer}
	if releaseErr := gate.releaseStartGate(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	var value [1]byte
	if _, readErr := reader.Read(value[:]); readErr != nil || value[0] != 1 {
		t.Fatalf("gate byte = %v, err=%v", value, readErr)
	}
	_ = reader.Close()

	reader, writer, pipeErr = os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	_ = reader.Close()
	_ = writer.Close()
	if releaseErr := (&processTreeCommand{startGate: writer}).releaseStartGate(); releaseErr == nil {
		t.Fatal("closed gate released")
	}
	(*processTreeCommand)(nil).abortStartGate()
	reader, writer, pipeErr = os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	_ = reader.Close()
	aborted := &processTreeCommand{startGate: writer}
	aborted.abortStartGate()
	if aborted.startGate != nil {
		t.Fatal("aborted gate retained")
	}

	if _, handled, err := handleVanishedProcessGroupLeader(nil, nil); handled || err != nil {
		t.Fatalf("vanished leader = handled %v, err %v", handled, err)
	}

	originalComplete := completeProcessContainmentRecord
	t.Cleanup(func() { completeProcessContainmentRecord = originalComplete })
	want := errors.New("complete failed")
	completeProcessContainmentRecord = func(containmentRecord, string) error { return want }
	if err := completeUnstartedContainment(containmentRecord{}); !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("unstarted containment = %v", err)
	}

	if _, err := prepareProcessTreeCommand(exec.Command("/bin/true"), ContainmentSpec{DarwinBestEffort: true}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("Linux Darwin containment = %v", err)
	}
}

func TestLinuxProcessTreeValidationFailures(t *testing.T) {
	originalGetpgid := syscallGetpgid
	originalKill := syscallKill
	originalActivate := activateProcessContainmentRecord
	originalHandleVanished := processHandleVanishedLeader
	t.Cleanup(func() {
		syscallGetpgid = originalGetpgid
		syscallKill = originalKill
		activateProcessContainmentRecord = originalActivate
		processHandleVanishedLeader = originalHandleVanished
	})

	syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	wantHandled := errors.New("handled fast exit")
	processHandleVanishedLeader = func(_ *processTreeCommand, direct *directChildWait) (*processTree, bool, error) {
		direct.begin()
		if err := direct.await(time.Second); err != nil {
			t.Fatal(err)
		}

		return &processTree{direct: direct}, true, wantHandled
	}
	if tree, err := startProcessTree(&processTreeCommand{cmd: exec.Command("/bin/true")}); tree == nil || !errors.Is(err, wantHandled) {
		t.Fatalf("handled fast exit = tree %v, err %v", tree, err)
	}
	processHandleVanishedLeader = originalHandleVanished

	for _, test := range []struct {
		name    string
		getpgid func(int) (int, error)
	}{
		{name: "vanished", getpgid: func(int) (int, error) { return 0, syscall.ESRCH }},
		{name: "probe error", getpgid: func(int) (int, error) { return 0, syscall.EPERM }},
		{name: "mismatch", getpgid: func(pid int) (int, error) { return pid + 1, nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			syscallGetpgid = test.getpgid
			if _, err := startProcessTree(&processTreeCommand{cmd: exec.Command("/bin/true")}); !errors.Is(err, ErrProcessContainmentIncomplete) {
				t.Fatalf("validation error = %v", err)
			}
		})
	}

	syscallGetpgid = func(pid int) (int, error) { return pid, nil }
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	want := errors.New("activate failed")
	activateProcessContainmentRecord = func(containmentRecord, int, int) error { return want }
	if _, err := startProcessTree(&processTreeCommand{cmd: exec.Command("/bin/true")}); err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("activation error = %v", err)
	}

	activateProcessContainmentRecord = func(containmentRecord, int, int) error { return nil }
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	_ = writer.Close()
	if _, err := startProcessTree(&processTreeCommand{cmd: exec.Command("/bin/true"), startGate: writer}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("gate error = %v", err)
	}
}

func TestLaunchEnvironmentDarwinMarkers(t *testing.T) {
	env := LaunchSpec{
		AgentDir: "/agent",
		Containment: ContainmentSpec{
			DarwinBestEffort: true,
			RuntimeID:        "runtime",
			GenerationRoot:   "/scratch",
		},
	}.Environ()
	joined := strings.Join(env, "\n")
	if !strings.Contains(joined, envRuntimeID+"=runtime") || !strings.Contains(joined, envScratchRoot+"=/scratch") {
		t.Fatalf("Darwin marker environment = %v", env)
	}
}
