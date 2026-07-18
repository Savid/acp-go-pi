//go:build darwin

package pi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type containmentTestFileInfo struct {
	name    string
	mode    os.FileMode
	modTime time.Time
}

func (info containmentTestFileInfo) Name() string       { return info.name }
func (containmentTestFileInfo) Size() int64             { return 0 }
func (info containmentTestFileInfo) Mode() os.FileMode  { return info.mode }
func (info containmentTestFileInfo) ModTime() time.Time { return info.modTime }
func (info containmentTestFileInfo) IsDir() bool        { return info.mode.IsDir() }
func (containmentTestFileInfo) Sys() any                { return nil }

type containmentTestDirEntry struct {
	name    string
	info    os.FileInfo
	infoErr error
}

func (entry containmentTestDirEntry) Name() string               { return entry.name }
func (containmentTestDirEntry) IsDir() bool                      { return false }
func (containmentTestDirEntry) Type() os.FileMode                { return 0 }
func (entry containmentTestDirEntry) Info() (os.FileInfo, error) { return entry.info, entry.infoErr }

func restoreContainmentOpsSeams(t *testing.T) {
	t.Helper()
	scan := containmentCandidateScan
	revalidate := containmentCandidateRevalidate
	signal := containmentPIDSignal
	readDir := containmentOpsReadDir
	lstat := containmentOpsLstat
	removeAll := containmentOpsRemoveAll
	abs := containmentOpsAbs
	rel := containmentOpsRel
	now := containmentOpsNow
	sleep := containmentOpsSleep
	list := containmentProcessList
	lookup := containmentProcessLookup
	arguments := containmentProcessArguments
	t.Cleanup(func() {
		containmentCandidateScan = scan
		containmentCandidateRevalidate = revalidate
		containmentPIDSignal = signal
		containmentOpsReadDir = readDir
		containmentOpsLstat = lstat
		containmentOpsRemoveAll = removeAll
		containmentOpsAbs = abs
		containmentOpsRel = rel
		containmentOpsNow = now
		containmentOpsSleep = sleep
		containmentProcessList = list
		containmentProcessLookup = lookup
		containmentProcessArguments = arguments
	})
}

func restoreContainmentRecordSeams(t *testing.T) {
	t.Helper()

	mkdir := containmentRecordMkdir
	abs := containmentRecordAbs
	readDir := containmentRecordReadDir
	now := containmentRecordNow
	remove := containmentRecordRemove
	open := containmentRecordOpen
	newFile := containmentRecordNewFile
	stat := containmentRecordFileStat
	readAll := containmentRecordReadAll
	marshal := containmentRecordMarshal
	createTemp := containmentRecordCreateTemp
	write := containmentRecordFileWrite
	syncFile := containmentRecordFileSync
	closeFile := containmentRecordFileClose
	link := containmentRecordLink
	rename := containmentRecordRename
	flock := containmentRecordFlock
	lstat := containmentRecordLstat
	sysctl := containmentRecordSysctl
	t.Cleanup(func() {
		containmentRecordMkdir = mkdir
		containmentRecordAbs = abs
		containmentRecordReadDir = readDir
		containmentRecordNow = now
		containmentRecordRemove = remove
		containmentRecordOpen = open
		containmentRecordNewFile = newFile
		containmentRecordFileStat = stat
		containmentRecordReadAll = readAll
		containmentRecordMarshal = marshal
		containmentRecordCreateTemp = createTemp
		containmentRecordFileWrite = write
		containmentRecordFileSync = syncFile
		containmentRecordFileClose = closeFile
		containmentRecordLink = link
		containmentRecordRename = rename
		containmentRecordFlock = flock
		containmentRecordLstat = lstat
		containmentRecordSysctl = sysctl
	})
}

func writeContainmentOpsRecord(t *testing.T, parent, runtimeID, root, state string, direct *ContainmentCandidate) containmentRecordData {
	t.Helper()
	record := containmentRecordData{
		SchemaVersion: containmentRecordSchema, Vendor: "pi", Containment: "best_effort",
		LifecycleKind: "session", RuntimeID: runtimeID, GenerationRoot: root,
		WrapperPID: os.Getpid(), WrapperStartSec: 1, WrapperStartUsec: 2, State: state,
	}
	if direct != nil {
		record.DirectChildPID = &direct.PID
		record.DirectChildStartSec = &direct.StartSec
		record.DirectChildStartUsec = &direct.StartUsec
		pgid := direct.PID
		record.OriginalProcessGID = &pgid
	}
	registry := filepath.Join(parent, containmentRegistryName)
	require.NoError(t, os.MkdirAll(registry, 0o700))
	require.NoError(t, writeContainmentRecord(filepath.Join(registry, runtimeID+".json"), record, true))

	return record
}

func TestDiagnoseContainmentReadOnly(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	diagnostics, err := DiagnoseContainment(parent)
	require.NoError(t, err)
	require.Equal(t, []ContainmentDiagnostic{}, diagnostics)

	runtimeID := strings.Repeat("1", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-one")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, "group_absent", nil)
	registry := filepath.Join(parent, containmentRegistryName)
	require.NoError(t, os.Mkdir(filepath.Join(registry, "ignored.json"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(registry, ".lock"), nil, 0o600))

	want := ContainmentCandidate{PID: 50, StartSec: 3, StartUsec: 4}
	containmentCandidateScan = func(record containmentRecordData) ([]ContainmentCandidate, error) {
		require.Equal(t, runtimeID, record.RuntimeID)

		return []ContainmentCandidate{want}, nil
	}
	diagnostics, err = DiagnoseContainment(parent)
	require.NoError(t, err)
	require.Equal(t, []ContainmentDiagnostic{{
		RuntimeID: runtimeID, LifecycleKind: "session", State: "group_absent",
		GenerationRoot: root, Candidates: []ContainmentCandidate{want}, AmbiguousPIDs: []int{},
	}}, diagnostics)
	_, err = os.Stat(root)
	require.NoError(t, err, "diagnose must not modify the generation")

	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		return nil, errors.New("scan failed")
	}
	_, err = DiagnoseContainment(parent)
	require.ErrorContains(t, err, "scan failed")

	require.NoError(t, os.WriteFile(filepath.Join(registry, runtimeID+".json"), []byte("bad"), 0o600))
	_, err = DiagnoseContainment(parent)
	require.ErrorContains(t, err, "decode containment record")
}

func TestCleanupContainmentNewCandidateAndKill(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("2", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-two")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, "cleanup_incomplete", nil)
	a := ContainmentCandidate{PID: 101, StartSec: 1, StartUsec: 1}
	b := ContainmentCandidate{PID: 102, StartSec: 2, StartUsec: 2}
	scans := 0
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		scans++
		switch scans {
		case 1:
			return []ContainmentCandidate{a}, nil
		case 2:
			return []ContainmentCandidate{b}, nil
		default:
			return []ContainmentCandidate{}, nil
		}
	}
	revalidations := map[int]int{}
	containmentCandidateRevalidate = func(_ containmentRecordData, candidate ContainmentCandidate) containmentCandidateState {
		revalidations[candidate.PID]++
		if revalidations[candidate.PID] == 1 {
			return containmentCandidateCorrelated
		}

		return containmentCandidateGone
	}
	type signalCall struct {
		pid int
		sig unix.Signal
	}
	var signals []signalCall
	containmentPIDSignal = func(pid int, signal unix.Signal) error {
		signals = append(signals, signalCall{pid: pid, sig: signal})

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.NoError(t, err)
	require.True(t, result.ResultReady)
	require.True(t, result.RootRemoved)
	require.Equal(t, []ContainmentCandidate{a, b}, result.TermSignalled)
	require.Empty(t, result.KillSignalled)
	require.Equal(t, []signalCall{{pid: 101, sig: unix.SIGTERM}, {pid: 102, sig: unix.SIGTERM}}, signals)
	require.GreaterOrEqual(t, scans, 4, "cleanup must rescan after the grace window")

	diagnostics, err := DiagnoseContainment(parent)
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)
	require.Equal(t, runtimeID, diagnostics[0].RuntimeID)
	require.Equal(t, root, diagnostics[0].GenerationRoot)
}

func TestCleanupContainmentRejectsReplacedRootIdentity(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("e", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-identity-race")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

	movedRoot := filepath.Join(parent, "original-generation")
	replacementMarker := filepath.Join(root, "replacement-marker")
	scans := 0
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		scans++
		if scans == 1 {
			if renameErr := os.Rename(root, movedRoot); renameErr != nil {
				return nil, renameErr
			}
			if mkdirErr := os.Mkdir(root, 0o700); mkdirErr != nil {
				return nil, mkdirErr
			}
			if writeErr := os.WriteFile(replacementMarker, []byte("unrelated"), 0o600); writeErr != nil {
				return nil, writeErr
			}
		}

		return nil, nil
	}
	containmentOpsSleep = func(time.Duration) {}
	removals := 0
	containmentOpsRemoveAll = func(string) error {
		removals++

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "identity changed")
	require.False(t, result.RootRemoved)
	require.Zero(t, removals)
	require.FileExists(t, replacementMarker)
}

func TestCleanupContainmentRepeatsParentContainmentBeforeRemoval(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("f", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-parent-race")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

	freshValidation := false
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		freshValidation = true

		return nil, nil
	}
	originalRel := containmentOpsRel
	freshRelCalls := 0
	containmentOpsRel = func(basepath, targetpath string) (string, error) {
		if freshValidation {
			freshRelCalls++

			return parentDirSegment, nil
		}

		return originalRel(basepath, targetpath)
	}
	removals := 0
	containmentOpsRemoveAll = func(string) error {
		removals++

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "outside the scratch parent")
	require.False(t, result.RootRemoved)
	require.Equal(t, 1, freshRelCalls)
	require.Zero(t, removals)
	require.DirExists(t, root)
}

func TestCleanupContainmentRejectsChangedValidatedRootPath(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("c", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-path-race")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

	freshValidation := false
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		freshValidation = true

		return nil, nil
	}
	originalAbs := containmentOpsAbs
	containmentOpsAbs = func(path string) (string, error) {
		if freshValidation && path == root {
			return filepath.Join(parent, "acp-go-pi-runtime-other"), nil
		}

		return originalAbs(path)
	}
	removals := 0
	containmentOpsRemoveAll = func(string) error {
		removals++

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "path changed")
	require.False(t, result.RootRemoved)
	require.Zero(t, removals)
	require.DirExists(t, root)
}

func TestContainmentCleanupDeadlineAfterCompleteRootValidation(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-pi-runtime-validation-deadline")
	require.NoError(t, os.Mkdir(root, 0o700))
	rootIdentity, err := os.Lstat(root)
	require.NoError(t, err)

	base := time.Unix(425, 0)
	deadline := base.Add(defaultProcessTreeWait)
	clockCalls := 0
	containmentOpsNow = func() time.Time {
		clockCalls++
		if clockCalls >= 4 {
			return deadline
		}

		return base
	}
	containmentOpsRemoveAll = func(string) error {
		t.Fatal("cleanup must not remove after complete validation exhausts the deadline")

		return nil
	}

	removed, err := removeValidatedCleanupRoot(parent, root, rootIdentity, deadline)
	require.ErrorIs(t, err, errContainmentCleanupDeadline)
	require.False(t, removed)
	require.Equal(t, 4, clockCalls)
	require.DirExists(t, root)
}

func TestContainmentCleanupRootRemovalConsumesBudget(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("d", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-removal-deadline")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

	now := time.Unix(450, 0)
	deadline := now.Add(defaultProcessTreeWait)
	containmentOpsNow = func() time.Time { return now }
	removed := false
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		require.False(t, removed, "cleanup must not scan after removal consumes its deadline")

		return nil, nil
	}
	containmentPIDSignal = func(int, unix.Signal) error {
		t.Fatal("empty-candidate cleanup must not signal")

		return nil
	}
	containmentOpsSleep = func(time.Duration) {}
	originalAbs := containmentOpsAbs
	containmentOpsAbs = func(path string) (string, error) {
		require.False(t, removed, "cleanup must not validate paths after removal consumes its deadline")

		return originalAbs(path)
	}
	originalRel := containmentOpsRel
	containmentOpsRel = func(basepath, targetpath string) (string, error) {
		require.False(t, removed, "cleanup must not validate paths after removal consumes its deadline")

		return originalRel(basepath, targetpath)
	}
	originalLstat := containmentOpsLstat
	containmentOpsLstat = func(path string) (os.FileInfo, error) {
		require.False(t, removed, "cleanup must not inspect roots after removal consumes its deadline")

		return originalLstat(path)
	}
	originalRemove := containmentOpsRemoveAll
	containmentOpsRemoveAll = func(path string) error {
		require.False(t, removed, "cleanup must remove its selected root at most once")
		if removeErr := originalRemove(path); removeErr != nil {
			return removeErr
		}

		removed = true
		now = deadline

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.ErrorIs(t, err, errContainmentCleanupDeadline)
	require.True(t, result.RootRemoved)
	require.True(t, result.ResultReady)
	require.True(t, removed)
	require.Empty(t, result.RemainingCorrelated)
	require.NoDirExists(t, root)
}

func TestTerminateThenKillContainmentCandidateReporting(t *testing.T) {
	restoreContainmentOpsSeams(t)
	record := containmentRecordData{}
	candidate := ContainmentCandidate{PID: 150, StartSec: 1, StartUsec: 2}
	containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
		return containmentCandidateCorrelated
	}

	result := ContainmentCleanupResult{
		TermSignalled:       []ContainmentCandidate{candidate},
		RemainingCorrelated: []ContainmentCandidate{candidate},
	}
	containmentPIDSignal = func(pid int, signal unix.Signal) error {
		require.Equal(t, candidate.PID, pid)
		require.Equal(t, unix.SIGKILL, signal)

		return nil
	}
	require.NoError(t, terminateThenKillContainmentCandidate(record, candidate, time.Now().Add(time.Second), &result))
	require.Equal(t, []ContainmentCandidate{candidate}, result.KillSignalled)
	require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)

	result.KillSignalled = nil
	containmentPIDSignal = func(int, unix.Signal) error { return syscall.ESRCH }
	require.NoError(t, terminateThenKillContainmentCandidate(record, candidate, time.Now().Add(time.Second), &result))
	require.Empty(t, result.KillSignalled, "ESRCH must not be reported as a delivered KILL")
	require.Empty(t, result.RemainingCorrelated, "ESRCH must remove the last positive correlation")

	result.TermSignalled = nil
	require.NoError(t, terminateThenKillContainmentCandidate(record, candidate, time.Now().Add(time.Second), &result))
	require.Empty(t, result.TermSignalled, "ESRCH must not be reported as a delivered TERM")
	require.Empty(t, result.RemainingCorrelated)
}

func TestCleanupContainmentAmbiguityAndRecordedPIDNotAuthority(t *testing.T) {
	t.Run("marker scrub becomes report-only ambiguity", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent := t.TempDir()
		runtimeID := strings.Repeat("3", 32)
		root := filepath.Join(parent, "acp-go-pi-runtime-three")
		require.NoError(t, os.Mkdir(root, 0o700))
		writeContainmentOpsRecord(t, parent, runtimeID, root, "group_absent", nil)
		candidate := ContainmentCandidate{PID: 201, StartSec: 1, StartUsec: 1}
		first := true
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			if first {
				first = false

				return []ContainmentCandidate{candidate}, nil
			}

			return []ContainmentCandidate{}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateAmbiguous
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			t.Fatal("an ambiguous candidate must never be signalled")

			return nil
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorContains(t, err, "ambiguous process identities")
		require.True(t, result.ResultReady)
		require.Equal(t, []int{candidate.PID}, result.AmbiguousPIDs)
		require.False(t, result.RootRemoved)
		_, statErr := os.Stat(root)
		require.NoError(t, statErr)
	})

	t.Run("recorded pid and pgid are never signal authority", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent := t.TempDir()
		runtimeID := strings.Repeat("4", 32)
		root := filepath.Join(parent, "acp-go-pi-runtime-four")
		require.NoError(t, os.Mkdir(root, 0o700))
		direct := ContainmentCandidate{PID: 301, StartSec: 1, StartUsec: 2}
		writeContainmentOpsRecord(t, parent, runtimeID, root, "cleanup_incomplete", &direct)
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			return []ContainmentCandidate{}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateAmbiguous
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			t.Fatal("recorded identifiers must never be signalled")

			return nil
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorContains(t, err, "ambiguous process identities")
		require.Equal(t, []int{direct.PID}, result.AmbiguousPIDs)
		require.False(t, result.RootRemoved)
	})
}

func TestCleanupContainmentValidation(t *testing.T) {
	parent := t.TempDir()
	runtimeID := strings.Repeat("5", 32)
	_, err := CleanupContainment(parent, runtimeID, false)
	require.ErrorContains(t, err, "requires -force")
	_, err = CleanupContainment(parent, "BAD", true)
	require.ErrorContains(t, err, "128-bit lowercase hex")
	_, err = CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "inspect containment registry")

	root := filepath.Join(parent, "acp-go-pi-runtime-five")
	require.NoError(t, os.Mkdir(root, 0o700))
	record := writeContainmentOpsRecord(t, parent, runtimeID, root, "group_absent", nil)
	record.RuntimeID = strings.Repeat("6", 32)
	require.NoError(t, writeContainmentRecord(filepath.Join(parent, containmentRegistryName, runtimeID+".json"), record, false))
	_, err = CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "filename does not match runtime id")

	record.RuntimeID = runtimeID
	record.GenerationRoot = filepath.Join(filepath.Dir(parent), "acp-go-pi-runtime-outside")
	require.NoError(t, writeContainmentRecord(filepath.Join(parent, containmentRegistryName, runtimeID+".json"), record, false))
	_, err = CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "outside the scratch parent")

	record.GenerationRoot = filepath.Join(parent, "wrong-prefix")
	require.NoError(t, writeContainmentRecord(filepath.Join(parent, containmentRegistryName, runtimeID+".json"), record, false))
	_, err = CleanupContainment(parent, runtimeID, true)
	require.ErrorContains(t, err, "generation root is invalid")

	for _, state := range []string{"running", "cleanup_incomplete"} {
		t.Run("missing root "+state, func(t *testing.T) {
			missingParent := t.TempDir()
			missingID := strings.Repeat(string(state[0]), 32)
			if !validRuntimeID(missingID) {
				missingID = strings.Repeat("a", 32)
			}
			missingRoot := filepath.Join(missingParent, "acp-go-pi-runtime-missing")
			writeContainmentOpsRecord(t, missingParent, missingID, missingRoot, state, nil)
			_, cleanupErr := CleanupContainment(missingParent, missingID, true)
			require.ErrorContains(t, cleanupErr, "missing before containment reached group_absent")
		})
	}
}

func TestCleanupContainmentDeadlineIsTerminalAfterPreEnumerationValidation(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("d", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-deadline")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, "cleanup_incomplete", nil)

	now := time.Unix(1, 0)
	containmentOpsNow = func() time.Time { return now }
	originalAbs := containmentOpsAbs
	absCalls := 0
	containmentOpsAbs = func(path string) (string, error) {
		absolute, err := originalAbs(path)
		absCalls++
		if absCalls == 1 {
			now = now.Add(defaultProcessTreeWait)
		}

		return absolute, err
	}

	scanCalls := 0
	containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
		scanCalls++

		return []ContainmentCandidate{}, nil
	}
	signalCalls := 0
	containmentPIDSignal = func(int, unix.Signal) error {
		signalCalls++

		return nil
	}
	containmentOpsSleep = func(time.Duration) {}
	removeCalls := 0
	containmentOpsRemoveAll = func(string) error {
		removeCalls++

		return nil
	}

	result, err := CleanupContainment(parent, runtimeID, true)
	require.ErrorIs(t, err, errContainmentCleanupDeadline)
	require.False(t, result.ResultReady)
	require.False(t, result.RootRemoved)
	require.Zero(t, scanCalls, "cleanup must not enumerate after pre-enumeration validation exhausts the deadline")
	require.Zero(t, signalCalls, "cleanup must not signal after pre-enumeration validation exhausts the deadline")
	require.Zero(t, removeCalls, "cleanup must not remove the selected root after its deadline")
	require.DirExists(t, root)
}

func TestContainmentCleanupDeadlineCheckpoints(t *testing.T) {
	t.Run("main cleanup", func(t *testing.T) {
		for expireAt := 2; expireAt <= 17; expireAt++ {
			t.Run(strconv.Itoa(expireAt), func(t *testing.T) {
				restoreContainmentOpsSeams(t)
				parent := t.TempDir()
				runtimeID := strings.Repeat("e", 32)
				root := filepath.Join(parent, "acp-go-pi-runtime-checkpoint")
				require.NoError(t, os.Mkdir(root, 0o700))
				writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

				base := time.Unix(1, 0)
				clockCalls := 0
				containmentOpsNow = func() time.Time {
					clockCalls++
					if clockCalls >= expireAt {
						return base.Add(defaultProcessTreeWait)
					}

					return base
				}
				containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
					return []ContainmentCandidate{}, nil
				}
				containmentOpsSleep = func(time.Duration) {}

				_, err := CleanupContainment(parent, runtimeID, true)
				require.ErrorIs(t, err, errContainmentCleanupDeadline)
			})
		}
	})

	t.Run("initial candidate", func(t *testing.T) {
		for _, expireAt := range []int{8, 10} {
			t.Run(strconv.Itoa(expireAt), func(t *testing.T) {
				restoreContainmentOpsSeams(t)
				parent := t.TempDir()
				runtimeID := strings.Repeat("e", 32)
				root := filepath.Join(parent, "acp-go-pi-runtime-candidate-checkpoint")
				require.NoError(t, os.Mkdir(root, 0o700))
				writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)
				candidate := ContainmentCandidate{PID: 901, StartSec: 1, StartUsec: 2}

				base := time.Unix(1, 0)
				clockCalls := 0
				containmentOpsNow = func() time.Time {
					clockCalls++
					if clockCalls >= expireAt {
						return base.Add(defaultProcessTreeWait)
					}

					return base
				}
				containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
					return []ContainmentCandidate{candidate}, nil
				}
				containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
					return containmentCandidateCorrelated
				}
				containmentPIDSignal = func(int, unix.Signal) error { return nil }

				_, err := CleanupContainment(parent, runtimeID, true)
				require.ErrorIs(t, err, errContainmentCleanupDeadline)
			})
		}
	})

	t.Run("candidate controller", func(t *testing.T) {
		candidate := ContainmentCandidate{PID: 902, StartSec: 1, StartUsec: 2}
		record := containmentRecordData{}

		t.Run("entry", func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			now := time.Unix(1, 0)
			containmentOpsNow = func() time.Time { return now }
			err := terminateThenKillContainmentCandidate(record, candidate, now, &ContainmentCleanupResult{})
			require.ErrorIs(t, err, errContainmentCleanupDeadline)
		})

		t.Run("term signal", func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			now := time.Unix(1, 0)
			deadline := now.Add(defaultProcessTreeWait)
			containmentOpsNow = func() time.Time { return now }
			containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
				return containmentCandidateCorrelated
			}
			containmentPIDSignal = func(int, unix.Signal) error {
				now = deadline

				return nil
			}

			err := terminateThenKillContainmentCandidate(record, candidate, deadline, &ContainmentCleanupResult{})
			require.ErrorIs(t, err, errContainmentCleanupDeadline)
		})

		t.Run("term wait", func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			now := time.Unix(1, 0)
			deadline := now.Add(defaultProcessTreeWait)
			validations := 0
			containmentOpsNow = func() time.Time { return now }
			containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
				validations++
				if validations == 2 {
					now = deadline
				}

				return containmentCandidateCorrelated
			}
			containmentPIDSignal = func(int, unix.Signal) error { return nil }

			err := terminateThenKillContainmentCandidate(record, candidate, deadline, &ContainmentCleanupResult{})
			require.ErrorIs(t, err, errContainmentCleanupDeadline)
		})

		t.Run("kill signal", func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			now := time.Unix(1, 0)
			deadline := now.Add(defaultProcessTreeWait)
			containmentOpsNow = func() time.Time { return now }
			containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
				return containmentCandidateCorrelated
			}
			containmentPIDSignal = func(int, unix.Signal) error {
				now = deadline

				return nil
			}

			result := &ContainmentCleanupResult{TermSignalled: []ContainmentCandidate{candidate}}
			err := terminateThenKillContainmentCandidate(record, candidate, deadline, result)
			require.ErrorIs(t, err, errContainmentCleanupDeadline)
		})
	})

	t.Run("selected candidate before validation", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		candidate := ContainmentCandidate{PID: 903, StartSec: 1, StartUsec: 2}
		base := time.Unix(1, 0)
		deadline := base.Add(defaultProcessTreeWait)
		clockCalls := 0
		containmentOpsNow = func() time.Time {
			clockCalls++
			if clockCalls >= 2 {
				return deadline
			}

			return base
		}

		remaining := awaitContainmentCandidates(containmentRecordData{}, []ContainmentCandidate{candidate}, deadline, &ContainmentCleanupResult{})
		require.Equal(t, []ContainmentCandidate{candidate}, remaining)
	})
}

func TestContainmentRecordStrictValidation(t *testing.T) {
	parent := t.TempDir()
	runtimeID := strings.Repeat("b", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-strict")
	record := writeContainmentOpsRecord(t, parent, runtimeID, root, "running", nil)
	path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")

	invalid := []struct {
		name   string
		mutate func(*containmentRecordData)
		want   string
	}{
		{name: "state", mutate: func(record *containmentRecordData) { record.State = "unknown" }, want: "state is invalid"},
		{name: "lifecycle", mutate: func(record *containmentRecordData) { record.LifecycleKind = "other" }, want: "lifecycle kind is invalid"},
		{name: "partial child", mutate: func(record *containmentRecordData) { pid := 4; record.DirectChildPID = &pid }, want: "all present or all absent"},
		{name: "wrapper", mutate: func(record *containmentRecordData) { record.WrapperPID = 0 }, want: "wrapper identity is invalid"},
		{name: "wrapper usec", mutate: func(record *containmentRecordData) { record.WrapperStartUsec = 1_000_000 }, want: "wrapper identity is invalid"},
		{name: "child pgid", mutate: func(record *containmentRecordData) {
			pid, sec, usec, pgid := 4, int64(5), int32(6), 7
			record.DirectChildPID, record.DirectChildStartSec, record.DirectChildStartUsec, record.OriginalProcessGID = &pid, &sec, &usec, &pgid
		}, want: "direct child identity is invalid"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			candidate := record
			test.mutate(&candidate)
			require.NoError(t, writeContainmentRecord(path, candidate, false))
			_, err := readContainmentRecord(path)
			require.ErrorContains(t, err, test.want)
		})
	}

	require.NoError(t, writeContainmentRecord(path, record, false))
	swappedPath := filepath.Join(parent, containmentRegistryName, strings.Repeat("c", 32)+".json")
	require.NoError(t, os.Rename(path, swappedPath))
	_, err := readContainmentRecord(swappedPath)
	require.ErrorContains(t, err, "filename does not match runtime id")
	require.ErrorContains(t, completeContainmentRecord(containmentRecord{path: swappedPath}, containmentStateAbsent), "filename does not match runtime id")
	_, err = DiagnoseContainment(parent)
	require.ErrorContains(t, err, "filename does not match runtime id")
}

func TestContainmentRecordSerializedReplacementAndStaleTemp(t *testing.T) {
	parent := t.TempDir()
	runtimeID := strings.Repeat("e", 32)
	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
	require.NoError(t, err)
	record := writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateRunning, nil)
	path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")
	stalePath := path + ".tmp"
	require.NoError(t, os.WriteFile(stalePath, []byte("stale"), 0o600))

	handle := containmentRecord{path: path}
	errs := make(chan error, 48)
	var group sync.WaitGroup

	for index := range 48 {
		group.Add(1)

		go func() {
			defer group.Done()

			switch index % 3 {
			case 0:
				errs <- activateContainmentRecord(handle, os.Getpid(), os.Getpid())
			case 1:
				errs <- completeContainmentRecord(handle, containmentStateAbsent)
			default:
				candidate := record
				candidate.State = containmentStateFailed
				errs <- writeContainmentRecord(path, candidate, false)
			}
		}()
	}

	group.Wait()
	close(errs)
	for updateErr := range errs {
		require.NoError(t, updateErr)
	}

	_, err = readContainmentRecord(path)
	require.NoError(t, err)
	stale, err := os.ReadFile(stalePath)
	require.NoError(t, err)
	require.Equal(t, "stale", string(stale))
}

func TestContainmentRecordReaderRejectsOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	runtimeID := strings.Repeat("f", 32)
	outsideRoot, err := os.MkdirTemp(t.TempDir(), "acp-go-pi-runtime-")
	require.NoError(t, err)
	record := writeContainmentOpsRecord(t, parent, runtimeID, outsideRoot, containmentStateAbsent, nil)
	path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")

	_, err = readContainmentRecord(path)
	require.ErrorContains(t, err, "outside the scratch parent")
	require.ErrorContains(t, completeContainmentRecord(containmentRecord{path: path}, containmentStateAbsent), "outside the scratch parent")

	nonJSONPath := filepath.Join(parent, containmentRegistryName, runtimeID+".record")
	require.NoError(t, writeContainmentRecord(nonJSONPath, record, true))
	_, err = readContainmentRecord(nonJSONPath)
	require.ErrorContains(t, err, "filename does not match runtime id")
}

func TestContainmentRegistryAndRecordIntegrity(t *testing.T) {
	t.Run("registry symlink is never followed", func(t *testing.T) {
		parent := t.TempDir()
		target := t.TempDir()
		require.NoError(t, os.Symlink(target, filepath.Join(parent, containmentRegistryName)))
		_, err := DiagnoseContainment(parent)
		require.ErrorContains(t, err, "non-symlink directory with mode 0700")
		_, err = prepareContainmentRecord(ContainmentSpec{
			DarwinBestEffort: true, ScratchParent: parent,
			GenerationRoot: filepath.Join(parent, "acp-go-pi-runtime-integrity"),
			RuntimeID:      strings.Repeat("d", 32), LifecycleKind: "session",
		})
		require.ErrorContains(t, err, "non-symlink directory with mode 0700")
	})

	t.Run("registry mode is exact", func(t *testing.T) {
		parent := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(parent, containmentRegistryName), 0o755))
		_, err := DiagnoseContainment(parent)
		require.ErrorContains(t, err, "mode 0700")
	})

	t.Run("record mode and symlink are rejected", func(t *testing.T) {
		parent := t.TempDir()
		runtimeID := strings.Repeat("e", 32)
		root := filepath.Join(parent, "acp-go-pi-runtime-integrity")
		require.NoError(t, os.Mkdir(root, 0o700))
		writeContainmentOpsRecord(t, parent, runtimeID, root, "group_absent", nil)
		path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")
		require.NoError(t, os.Chmod(path, 0o644))
		_, err := DiagnoseContainment(parent)
		require.ErrorContains(t, err, "mode 0600")

		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.Remove(path))
		target := filepath.Join(parent, "record-target")
		require.NoError(t, os.WriteFile(target, contents, 0o600))
		require.NoError(t, os.Symlink(target, path))
		_, err = DiagnoseContainment(parent)
		require.ErrorContains(t, err, "non-symlink regular file")
	})

	t.Run("unknown and trailing JSON are rejected", func(t *testing.T) {
		parent := t.TempDir()
		runtimeID := strings.Repeat("f", 32)
		root := filepath.Join(parent, "acp-go-pi-runtime-integrity")
		require.NoError(t, os.Mkdir(root, 0o700))
		writeContainmentOpsRecord(t, parent, runtimeID, root, "group_absent", nil)
		path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		unknown := strings.Replace(string(contents), `"state":"group_absent"`, `"extra":true,"state":"group_absent"`, 1)
		require.NoError(t, os.WriteFile(path, []byte(unknown), 0o600))
		_, err = DiagnoseContainment(parent)
		require.ErrorContains(t, err, "unknown field")
		require.NoError(t, os.WriteFile(path, append(contents, []byte("{}\n")...), 0o600))
		_, err = DiagnoseContainment(parent)
		require.ErrorContains(t, err, "trailing JSON value")
	})

	t.Run("diagnose rejects outside root", func(t *testing.T) {
		parent := t.TempDir()
		runtimeID := strings.Repeat("a", 32)
		outside := filepath.Join(t.TempDir(), "acp-go-pi-runtime-outside")
		require.NoError(t, os.Mkdir(outside, 0o700))
		writeContainmentOpsRecord(t, parent, runtimeID, outside, "group_absent", nil)
		_, err := DiagnoseContainment(parent)
		require.ErrorContains(t, err, "outside the scratch parent")
	})

	t.Run("lifecycle updates reject registry swap", func(t *testing.T) {
		parent := t.TempDir()
		root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
		require.NoError(t, err)
		handle, err := prepareContainmentRecord(ContainmentSpec{
			DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: root,
			RuntimeID: strings.Repeat("1", 32), LifecycleKind: "session",
		})
		require.NoError(t, err)
		registry := filepath.Join(parent, containmentRegistryName)
		require.NoError(t, os.Rename(registry, registry+"-saved"))
		require.NoError(t, os.Symlink(t.TempDir(), registry))
		require.ErrorContains(t, activateContainmentRecord(handle, os.Getpid(), os.Getpid()), "non-symlink directory")
		require.ErrorContains(t, completeContainmentRecord(handle, "group_absent"), "non-symlink directory")
	})
}

func TestContainmentRecordPreparationFailures(t *testing.T) {
	wantErr := errors.New("injected containment record failure")
	_, err := prepareContainmentRecord(ContainmentSpec{})
	require.NoError(t, err)
	_, err = prepareContainmentRecord(ContainmentSpec{DarwinBestEffort: true})
	require.ErrorContains(t, err, "runtime id")

	for _, test := range []struct {
		name  string
		setup func(*testing.T, string, time.Time)
		want  string
	}{
		{name: "mkdir", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordMkdir = func(string, os.FileMode) error { return wantErr }
		}, want: "create containment registry"},
		{name: "read registry", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordReadDir = func(string) ([]os.DirEntry, error) { return nil, wantErr }
		}, want: "read containment registry"},
		{name: "lock registry", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordFlock = func(int, int) error { return wantErr }
		}, want: "lock containment registry"},
		{name: "entry info", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordReadDir = func(string) ([]os.DirEntry, error) {
				return []os.DirEntry{containmentTestDirEntry{name: "entry.json", infoErr: wantErr}}, nil
			}
		}, want: "inspect containment record"},
		{name: "expire", setup: func(t *testing.T, parent string, now time.Time) {
			t.Helper()

			runtimeID := strings.Repeat("b", 32)
			root := filepath.Join(parent, "acp-go-pi-runtime-expired")
			writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)
			path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")
			expired := now.Add(-containmentRecordMaxAge - time.Hour)
			require.NoError(t, os.Chtimes(path, expired, expired))
			containmentRecordRemove = func(string) error { return wantErr }
		}, want: "expire containment record"},
		{name: "limit", setup: func(_ *testing.T, _ string, now time.Time) {
			entries := make([]os.DirEntry, containmentRecordLimit)
			for index := range entries {
				entries[index] = containmentTestDirEntry{
					name: fmt.Sprintf("%032x.json", index),
					info: containmentTestFileInfo{name: "record.json", mode: 0o600, modTime: now},
				}
			}
			containmentRecordReadDir = func(string) ([]os.DirEntry, error) { return entries, nil }
		}, want: "reached 4096"},
		{name: "wrapper identity", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordSysctl = func(string, ...int) (*unix.KinfoProc, error) { return nil, wantErr }
		}, want: "identify containment wrapper"},
		{name: "publish", setup: func(_ *testing.T, _ string, _ time.Time) {
			containmentRecordMarshal = func(any) ([]byte, error) { return nil, wantErr }
		}, want: "encode containment record"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreContainmentRecordSeams(t)
			parent := t.TempDir()
			root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
			require.NoError(t, err)
			now := time.Now()
			containmentRecordNow = func() time.Time { return now }
			test.setup(t, parent, now)
			_, err = prepareContainmentRecord(ContainmentSpec{
				DarwinBestEffort: true,
				ScratchParent:    parent,
				GenerationRoot:   root,
				RuntimeID:        strings.Repeat("a", 32),
				LifecycleKind:    "session",
			})
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("expired record already absent", func(t *testing.T) {
		restoreContainmentRecordSeams(t)
		parent := t.TempDir()
		root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
		require.NoError(t, err)
		now := time.Now()
		containmentRecordNow = func() time.Time { return now }
		expiredID := strings.Repeat("8", 32)
		expiredRoot := filepath.Join(parent, "acp-go-pi-runtime-expired")
		writeContainmentOpsRecord(t, parent, expiredID, expiredRoot, containmentStateAbsent, nil)
		expiredPath := filepath.Join(parent, containmentRegistryName, expiredID+".json")
		expired := now.Add(-containmentRecordMaxAge - time.Hour)
		require.NoError(t, os.Chtimes(expiredPath, expired, expired))
		containmentRecordRemove = func(string) error { return os.ErrNotExist }
		handle, err := prepareContainmentRecord(ContainmentSpec{
			DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: root,
			RuntimeID: strings.Repeat("9", 32), LifecycleKind: "session",
		})
		require.NoError(t, err)
		require.NotEmpty(t, handle.path)
	})
}

func TestContainmentRecordExpiryRetainsUnresolvedStates(t *testing.T) {
	restoreContainmentRecordSeams(t)
	parent := t.TempDir()
	now := time.Now()
	containmentRecordNow = func() time.Time { return now }
	expired := now.Add(-containmentRecordMaxAge - time.Hour)

	paths := make(map[string]string)
	for index, state := range []string{containmentStateAbsent, containmentStateRunning, containmentStateFailed} {
		runtimeID := strings.Repeat(strconv.Itoa(index+1), 32)
		root := filepath.Join(parent, fmt.Sprintf("acp-go-pi-runtime-%d", index+1))
		writeContainmentOpsRecord(t, parent, runtimeID, root, state, nil)
		path := filepath.Join(parent, containmentRegistryName, runtimeID+".json")
		require.NoError(t, os.Chtimes(path, expired, expired))
		paths[state] = path
	}

	newRoot, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
	require.NoError(t, err)
	_, err = prepareContainmentRecord(ContainmentSpec{
		DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: newRoot,
		RuntimeID: strings.Repeat("a", 32), LifecycleKind: containmentSessionKind,
	})
	require.NoError(t, err)

	_, err = os.Stat(paths[containmentStateAbsent])
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, state := range []string{containmentStateRunning, containmentStateFailed} {
		_, err = os.Stat(paths[state])
		require.NoError(t, err, "%s records must remain actionable", state)
	}
}

func TestContainmentRecordExpiryRejectsUnreadableOldRecord(t *testing.T) {
	restoreContainmentRecordSeams(t)
	parent := t.TempDir()
	registry := filepath.Join(parent, containmentRegistryName)
	require.NoError(t, os.Mkdir(registry, 0o700))
	path := filepath.Join(registry, strings.Repeat("b", 32)+".json")
	require.NoError(t, os.WriteFile(path, []byte("bad"), 0o600))
	now := time.Now()
	expired := now.Add(-containmentRecordMaxAge - time.Hour)
	require.NoError(t, os.Chtimes(path, expired, expired))
	containmentRecordNow = func() time.Time { return now }

	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
	require.NoError(t, err)
	_, err = prepareContainmentRecord(ContainmentSpec{
		DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: root,
		RuntimeID: strings.Repeat("a", 32), LifecycleKind: containmentSessionKind,
	})
	require.ErrorContains(t, err, "decode containment record")
}

func TestContainmentCleanupDeadlineStopsSignals(t *testing.T) {
	restoreContainmentOpsSeams(t)
	parent := t.TempDir()
	runtimeID := strings.Repeat("c", 32)
	root := filepath.Join(parent, "acp-go-pi-runtime-deadline")
	require.NoError(t, os.Mkdir(root, 0o700))
	writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)
	candidate := ContainmentCandidate{PID: 901, StartSec: 1, StartUsec: 2}

	t.Run("deadline expires after enumeration", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Now()
		containmentOpsNow = func() time.Time { return now }
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			now = now.Add(defaultProcessTreeWait + time.Second)

			return []ContainmentCandidate{candidate}, nil
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			t.Fatal("cleanup must not signal after its deadline")

			return nil
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)
		require.True(t, result.ResultReady)
	})

	t.Run("deadline expires after initial revalidation", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Now()
		containmentOpsNow = func() time.Time { return now }
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			return []ContainmentCandidate{candidate}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			now = now.Add(defaultProcessTreeWait + time.Second)

			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			t.Fatal("cleanup must not signal after its deadline")

			return nil
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)
		require.True(t, result.ResultReady)
	})

	for _, test := range []struct {
		name          string
		state         containmentCandidateState
		wantRemaining []ContainmentCandidate
		wantAmbiguous []int
	}{
		{name: "gone immediately before deadline", state: containmentCandidateGone, wantRemaining: []ContainmentCandidate{}, wantAmbiguous: []int{}},
		{name: "ambiguous immediately before deadline", state: containmentCandidateAmbiguous, wantRemaining: []ContainmentCandidate{}, wantAmbiguous: []int{candidate.PID}},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			now := time.Unix(30, 0)
			deadline := now.Add(defaultProcessTreeWait)
			containmentOpsNow = func() time.Time { return now }
			containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
				return []ContainmentCandidate{candidate}, nil
			}
			containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
				now = deadline

				return test.state
			}
			containmentPIDSignal = func(int, unix.Signal) error {
				t.Fatal("cleanup must not signal a gone or ambiguous identity")

				return nil
			}

			result, err := CleanupContainment(parent, runtimeID, true)
			require.ErrorIs(t, err, errContainmentCleanupDeadline)
			require.True(t, result.ResultReady)
			require.Equal(t, test.wantRemaining, result.RemainingCorrelated)
			require.Equal(t, test.wantAmbiguous, result.AmbiguousPIDs)
		})
	}

	t.Run("TERM ESRCH immediately before deadline", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Unix(40, 0)
		deadline := now.Add(defaultProcessTreeWait)
		containmentOpsNow = func() time.Time { return now }
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			return []ContainmentCandidate{candidate}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			now = deadline

			return syscall.ESRCH
		}

		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.True(t, result.ResultReady)
		require.Empty(t, result.TermSignalled)
		require.Empty(t, result.RemainingCorrelated)
		require.Empty(t, result.AmbiguousPIDs)
	})

	t.Run("KILL ESRCH immediately before deadline", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Unix(50, 0)
		deadline := now.Add(defaultProcessTreeWait)
		containmentOpsNow = func() time.Time { return now }
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++

			return []ContainmentCandidate{candidate}, nil
		}
		revalidations := 0
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			revalidations++
			if revalidations == 2 {
				return containmentCandidateGone
			}

			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(_ int, signal unix.Signal) error {
			if signal == unix.SIGKILL {
				now = deadline

				return syscall.ESRCH
			}

			return nil
		}

		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.True(t, result.ResultReady)
		require.Equal(t, []ContainmentCandidate{candidate}, result.TermSignalled)
		require.Empty(t, result.KillSignalled)
		require.Empty(t, result.RemainingCorrelated)
		require.Empty(t, result.AmbiguousPIDs)
		require.Equal(t, 2, scans)
	})

	t.Run("new candidate deadline checks", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Now()
		containmentOpsNow = func() time.Time { return now }
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			now = now.Add(defaultProcessTreeWait + time.Second)

			return containmentCandidateCorrelated
		}
		result := &ContainmentCleanupResult{}
		require.ErrorIs(t, terminateThenKillContainmentCandidate(containmentRecordData{}, candidate, now.Add(defaultProcessTreeWait), result), errContainmentCleanupDeadline)
		require.Empty(t, result.TermSignalled)

		now = time.Now()
		result.TermSignalled = []ContainmentCandidate{candidate}
		require.ErrorIs(t, terminateThenKillContainmentCandidate(containmentRecordData{}, candidate, now.Add(defaultProcessTreeWait), result), errContainmentCleanupDeadline)
		require.Empty(t, result.KillSignalled)
	})

	t.Run("new candidate TERM ESRCH immediately before deadline", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		now := time.Unix(60, 0)
		deadline := now.Add(defaultProcessTreeWait)
		containmentOpsNow = func() time.Time { return now }
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error {
			now = deadline

			return syscall.ESRCH
		}
		result := &ContainmentCleanupResult{RemainingCorrelated: []ContainmentCandidate{candidate}}

		err := terminateThenKillContainmentCandidate(containmentRecordData{}, candidate, deadline, result)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.Empty(t, result.TermSignalled)
		require.Empty(t, result.RemainingCorrelated)
	})
}

func TestContainmentSpecAndIdentityFailures(t *testing.T) {
	wantErr := errors.New("injected containment identity failure")
	valid := ContainmentSpec{
		DarwinBestEffort: true,
		ScratchParent:    "/scratch",
		GenerationRoot:   "/scratch/acp-go-pi-runtime-one",
		RuntimeID:        strings.Repeat("a", 32),
		LifecycleKind:    "session",
	}

	for _, mutate := range []func(*ContainmentSpec){
		func(spec *ContainmentSpec) { spec.RuntimeID = "bad" },
		func(spec *ContainmentSpec) { spec.LifecycleKind = "bad" },
		func(spec *ContainmentSpec) { spec.GenerationRoot = "/outside/acp-go-pi-runtime-one" },
		func(spec *ContainmentSpec) { spec.GenerationRoot = "/scratch/wrong" },
	} {
		candidate := valid
		mutate(&candidate)
		require.Error(t, validateContainmentSpec(candidate))
	}

	restoreContainmentRecordSeams(t)
	containmentRecordAbs = func(string) (string, error) { return "", wantErr }
	require.ErrorContains(t, validateContainmentSpec(valid), "resolve containment scratch parent")

	containmentRecordAbs = filepath.Abs
	calls := 0
	containmentRecordAbs = func(path string) (string, error) {
		calls++
		if calls == 2 {
			return "", wantErr
		}

		return path, nil
	}
	require.ErrorIs(t, validateContainmentSpec(valid), wantErr)

	require.NoError(t, activateContainmentRecord(containmentRecord{}, 1, 1))
	require.NoError(t, completeContainmentRecord(containmentRecord{}, containmentStateAbsent))
	containmentRecordSysctl = func(string, ...int) (*unix.KinfoProc, error) { return nil, wantErr }
	require.ErrorContains(t, activateContainmentRecord(containmentRecord{path: "record"}, 1, 1), "identify contained direct child")
	containmentRecordSysctl = func(string, ...int) (*unix.KinfoProc, error) {
		return &unix.KinfoProc{}, nil
	}
	_, _, err := darwinProcessStartIdentity(123)
	require.ErrorIs(t, err, syscall.ESRCH)

	require.ErrorContains(t, validateContainmentRecordData(containmentRecordData{}), "runtime id")
}

func TestContainmentRecordIOFailures(t *testing.T) {
	wantErr := errors.New("injected record I/O failure")
	newRecord := func(t *testing.T) (string, containmentRecordData) {
		t.Helper()
		parent := t.TempDir()
		registry := filepath.Join(parent, containmentRegistryName)
		require.NoError(t, os.Mkdir(registry, 0o700))
		root := filepath.Join(parent, "acp-go-pi-runtime-io")
		require.NoError(t, os.Mkdir(root, 0o700))
		record := containmentRecordData{
			SchemaVersion: containmentRecordSchema,
			Vendor:        containmentVendor, Containment: containmentBestEffort,
			LifecycleKind: "session", RuntimeID: strings.Repeat("d", 32),
			GenerationRoot: root, WrapperPID: 1, WrapperStartSec: 1, State: containmentStateAbsent,
		}

		return filepath.Join(registry, record.RuntimeID+".json"), record
	}

	for _, test := range []struct {
		name      string
		exclusive bool
		setup     func()
		want      string
	}{
		{name: "marshal", setup: func() {
			containmentRecordMarshal = func(any) ([]byte, error) { return nil, wantErr }
		}, want: "encode containment record"},
		{name: "create temporary", setup: func() {
			containmentRecordCreateTemp = func(string, string) (*os.File, error) { return nil, wantErr }
		}, want: "create containment record temporary"},
		{name: "write temporary", setup: func() {
			containmentRecordFileWrite = func(*os.File, []byte) (int, error) { return 0, wantErr }
		}, want: "write containment record"},
		{name: "sync temporary", setup: func() {
			containmentRecordFileSync = func(*os.File) error { return wantErr }
		}, want: "write containment record"},
		{name: "close temporary", setup: func() {
			containmentRecordFileClose = func(file *os.File) error {
				_ = file.Close()

				return wantErr
			}
		}, want: "write containment record"},
		{name: "link temporary", exclusive: true, setup: func() {
			containmentRecordLink = func(string, string) error { return wantErr }
		}, want: "publish exclusive containment record"},
		{name: "rename temporary", setup: func() {
			containmentRecordRename = func(string, string) error { return wantErr }
		}, want: "publish containment record"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreContainmentRecordSeams(t)
			path, record := newRecord(t)
			test.setup()
			err := writeContainmentRecord(path, record, test.exclusive)
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("read stat", func(t *testing.T) {
		restoreContainmentRecordSeams(t)
		path, record := newRecord(t)
		require.NoError(t, writeContainmentRecord(path, record, true))
		containmentRecordFileStat = func(*os.File) (os.FileInfo, error) { return nil, wantErr }
		_, err := readContainmentRecord(path)
		require.ErrorContains(t, err, "inspect containment record")
	})

	t.Run("read contents", func(t *testing.T) {
		restoreContainmentRecordSeams(t)
		path, record := newRecord(t)
		require.NoError(t, writeContainmentRecord(path, record, true))
		containmentRecordReadAll = func(io.Reader) ([]byte, error) { return nil, wantErr }
		_, err := readContainmentRecord(path)
		require.ErrorContains(t, err, "read containment record")
	})

	t.Run("wrong format", func(t *testing.T) {
		path, record := newRecord(t)
		record.Vendor = "other"
		require.NoError(t, writeContainmentRecord(path, record, true))
		_, err := readContainmentRecord(path)
		require.ErrorContains(t, err, "not current pi format")
	})
}

func TestContainmentRegistryLockFailures(t *testing.T) {
	wantErr := errors.New("injected registry lock failure")

	for _, test := range []struct {
		name  string
		setup func()
		want  string
	}{
		{name: "open", setup: func() {
			containmentRecordOpen = func(string, int, uint32) (int, error) { return -1, wantErr }
		}, want: "open containment registry lock"},
		{name: "stat", setup: func() {
			containmentRecordFileStat = func(*os.File) (os.FileInfo, error) { return nil, wantErr }
		}, want: "mode 0600"},
		{name: "mode", setup: func() {
			containmentRecordFileStat = func(*os.File) (os.FileInfo, error) {
				return containmentTestFileInfo{name: ".lock", mode: 0o644}, nil
			}
		}, want: "mode 0600"},
		{name: "flock", setup: func() {
			containmentRecordFlock = func(int, int) error { return wantErr }
		}, want: "lock containment registry"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreContainmentRecordSeams(t)
			registry := filepath.Join(t.TempDir(), containmentRegistryName)
			require.NoError(t, os.Mkdir(registry, 0o700))
			test.setup()
			lock, err := lockContainmentRegistry(registry)
			require.Nil(t, lock)
			require.ErrorContains(t, err, test.want)
		})
	}

	restoreContainmentRecordSeams(t)
	containmentRecordLstat = func(string) (os.FileInfo, error) { return nil, wantErr }
	require.ErrorContains(t, validateContainmentRegistry("registry"), "inspect containment registry")
	_, err := readContainmentRecord(filepath.Join("registry", "record.json"))
	require.ErrorContains(t, err, "inspect containment registry")
	require.ErrorContains(t, writeContainmentRecordLocked(filepath.Join("registry", "record.json"), containmentRecordData{}, false), "inspect containment registry")
	require.ErrorContains(t, writeContainmentRecord(filepath.Join("registry", "record.json"), containmentRecordData{}, false), "inspect containment registry")
}

func TestContainmentOpsSystemAndPathFailures(t *testing.T) {
	wantErr := errors.New("injected containment operation failure")

	t.Run("default Darwin process sources", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		processes, err := containmentProcessList()
		require.NoError(t, err)
		require.NotEmpty(t, processes)
		process, err := containmentProcessLookup(os.Getpid())
		require.NoError(t, err)
		require.Equal(t, int32(os.Getpid()), process.Proc.P_pid)
		raw, err := containmentProcessArguments(os.Getpid())
		require.NoError(t, err)
		require.NotEmpty(t, raw)
	})

	t.Run("diagnose path and read failures", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		_, err := DiagnoseContainment(" ")
		require.ErrorContains(t, err, "scratch parent is required")

		parent := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(parent, containmentRegistryName), 0o700))
		containmentOpsReadDir = func(string) ([]os.DirEntry, error) { return nil, wantErr }
		_, err = DiagnoseContainment(parent)
		require.ErrorContains(t, err, "read containment registry")
	})

	t.Run("registry absolute path", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		containmentOpsAbs = func(string) (string, error) { return "", wantErr }
		_, err := containmentRegistryPath("scratch")
		require.ErrorContains(t, err, "resolve containment scratch parent")
	})

	t.Run("selected root path", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		calls := 0
		containmentOpsAbs = func(path string) (string, error) {
			calls++
			if calls == 2 {
				return "", wantErr
			}

			return filepath.Clean(path), nil
		}
		_, err := validatedCleanupRoot("/scratch", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.ErrorContains(t, err, "resolve selected generation root")

		containmentOpsAbs = filepath.Abs
		containmentOpsRel = func(string, string) (string, error) { return "", wantErr }
		_, err = validatedCleanupRoot("/scratch", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.ErrorContains(t, err, "outside the scratch parent")

		containmentOpsRel = filepath.Rel
		containmentOpsLstat = func(string) (os.FileInfo, error) { return nil, wantErr }
		_, err = validatedCleanupRoot("/scratch", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.ErrorContains(t, err, "inspect selected generation root")

		containmentOpsLstat = func(string) (os.FileInfo, error) {
			return containmentTestFileInfo{name: "root", mode: 0o600}, nil
		}
		_, err = validatedCleanupRoot("/scratch", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.ErrorContains(t, err, "not a directory")

		containmentOpsLstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
		root, err := validatedCleanupRoot("/scratch", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.NoError(t, err)
		require.Equal(t, "/scratch/acp-go-pi-runtime-one", root)

		_, err = validatedCleanupRoot(" ", "/scratch/acp-go-pi-runtime-one", containmentStateAbsent)
		require.ErrorContains(t, err, "scratch parent is required")
		_, err = validatedCleanupRoot("/scratch", "/scratch/wrong", containmentStateAbsent)
		require.ErrorContains(t, err, "invalid prefix")
	})
}

func TestContainmentCandidateControlFailures(t *testing.T) {
	wantErr := errors.New("injected containment candidate failure")
	record := containmentRecordData{}
	candidate := ContainmentCandidate{PID: 701, StartSec: 1, StartUsec: 2}

	t.Run("await all", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) { return nil, wantErr }
		result := &ContainmentCleanupResult{}
		err := awaitAllContainmentCandidates(record, time.Now().Add(time.Second), result)
		require.ErrorIs(t, err, wantErr)
		require.Empty(t, result.RemainingCorrelated)

		restoreContainmentOpsSeams(t)
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++

			return []ContainmentCandidate{}, nil
		}
		containmentOpsSleep = func(time.Duration) {}
		result = &ContainmentCleanupResult{}
		err = awaitAllContainmentCandidates(record, time.Now().Add(time.Second), result)
		require.NoError(t, err)
		require.Empty(t, result.RemainingCorrelated)
		require.Equal(t, 2, scans)

		restoreContainmentOpsSeams(t)
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			return []ContainmentCandidate{candidate}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return wantErr }
		result = &ContainmentCleanupResult{}
		err = awaitAllContainmentCandidates(record, time.Now().Add(time.Second), result)
		require.ErrorContains(t, err, "newly correlated process")
		require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)

		restoreContainmentOpsSeams(t)
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			t.Fatal("deadline must stop before the next process enumeration")

			return []ContainmentCandidate{candidate}, nil
		}
		containmentOpsNow = func() time.Time { return time.Now().Add(time.Hour) }
		result = &ContainmentCleanupResult{RemainingCorrelated: []ContainmentCandidate{candidate}}
		err = awaitAllContainmentCandidates(record, time.Now(), result)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)

		restoreContainmentOpsSeams(t)
		scans = 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 1 {
				return []ContainmentCandidate{candidate}, nil
			}

			return []ContainmentCandidate{}, nil
		}
		states := []containmentCandidateState{containmentCandidateCorrelated, containmentCandidateGone}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			state := states[0]
			states = states[1:]

			return state
		}
		containmentPIDSignal = func(int, unix.Signal) error { return nil }
		sleeps := 0
		containmentOpsSleep = func(time.Duration) { sleeps++ }
		containmentOpsNow = time.Now
		result = &ContainmentCleanupResult{}
		err = awaitAllContainmentCandidates(record, time.Now().Add(time.Second), result)
		require.NoError(t, err)
		require.Empty(t, result.RemainingCorrelated)
		require.Positive(t, sleeps)
	})

	t.Run("terminate then kill", func(t *testing.T) {
		for _, state := range []containmentCandidateState{containmentCandidateGone, containmentCandidateAmbiguous} {
			restoreContainmentOpsSeams(t)
			result := &ContainmentCleanupResult{}
			containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState { return state }
			require.NoError(t, terminateThenKillContainmentCandidate(record, candidate, time.Now().Add(time.Second), result))
			if state == containmentCandidateAmbiguous {
				require.Equal(t, []int{candidate.PID}, result.AmbiguousPIDs)
			}
		}

		restoreContainmentOpsSeams(t)
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return wantErr }
		require.ErrorContains(t, terminateThenKillContainmentCandidate(record, candidate, time.Now().Add(time.Second), &ContainmentCleanupResult{}), "newly correlated process")

		restoreContainmentOpsSeams(t)
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(_ int, signal unix.Signal) error {
			if signal == unix.SIGKILL {
				return wantErr
			}

			return nil
		}
		now := time.Now()
		containmentOpsNow = func() time.Time {
			now = now.Add(time.Second)

			return now
		}
		result := &ContainmentCleanupResult{}
		require.ErrorContains(t, terminateThenKillContainmentCandidate(record, candidate, now.Add(30*time.Second), result), "SIGKILL")

		restoreContainmentOpsSeams(t)
		states := []containmentCandidateState{containmentCandidateCorrelated, containmentCandidateAmbiguous}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			state := states[0]
			states = states[1:]

			return state
		}
		containmentPIDSignal = func(int, unix.Signal) error { return nil }
		now = time.Now()
		containmentOpsNow = func() time.Time {
			now = now.Add(time.Second)

			return now
		}
		result = &ContainmentCleanupResult{}
		require.NoError(t, terminateThenKillContainmentCandidate(record, candidate, now.Add(30*time.Second), result))
		require.Equal(t, []int{candidate.PID}, result.AmbiguousPIDs)
	})

	t.Run("await selected", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		states := []containmentCandidateState{containmentCandidateAmbiguous, containmentCandidateCorrelated, containmentCandidateGone}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			state := states[0]
			states = states[1:]

			return state
		}
		containmentOpsSleep = func(time.Duration) {}
		result := &ContainmentCleanupResult{}
		remaining := awaitContainmentCandidates(record, []ContainmentCandidate{candidate, {PID: 702}}, time.Now().Add(time.Second), result)
		require.Empty(t, remaining)
		require.Equal(t, []int{candidate.PID}, result.AmbiguousPIDs)
	})

	t.Run("recorded and appended ambiguity", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		pid, sec, usec := 703, int64(1), int32(2)
		record := containmentRecordData{State: containmentStateRunning, DirectChildPID: &pid, DirectChildStartSec: &sec, DirectChildStartUsec: &usec}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		require.Empty(t, recordedAmbiguousPIDs(record))
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateAmbiguous
		}
		require.Equal(t, []int{pid}, recordedAmbiguousPIDs(record))
		result := &ContainmentCleanupResult{}
		appendAmbiguousPID(result, pid)
		appendAmbiguousPID(result, pid)
		require.Equal(t, []int{pid}, result.AmbiguousPIDs)

		other := ContainmentCandidate{PID: pid + 1}
		result.RemainingCorrelated = []ContainmentCandidate{other, {PID: pid}}
		removeRemainingContainmentCandidate(result, ContainmentCandidate{PID: pid})
		require.Equal(t, []ContainmentCandidate{other}, result.RemainingCorrelated)
	})
}

func TestCleanupContainmentOperationFailures(t *testing.T) {
	wantErr := errors.New("injected containment cleanup failure")
	setup := func(t *testing.T) (string, string, string) {
		t.Helper()
		parent := t.TempDir()
		runtimeID := strings.Repeat("7", 32)
		root := filepath.Join(parent, "acp-go-pi-runtime-cleanup")
		require.NoError(t, os.Mkdir(root, 0o700))
		writeContainmentOpsRecord(t, parent, runtimeID, root, containmentStateAbsent, nil)

		return parent, runtimeID, root
	}

	t.Run("initial scan", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) { return nil, wantErr }
		_, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("scratch path", func(t *testing.T) {
		_, err := CleanupContainment(" ", strings.Repeat("7", 32), true)
		require.ErrorContains(t, err, "scratch parent is required")
	})

	t.Run("initial TERM", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		candidate := ContainmentCandidate{PID: 704}
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			return []ContainmentCandidate{candidate}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return wantErr }
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorContains(t, err, "SIGTERM")
		require.False(t, result.ResultReady)
	})

	t.Run("initial TERM already gone", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		candidate := ContainmentCandidate{PID: 705}
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 1 {
				return []ContainmentCandidate{candidate}, nil
			}

			return []ContainmentCandidate{}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return syscall.ESRCH }
		containmentOpsSleep = func(time.Duration) {}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.NoError(t, err)
		require.Empty(t, result.TermSignalled)
	})

	t.Run("second scan", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 2 {
				return nil, wantErr
			}

			return []ContainmentCandidate{}, nil
		}
		_, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("new candidate termination", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		candidate := ContainmentCandidate{PID: 706}
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 2 {
				return []ContainmentCandidate{candidate}, nil
			}

			return []ContainmentCandidate{}, nil
		}
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return wantErr }
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorContains(t, err, "newly correlated process")
		require.False(t, result.ResultReady)
	})

	t.Run("await all scan", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 3 {
				return nil, wantErr
			}

			return []ContainmentCandidate{}, nil
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, wantErr)
		require.False(t, result.ResultReady)
	})

	t.Run("remaining at deadline", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, _ := setup(t)
		candidate := ContainmentCandidate{PID: 707}
		scans := 0
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) {
			scans++
			if scans == 1 || scans >= 3 {
				return []ContainmentCandidate{candidate}, nil
			}

			return []ContainmentCandidate{}, nil
		}
		revalidations := 0
		containmentCandidateRevalidate = func(containmentRecordData, ContainmentCandidate) containmentCandidateState {
			revalidations++
			if revalidations == 2 {
				return containmentCandidateGone
			}

			return containmentCandidateCorrelated
		}
		containmentPIDSignal = func(int, unix.Signal) error { return nil }
		now := time.Unix(100, 0)
		deadline := now.Add(defaultProcessTreeWait)
		containmentOpsNow = func() time.Time { return now }
		containmentOpsSleep = func(time.Duration) {
			if scans >= 3 {
				now = deadline
			}
		}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.ErrorIs(t, err, errContainmentCleanupDeadline)
		require.Equal(t, []ContainmentCandidate{candidate}, result.RemainingCorrelated)
		require.True(t, result.ResultReady)
		require.False(t, result.RootRemoved)
		require.Equal(t, 3, scans, "deadline must stop before another process enumeration")
	})

	t.Run("missing root after validation", func(t *testing.T) {
		restoreContainmentOpsSeams(t)
		parent, runtimeID, root := setup(t)
		require.NoError(t, os.RemoveAll(root))
		containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) { return []ContainmentCandidate{}, nil }
		containmentOpsSleep = func(time.Duration) {}
		result, err := CleanupContainment(parent, runtimeID, true)
		require.NoError(t, err)
		require.False(t, result.RootRemoved)
	})

	for _, test := range []struct {
		name  string
		setup func(string)
		want  string
	}{
		{name: "inspect root", setup: func(string) {
			calls := 0
			containmentOpsLstat = func(path string) (os.FileInfo, error) {
				calls++
				if calls == 1 {
					return os.Lstat(path)
				}

				return nil, wantErr
			}
		}, want: "revalidate selected generation root identity"},
		{name: "remove root", setup: func(string) {
			calls := 0
			containmentOpsLstat = func(path string) (os.FileInfo, error) {
				calls++

				return os.Lstat(path)
			}
			containmentOpsRemoveAll = func(string) error { return wantErr }
		}, want: "remove selected generation root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			restoreContainmentOpsSeams(t)
			parent, runtimeID, root := setup(t)
			containmentCandidateScan = func(containmentRecordData) ([]ContainmentCandidate, error) { return []ContainmentCandidate{}, nil }
			containmentOpsSleep = func(time.Duration) {}
			test.setup(root)
			_, err := CleanupContainment(parent, runtimeID, true)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestDarwinProcessEnvironmentAndMarkerScan(t *testing.T) {
	restoreContainmentOpsSeams(t)
	requireErrorCases := [][]byte{
		nil,
		{0, 0, 0, 0},
		{1, 0, 0, 0},
		{2, 0, 0, 0, '/', 'x', 0, 0, 'a', 0},
	}
	for _, raw := range requireErrorCases {
		_, err := darwinProcessEnvironment(raw)
		require.Error(t, err)
	}

	raw := darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{"A=1", "B=2"})
	environment, err := darwinProcessEnvironment(raw)
	require.NoError(t, err)
	require.Equal(t, []string{"A=1", "B=2"}, environment)
	withOpaqueTail := append(append([]byte(nil), raw...), 0, 0xff, 0x7f, 1)
	environment, err = darwinProcessEnvironment(withOpaqueTail)
	require.NoError(t, err)
	require.Equal(t, []string{"A=1", "B=2"}, environment, "the first empty entry terminates the environment before Darwin's opaque tail")
	_, err = darwinProcessEnvironment(raw[:len(raw)-1])
	require.ErrorContains(t, err, "unterminated")
	_, err = darwinProcessEnvironment(raw[:len(raw)-2])
	require.ErrorContains(t, err, "incomplete")
	unterminatedExecutable := make([]byte, 4, 13)
	binary.LittleEndian.PutUint32(unterminatedExecutable, 1)
	unterminatedExecutable = append(unterminatedExecutable, "/bin/test"...)
	_, err = darwinProcessEnvironment(unterminatedExecutable)
	require.ErrorContains(t, err, "executable path is missing")
	_, err = darwinProcessEnvironment(darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{""}))
	require.ErrorContains(t, err, "boundary is ambiguous")

	runtimeID := strings.Repeat("7", 32)
	root := "/tmp/acp-go-pi-runtime-seven"
	containmentProcessArguments = func(pid int) ([]byte, error) {
		switch pid {
		case 10:
			return darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{envRuntimeID + "=" + runtimeID, envScratchRoot + "=" + root}), nil
		case 11:
			return darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{envRuntimeID + "=" + runtimeID}), nil
		default:
			return nil, syscall.ESRCH
		}
	}
	matched, err := processHasContainmentMarkers(10, runtimeID, root)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = processHasContainmentMarkers(11, runtimeID, root)
	require.NoError(t, err)
	require.False(t, matched)
	containmentProcessArguments = func(int) ([]byte, error) {
		return darwinProcessArgsRaw("/bin/test", []string{"test", envRuntimeID + "=" + runtimeID, envScratchRoot + "=" + root}, []string{"A=1"}), nil
	}
	matched, err = processHasContainmentMarkers(11, runtimeID, root)
	require.NoError(t, err)
	require.False(t, matched, "marker-looking argv entries must never correlate a process")
	containmentProcessArguments = func(int) ([]byte, error) { return nil, syscall.ESRCH }
	_, err = processHasContainmentMarkers(12, runtimeID, root)
	require.ErrorIs(t, err, syscall.ESRCH)
}

func TestCorrelatedCandidatesAndRevalidation(t *testing.T) {
	restoreContainmentOpsSeams(t)
	runtimeID := strings.Repeat("8", 32)
	root := "/tmp/acp-go-pi-runtime-eight"
	makeProcess := func(pid int, uid uint32, sec int64, usec int32) unix.KinfoProc {
		var process unix.KinfoProc
		process.Proc.P_pid = int32(pid)
		process.Eproc.Ucred.Uid = uid
		process.Proc.P_starttime.Sec = sec
		process.Proc.P_starttime.Usec = usec

		return process
	}
	valid := makeProcess(410, uint32(os.Getuid()), 11, 12)
	containmentProcessList = func() ([]unix.KinfoProc, error) {
		return []unix.KinfoProc{
			makeProcess(1, uint32(os.Getuid()), 1, 1),
			makeProcess(os.Getpid(), uint32(os.Getuid()), 1, 1),
			makeProcess(409, uint32(os.Getuid()+1), 1, 1),
			valid,
		}, nil
	}
	containmentProcessArguments = func(pid int) ([]byte, error) {
		return darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{envRuntimeID + "=" + runtimeID, envScratchRoot + "=" + root}), nil
	}
	record := containmentRecordData{RuntimeID: runtimeID, GenerationRoot: root}
	candidates, err := correlatedContainmentCandidates(record)
	require.NoError(t, err)
	require.Equal(t, []ContainmentCandidate{{PID: 410, StartSec: 11, StartUsec: 12}}, candidates)
	containmentProcessList = func() ([]unix.KinfoProc, error) { return nil, errors.New("list failed") }
	_, err = correlatedContainmentCandidates(record)
	require.ErrorContains(t, err, "enumerate Darwin processes")

	candidate := candidates[0]
	containmentProcessLookup = func(int) (*unix.KinfoProc, error) { return nil, syscall.ESRCH }
	require.Equal(t, containmentCandidateGone, revalidateContainmentCandidate(record, candidate))
	containmentProcessLookup = func(int) (*unix.KinfoProc, error) { return nil, errors.New("denied") }
	require.Equal(t, containmentCandidateAmbiguous, revalidateContainmentCandidate(record, candidate))
	containmentProcessLookup = func(int) (*unix.KinfoProc, error) {
		processCopy := valid

		return &processCopy, nil
	}
	containmentProcessArguments = func(int) ([]byte, error) { return nil, syscall.ESRCH }
	require.Equal(t, containmentCandidateGone, revalidateContainmentCandidate(record, candidate))
	containmentProcessArguments = func(int) ([]byte, error) { return nil, errors.New("scrubbed") }
	require.Equal(t, containmentCandidateAmbiguous, revalidateContainmentCandidate(record, candidate))
	containmentProcessArguments = func(int) ([]byte, error) {
		return darwinProcessArgsRaw("/bin/test", []string{"test"}, []string{envRuntimeID + "=" + runtimeID, envScratchRoot + "=" + root}), nil
	}
	require.Equal(t, containmentCandidateCorrelated, revalidateContainmentCandidate(record, candidate))
	changed := valid
	changed.Proc.P_starttime.Usec++
	containmentProcessLookup = func(int) (*unix.KinfoProc, error) { return &changed, nil }
	require.Equal(t, containmentCandidateAmbiguous, revalidateContainmentCandidate(record, candidate))
}

func TestDarwinSetsidEscapeIsDiagnosableAndExplicitlyCleaned(t *testing.T) {
	if role := os.Getenv("PI_TEST_SETSID_ROLE"); role != "" {
		runDarwinSetsidHelper(role)

		return
	}

	parent := t.TempDir()
	root, err := os.MkdirTemp(parent, "acp-go-pi-runtime-")
	require.NoError(t, err)
	runtimeID := strings.Repeat("9", 32)
	pidFile := filepath.Join(parent, "detached.pid")
	script := filepath.Join(parent, "pi-helper")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nexec \"$PI_TEST_BINARY\" -test.run=TestDarwinSetsidEscapeIsDiagnosableAndExplicitlyCleaned\n"), 0o700))

	process, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       filepath.Join(root, "agent"),
		SessionDir:     filepath.Join(root, "sessions"),
		Env: map[string]string{
			"PI_TEST_BINARY":      os.Args[0],
			"PI_TEST_SETSID_ROLE": "parent",
			"PI_TEST_PID_FILE":    pidFile,
		},
		Containment: ContainmentSpec{
			DarwinBestEffort: true, ScratchParent: parent, GenerationRoot: root,
			RuntimeID: runtimeID, LifecycleKind: "session",
		},
	})
	require.NoError(t, err)
	select {
	case <-process.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("setsid helper parent did not exit")
	}
	require.NoError(t, process.Close(), "original process group should disappear despite the escaped descendant")
	require.NoError(t, process.WaitErr(), "inherited output pipes must not turn a completed best-effort boundary into ErrWaitDelay")

	pidBytes, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	detachedPID, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	require.NoError(t, err)
	require.NoError(t, unix.Kill(detachedPID, 0), "setsid descendant must survive original-group cleanup")
	t.Cleanup(func() {
		if unix.Kill(detachedPID, 0) == nil {
			_ = unix.Kill(detachedPID, unix.SIGKILL)
		}
		if !waitDarwinPIDGone(detachedPID, 5*time.Second) {
			t.Errorf("detached helper pid %d was not reaped", detachedPID)
		}
	})
	matched, markerErr := processHasContainmentMarkers(detachedPID, runtimeID, root)
	require.NoError(t, markerErr)
	require.True(t, matched)

	diagnostics, err := DiagnoseContainment(parent)
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)
	require.Equal(t, detachedPID, diagnostics[0].Candidates[0].PID)

	result, err := CleanupContainment(parent, runtimeID, true)
	require.Contains(t, candidatePIDsForTest(result.TermSignalled), detachedPID)
	if err != nil {
		require.ErrorContains(t, err, "ambiguous process identities")
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && unix.Kill(detachedPID, 0) == nil {
			time.Sleep(10 * time.Millisecond)
		}
		result, err = CleanupContainment(parent, runtimeID, true)
	}
	require.NoError(t, err)
	require.True(t, result.RootRemoved)
	require.True(t, waitDarwinPIDGone(detachedPID, 5*time.Second), "cleanup reported success while detached pid remained observable")
}

func runDarwinSetsidHelper(role string) {
	switch role {
	case "parent":
		command := exec.Command(os.Args[0], "-test.run=TestDarwinSetsidEscapeIsDiagnosableAndExplicitlyCleaned") // #nosec G204 -- test helper binary.
		command.Env = make([]string, 0, len(os.Environ()))
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "PI_TEST_SETSID_ROLE=") {
				command.Env = append(command.Env, entry)
			}
		}
		command.Env = append(command.Env, "PI_TEST_SETSID_ROLE=descendant")
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			os.Exit(91)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(os.Getenv("PI_TEST_PID_FILE")); err == nil {
				os.Exit(0)
			}
			time.Sleep(10 * time.Millisecond)
		}
		os.Exit(92)
	case "descendant":
		if _, err := unix.Setsid(); err != nil {
			os.Exit(93)
		}
		if err := os.WriteFile(os.Getenv("PI_TEST_PID_FILE"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
			os.Exit(94)
		}
		for {
			time.Sleep(time.Second)
		}
	}
}

func candidatePIDsForTest(candidates []ContainmentCandidate) []int {
	pids := make([]int, 0, len(candidates))
	for _, candidate := range candidates {
		pids = append(pids, candidate.PID)
	}

	return pids
}

func waitDarwinPIDGone(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if errors.Is(unix.Kill(pid, 0), syscall.ESRCH) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return errors.Is(unix.Kill(pid, 0), syscall.ESRCH)
}

func darwinProcessArgsRaw(executable string, arguments []string, environment []string) []byte {
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, uint32(len(arguments)))
	raw = append(raw, executable...)
	raw = append(raw, 0, 0)
	for _, argument := range arguments {
		raw = append(raw, argument...)
		raw = append(raw, 0)
	}
	for _, entry := range environment {
		raw = append(raw, entry...)
		raw = append(raw, 0)
	}
	if len(environment) > 0 {
		raw = append(raw, 0)
	}

	return raw
}
