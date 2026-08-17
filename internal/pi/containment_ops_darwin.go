//go:build darwin

package pi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type ContainmentCandidate struct {
	PID       int
	StartSec  int64
	StartUsec int32
}

type ContainmentDiagnostic struct {
	RuntimeID      string
	LifecycleKind  string
	State          string
	GenerationRoot string
	Candidates     []ContainmentCandidate
	AmbiguousPIDs  []int
}

type ContainmentCleanupResult struct {
	RuntimeID           string
	GenerationRoot      string
	TermSignalled       []ContainmentCandidate
	KillSignalled       []ContainmentCandidate
	RemainingCorrelated []ContainmentCandidate
	AmbiguousPIDs       []int
	RootRemoved         bool
	ResultReady         bool
}

var containmentCandidateScan = correlatedContainmentCandidates
var containmentCandidateRevalidate = revalidateContainmentCandidate
var containmentPIDSignal = unix.Kill
var containmentOpsReadDir = os.ReadDir
var containmentOpsLstat = os.Lstat
var containmentOpsRemoveAll = os.RemoveAll
var containmentOpsAbs = filepath.Abs
var containmentOpsRel = filepath.Rel
var containmentOpsNow = time.Now
var containmentOpsSleep = time.Sleep
var containmentProcessList = func() ([]unix.KinfoProc, error) {
	return unix.SysctlKinfoProcSlice("kern.proc.all")
}
var containmentProcessLookup = func(pid int) (*unix.KinfoProc, error) {
	return unix.SysctlKinfoProc("kern.proc.pid", pid)
}
var containmentProcessArguments = func(pid int) ([]byte, error) {
	return unix.SysctlRaw("kern.procargs2", pid)
}

var errContainmentCleanupDeadline = errors.New("containment cleanup deadline reached")

func DiagnoseContainment(scratchParent string) ([]ContainmentDiagnostic, error) {
	registry, err := containmentRegistryPath(scratchParent)
	if err != nil {
		return nil, err
	}

	if registryErr := validateContainmentRegistry(registry); errors.Is(registryErr, os.ErrNotExist) {
		return []ContainmentDiagnostic{}, nil
	} else if registryErr != nil {
		return nil, registryErr
	}

	entries, err := containmentOpsReadDir(registry)
	if err != nil {
		return nil, fmt.Errorf("read containment registry: %w", err)
	}

	diagnostics := make([]ContainmentDiagnostic, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		record, err := readContainmentRecord(filepath.Join(registry, entry.Name()))
		if err != nil {
			return nil, err
		}

		candidates, err := containmentCandidateScan(record)
		if err != nil {
			return nil, err
		}

		diagnostics = append(diagnostics, ContainmentDiagnostic{
			RuntimeID:      record.RuntimeID,
			LifecycleKind:  record.LifecycleKind,
			State:          record.State,
			GenerationRoot: record.GenerationRoot,
			Candidates:     candidates,
			AmbiguousPIDs:  recordedAmbiguousPIDs(record),
		})
	}

	return diagnostics, nil
}

func CleanupContainment(scratchParent string, runtimeID string, force bool) (ContainmentCleanupResult, error) {
	if !force {
		return ContainmentCleanupResult{}, errors.New("containment cleanup requires -force")
	}

	if !validRuntimeID(runtimeID) {
		return ContainmentCleanupResult{}, errors.New("containment cleanup runtime id must be 128-bit lowercase hex")
	}

	deadline := containmentOpsNow().Add(defaultProcessTreeWait)

	record, root, rootIdentity, candidates, err := containmentCleanupInputs(scratchParent, runtimeID, deadline)
	if err != nil {
		return ContainmentCleanupResult{}, err
	}

	result := ContainmentCleanupResult{
		RuntimeID:           runtimeID,
		GenerationRoot:      root,
		TermSignalled:       make([]ContainmentCandidate, 0, len(candidates)),
		KillSignalled:       make([]ContainmentCandidate, 0),
		RemainingCorrelated: append([]ContainmentCandidate(nil), candidates...),
		AmbiguousPIDs:       make([]int, 0),
	}
	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return readyContainmentCleanupResult(result), deadlineErr
	}

	observeRecordedContainmentCandidate(record, &result)

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return readyContainmentCleanupResult(result), deadlineErr
	}

	for _, candidate := range candidates {
		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return readyContainmentCleanupResult(result), deadlineErr
		}

		state := containmentCandidateRevalidate(record, candidate)
		observeContainmentCandidate(&result, candidate, state)

		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return readyContainmentCleanupResult(result), deadlineErr
		}

		if state != containmentCandidateCorrelated {
			continue
		}

		signalErr := containmentPIDSignal(candidate.PID, unix.SIGTERM)
		if signalErr != nil {
			if errors.Is(signalErr, syscall.ESRCH) {
				removeRemainingContainmentCandidate(&result, candidate)

				if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
					return readyContainmentCleanupResult(result), deadlineErr
				}

				continue
			}

			return result, fmt.Errorf("signal correlated process %d with SIGTERM: %w", candidate.PID, signalErr)
		}

		result.TermSignalled = append(result.TermSignalled, candidate)

		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return readyContainmentCleanupResult(result), deadlineErr
		}
	}

	_ = awaitContainmentCandidates(record, result.TermSignalled, minTime(deadline, containmentOpsNow().Add(darwinTermGrace)), &result)
	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return readyContainmentCleanupResult(result), deadlineErr
	}

	remaining, err := containmentCandidateScan(record)
	if err != nil {
		return result, err
	}

	replaceRemainingContainmentCandidates(&result, remaining)

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return readyContainmentCleanupResult(result), deadlineErr
	}

	for _, candidate := range remaining {
		if terminateErr := terminateThenKillContainmentCandidate(record, candidate, deadline, &result); terminateErr != nil {
			if errors.Is(terminateErr, errContainmentCleanupDeadline) {
				result.ResultReady = true
			}

			return result, terminateErr
		}
	}

	err = awaitAllContainmentCandidates(record, deadline, &result)
	if err != nil {
		if errors.Is(err, errContainmentCleanupDeadline) {
			result.ResultReady = true
		}

		return result, err
	}

	if len(result.AmbiguousPIDs) > 0 {
		result.ResultReady = true

		return result, fmt.Errorf("%d ambiguous process identities require operator review", len(result.AmbiguousPIDs))
	}

	rootRemoved, err := removeValidatedCleanupRoot(scratchParent, root, rootIdentity, deadline)

	result.RootRemoved = rootRemoved
	if err == nil || errors.Is(err, errContainmentCleanupDeadline) {
		result.ResultReady = true
	}

	return result, err
}

func removeValidatedCleanupRoot(scratchParent string, root string, rootIdentity os.FileInfo, deadline time.Time) (bool, error) {
	if rootIdentity == nil {
		return false, nil
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return false, deadlineErr
	}

	validatedRoot, err := validatedContainmentRootPath(scratchParent, root)

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return false, deadlineErr
	}

	if err != nil {
		return false, fmt.Errorf("revalidate selected generation root path: %w", err)
	}

	if validatedRoot != root {
		return false, errors.New("selected generation root path changed before removal")
	}

	currentIdentity, err := containmentOpsLstat(validatedRoot)

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return false, deadlineErr
	}

	if err != nil {
		return false, fmt.Errorf("revalidate selected generation root identity: %w", err)
	}

	if !currentIdentity.IsDir() || currentIdentity.Mode()&os.ModeSymlink != 0 || !os.SameFile(rootIdentity, currentIdentity) {
		return false, errors.New("selected generation root identity changed before removal")
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return false, deadlineErr
	}

	if err := containmentOpsRemoveAll(validatedRoot); err != nil {
		return false, fmt.Errorf("remove selected generation root: %w", err)
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return true, deadlineErr
	}

	return true, nil
}

func containmentCleanupInputs(
	scratchParent string,
	runtimeID string,
	deadline time.Time,
) (containmentRecordData, string, os.FileInfo, []ContainmentCandidate, error) {
	registry, err := containmentRegistryPath(scratchParent)
	if err != nil {
		return containmentRecordData{}, "", nil, nil, err
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return containmentRecordData{}, "", nil, nil, deadlineErr
	}

	if registryErr := validateContainmentRegistry(registry); registryErr != nil {
		return containmentRecordData{}, "", nil, nil, registryErr
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return containmentRecordData{}, "", nil, nil, deadlineErr
	}

	record, err := readContainmentRecord(filepath.Join(registry, runtimeID+".json"))
	if err != nil {
		return containmentRecordData{}, "", nil, nil, err
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return containmentRecordData{}, "", nil, nil, deadlineErr
	}

	root, rootIdentity, err := inspectValidatedCleanupRoot(scratchParent, record.GenerationRoot, record.State)
	if err != nil {
		return containmentRecordData{}, "", nil, nil, err
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return containmentRecordData{}, "", nil, nil, deadlineErr
	}

	candidates, err := containmentCandidateScan(record)
	if err != nil {
		return containmentRecordData{}, "", nil, nil, err
	}

	return record, root, rootIdentity, candidates, nil
}

func containmentCleanupDeadlineError(deadline time.Time) error {
	if containmentOpsNow().Before(deadline) {
		return nil
	}

	return errContainmentCleanupDeadline
}

func awaitAllContainmentCandidates(
	record containmentRecordData,
	deadline time.Time,
	result *ContainmentCleanupResult,
) error {
	emptyScans := 0

	for {
		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return deadlineErr
		}

		candidates, err := containmentCandidateScan(record)
		if err != nil {
			return err
		}

		replaceRemainingContainmentCandidates(result, candidates)

		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return deadlineErr
		}

		if len(candidates) == 0 {
			emptyScans++

			if emptyScans >= 2 {
				return nil
			}

			containmentOpsSleep(10 * time.Millisecond)

			continue
		}

		emptyScans = 0

		for _, candidate := range candidates {
			if err := terminateThenKillContainmentCandidate(record, candidate, deadline, result); err != nil {
				return err
			}
		}

		containmentOpsSleep(10 * time.Millisecond)
	}
}

func terminateThenKillContainmentCandidate(
	record containmentRecordData,
	candidate ContainmentCandidate,
	deadline time.Time,
	result *ContainmentCleanupResult,
) error {
	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return deadlineErr
	}

	if !containsContainmentCandidate(result.TermSignalled, candidate) {
		state := containmentCandidateRevalidate(record, candidate)
		observeContainmentCandidate(result, candidate, state)

		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return deadlineErr
		}

		if state != containmentCandidateCorrelated {
			return nil
		}

		signalErr := containmentPIDSignal(candidate.PID, unix.SIGTERM)
		if signalErr != nil {
			if errors.Is(signalErr, syscall.ESRCH) {
				removeRemainingContainmentCandidate(result, candidate)

				if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
					return deadlineErr
				}

				return nil
			}

			return fmt.Errorf("signal newly correlated process %d with SIGTERM: %w", candidate.PID, signalErr)
		}

		result.TermSignalled = append(result.TermSignalled, candidate)

		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return deadlineErr
		}

		remaining := awaitContainmentCandidates(record, []ContainmentCandidate{candidate}, minTime(deadline, containmentOpsNow().Add(darwinTermGrace)), result)
		if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
			return deadlineErr
		}

		if len(remaining) == 0 {
			return nil
		}
	}

	state := containmentCandidateRevalidate(record, candidate)
	observeContainmentCandidate(result, candidate, state)

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return deadlineErr
	}

	if state != containmentCandidateCorrelated {
		return nil
	}

	signalErr := containmentPIDSignal(candidate.PID, unix.SIGKILL)
	if signalErr != nil {
		if errors.Is(signalErr, syscall.ESRCH) {
			removeRemainingContainmentCandidate(result, candidate)

			if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
				return deadlineErr
			}

			return nil
		}

		return fmt.Errorf("signal correlated process %d with SIGKILL: %w", candidate.PID, signalErr)
	}

	if !containsContainmentCandidate(result.KillSignalled, candidate) {
		result.KillSignalled = append(result.KillSignalled, candidate)
	}

	if deadlineErr := containmentCleanupDeadlineError(deadline); deadlineErr != nil {
		return deadlineErr
	}

	return nil
}

func containsContainmentCandidate(candidates []ContainmentCandidate, candidate ContainmentCandidate) bool {
	for _, existing := range candidates {
		if existing == candidate {
			return true
		}
	}

	return false
}

func correlatedContainmentCandidates(record containmentRecordData) ([]ContainmentCandidate, error) {
	processes, err := containmentProcessList()
	if err != nil {
		return nil, fmt.Errorf("enumerate Darwin processes: %w", err)
	}

	uid := containmentUID()
	candidates := make([]ContainmentCandidate, 0)

	for index := range processes {
		process := &processes[index]

		pid := int(process.Proc.P_pid)
		if pid <= 1 || pid == os.Getpid() || process.Eproc.Ucred.Uid != uid {
			continue
		}

		matched, markerErr := processHasContainmentMarkers(pid, record.RuntimeID, record.GenerationRoot)
		if markerErr != nil || !matched {
			continue
		}

		candidates = append(candidates, ContainmentCandidate{
			PID:       pid,
			StartSec:  process.Proc.P_starttime.Sec,
			StartUsec: process.Proc.P_starttime.Usec,
		})
	}

	return candidates, nil
}

func processHasContainmentMarkers(pid int, runtimeID string, generationRoot string) (bool, error) {
	raw, err := containmentProcessArguments(pid)
	if err != nil {
		return false, err
	}

	environment, err := darwinProcessEnvironment(raw)
	if err != nil {
		return false, err
	}

	wantID := envRuntimeID + "=" + runtimeID
	wantRoot := envScratchRoot + "=" + generationRoot
	foundID, foundRoot := false, false

	for _, entry := range environment {
		foundID = foundID || entry == wantID
		foundRoot = foundRoot || entry == wantRoot
	}

	return foundID && foundRoot, nil
}

func darwinProcessEnvironment(raw []byte) ([]string, error) {
	if len(raw) < 4 {
		return nil, errors.New("darwin process arguments are truncated")
	}

	argc := int(binary.LittleEndian.Uint32(raw[:4]))
	if argc <= 0 || argc > len(raw)-4 {
		return nil, errors.New("darwin process argument count is invalid")
	}

	position := 4

	next := func() (string, bool) {
		if position >= len(raw) {
			return "", false
		}

		end := position
		for end < len(raw) && raw[end] != 0 {
			end++
		}

		if end == len(raw) {
			return "", false
		}

		value := string(raw[position:end])
		position = end + 1

		return value, true
	}
	if executable, ok := next(); !ok || executable == "" {
		return nil, errors.New("darwin process executable path is missing")
	}

	for position < len(raw) && raw[position] == 0 {
		position++
	}

	for range argc {
		value, ok := next()
		if !ok || value == "" {
			return nil, errors.New("darwin process arguments are incomplete")
		}
	}

	environment := make([]string, 0)
	terminated := false

	for position < len(raw) {
		value, ok := next()
		if !ok {
			return nil, errors.New("darwin process environment is incomplete")
		}

		if value == "" {
			if len(environment) == 0 {
				return nil, errors.New("darwin process environment boundary is ambiguous")
			}

			terminated = true

			break
		}

		environment = append(environment, value)
	}

	if !terminated {
		return nil, errors.New("darwin process environment is unterminated")
	}

	return environment, nil
}

type containmentCandidateState uint8

const (
	containmentCandidateGone containmentCandidateState = iota
	containmentCandidateCorrelated
	containmentCandidateAmbiguous
)

func revalidateContainmentCandidate(record containmentRecordData, candidate ContainmentCandidate) containmentCandidateState {
	process, err := containmentProcessLookup(candidate.PID)
	if errors.Is(err, syscall.ESRCH) {
		return containmentCandidateGone
	}

	if err != nil {
		return containmentCandidateAmbiguous
	}

	if int(process.Proc.P_pid) != candidate.PID || process.Eproc.Ucred.Uid != containmentUID() ||
		process.Proc.P_starttime.Sec != candidate.StartSec || process.Proc.P_starttime.Usec != candidate.StartUsec {
		return containmentCandidateAmbiguous
	}

	matched, err := processHasContainmentMarkers(candidate.PID, record.RuntimeID, record.GenerationRoot)
	if errors.Is(err, syscall.ESRCH) {
		return containmentCandidateGone
	}

	if err != nil || !matched {
		return containmentCandidateAmbiguous
	}

	return containmentCandidateCorrelated
}

func containmentUID() uint32 {
	return uint32(os.Getuid()) //nolint:gosec // Darwin UIDs are non-negative and represented as uint32 by KinfoProc.
}

func awaitContainmentCandidates(
	record containmentRecordData,
	candidates []ContainmentCandidate,
	deadline time.Time,
	result *ContainmentCleanupResult,
) []ContainmentCandidate {
	remaining := append([]ContainmentCandidate(nil), candidates...)
	for len(remaining) > 0 && containmentOpsNow().Before(deadline) {
		next := remaining[:0]
		for index, candidate := range remaining {
			if !containmentOpsNow().Before(deadline) {
				next = append(next, remaining[index:]...)

				break
			}

			state := containmentCandidateRevalidate(record, candidate)
			observeContainmentCandidate(result, candidate, state)

			if !containmentOpsNow().Before(deadline) {
				if state == containmentCandidateCorrelated {
					next = append(next, candidate)
				}

				next = append(next, remaining[index+1:]...)

				break
			}

			if state == containmentCandidateCorrelated {
				next = append(next, candidate)
			}
		}

		remaining = next
		if len(remaining) > 0 {
			containmentOpsSleep(10 * time.Millisecond)
		}
	}

	return remaining
}

func observeRecordedContainmentCandidate(record containmentRecordData, result *ContainmentCleanupResult) {
	candidate, ok := recordedContainmentCandidate(record)
	if !ok {
		return
	}

	observeContainmentCandidate(result, candidate, containmentCandidateRevalidate(record, candidate))
}

func recordedAmbiguousPIDs(record containmentRecordData) []int {
	candidate, ok := recordedContainmentCandidate(record)
	if !ok {
		return []int{}
	}

	if containmentCandidateRevalidate(record, candidate) == containmentCandidateAmbiguous {
		return []int{candidate.PID}
	}

	return []int{}
}

func recordedContainmentCandidate(record containmentRecordData) (ContainmentCandidate, bool) {
	if record.State == containmentStateAbsent || record.DirectChildPID == nil || record.DirectChildStartSec == nil || record.DirectChildStartUsec == nil {
		return ContainmentCandidate{}, false
	}

	return ContainmentCandidate{
		PID:       *record.DirectChildPID,
		StartSec:  *record.DirectChildStartSec,
		StartUsec: *record.DirectChildStartUsec,
	}, true
}

func readyContainmentCleanupResult(result ContainmentCleanupResult) ContainmentCleanupResult {
	result.ResultReady = true

	return result
}

func replaceRemainingContainmentCandidates(result *ContainmentCleanupResult, candidates []ContainmentCandidate) {
	result.RemainingCorrelated = append(result.RemainingCorrelated[:0], candidates...)
}

func observeContainmentCandidate(
	result *ContainmentCleanupResult,
	candidate ContainmentCandidate,
	state containmentCandidateState,
) {
	if state == containmentCandidateCorrelated {
		if !containsContainmentCandidate(result.RemainingCorrelated, candidate) {
			result.RemainingCorrelated = append(result.RemainingCorrelated, candidate)
		}

		return
	}

	removeRemainingContainmentCandidate(result, candidate)

	if state == containmentCandidateAmbiguous {
		appendAmbiguousPID(result, candidate.PID)
	}
}

func removeRemainingContainmentCandidate(result *ContainmentCleanupResult, candidate ContainmentCandidate) {
	for index, existing := range result.RemainingCorrelated {
		if existing != candidate {
			continue
		}

		copy(result.RemainingCorrelated[index:], result.RemainingCorrelated[index+1:])
		result.RemainingCorrelated = result.RemainingCorrelated[:len(result.RemainingCorrelated)-1]

		return
	}
}

func appendAmbiguousPID(result *ContainmentCleanupResult, pid int) {
	for _, existing := range result.AmbiguousPIDs {
		if existing == pid {
			return
		}
	}

	result.AmbiguousPIDs = append(result.AmbiguousPIDs, pid)
}

func containmentRegistryPath(scratchParent string) (string, error) {
	if strings.TrimSpace(scratchParent) == "" {
		return "", errors.New("containment scratch parent is required")
	}

	absolute, err := containmentOpsAbs(scratchParent)
	if err != nil {
		return "", fmt.Errorf("resolve containment scratch parent: %w", err)
	}

	return filepath.Join(absolute, containmentRegistryName), nil
}

func validRuntimeID(runtimeID string) bool {
	if len(runtimeID) != 32 || runtimeID != strings.ToLower(runtimeID) {
		return false
	}

	for _, value := range runtimeID {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return false
		}
	}

	return true
}

func validatedCleanupRoot(scratchParent string, generationRoot string, state string) (string, error) {
	root, _, err := inspectValidatedCleanupRoot(scratchParent, generationRoot, state)

	return root, err
}

func inspectValidatedCleanupRoot(scratchParent string, generationRoot string, state string) (string, os.FileInfo, error) {
	root, err := validatedContainmentRootPath(scratchParent, generationRoot)
	if err != nil {
		return "", nil, err
	}

	info, err := containmentOpsLstat(root)
	if errors.Is(err, os.ErrNotExist) {
		if state != containmentStateAbsent {
			return "", nil, errors.New("selected generation root is missing before containment reached group_absent")
		}

		return root, nil, nil
	}

	if err != nil {
		return "", nil, fmt.Errorf("inspect selected generation root: %w", err)
	}

	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, errors.New("selected generation root is not a directory")
	}

	return root, info, nil
}

func validatedContainmentRootPath(scratchParent string, generationRoot string) (string, error) {
	registry, err := containmentRegistryPath(scratchParent)
	if err != nil {
		return "", err
	}

	parent := filepath.Dir(registry)

	root, err := containmentOpsAbs(generationRoot)
	if err != nil {
		return "", fmt.Errorf("resolve selected generation root: %w", err)
	}

	relative, err := containmentOpsRel(parent, root)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("selected generation root is outside the scratch parent")
	}

	if !strings.HasPrefix(filepath.Base(root), "acp-go-pi-runtime-") {
		return "", errors.New("selected generation root has an invalid prefix")
	}

	return root, nil
}
