//go:build darwin

package main

import (
	"path/filepath"
	"slices"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

var (
	containmentResolveAbs       = filepath.Abs
	containmentDiagnoseRegistry = internalpi.DiagnoseContainment
	containmentCleanupRegistry  = internalpi.CleanupContainment
)

func containmentDiagnose(scratchDir string) (containmentDiagnoseOutput, error) {
	parent, err := containmentResolveAbs(scratchDir)
	if err != nil {
		return containmentDiagnoseOutput{}, err
	}

	diagnostics, err := containmentDiagnoseRegistry(parent)
	if err != nil {
		return containmentDiagnoseOutput{}, err
	}

	records := make([]containmentDiagnoseRecord, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		record := containmentDiagnoseRecord{
			RuntimeID:      diagnostic.RuntimeID,
			State:          diagnostic.State,
			GenerationRoot: diagnostic.GenerationRoot,
			CorrelatedPIDs: make([]int, 0, len(diagnostic.Candidates)),
			AmbiguousPIDs:  append(make([]int, 0, len(diagnostic.AmbiguousPIDs)), diagnostic.AmbiguousPIDs...),
		}
		for _, candidate := range diagnostic.Candidates {
			record.CorrelatedPIDs = append(record.CorrelatedPIDs, candidate.PID)
		}

		slices.Sort(record.CorrelatedPIDs)
		slices.Sort(record.AmbiguousPIDs)
		records = append(records, record)
	}

	slices.SortFunc(records, compareContainmentDiagnoseRecords)

	return containmentDiagnoseOutput{Vendor: containmentVendor, Containment: containmentBestEffort, ScratchParent: parent, Warning: containmentOutputWarning, Records: records}, nil
}

func compareContainmentDiagnoseRecords(left containmentDiagnoseRecord, right containmentDiagnoseRecord) int {
	if left.RuntimeID < right.RuntimeID {
		return -1
	}

	if left.RuntimeID > right.RuntimeID {
		return 1
	}

	return 0
}

func containmentCleanup(scratchDir string, runtimeID string, force bool) (containmentCleanupOutput, error) {
	parent, err := containmentResolveAbs(scratchDir)
	if err != nil {
		return containmentCleanupOutput{}, err
	}

	result, err := containmentCleanupRegistry(parent, runtimeID, force)
	output := containmentCleanupOutput{
		Vendor:                  containmentVendor,
		Containment:             containmentBestEffort,
		ScratchParent:           parent,
		Warning:                 containmentOutputWarning,
		RuntimeID:               result.RuntimeID,
		GenerationRoot:          result.GenerationRoot,
		TermSignalledPIDs:       candidatePIDs(result.TermSignalled),
		KillSignalledPIDs:       candidatePIDs(result.KillSignalled),
		RemainingCorrelatedPIDs: candidatePIDs(result.RemainingCorrelated),
		AmbiguousPIDs:           append(make([]int, 0, len(result.AmbiguousPIDs)), result.AmbiguousPIDs...),
		RootRemoved:             result.RootRemoved,
		ResultReady:             result.ResultReady,
	}
	slices.Sort(output.AmbiguousPIDs)

	return output, err
}

func candidatePIDs(candidates []internalpi.ContainmentCandidate) []int {
	pids := make([]int, 0, len(candidates))
	for _, candidate := range candidates {
		pids = append(pids, candidate.PID)
	}

	slices.Sort(pids)

	return pids
}
