package piacp

import "testing"

func restoreRuntimeGenerationSeams(t *testing.T) {
	t.Helper()
	ensureParent := runtimeGenerationEnsureScratchParent
	abs := runtimeGenerationAbs
	mkdirTemp := runtimeGenerationMkdirTemp
	chmod := runtimeGenerationChmod
	removeAll := runtimeGenerationRemoveAll
	writeProbeAgentDir := runtimeGenerationWriteProbeAgentDir
	t.Cleanup(func() {
		runtimeGenerationEnsureScratchParent = ensureParent
		runtimeGenerationAbs = abs
		runtimeGenerationMkdirTemp = mkdirTemp
		runtimeGenerationChmod = chmod
		runtimeGenerationRemoveAll = removeAll
		runtimeGenerationWriteProbeAgentDir = writeProbeAgentDir
	})
}
