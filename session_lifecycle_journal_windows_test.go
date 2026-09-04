//go:build windows

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLastLifecycleBoundaryRefusesCaseDistinctEnvironmentKeys is the Windows
// half of the same rule. A process environment here folds names to one case,
// so a boundary naming both "Token" and "TOKEN" describes a configuration this
// platform cannot reproduce; the resume is refused rather than resumed against
// whichever value happened to survive the fold.
func TestLastLifecycleBoundaryRefusesCaseDistinctEnvironmentKeys(t *testing.T) {
	t.Parallel()

	agent := caseDistinctEnvironmentBoundary(t)

	_, found, err := agent.lastLifecycleBoundary(t.Context(), "session")
	require.False(t, found)
	require.ErrorContains(t, err, "session resume incompatible")
}
