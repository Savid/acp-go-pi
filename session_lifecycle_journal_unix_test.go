//go:build !windows

package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLastLifecycleBoundaryAcceptsCaseDistinctEnvironmentKeys pins that a
// stored boundary carrying two names that differ only in case reads back with
// both, because a process environment here tells them apart. Windows folds
// them, and refuses the same boundary; that half is pinned beside this one.
func TestLastLifecycleBoundaryAcceptsCaseDistinctEnvironmentKeys(t *testing.T) {
	t.Parallel()

	agent := caseDistinctEnvironmentBoundary(t)

	record, found, err := agent.lastLifecycleBoundary(t.Context(), "session")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, map[string]string{"Token": "one", "TOKEN": "two"}, record.Configuration.Env)
}
