package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreStringHelpers(t *testing.T) {
	require.True(t, validUUIDShape("01234567-89ab-cdef-0123-456789ABCDEF"))
	require.False(t, validUUIDShape("short"))
	require.False(t, validUUIDShape("01234567x89ab-cdef-0123-456789abcdef"))
	require.False(t, validUUIDShape("01234567-89ab-cdef-0123-456789abcdeg"))
	require.Equal(t, "second", firstNonEmptyString("", "second", "third"))
	require.Empty(t, firstNonEmptyString("", ""))
}
