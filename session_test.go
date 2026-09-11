package piacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestStderrTailLastLine(t *testing.T) {
	t.Parallel()

	tail := &stderrTail{}
	_, _ = tail.Write([]byte("first\nsecond\n"))
	require.Equal(t, "second", tail.lastLine())

	big := make([]byte, stderrTailBytes+100)
	for index := range big {
		big[index] = 'x'
	}

	_, _ = tail.Write(append(big, []byte("\nlast")...))
	require.Equal(t, "last", tail.lastLine())
}

func TestStateModelRef(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", stateModelRef(pi.SessionState{}))
	require.Equal(t, "", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "unknown", ID: "unknown"}}))
	require.Equal(t, "a/b", stateModelRef(pi.SessionState{Model: &pi.Model{Provider: "a", ID: "b"}}))
}

func TestPermissionModeDefault(t *testing.T) {
	t.Parallel()

	s := &session{}
	require.Equal(t, pi.PermissionModeAsk, s.permissionMode())

	s.options.Permission = pi.PermissionModeAllow
	require.Equal(t, pi.PermissionModeAllow, s.permissionMode())
}
