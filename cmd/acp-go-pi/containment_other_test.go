//go:build !darwin

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainmentCommandsFailOperationallyOffDarwin(t *testing.T) {
	runtimeID := strings.Repeat("a", 32)
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"diagnose", "-scratch-dir", "."}, want: "containment diagnostics are available only on darwin"},
		{args: []string{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force"}, want: "containment cleanup is available only on darwin"},
	} {
		var stdout, stderr bytes.Buffer
		require.Equal(t, 1, runContainmentCommand(test.args, &stdout, &stderr))
		require.Empty(t, stdout.String())
		require.Equal(t, "acp-go-pi: "+test.want+"\n", stderr.String())
	}
}

func TestContainmentCommandUsageOffDarwin(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, runContainmentCommand([]string{"diagnose"}, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Equal(t, "acp-go-pi: containment diagnose requires -scratch-dir\n", stderr.String())
}
