//go:build linux || darwin || freebsd || openbsd

package pi

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryProcessUsesCurrentIdentityWithoutAuthorityOrInventory(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "identity-pi")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nprintf '%s:%s\\n' \"$(id -u)\" \"$(id -g)\"\n"), 0o700))

	process, err := StartProcess(context.Background(), LaunchSpec{
		ExecutablePath: executable,
		AgentDir:       filepath.Join(dir, "agent"),
		SessionDir:     filepath.Join(dir, "sessions"),
		Cwd:            dir,
		Containment: ContainmentSpec{
			OrdinaryEnvironment: map[string]string{
				"PATH": os.Getenv("PATH"),
			},
		},
	})
	require.NoError(t, err)
	require.True(t, process.tree.ordinary)
	require.False(t, process.tree.supervised)
	require.Nil(t, process.tree.control)

	<-process.Exited()
	output, err := io.ReadAll(process.Stdout())
	require.NoError(t, err)
	require.NoError(t, process.WaitErr())
	require.Equal(t, strings.TrimSpace(outputString(os.Geteuid(), os.Getegid())), strings.TrimSpace(string(output)))
	count, available := process.ProviderDescendantCount()
	require.Zero(t, count)
	require.False(t, available)
	require.NoError(t, process.Close())
}

func outputString(uid, gid int) string {
	return fmt.Sprintf("%d:%d", uid, gid)
}
