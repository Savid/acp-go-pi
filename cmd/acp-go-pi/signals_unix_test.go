//go:build unix

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"syscall"
	"testing"

	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

func TestUnixSignals(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)

	require.Contains(t, forwardedSignals(), os.Interrupt)
	require.Contains(t, forwardedSignals(), syscall.SIGHUP)
	require.Equal(t, 128+int(syscall.SIGTERM), signalCode(syscall.SIGTERM))

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, _ ...piacp.Option) error {
		require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
		<-ctx.Done()

		return nil
	}
	require.Equal(t, 128+int(syscall.SIGHUP), run(t.Context(), nil, bytes.NewReader(nil), io.Discard, io.Discard))
}
