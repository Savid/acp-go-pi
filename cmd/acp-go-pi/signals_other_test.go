//go:build !unix

package main

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOtherPlatformSignals(t *testing.T) {
	require.Equal(t, []os.Signal{os.Interrupt, syscall.SIGTERM}, forwardedSignals())
	require.Equal(t, 130, signalCode(os.Interrupt))
	require.Equal(t, 128+int(syscall.SIGTERM), signalCode(syscall.SIGTERM))
	require.Equal(t, 1, signalCode(namedSignal("other")))
}
