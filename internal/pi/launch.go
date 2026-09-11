package pi

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
)

// Launch describes one pi RPC process.
type Launch struct {
	// ExtensionPaths are the wrapper-owned extensions pi loads with -e, in
	// load order. pi still discovers the operator's own extensions, skills, and
	// prompt templates from its home.
	ExtensionPaths []string
	// SessionPath is the native session file to continue, or empty for a new
	// session.
	SessionPath string
}

// The flag and value that select pi's RPC mode.
const (
	modeFlag = "--mode"
	modeRPC  = "rpc"
)

// Args renders the pi command line.
func (l Launch) Args() []string {
	args := []string{modeFlag, modeRPC}

	for _, path := range l.ExtensionPaths {
		args = append(args, "-e", path)
	}

	if l.SessionPath != "" {
		args = append(args, "--session", l.SessionPath)
	}

	return args
}

// ProbeVersion runs `pi --version` against the resolved executable with the
// given environment and returns the reported version.
func ProbeVersion(ctx context.Context, executable string, environment []string) (string, error) {
	command := exec.CommandContext(ctx, executable, "--version")
	command.Env = slices.Clone(environment)

	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("probe pi version: %w", err)
	}

	version := strings.TrimSpace(string(output))
	if version == "" {
		return "", errors.New("probe pi version: empty output")
	}

	return version, nil
}
