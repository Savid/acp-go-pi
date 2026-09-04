package pi

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

func ProbeOrdinaryVersion(ctx context.Context, executable string, environment []string) (string, error) {
	resolved, err := lookPathInOrdinaryEnvironment(executable, environment)
	if err != nil {
		return "", fmt.Errorf("resolve pi version executable: %w", err)
	}

	command := exec.CommandContext(ctx, resolved, "--version")

	command.Env = append([]string(nil), environment...)

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
