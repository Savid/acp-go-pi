package pi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// DefaultMinimumVersion is the minimum `pi --version` the adapter accepts by
// default: the version its behavior was verified against.
const DefaultMinimumVersion = "0.80.6"

// execCommandContext is a seam for version-probe tests.
var execCommandContext = exec.CommandContext

// ProbeVersion runs `pi --version` and returns the reported version string.
func ProbeVersion(ctx context.Context, executablePath string) (string, error) {
	cmd := execCommandContext(ctx, executablePath, "--version")
	configureProcessCommandPlatform(cmd)

	var output bytes.Buffer

	cmd.Stdout = &output
	cmd.WaitDelay = defaultShutdownStepTimeout

	tree, err := startProcessTree(cmd)
	if err != nil {
		return "", fmt.Errorf("probe pi version: %w", err)
	}

	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)

		_ = tree.kill()
	})
	waitErr := cmd.Wait()

	if stopCancellation() {
		close(cancellationDone)
	}

	<-cancellationDone

	quiescenceErr := tree.terminateAndWait(defaultProcessTreeWait)
	if errors.Is(waitErr, exec.ErrWaitDelay) && quiescenceErr == nil {
		waitErr = nil
	}

	if waitErr != nil || quiescenceErr != nil {
		var probeErr error
		if waitErr != nil {
			probeErr = fmt.Errorf("probe pi version: %w", waitErr)
		}

		return "", errors.Join(probeErr, quiescenceErr)
	}

	version := strings.TrimSpace(output.String())
	if version == "" {
		return "", fmt.Errorf("probe pi version: empty output")
	}

	return version, nil
}

// CheckMinimumVersion fails when version sorts below minimum.
func CheckMinimumVersion(version string, minimum string) error {
	comparison, err := compareVersions(version, minimum)
	if err != nil {
		return err
	}

	if comparison < 0 {
		return fmt.Errorf("pi version %s is below the minimum supported version %s", version, minimum)
	}

	return nil
}

func compareVersions(left string, right string) (int, error) {
	leftParts, err := versionParts(left)
	if err != nil {
		return 0, err
	}

	rightParts, err := versionParts(right)
	if err != nil {
		return 0, err
	}

	for index := range max(len(leftParts), len(rightParts)) {
		leftValue := 0
		if index < len(leftParts) {
			leftValue = leftParts[index]
		}

		rightValue := 0
		if index < len(rightParts) {
			rightValue = rightParts[index]
		}

		if leftValue != rightValue {
			if leftValue < rightValue {
				return -1, nil
			}

			return 1, nil
		}
	}

	return 0, nil
}

func versionParts(version string) ([]int, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if release, _, found := strings.Cut(trimmed, "-"); found {
		trimmed = release
	}

	if trimmed == "" {
		return nil, fmt.Errorf("invalid version %q", version)
	}

	segments := strings.Split(trimmed, ".")
	parts := make([]int, 0, len(segments))

	for _, segment := range segments {
		value, err := strconv.Atoi(segment)
		if err != nil || value < 0 {
			return nil, fmt.Errorf("invalid version %q", version)
		}

		parts = append(parts, value)
	}

	return parts, nil
}
