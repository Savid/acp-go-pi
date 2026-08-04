package pi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// DefaultMinimumVersion is the minimum `pi --version` the adapter accepts by
// default: the version its behavior was verified against.
const DefaultMinimumVersion = "0.80.6"

// execCommand is a seam for version-probe tests. Probe cancellation is joined
// only after the selected process boundary has been captured.
var execCommand = exec.Command
var versionPrepareTreeCommand = prepareProcessTreeCommand
var versionPrepareContainmentRecord = prepareContainmentRecord
var versionStartTree = startProcessTree
var versionAfterPrepare = func() {}
var versionTreeKill = func(tree *processTree) error { return tree.kill() }
var versionTreeTerminateAndWait = func(tree *processTree, timeout time.Duration) error { return tree.terminateAndWait(timeout) }

// ProbeVersion runs `pi --version` and returns the reported version string.
func ProbeVersion(ctx context.Context, executablePath string, containment ContainmentSpec) (string, error) {
	if contextErr := ctx.Err(); contextErr != nil {
		return "", contextErr
	}

	if err := validateProcessIsolation(containment.Isolation); err != nil {
		return "", fmt.Errorf("validate pi version process isolation: %w", err)
	}

	environment := (LaunchSpec{Containment: containment}).Environ()

	resolved, err := lookPathInEnvironment(executablePath, environment)
	if err != nil {
		return "", fmt.Errorf("resolve pi version executable: %w", err)
	}

	cmd := execCommand(resolved, "--version")

	var output bytes.Buffer

	cmd.Stdout = &output
	cmd.WaitDelay = defaultShutdownStepTimeout
	cmd.Env = environment

	launch, err := versionPrepareTreeCommand(cmd, containment)
	if err != nil {
		return "", fmt.Errorf("prepare pi version probe: %w", err)
	}

	if isolationErr := applyProcessIsolation(launch.cmd, containment.Isolation); isolationErr != nil {
		launch.close()

		return "", fmt.Errorf("apply pi version process isolation: %w", isolationErr)
	}

	launch.containment, err = versionPrepareContainmentRecord(containment)
	if err != nil {
		launch.close()

		return "", fmt.Errorf("prepare pi version containment record: %w", err)
	}

	versionAfterPrepare()

	if contextErr := ctx.Err(); contextErr != nil {
		recordErr := completeUnstartedContainment(launch.containment)
		launch.close()

		return "", errors.Join(contextErr, recordErr)
	}

	tree, err := versionStartTree(launch)
	if err != nil {
		return "", fmt.Errorf("probe pi version: %w", err)
	}

	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)

		_ = versionTreeKill(tree)
	})
	waitErr := tree.directChildWait().await(defaultProcessTreeWait)

	if stopCancellation() {
		close(cancellationDone)
	}

	<-cancellationDone

	containmentErr := versionTreeTerminateAndWait(tree, defaultProcessTreeWait)
	if containmentErr == nil && errors.Is(waitErr, exec.ErrWaitDelay) {
		waitErr = nil
	}

	if waitErr != nil || containmentErr != nil {
		var probeErr error
		if waitErr != nil {
			probeErr = fmt.Errorf("probe pi version: %w", waitErr)
		}

		return "", errors.Join(probeErr, containmentErr)
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
