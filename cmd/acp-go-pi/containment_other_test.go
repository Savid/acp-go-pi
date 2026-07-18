//go:build !darwin

package main

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type containmentOtherErrorWriter struct{}

func (containmentOtherErrorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func restoreContainmentCommandSeams(t *testing.T) {
	t.Helper()
	diagnose := containmentDiagnoseCommand
	cleanup := containmentCleanupCommand
	t.Cleanup(func() {
		containmentDiagnoseCommand = diagnose
		containmentCleanupCommand = cleanup
	})
}

func TestContainmentCommandCoverageOffDarwin(t *testing.T) {
	restoreContainmentCommandSeams(t)
	runtimeID := strings.Repeat("a", 32)
	for _, args := range [][]string{
		nil,
		{"other"},
		{"diagnose", "-bad"},
		{"diagnose", "-scratch-dir", ".", "extra"},
		{"diagnose"},
		{"cleanup", "-bad"},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force", "extra"},
		{"cleanup"},
		{"cleanup", "-runtime-id", runtimeID},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", strings.Repeat("A", 32), "-force"},
		{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID},
	} {
		if code := runContainmentCommand(args, io.Discard, io.Discard); code != 2 {
			t.Fatalf("runContainmentCommand(%v) = %d", args, code)
		}
	}
	if !validContainmentRuntimeID(runtimeID) || validContainmentRuntimeID(strings.Repeat("g", 32)) {
		t.Fatal("runtime id validation mismatch")
	}

	containmentDiagnoseCommand = func(string) (containmentDiagnoseOutput, error) {
		return containmentDiagnoseOutput{Vendor: containmentVendor}, nil
	}
	if code := runContainmentCommand([]string{"diagnose", "-scratch-dir", "."}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("diagnose success = %d", code)
	}
	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return containmentCleanupOutput{Vendor: containmentVendor}, nil
	}
	cleanupArgs := []string{"cleanup", "-scratch-dir", ".", "-runtime-id", runtimeID, "-force"}
	if code := runContainmentCommand(cleanupArgs, io.Discard, io.Discard); code != 0 {
		t.Fatalf("cleanup success = %d", code)
	}
	if code := runContainmentCommand(cleanupArgs, containmentOtherErrorWriter{}, io.Discard); code != 1 {
		t.Fatalf("cleanup encode error = %d", code)
	}
	containmentCleanupCommand = func(string, string, bool) (containmentCleanupOutput, error) {
		return containmentCleanupOutput{ResultReady: true}, errors.New("partial")
	}
	if code := runContainmentCommand(cleanupArgs, io.Discard, io.Discard); code != 1 {
		t.Fatalf("partial cleanup = %d", code)
	}
	if code := runContainmentCommand(cleanupArgs, containmentOtherErrorWriter{}, io.Discard); code != 1 {
		t.Fatalf("partial encode error = %d", code)
	}

	if _, err := containmentDiagnose("scratch"); err == nil {
		t.Fatal("platform diagnose unexpectedly available")
	}
	if _, err := containmentCleanup("scratch", runtimeID, true); err == nil {
		t.Fatal("platform cleanup unexpectedly available")
	}
}
