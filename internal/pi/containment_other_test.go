//go:build !darwin

package pi

import "testing"

func TestContainmentOperationsUnavailableOffDarwin(t *testing.T) {
	if _, err := DiagnoseContainment("scratch"); err == nil {
		t.Fatal("diagnose unexpectedly available")
	}
	if _, err := CleanupContainment("scratch", "runtime", true); err == nil {
		t.Fatal("cleanup unexpectedly available")
	}
}
