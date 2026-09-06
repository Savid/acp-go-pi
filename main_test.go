package piacp

import (
	"fmt"
	"os"
	"testing"
)

// TestMain gives the suite one temp root. The unit fake harness is built once per process under
// os.TempDir and nothing removes the image, so the root removes on the way
// out whatever a test left behind.
// TMP and TEMP cover the Windows lookup.
func TestMain(m *testing.M) {
	suiteTemp, err := os.MkdirTemp("", "acp-go-pi-suite-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create suite temp root:", err)
		os.Exit(1)
	}

	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		if err = os.Setenv(name, suiteTemp); err != nil {
			fmt.Fprintln(os.Stderr, "set suite "+name+":", err)
			os.Exit(1)
		}
	}

	code := m.Run()
	_ = os.RemoveAll(suiteTemp)
	os.Exit(code)
}
