package pi

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// The process tests launch this binary as a fake pi. That re-execution
	// arrives with pi's arguments, so the child role is claimed here, before
	// the test framework reads any of them.
	if mode := os.Getenv(ordinaryChildEnvKey); mode != "" {
		runOrdinaryChild(mode)
	}

	goleak.VerifyTestMain(m)
}
