//go:build darwin

package pi

import (
	"errors"
	"os"
	"strconv"
)

const envValueTrue = "true"

// inheritedProcessIsolation decodes the isolation policy a Darwin supervisor
// bootstrap inherits through its environment. It belongs to Darwin because the
// environment is the only channel that carries it: on Linux the supervisor
// receives its whole configuration over a sealed memfd, and
// turnSupervisorEnvironment deliberately passes nothing but the mode variable.
func inheritedProcessIsolation() (*ProcessIsolation, error) {
	uid, uidErr := strconv.ParseUint(os.Getenv(envIsolationUID), 10, 32)

	gid, gidErr := strconv.ParseUint(os.Getenv(envIsolationGID), 10, 32)
	if uidErr != nil || gidErr != nil {
		return nil, errors.New("process isolation bootstrap identity is invalid")
	}

	isolation := &ProcessIsolation{UID: uint32(uid), GID: uint32(gid), BaseEnvironment: map[string]string{}}
	if os.Getenv(envIsolationTest) == envValueTrue {
		isolation.TestOnlyNoCredential = true

		return isolation, nil
	}

	if err := verifyProcessIsolation(isolation); err != nil {
		return nil, err
	}

	return isolation, nil
}
