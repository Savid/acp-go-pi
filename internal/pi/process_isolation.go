package pi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// ProcessIsolation is the internal launch policy copied from the public
// adapter option. A nil policy is never a request to inherit ambient state.
type ProcessIsolation struct {
	UID                      uint32
	GID                      uint32
	BaseEnvironment          map[string]string
	TestOnlyNoCredential     bool
	TestOnlyIdentityLockRoot string
}

var errProcessIsolationRequired = errors.New("process isolation policy is required")

const (
	envIsolationUID  = privateEnvPrefix + "ISOLATION_UID"
	envIsolationGID  = privateEnvPrefix + "ISOLATION_GID"
	envIsolationTest = privateEnvPrefix + "ISOLATION_TEST_ONLY"
	envValueTrue     = "true"
)

func validateProcessIsolation(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errProcessIsolationRequired
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}
	if isolation.BaseEnvironment == nil {
		return errors.New("process isolation base environment is required")
	}

	for key := range isolation.BaseEnvironment {
		if !validEnvironmentName(key) {
			return fmt.Errorf("process isolation base environment contains invalid key %q", key)
		}

		if strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
			return fmt.Errorf("process isolation base environment contains reserved key %q", key)
		}
	}

	return validateProcessIsolationPlatform()
}

func validEnvironmentName(key string) bool {
	return key != "" && !strings.ContainsRune(key, '=') && strings.IndexByte(key, 0) < 0
}

func isolationEnvironment(isolation *ProcessIsolation, overlays ...map[string]string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	env := make(map[string]string, len(isolation.BaseEnvironment))
	for key, value := range isolation.BaseEnvironment {
		env[key] = value
	}

	for _, overlay := range overlays {
		for key, value := range overlay {
			if !validEnvironmentName(key) || strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			env[key] = value
		}
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}

	return out, nil
}

func environmentValue(environment []string, name string) string {
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == name {
			return value
		}
	}

	return ""
}

func lookPathInEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		if !filepath.IsAbs(file) {
			return "", fmt.Errorf("executable path %q is not absolute", file)
		}

		return executableFile(file)
	}

	search := environmentValue(environment, envPath)
	if search == "" {
		return "", fmt.Errorf("executable %q cannot be resolved without policy PATH", file)
	}

	for _, dir := range filepath.SplitList(search) {
		if !filepath.IsAbs(dir) {
			return "", fmt.Errorf("policy PATH entry %q is not absolute", dir)
		}

		if path, err := executableFile(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in policy PATH", file)
}

func executableFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
}

// ResolveExecutable resolves file only through the complete policy
// environment plus explicit overlays.
func ResolveExecutable(file string, isolation *ProcessIsolation, overlays ...map[string]string) (string, error) {
	environment, err := isolationEnvironment(isolation, overlays...)
	if err != nil {
		return "", err
	}

	return lookPathInEnvironment(file, environment)
}

func supervisorEnvironment(native []string, isolation *ProcessIsolation, modeKey, modeValue string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	environment := make([]string, 0, len(native)+3)
	for _, entry := range native {
		name, _, ok := strings.Cut(entry, "=")
		if ok && name != modeKey && name != envIsolationUID && name != envIsolationGID && name != envIsolationTest {
			environment = append(environment, entry)
		}
	}

	return append(environment,
		modeKey+"="+modeValue,
		envIsolationUID+"="+strconv.FormatUint(uint64(isolation.UID), 10),
		envIsolationGID+"="+strconv.FormatUint(uint64(isolation.GID), 10),
		envIsolationTest+"="+strconv.FormatBool(isolation.TestOnlyNoCredential),
	), nil
}

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
