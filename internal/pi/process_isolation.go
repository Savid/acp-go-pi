package pi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// ProcessIsolation is the internal explicit launch policy copied from the
// public adapter option. A nil policy selects ordinary current-identity
// execution with the separately captured sanitized environment.
type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

type ProcessIsolation struct {
	UID                      uint32
	GID                      uint32
	BaseEnvironment          map[string]string
	TestOnlyNoCredential     bool
	TestOnlyIdentityLockRoot string
	IdentityLock             ProcessIdentityLockCapability `json:"-"`
	AuthorityDomain          ProcessIdentityLockCapability `json:"-"`
	StandaloneOwnerID        string                        `json:"standaloneOwnerId"`
	StandaloneStateRoot      string                        `json:"standaloneStateRoot"`
}

var errProcessIsolationRequired = errors.New("process isolation policy is required")

var processIsolationGOOS = runtime.GOOS

var ordinaryEnvironmentEntries = os.Environ
var ordinaryExecutableAbs = filepath.Abs

// CaptureOrdinaryEnvironment captures Pi's deliberately narrow, non-secret
// execution baseline. Provider credentials and loader variables are ambient
// state and never cross this boundary; explicit agent/session Env remains the
// credential route.
func CaptureOrdinaryEnvironment() map[string]string {
	environment := make(map[string]string)

	for _, entry := range ordinaryEnvironmentEntries() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !ordinaryEnvironmentKey(key) {
			continue
		}

		environment[canonicalEnvironmentKey(key)] = value
	}

	return environment
}

func ordinaryEnvironmentKey(key string) bool {
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, privateEnvPrefix) || !validEnvironmentName(key) {
		return false
	}

	switch upper {
	case envPath, envHome, "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP",
		"LANG", "TERM", "COLORTERM", "NO_COLOR", "FORCE_COLOR", "SYSTEMROOT",
		"WINDIR", "COMSPEC", "PATHEXT", "USERPROFILE", "__CF_USER_TEXT_ENCODING":
		return true
	default:
		return strings.HasPrefix(upper, "LC_")
	}
}

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

	return validateProcessIsolationPlatform(isolation)
}

func validEnvironmentName(key string) bool {
	return key != "" && !strings.ContainsRune(key, '=') && strings.IndexByte(key, 0) < 0
}

// ComposeEnvironment applies environment phases from left to right.
func ComposeEnvironment(phases ...map[string]string) map[string]string {
	environment := make(map[string]string)

	for _, phase := range phases {
		for _, key := range sortedEnvironmentKeys(phase) {
			environment[canonicalEnvironmentKey(key)] = phase[key]
		}
	}

	return environment
}

func sortedEnvironmentKeys(environment map[string]string) []string {
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}

func environmentEntries(environment map[string]string) []string {
	keys := sortedEnvironmentKeys(environment)

	entries := make([]string, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, key+"="+environment[key])
	}

	return entries
}

func isolationEnvironment(isolation *ProcessIsolation, overlays ...map[string]string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	for _, overlay := range overlays {
		for key := range overlay {
			if !validEnvironmentName(key) || strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			if strings.EqualFold(key, envPath) {
				return nil, errors.New("process environment PATH must use ExtraPathDirs")
			}
		}
	}

	phases := make([]map[string]string, 0, len(overlays)+1)
	phases = append(phases, isolation.BaseEnvironment)
	phases = append(phases, overlays...)
	environment := ComposeEnvironment(phases...)

	return environmentEntries(environment), nil
}

func ordinaryEnvironment(base map[string]string, overlays ...map[string]string) ([]string, error) {
	ordinaryBase := make(map[string]string, len(base))
	for key, value := range base {
		if ordinaryEnvironmentKey(key) {
			ordinaryBase[key] = value
		}
	}

	for _, overlay := range overlays {
		for key := range overlay {
			if !validEnvironmentName(key) || strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
				return nil, fmt.Errorf("process environment contains invalid key %q", key)
			}

			if strings.EqualFold(key, envPath) {
				return nil, errors.New("process environment PATH must use ExtraPathDirs")
			}
		}
	}

	phases := make([]map[string]string, 0, len(overlays)+1)
	phases = append(phases, ordinaryBase)
	phases = append(phases, overlays...)
	environment := ComposeEnvironment(phases...)

	return environmentEntries(environment), nil
}

func environmentValue(environment []string, name string) string {
	for index := len(environment) - 1; index >= 0; index-- {
		entry := environment[index]

		key, value, ok := strings.Cut(entry, "=")
		if ok && environmentKeyEqual(key, name) {
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

// ResolveExecutable resolves file through the explicit closed policy when
// present, otherwise through the captured ordinary environment. Ordinary PATH
// entries and configured paths are not subject to explicit-policy absolute
// path constraints.
func ResolveExecutable(file string, isolation *ProcessIsolation, ordinary map[string]string, overlays ...map[string]string) (string, error) {
	if isolation == nil {
		environment, err := ordinaryEnvironment(ordinary, overlays...)
		if err != nil {
			return "", err
		}

		return lookPathInOrdinaryEnvironment(file, environment)
	}

	environment, err := isolationEnvironment(isolation, overlays...)
	if err != nil {
		return "", err
	}

	return lookPathInEnvironment(file, environment)
}
