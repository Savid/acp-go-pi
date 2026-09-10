package pi

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

var ordinaryExecutableAbs = filepath.Abs

func CaptureOrdinaryEnvironment(entries []string) map[string]string {
	environment := make(map[string]string)

	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok && ordinaryEnvironmentKey(key) {
			environment[EnvironmentKey(key)] = value
		}
	}

	return environment
}

func ordinaryEnvironmentKey(key string) bool {
	upper := strings.ToUpper(key)
	if strings.HasPrefix(upper, privateEnvPrefix) || !validEnvironmentName(key) {
		return false
	}

	switch upper {
	case envPath, envHome, "USER", "LOGNAME", "SHELL", "TMPDIR", "TMP", "TEMP", "LANG", "TERM", "COLORTERM", "NO_COLOR", "FORCE_COLOR", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "USERPROFILE", "__CF_USER_TEXT_ENCODING":
		return true
	default:
		return strings.HasPrefix(upper, "LC_")
	}
}

func validEnvironmentName(key string) bool {
	return key != "" && !strings.ContainsRune(key, '=') && strings.IndexByte(key, 0) < 0
}

func ComposeEnvironment(phases ...map[string]string) map[string]string {
	result := make(map[string]string)

	for _, phase := range phases {
		for key, value := range phase {
			result[EnvironmentKey(key)] = value
		}
	}

	return result
}

func environmentEntries(environment map[string]string) []string {
	entries := make([]string, 0, len(environment))
	for key, value := range environment {
		entries = append(entries, key+"="+value)
	}

	sort.Strings(entries)

	return entries
}

func environmentValue(environment []string, name string) string {
	for index := len(environment) - 1; index >= 0; index-- {
		key, value, ok := strings.Cut(environment[index], "=")
		if ok && environmentKeyEqual(key, name) {
			return value
		}
	}

	return ""
}

func ResolveExecutable(file string, base map[string]string, overlays ...map[string]string) (string, error) {
	environment := ComposeEnvironment(append([]map[string]string{base}, overlays...)...)

	resolved, err := lookPathInOrdinaryEnvironment(file, environmentEntries(environment))
	if err != nil {
		return "", fmt.Errorf("resolve pi executable: %w", err)
	}

	return resolved, nil
}
