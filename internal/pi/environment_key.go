package pi

import (
	"runtime"
	"strings"
)

const platformWindows = "windows"

// Platform is the operating system whose environment semantics the adapter
// applies. Tests pin it to exercise another platform's name resolution.
var Platform = runtime.GOOS

// EnvironmentKey is the name the target platform resolves an environment key
// by: the exact bytes on Unix, where PATH and path are two variables, and the
// upper-cased spelling on Windows, where they are one.
func EnvironmentKey(key string) string {
	if Platform == platformWindows {
		return strings.ToUpper(key)
	}

	return key
}
