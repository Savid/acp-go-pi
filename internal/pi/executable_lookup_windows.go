//go:build windows

package pi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func canonicalEnvironmentKey(key string) string { return strings.ToUpper(key) }

func environmentKeyEqual(left string, right string) bool { return strings.EqualFold(left, right) }

func lookPathInOrdinaryEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	exts := windowsPathExtensions(environment)
	resolve := func(path string) (string, error) {
		if !filepath.IsAbs(path) {
			absolute, err := ordinaryExecutableAbs(path)
			if err != nil {
				return "", err
			}

			path = absolute
		}

		return windowsExecutableFile(path, exts)
	}

	if strings.ContainsAny(file, `:\/`) {
		return resolve(file)
	}

	for _, dir := range filepath.SplitList(environmentValue(environment, envPath)) {
		if dir == "" {
			continue
		}

		if path, err := resolve(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func windowsPathExtensions(environment []string) []string {
	value := environmentValue(environment, "PATHEXT")
	if value == "" {
		value = ".COM;.EXE;.BAT;.CMD"
	}

	exts := make([]string, 0, 4)
	for extension := range strings.SplitSeq(value, ";") {
		if extension == "" {
			continue
		}
		if extension[0] != '.' {
			extension = "." + extension
		}
		exts = append(exts, strings.ToLower(extension))
	}

	return exts
}

func windowsExecutableFile(path string, extensions []string) (string, error) {
	if filepath.Ext(path) != "" {
		if resolved, err := executableFile(path); err == nil {
			return resolved, nil
		}
	}

	for _, extension := range extensions {
		if resolved, err := executableFile(path + extension); err == nil {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("%q is not executable", path)
}

func executableFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
}
