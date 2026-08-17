//go:build !windows

package pi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func canonicalEnvironmentKey(key string) string { return key }

func environmentKeyEqual(left string, right string) bool { return left == right }

func lookPathInOrdinaryEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	resolve := func(path string) (string, error) {
		if !filepath.IsAbs(path) {
			absolute, err := ordinaryExecutableAbs(path)
			if err != nil {
				return "", err
			}

			path = absolute
		}

		return executableFile(path)
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		return resolve(file)
	}

	for _, dir := range filepath.SplitList(environmentValue(environment, envPath)) {
		if dir == "" {
			dir = "."
		}

		if path, err := resolve(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
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
