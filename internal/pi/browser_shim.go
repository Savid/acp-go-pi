package pi

import (
	"os"
	"path/filepath"
	"strings"
)

// browserShimPrefix names every shim directory created beneath a scratch
// parent.
const browserShimPrefix = "acp-go-pi-browser-shim-"

const (
	browserShimPathEnv    = "PATH"
	browserShimBrowserEnv = "BROWSER"
)

// browserShimScript is what each shadowed launcher becomes: a program that
// accepts any arguments, opens nothing, and reports success. Launchers walk a
// candidate list and stop at the first success, so failing here would hand the
// URL to a candidate this directory does not shadow.
var browserShimScript = []byte("#!/bin/sh\nexit 0\n")

// browserLauncherNames are the programs a harness execs to open a URL. Darwin's
// `open` is the entry a BROWSER-only remedy misses: the launcher execs it
// directly and never reads BROWSER.
var browserLauncherNames = []string{
	"open",
	"xdg-open",
	"x-www-browser",
	"www-browser",
	"sensible-browser",
}

// browserShim is a scratch directory of no-op launchers that shadows every
// browser a native login leg would otherwise open on the operator's desktop.
type browserShim struct {
	dir string
}

// environ returns env with the shim ahead of PATH and BROWSER pointed at one of
// its no-ops.
func (s *browserShim) environ(env []string) []string {
	return browserShimEnviron(env, s.dir)
}

// remove deletes the shim directory.
func (s *browserShim) remove() error {
	if s == nil {
		return nil
	}

	return os.RemoveAll(s.dir)
}

// browserShimEnviron rewrites a child environment so dir precedes every other
// PATH entry and BROWSER names a no-op inside it. Both mechanisms are set
// because harnesses split across them: one execs a launcher off PATH, another
// reads BROWSER, and neither alone covers both.
func browserShimEnviron(env []string, dir string) []string {
	kept := make([]string, 0, len(env)+2)
	search := dir

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			kept = append(kept, entry)

			continue
		}

		switch {
		case environmentKeyEqual(key, browserShimPathEnv):
			search = dir + string(os.PathListSeparator) + value
		case environmentKeyEqual(key, browserShimBrowserEnv):
		default:
			kept = append(kept, entry)
		}
	}

	return append(kept,
		browserShimPathEnv+"="+search,
		browserShimBrowserEnv+"="+browserShimCommand(dir),
	)
}

// browserShimCommand is the value BROWSER carries: an absolute no-op that
// ignores the URL it is handed.
func browserShimCommand(dir string) string {
	return filepath.Join(dir, browserLauncherNames[0])
}

// BrowserShim is the handle a caller outside this package holds on one shim
// directory: it rewrites the environment of the child that owns it and deletes
// the directory when that child is torn down.
type BrowserShim struct {
	shim *browserShim
}

// NewBrowserShim materialises a shim beneath parent, which must already exist.
// It fails on any platform where a directory on PATH cannot shadow the launcher
// a browser is opened with, so a caller that cannot neutralise the launch
// learns it before starting the child.
func NewBrowserShim(parent string) (*BrowserShim, error) {
	shim, err := newBrowserShim(parent)
	if err != nil {
		return nil, err
	}

	return &BrowserShim{shim: shim}, nil
}

// Environ returns env with the shim ahead of PATH and BROWSER pointed at one of
// its no-ops. A nil shim leaves env untouched.
func (s *BrowserShim) Environ(env []string) []string {
	if s == nil {
		return env
	}

	return s.shim.environ(env)
}

// Remove deletes the shim directory. A nil shim owns nothing to delete.
func (s *BrowserShim) Remove() error {
	if s == nil {
		return nil
	}

	return s.shim.remove()
}
