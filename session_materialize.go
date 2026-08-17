package piacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
)

const defaultSessionStoreLoadTimeout = 10 * time.Second

var (
	materializeMkdirAll  = os.MkdirAll
	materializeMkdirTemp = os.MkdirTemp
	materializeChmod     = os.Chmod
	materializeRemoveAll = os.RemoveAll
	materializeWriteFile = os.WriteFile
	materializeStat      = os.Stat
	materializeReadFile  = os.ReadFile
	materializeWalkDir   = filepath.WalkDir
	materializeRel       = filepath.Rel
)

// sessionDirs is one session's isolated on-disk layout: an agent directory
// (PI_CODING_AGENT_DIR) and a session storage directory (--session-dir),
// both under one removable root.
type sessionDirs struct {
	SessionRoot string
	Root        string
	AgentDir    string
	SessionDir  string
}

// createSessionDirs creates a fresh isolated per-session root under the
// scratch parent. A configured Home replaces only the generated agent
// directory; session storage and generations remain removable scratch.
func (a *Agent) createSessionDirs() (sessionDirs, error) {
	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return sessionDirs{}, err
	}

	sessionRoot, err := materializeMkdirTemp(parent, "acp-go-pi-session-*")
	if err != nil {
		return sessionDirs{}, fmt.Errorf("create session root: %w", err)
	}

	if chmodErr := materializeChmod(sessionRoot, 0o711); chmodErr != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, fmt.Errorf("protect session root: %w", chmodErr)
	}

	dirs, err := createSessionGeneration(sessionRoot)
	if err != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, err
	}

	dirs.SessionRoot = sessionRoot

	if err := a.applyGenerationAgentDir(&dirs); err != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, err
	}

	return dirs, nil
}

func (a *Agent) createSessionRuntime() (sessionDirs, *pi.BrowserShim, error) {
	dirs, err := a.createSessionDirs()
	if err != nil {
		return sessionDirs{}, nil, err
	}

	shim, err := a.newOwnedSessionBrowserShim()

	return dirs, shim, err
}

// durableHome materializes Pi's stable native auth residence and reports its
// path, or the empty string when no durable home is configured. Hardened
// distinct-identity sessions refuse it until a proven credential-delivery
// design exists; ordinary current-identity sessions use it directly and Pi's
// native cross-process lock serializes auth.json updates.
func (a *Agent) durableHome() (string, error) {
	home := a.options.Home
	if home == "" {
		return "", nil
	}

	if a.options.ProcessIsolation != nil {
		return "", errors.New("durable pi agent directory is unavailable with explicit process isolation")
	}

	if !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return "", errors.New("durable pi agent directory must be a clean absolute path")
	}

	if err := materializeMkdirAll(home, 0o700); err != nil {
		return "", fmt.Errorf("create durable agent directory: %w", err)
	}

	if err := materializeChmod(home, 0o700); err != nil {
		return "", fmt.Errorf("protect durable agent directory: %w", err)
	}

	return home, nil
}

// reconcileHomeStartupDefaults puts the durable home's settings.json back to
// the operator baseline for the keys pi reads at process start to choose a
// model and thinking level. Every session launches against that one file and
// pi writes its own choice into it whenever a session changes model or
// thinking level, so without this a launch would start on whatever another
// session last selected. The restored content is the operator's, identical for
// every session, so concurrent launches cannot disagree about it. An ephemeral
// per-session agent directory shares nothing and needs no reconciliation.
func (a *Agent) reconcileHomeStartupDefaults(agentDir string) error {
	if a.options.Home == "" {
		return nil
	}

	a.startupDefaultsOnce.Do(func() {
		a.startupDefaults, a.startupDefaultsErr = pi.CaptureStartupDefaults(agentDir)
	})

	if a.startupDefaultsErr != nil {
		return a.startupDefaultsErr
	}

	return a.startupDefaults.Restore(agentDir)
}

// applyGenerationAgentDir points one runtime generation at the durable home
// when one is configured, and otherwise creates the generation's own private
// agent directory. Exactly one of the two is ever created, so a configured home
// leaves no unused generation agent directory behind.
func (a *Agent) applyGenerationAgentDir(dirs *sessionDirs) error {
	home, err := a.durableHome()
	if err != nil {
		return err
	}

	if home != "" {
		dirs.AgentDir = home

		return nil
	}

	dirs.AgentDir = filepath.Join(dirs.Root, "agent")
	if err := materializeMkdirAll(dirs.AgentDir, 0o700); err != nil {
		return fmt.Errorf("create session directory: %w", err)
	}

	return nil
}

func createSessionGeneration(sessionRoot string) (sessionDirs, error) {
	root, err := materializeMkdirTemp(sessionRoot, "acp-go-pi-runtime-*")
	if err != nil {
		return sessionDirs{}, fmt.Errorf("create session runtime generation: %w", err)
	}

	dirs := sessionDirs{Root: root, SessionDir: filepath.Join(root, "sessions")}
	if err := materializeMkdirAll(dirs.SessionDir, 0o700); err != nil {
		_ = materializeRemoveAll(root)

		return sessionDirs{}, fmt.Errorf("create session directory: %w", err)
	}

	return dirs, nil
}

func copyGenerationAgentDir(source string, target string) error {
	return materializeWalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		relative, err := materializeRel(source, path)
		if err != nil {
			return err
		}

		if relative == "." {
			return nil
		}

		destination := filepath.Join(target, relative)

		info, err := entry.Info()
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return materializeMkdirAll(destination, info.Mode().Perm())
		}

		if !info.Mode().IsRegular() {
			return fmt.Errorf("copy runtime generation agent path %q: non-regular entry", relative)
		}

		contents, err := materializeReadFile(path)
		if err != nil {
			return err
		}

		return materializeWriteFile(destination, contents, info.Mode().Perm())
	})
}

func rebaseGenerationPath(path string, oldRoot string, newRoot string) (string, error) {
	if path == "" {
		return "", nil
	}

	relative, err := materializeRel(oldRoot, path)
	if err != nil || relative == handoffParentDir || strings.HasPrefix(relative, handoffParentDir+string(filepath.Separator)) {
		return "", fmt.Errorf("runtime generation path %q is outside %q", path, oldRoot)
	}

	return filepath.Join(newRoot, relative), nil
}

// writeHydratedSessionFile materializes stored rows as a native session file
// inside the session directory for --session / switch_session.
func writeHydratedSessionFile(dirs sessionDirs, sessionID string, entries []SessionStoreEntry) (string, error) {
	var builder strings.Builder

	for _, entry := range entries {
		trimmed := strings.TrimSpace(string(entry))
		if trimmed == "" {
			continue
		}

		builder.WriteString(trimmed)
		builder.WriteByte('\n')
	}

	path := filepath.Join(dirs.SessionDir, sessionID+".jsonl")
	if err := materializeWriteFile(path, []byte(builder.String()), 0o600); err != nil {
		return "", fmt.Errorf("write hydrated session file: %w", err)
	}

	return path, nil
}

func (s *agentSession) sessionFileExists() bool {
	s.mu.Lock()
	path := s.sessionFilePath
	s.mu.Unlock()

	if path == "" {
		return false
	}

	info, err := materializeStat(path)

	return err == nil && !info.IsDir()
}

func (s *agentSession) removeSessionRoot() error {
	if s.sessionRoot == "" {
		return nil
	}

	return materializeRemoveAll(s.sessionRoot)
}

func (a *Agent) loadStoreEntries(ctx context.Context, store SessionStore, key SessionKey) ([]SessionStoreEntry, error) {
	loadCtx, cancel := context.WithTimeout(ctx, a.sessionStoreLoadTimeout())
	defer cancel()

	loadCtx, finishLoad := a.observe.StartSessionStore(loadCtx, "load")
	entries, err := store.Load(loadCtx, key)
	finishLoad(err)

	if err != nil {
		return nil, fmt.Errorf("load session store key: %w", err)
	}

	return entries, nil
}

func (a *Agent) sessionStoreLoadTimeout() time.Duration {
	if a.options.SessionStoreLoadTimeout > 0 {
		return a.options.SessionStoreLoadTimeout
	}

	return defaultSessionStoreLoadTimeout
}
