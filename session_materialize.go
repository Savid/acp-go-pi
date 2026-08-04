package piacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultSessionStoreLoadTimeout = 10 * time.Second

var (
	materializeMkdirAll  = os.MkdirAll
	materializeMkdirTemp = os.MkdirTemp
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
// scratch parent (ScratchDir, or the system temp directory when unset). A
// configured Home replaces the generated agent directory with the operator's
// durable one, which is what pi's cross-process credential lock is keyed on;
// session storage and every scratch generation stay under the removable root.
func (a *Agent) createSessionDirs() (sessionDirs, error) {
	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return sessionDirs{}, err
	}

	sessionRoot, err := materializeMkdirTemp(parent, "acp-go-pi-session-*")
	if err != nil {
		return sessionDirs{}, fmt.Errorf("create session root: %w", err)
	}
	if err := os.Chmod(sessionRoot, 0o711); err != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, fmt.Errorf("protect session root: %w", err)
	}

	dirs, err := createSessionGeneration(sessionRoot)
	if err != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, err
	}

	dirs.SessionRoot = sessionRoot

	if err := a.applyDurableHome(&dirs); err != nil {
		_ = materializeRemoveAll(sessionRoot)

		return sessionDirs{}, err
	}

	return dirs, nil
}

// applyDurableHome points a generation's agent directory at the configured
// durable home. The home outlives every generation, so it is never created
// under the removable session root and never copied forward.
func (a *Agent) applyDurableHome(dirs *sessionDirs) error {
	home := a.options.Home
	if home == "" {
		return nil
	}
	if a.options.ProcessIsolation != nil {
		if err := validateNativeOwnedDirectory(home, a.options.ProcessIsolation); err != nil {
			return err
		}

		return errors.New("durable pi agent directory is unsupported with process isolation")
	}

	if err := materializeMkdirAll(home, 0o700); err != nil {
		return fmt.Errorf("create durable agent directory: %w", err)
	}

	dirs.AgentDir = home

	return nil
}

func createSessionGeneration(sessionRoot string) (sessionDirs, error) {
	root, err := materializeMkdirTemp(sessionRoot, "acp-go-pi-runtime-*")
	if err != nil {
		return sessionDirs{}, fmt.Errorf("create session runtime generation: %w", err)
	}

	dirs := sessionDirs{Root: root, AgentDir: filepath.Join(root, "agent"), SessionDir: filepath.Join(root, "sessions")}

	for _, dir := range []string{dirs.AgentDir, dirs.SessionDir} {
		if err := materializeMkdirAll(dir, 0o700); err != nil {
			_ = materializeRemoveAll(root)

			return sessionDirs{}, fmt.Errorf("create session directory: %w", err)
		}
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
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
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
