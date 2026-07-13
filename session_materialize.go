package piacp

import (
	"context"
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
)

// sessionDirs is one session's isolated on-disk layout: an agent directory
// (PI_CODING_AGENT_DIR) and a session storage directory (--session-dir),
// both under one removable root.
type sessionDirs struct {
	Root       string
	AgentDir   string
	SessionDir string
}

// createSessionDirs creates a fresh isolated per-session root under the
// scratch parent (ScratchDir, or the system temp directory when unset).
func (a *Agent) createSessionDirs() (sessionDirs, error) {
	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return sessionDirs{}, err
	}

	root, err := materializeMkdirTemp(parent, "acp-go-pi-session-*")
	if err != nil {
		return sessionDirs{}, fmt.Errorf("create session root: %w", err)
	}

	dirs := sessionDirs{
		Root:       root,
		AgentDir:   filepath.Join(root, "agent"),
		SessionDir: filepath.Join(root, "sessions"),
	}

	for _, dir := range []string{dirs.AgentDir, dirs.SessionDir} {
		if err := materializeMkdirAll(dir, 0o700); err != nil {
			_ = materializeRemoveAll(root)

			return sessionDirs{}, fmt.Errorf("create session directory: %w", err)
		}
	}

	return dirs, nil
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
