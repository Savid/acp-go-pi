package pi

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"time"
)

// Native environment variables pi reads for its home.
const (
	EnvAgentDir = "PI_CODING_AGENT_DIR"
	EnvHome     = "HOME"
)

// SessionHeader is the first row of a native session file.
type SessionHeader struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
}

// HeaderRowType is the type member of the session header row.
const HeaderRowType = "session"

// ParseHeader decodes a session header row. It reports false for any other
// row.
func ParseHeader(row []byte) (SessionHeader, bool) {
	var header SessionHeader
	if err := json.Unmarshal(row, &header); err != nil || header.Type != HeaderRowType || header.ID == "" {
		return SessionHeader{}, false
	}

	return header, true
}

// AgentDir resolves the agent directory pi would use under an environment: an
// explicit home, else PI_CODING_AGENT_DIR, else $HOME/.pi/agent.
func AgentDir(home string, lookup func(string) (string, bool)) string {
	if home != "" {
		return home
	}

	if dir, ok := lookup(EnvAgentDir); ok && dir != "" {
		return dir
	}

	userHome, _ := lookup(EnvHome)

	return filepath.Join(userHome, ".pi", "agent")
}

// SessionDir is the directory pi keeps sessions for cwd under agentDir.
func SessionDir(agentDir string, cwd string) string {
	resolved := filepath.Clean(cwd)
	trimmed := strings.TrimLeft(resolved, `/\`)
	safe := "--" + strings.NewReplacer("/", "-", `\`, "-", ":", "-").Replace(trimmed) + "--"

	return filepath.Join(agentDir, "sessions", safe)
}

// SessionFile is the path pi would give a session created at timestamp for
// cwd under agentDir.
func SessionFile(agentDir string, cwd string, sessionID string, timestamp time.Time) string {
	stamp := strings.NewReplacer(":", "-", ".", "-").Replace(timestamp.UTC().Format("2006-01-02T15:04:05.000Z07:00"))

	return filepath.Join(SessionDir(agentDir, cwd), stamp+"_"+sessionID+".jsonl")
}
