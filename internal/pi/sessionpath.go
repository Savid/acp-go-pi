package pi

import (
	"encoding/json"
	"os"
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

// SplitRows splits a session file into its non-empty rows.
func SplitRows(data []byte) [][]byte {
	lines := strings.Split(string(data), "\n")
	rows := make([][]byte, 0, len(lines))

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		rows = append(rows, []byte(trimmed))
	}

	return rows
}

// ReadRows reads a session file's rows. A missing file reads as no rows.
func ReadRows(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	return SplitRows(data), nil
}

// WriteRows materializes rows as a session file, creating its directory. The
// file is staged beside its final name and renamed into place.
func WriteRows(path string, rows [][]byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	var builder strings.Builder
	for _, row := range rows {
		builder.Write(row)
		builder.WriteByte('\n')
	}

	staging, err := os.CreateTemp(filepath.Dir(path), ".acp-go-pi-*.jsonl")
	if err != nil {
		return err
	}

	_, writeErr := staging.WriteString(builder.String())
	if closeErr := staging.Close(); writeErr == nil {
		writeErr = closeErr
	}

	if writeErr != nil {
		_ = os.Remove(staging.Name())

		return writeErr
	}

	if err := os.Chmod(staging.Name(), 0o600); err != nil {
		_ = os.Remove(staging.Name())

		return err
	}

	return os.Rename(staging.Name(), path)
}
