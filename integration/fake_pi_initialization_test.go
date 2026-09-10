//go:build integration

package integration

import "encoding/json"

func (s *fakePiServer) announceInitialization() {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]json.RawMessage, 0, 1+len(s.session.entries))
	rows = append(rows, mustJSON(fakeSessionHeader{
		Type: "session", Version: 3, ID: s.session.id,
		Timestamp: s.session.timestamp, Cwd: s.cwd, ParentSession: s.session.parentSession,
	}))
	for _, entry := range s.session.entries {
		rows = append(rows, entry.row)
	}
	s.out.writeJSON(map[string]any{
		"type": "extension_ui_request", "id": "initialization", "method": "notify",
		"message": "acp-go-pi:session-initialization:" + string(mustJSON(rows)), "notifyType": "info",
	})
}
