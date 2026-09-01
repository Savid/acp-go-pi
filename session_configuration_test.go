package piacp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestPiOptionsMetaPreservesExplicitEmptyConfiguration(t *testing.T) {
	meta := PiOptions{
		Env:           map[string]string{},
		ExtraPathDirs: []string{},
	}.Meta()

	options, presence, err := piOptionsFromMetaWithConfigurationPresence(meta)
	require.NoError(t, err)
	require.True(t, presence.Env)
	require.True(t, presence.ExtraPathDirs)
	require.NotNil(t, options.Env)
	require.NotNil(t, options.ExtraPathDirs)
}

func TestResolveSessionConfigurationDistinguishesOmittedAndExplicitEmpty(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "stored"},
		ExtraPathDirs: []string{"/stored/first", "/stored/second"},
	}

	omitted, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, stored)
	require.NoError(t, err)
	require.Equal(t, stored.Env, omitted.Env)
	require.Equal(t, stored.ExtraPathDirs, omitted.ExtraPathDirs)

	explicit, err := resolveSessionConfiguration(
		PiOptions{Env: map[string]string{}, ExtraPathDirs: []string{}},
		sessionConfigurationPresence{Env: true, ExtraPathDirs: true},
		stored,
	)
	require.NoError(t, err)
	require.Empty(t, explicit.Env)
	require.Empty(t, explicit.ExtraPathDirs)
	require.NotNil(t, explicit.Env)
	require.NotNil(t, explicit.ExtraPathDirs)
}

func TestRecoveredEmptyConfigurationKeepsLifecycleFingerprint(t *testing.T) {
	start := sessionStart{Cwd: "/cwd", ResumeID: "session"}
	recovered := start
	var err error
	recovered.MetaOptions, err = resolveSessionConfiguration(
		PiOptions{},
		sessionConfigurationPresence{},
		sessionConfiguration(PiOptions{}),
	)
	require.NoError(t, err)
	require.Equal(t, sessionStartFingerprint(start), sessionStartFingerprint(recovered))
}

func TestLifecycleBoundaryPersistsSessionConfiguration(t *testing.T) {
	store := NewInMemorySessionStore()
	session := &agentSession{
		agent: NewAgent(WithSessionStore(store)),
		id:    "session",
		configuration: sessionConfigurationRecord{
			Env:           map[string]string{"TOKEN": "durable"},
			ExtraPathDirs: []string{"/first", "/second"},
		},
	}

	require.NoError(t, session.commitLifecycleBoundary(t.Context(), lifecycleBoundaryRecord{
		StreamID:    "stream",
		NativeState: nativeStateCommitted,
	}))

	entries, err := store.Load(t.Context(), SessionKey{
		SessionID: "session",
		Subpath:   SessionStoreLifecycleSubpath,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0], &raw))
	require.Contains(t, raw, "configuration")

	record, err := decodeLifecycleBoundaryRecord(entries[0])
	require.NoError(t, err)
	require.Equal(t, session.configuration, record.Configuration)
}

func TestStoredSessionConfigurationRequiresBothFields(t *testing.T) {
	tests := map[string]json.RawMessage{
		"missing configuration": json.RawMessage(`{"version":1,"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
		"missing env":           json.RawMessage(`{"version":1,"configuration":{"extraPathDirs":[]},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
		"missing path":          json.RawMessage(`{"version":1,"configuration":{"env":{}},"streamId":"stream","nativeRows":1,"nativeState":"committed","recordedAt":1}`),
	}

	for name, boundary := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: "session"}, []SessionStoreEntry{
				json.RawMessage(`{"type":"session"}`),
			}))
			require.NoError(t, store.Append(t.Context(), SessionKey{
				SessionID: "session",
				Subpath:   SessionStoreLifecycleSubpath,
			}, []SessionStoreEntry{boundary}))

			agent := NewAgent(WithSessionStore(store))
			_, _, err := agent.loadCurrentStoreEntries(t.Context(), "session")
			requireSessionResumeIncompatible(t, err, "configuration")
		})
	}
}

func TestStoredSessionConfigurationRejectsUnsafeValues(t *testing.T) {
	store := NewInMemorySessionStore()
	appendStoredSessionWithConfiguration(t, store, "session", sessionConfigurationRecord{
		Env:           map[string]string{"PATH": "/untrusted"},
		ExtraPathDirs: []string{},
	})

	agent := NewAgent(WithSessionStore(store))
	_, _, err := agent.loadCurrentStoreEntries(t.Context(), "session")
	requireSessionResumeIncompatible(t, err, metaOptionPath(metaEnvKey))
}

func TestColdRestoreReconstructsOmittedConfigurationAndHonorsExplicitEmpty(t *testing.T) {
	stored := sessionConfigurationRecord{
		Env:           map[string]string{"TOKEN": "stored"},
		ExtraPathDirs: []string{"/stored/first", "/stored/second"},
	}

	tests := map[string]struct {
		meta      map[string]any
		wantEnv   bool
		wantPaths []string
	}{
		"omitted": {
			wantEnv:   true,
			wantPaths: stored.ExtraPathDirs,
		},
		"explicit empty": {
			meta:      PiOptions{Env: map[string]string{}, ExtraPathDirs: []string{}}.Meta(),
			wantPaths: []string{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewInMemorySessionStore()
			appendStoredSessionWithConfiguration(t, store, validSessionUUID, stored)

			client := newStubPiClient()
			client.state = pi.SessionState{SessionID: validSessionUUID}
			agent := newStubClientAgent(t, client, WithSessionStore(store))
			restored, err := agent.restoreSession(t.Context(), validSessionUUID, sessionStart{
				Cwd:      t.TempDir(),
				ResumeID: validSessionUUID,
			}, test.meta)
			require.NoError(t, err)
			require.True(t, restored.started)
			restored.finish()

			require.Equal(t, test.wantPaths, restored.session.launch.ExtraPathDirs)
			value, found := restored.session.launch.Env["TOKEN"]
			require.Equal(t, test.wantEnv, found)
			if found {
				require.Equal(t, "stored", value)
			}
			require.NoError(t, restored.session.Close(t.Context()))
		})
	}
}

func appendStoredSessionWithConfiguration(
	t *testing.T,
	store SessionStore,
	sessionID string,
	configuration sessionConfigurationRecord,
) {
	t.Helper()

	require.NoError(t, store.Append(t.Context(), SessionKey{SessionID: sessionID}, []SessionStoreEntry{
		json.RawMessage(`{"type":"session","id":"` + sessionID + `"}`),
	}))

	boundary, err := json.Marshal(lifecycleBoundaryRecord{
		Version:             lifecycleBoundaryVersion,
		Configuration:       configuration,
		StreamID:            "stream",
		NativeRows:          1,
		NativeState:         nativeStateCommitted,
		RecordedAtUnixMilli: 1,
	})
	require.NoError(t, err)
	require.NoError(t, store.Append(t.Context(), SessionKey{
		SessionID: sessionID,
		Subpath:   SessionStoreLifecycleSubpath,
	}, []SessionStoreEntry{boundary}))
}

func requireSessionResumeIncompatible(t *testing.T, err error, field string) {
	t.Helper()

	requireInvalidParams(t, err)
	require.ErrorContains(t, err, "session resume incompatible")
	require.ErrorContains(t, err, field)
}
