package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func testLedger(t *testing.T) *authLedger {
	t.Helper()

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), Home: t.TempDir()})
	require.NoError(t, err)

	return ledger
}

func sampleLedgerRecord(providerID string) authLedgerRecord {
	return authLedgerRecord{
		ProviderID:         providerID,
		ConnectionID:       "conn-1",
		Revision:           1,
		BindingGeneration:  1,
		FlowID:             "flow-1",
		AuthorizeRequestID: "req-1",
		State:              authLedgerIntent,
		CreatedAt:          1,
		UpdatedAt:          2,
	}
}

// TestAuthLedgerContentIsClosed pins that the ledger holds provenance only: no
// credential material, no URL, no user code, no prompt answer.
func TestAuthLedgerContentIsClosed(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(sampleLedgerRecord("anthropic"))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.Equal(t, []string{
		"authorizeRequestId", "bindingGeneration", "connectionId", "createdAt",
		"flowId", "providerId", "revision", "state", "updatedAt",
	}, sortedKeys(decoded))
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	return keys
}

func TestAuthLedgerRoundTrip(t *testing.T) {
	t.Parallel()

	ledger := testLedger(t)
	record := sampleLedgerRecord("anthropic")

	_, ok, err := ledger.read("anthropic")
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, ledger.write(record))

	stored, ok, err := ledger.read("anthropic")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, record, stored)

	records, err := ledger.list()
	require.NoError(t, err)
	require.Equal(t, []authLedgerRecord{record}, records)
}

// TestAuthLedgerRestrictsModes pins that the ledger is 0700/0600 whatever the
// harness's own more permissive agent-directory mode is.
func TestAuthLedgerRestrictsModes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	ledger, err := newAuthLedger(Options{ProviderAuthRoot: root, Home: absTestPath("srv", "pi-home")})
	require.NoError(t, err)
	require.NoError(t, ledger.write(sampleLedgerRecord("anthropic")))

	requireRestrictedMode(t, ledger.dir, authLedgerDirMode)
	requireRestrictedMode(t, ledger.path("anthropic"), authLedgerFileMode)
}

// TestAuthLedgerHomeKeyScopesRecords pins that two agents pointed at different
// agent directories describe different slots and never alias.
func TestAuthLedgerHomeKeyScopesRecords(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	first, err := newAuthLedger(Options{ProviderAuthRoot: root, Home: absTestPath("srv", "a")})
	require.NoError(t, err)

	second, err := newAuthLedger(Options{ProviderAuthRoot: root, Home: absTestPath("srv", "b")})
	require.NoError(t, err)
	require.NotEqual(t, first.dir, second.dir)

	same, err := newAuthLedger(Options{ProviderAuthRoot: root, Home: absTestPath("srv", "a") + string(filepath.Separator)})
	require.NoError(t, err)
	require.Equal(t, first.dir, same.dir)

	require.NoError(t, first.write(sampleLedgerRecord("anthropic")))

	_, ok, err := second.read("anthropic")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAuthLedgerRootConfigured(t *testing.T) {
	t.Parallel()

	require.False(t, authLedgerRootConfigured(Options{}))
	require.True(t, authLedgerRootConfigured(Options{ProviderAuthRoot: absTestPath("srv", "auth")}))
}

func TestNewAuthLedgerRejectsUnusableRoots(t *testing.T) {
	t.Parallel()

	_, err := newAuthLedger(Options{ProviderAuthRoot: "relative"})
	require.Error(t, err)

	file := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err = newAuthLedger(Options{ProviderAuthRoot: file})
	require.Error(t, err)
}

// TestNewAuthLedgerNarrowsTheConfiguredRoot pins that the operator-configured
// root itself is created 0700, and that a root the host left wider is chmodded
// rather than accepted as found.
func TestNewAuthLedgerNarrowsTheConfiguredRoot(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	missing := filepath.Join(parent, "missing")
	_, err := newAuthLedger(Options{ProviderAuthRoot: missing, Home: absTestPath("srv", "pi")})
	require.NoError(t, err)

	requireRestrictedMode(t, missing, authLedgerDirMode)

	existing := filepath.Join(parent, "existing")
	require.NoError(t, os.Mkdir(existing, 0o755))

	_, err = newAuthLedger(Options{ProviderAuthRoot: existing, Home: absTestPath("srv", "pi")})
	require.NoError(t, err)

	requireRestrictedMode(t, existing, authLedgerDirMode)
}

func TestValidateProviderAuthRoot(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateProviderAuthRoot(Options{}))
	require.NoError(t, validateProviderAuthRoot(Options{ProviderAuthRoot: t.TempDir()}))
	require.ErrorContains(t, validateProviderAuthRoot(Options{ProviderAuthRoot: "relative/auth"}), "absolute path")
}

// TestWithProviderAuthRootRejectsRelativePath pins that a relative root is a
// construction-time verdict, not merely an unadvertised surface.
func TestWithProviderAuthRootRejectsRelativePath(t *testing.T) {
	t.Parallel()

	_, err := NewAgent(WithProviderAuthRoot("relative/auth")).Initialize(t.Context(), defaultInitializeRequest())

	requireUnsupportedOption(t, err, optionFieldProviderAuthRoot)
}

func TestNewAuthLedgerReportsFilesystemFailures(t *testing.T) {
	root := t.TempDir()
	options := Options{ProviderAuthRoot: root, Home: absTestPath("srv", "pi")}

	restore := func(name string, apply func()) {
		t.Helper()
		apply()
	}

	original := struct {
		mkdirAll   func(string, os.FileMode) error
		chmod      func(string, os.FileMode) error
		stat       func(string) (os.FileInfo, error)
		createTemp func(string, string) (ledgerFile, error)
	}{ledgerMkdirAll, ledgerChmod, ledgerStat, ledgerCreateTemp}

	t.Cleanup(func() {
		ledgerMkdirAll = original.mkdirAll
		ledgerChmod = original.chmod
		ledgerStat = original.stat
		ledgerCreateTemp = original.createTemp
	})

	// The root and the leaf are prepared separately, so each step fails on its
	// own path rather than on whichever comes first.
	restore("root mkdir", func() {
		ledgerMkdirAll = func(path string, mode os.FileMode) error {
			if path == root {
				return errors.New("mkdir")
			}

			return original.mkdirAll(path, mode)
		}
	})

	_, err := newAuthLedger(options)
	require.Error(t, err)

	restore("leaf mkdir", func() {
		ledgerMkdirAll = func(path string, mode os.FileMode) error {
			if path != root {
				return errors.New("mkdir")
			}

			return original.mkdirAll(path, mode)
		}
	})

	_, err = newAuthLedger(options)
	require.Error(t, err)

	ledgerMkdirAll = original.mkdirAll
	restore("root chmod", func() {
		ledgerChmod = func(path string, mode os.FileMode) error {
			if path == root {
				return errors.New("chmod")
			}

			return original.chmod(path, mode)
		}
	})

	_, err = newAuthLedger(options)
	require.Error(t, err)

	restore("leaf chmod", func() {
		ledgerChmod = func(path string, mode os.FileMode) error {
			if path != root {
				return errors.New("chmod")
			}

			return original.chmod(path, mode)
		}
	})

	_, err = newAuthLedger(options)
	require.Error(t, err)

	ledgerChmod = original.chmod
	restore("stat", func() {
		ledgerStat = func(string) (os.FileInfo, error) { return nil, errors.New("stat") }
	})
	_, err = newAuthLedger(options)
	require.Error(t, err)

	ledgerStat = original.stat
	restore("createTemp", func() {
		ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, errors.New("temp") }
	})
	_, err = newAuthLedger(options)
	require.Error(t, err)
}

// failingLedgerFile injects a fault at one step of the atomic write sequence.
type failingLedgerFile struct {
	ledgerFile

	writeErr error
	chmodErr error
	syncErr  error
}

func (f *failingLedgerFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}

	return f.ledgerFile.Write(data)
}

func (f *failingLedgerFile) Chmod(mode os.FileMode) error {
	if f.chmodErr != nil {
		return f.chmodErr
	}

	return f.ledgerFile.Chmod(mode)
}

func (f *failingLedgerFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}

	return f.ledgerFile.Sync()
}

func TestAuthLedgerWriteReportsFailures(t *testing.T) {
	ledger := testLedger(t)
	record := sampleLedgerRecord("anthropic")

	originalMarshal := ledgerMarshal
	originalTemp := ledgerCreateTemp
	originalRename := ledgerRename

	t.Cleanup(func() {
		ledgerMarshal = originalMarshal
		ledgerCreateTemp = originalTemp
		ledgerRename = originalRename
	})

	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal") }
	require.Error(t, ledger.write(record))
	ledgerMarshal = originalMarshal

	ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, errors.New("temp") }
	require.Error(t, ledger.write(record))
	ledgerCreateTemp = originalTemp

	for _, fault := range []*failingLedgerFile{
		{writeErr: errors.New("write")},
		{chmodErr: errors.New("chmod")},
		{syncErr: errors.New("sync")},
	} {
		ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, err
			}

			fault.ledgerFile = file

			return fault, nil
		}

		require.Error(t, ledger.write(record))
	}

	ledgerCreateTemp = originalTemp

	ledgerRename = func(string, string) error { return errors.New("rename") }
	require.Error(t, ledger.write(record))
	ledgerRename = originalRename
}

func TestAuthLedgerReadAndListReportFailures(t *testing.T) {
	ledger := testLedger(t)
	require.NoError(t, ledger.write(sampleLedgerRecord("anthropic")))

	originalRead := ledgerReadFile
	originalDir := ledgerReadDir

	t.Cleanup(func() {
		ledgerReadFile = originalRead
		ledgerReadDir = originalDir
	})

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read") }
	_, _, err := ledger.read("anthropic")
	require.Error(t, err)

	_, err = ledger.list()
	require.Error(t, err)

	ledgerReadFile = func(string) ([]byte, error) { return []byte("not json"), nil }
	_, _, err = ledger.read("anthropic")
	require.Error(t, err)

	_, err = ledger.list()
	require.Error(t, err)

	ledgerReadFile = originalRead

	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }
	_, err = ledger.list()
	require.Error(t, err)
}

// TestAuthLedgerListSkipsForeignEntries pins that only the ledger's own records
// are read back.
func TestAuthLedgerListSkipsForeignEntries(t *testing.T) {
	t.Parallel()

	ledger := testLedger(t)
	require.NoError(t, ledger.write(sampleLedgerRecord("anthropic")))
	require.NoError(t, os.WriteFile(filepath.Join(ledger.dir, "notes.txt"), []byte("x"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(ledger.dir, "nested.json"), 0o700))

	records, err := ledger.list()
	require.NoError(t, err)
	require.Len(t, records, 1)
}

func TestAuthLedgerListSortsByProvider(t *testing.T) {
	t.Parallel()

	ledger := testLedger(t)
	require.NoError(t, ledger.write(sampleLedgerRecord("zeta")))
	require.NoError(t, ledger.write(sampleLedgerRecord("alpha")))

	records, err := ledger.list()
	require.NoError(t, err)
	require.Equal(t, "alpha", records[0].ProviderID)
	require.Equal(t, "zeta", records[1].ProviderID)
}

// TestAuthProofSourceMatrix pins the total function: presence alone is never
// enough without a durable confirmation.
func TestAuthProofSourceMatrix(t *testing.T) {
	t.Parallel()

	require.Equal(t, authProofConfirmedPresent, authProofSource(authLedgerConfirmed, true))
	require.Equal(t, authProofConfirmedAbsent, authProofSource(authLedgerConfirmed, false))
	require.Equal(t, authProofNotConfirmed, authProofSource(authLedgerIntent, true))
	require.Equal(t, authProofNotConfirmed, authProofSource(authLedgerIntent, false))
	require.Equal(t, authProofNotConfirmed, authProofSource(authLedgerRemoved, true))
}

// TestInventoryOmitsRemovedEntries pins that a disconnected slot leaves no
// residence claim behind.
func TestInventoryOmitsRemovedEntries(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	require.NoError(t, harness.broker.ledger.write(authLedgerRecord{
		ProviderID: "anthropic",
		State:      authLedgerRemoved,
	}))

	harness.scriptBridge(func(_ context.Context, _ pi.AuthRequest) error {
		require.FailNow(t, "no live record means no native probe")

		return nil
	})

	result, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	require.NoError(t, err)
	require.Empty(t, inventoryResult(t, result).Entries)
}

func TestInventoryRejectsBadParams(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{"extra": 1})
	requireInvalidParams(t, err)

	_, err = harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: ""})
	requireInvalidParams(t, err)

	_, err = harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: "missing"})
	require.Error(t, err)
}

func TestInventoryReportsLedgerFailure(t *testing.T) {
	harness := newAuthHarness(t)

	original := ledgerReadDir
	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir") }

	t.Cleanup(func() { ledgerReadDir = original })

	_, err := harness.call(t.Context(), AuthInventoryMethod, map[string]any{authFieldSessionID: string(harness.session.id)})
	requireAuthFailed(t, err, authCauseHarvestFailed)
}

// TestNewAuthLedgerRejectsNonDirectory pins that a root that is not a directory
// leaves the surface unadvertised.
func TestNewAuthLedgerRejectsNonDirectory(t *testing.T) {
	original := ledgerStat
	ledgerStat = func(string) (os.FileInfo, error) {
		info, err := os.Stat(os.DevNull)

		return info, err
	}

	t.Cleanup(func() { ledgerStat = original })

	_, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir(), Home: absTestPath("srv", "pi")})
	require.ErrorContains(t, err, "not a directory")
}

// TestAuthLedgerWriteIfCurrentGuardsTheStoredLineage pins the compare half of
// the compare-and-set: a record whose lineage no longer matches the stored
// entry is refused rather than renamed over it, while a first entry and a
// repeat of the same lineage both commit.
func TestAuthLedgerWriteIfCurrentGuardsTheStoredLineage(t *testing.T) {
	t.Parallel()

	ledger := testLedger(t)

	minted := authLedgerRecord{ProviderID: "openai", ConnectionID: "conn-1", Revision: 1, BindingGeneration: 1, State: authLedgerIntent}

	written, err := ledger.writeIfCurrent(minted)
	require.NoError(t, err)
	require.True(t, written)

	confirmed := minted
	confirmed.State = authLedgerConfirmed

	written, err = ledger.writeIfCurrent(confirmed)
	require.NoError(t, err)
	require.True(t, written)

	for _, stale := range []authLedgerRecord{
		{ProviderID: "openai", ConnectionID: "conn-2", Revision: 1, BindingGeneration: 1, State: authLedgerConfirmed},
		{ProviderID: "openai", ConnectionID: "conn-1", Revision: 0, BindingGeneration: 1, State: authLedgerConfirmed},
		{ProviderID: "openai", ConnectionID: "conn-1", Revision: 1, BindingGeneration: 0, State: authLedgerConfirmed},
	} {
		written, err = ledger.writeIfCurrent(stale)
		require.NoError(t, err)
		require.False(t, written)
	}

	record, ok, err := ledger.read("openai")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, confirmed, record)
}

func TestAuthLedgerWriteIfCurrentFailures(t *testing.T) {
	ledger := testLedger(t)

	t.Cleanup(func() {
		ledgerReadFile = os.ReadFile
		ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) { return os.CreateTemp(dir, pattern) }
	})

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("io") }

	written, err := ledger.writeIfCurrent(authLedgerRecord{ProviderID: "openai"})
	require.Error(t, err)
	require.False(t, written)

	ledgerReadFile = os.ReadFile
	ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, errors.New("create") }

	written, err = ledger.writeIfCurrent(authLedgerRecord{ProviderID: "openai"})
	require.Error(t, err)
	require.False(t, written)
}
