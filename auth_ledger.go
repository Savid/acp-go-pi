package piacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Ledger record states. The proof a residence answer can carry is a total
// function of these and a native probe, so the set is closed.
const (
	authLedgerIntent    = "intent"
	authLedgerConfirmed = "confirmed"
	authLedgerRemoved   = "removed"
)

// Closed proofSource enum. Presence alone is never enough: without durable
// provenance binding the resident credential to this connection generation the
// honest answer is not_confirmed however plainly the slot is occupied.
const (
	authProofConfirmedPresent = "confirmed_present"
	authProofConfirmedAbsent  = "confirmed_absent"
	authProofNotConfirmed     = "not_confirmed"
)

const (
	authLedgerVendorDir = "pi"
	authLedgerLeafDir   = "ledger"
	// pi creates its own agent directory 0777 & ~umask and its auth.json 0600.
	// The ledger is stricter than the harness on both regardless.
	authLedgerFileMode = 0o600
	authLedgerDirMode  = 0o700
)

// authLedgerRecord is the whole content a ledger entry may carry. It never
// holds credential material, authorization URLs, user codes, prompt answers, or
// native text.
type authLedgerRecord struct {
	ProviderID         string `json:"providerId"`
	ConnectionID       string `json:"connectionId"`
	Revision           int64  `json:"revision"`
	BindingGeneration  int64  `json:"bindingGeneration"`
	FlowID             string `json:"flowId"`
	AuthorizeRequestID string `json:"authorizeRequestId"`
	State              string `json:"state"`
	CreatedAt          int64  `json:"createdAt"`
	UpdatedAt          int64  `json:"updatedAt"`
}

var (
	ledgerMkdirAll = os.MkdirAll
	ledgerChmod    = os.Chmod
	ledgerStat     = os.Stat
	ledgerRename   = os.Rename
	ledgerOpen     = os.Open
	ledgerReadFile = os.ReadFile
	ledgerReadDir  = os.ReadDir
	ledgerRemove   = os.Remove
	ledgerMarshal  = json.Marshal
)

// ledgerFile is the file surface an atomic ledger write drives.
type ledgerFile interface {
	Name() string
	Write([]byte) (int, error)
	Chmod(os.FileMode) error
	Sync() error
	Close() error
}

var ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
	return os.CreateTemp(dir, pattern)
}

// authLedger is the durable values-free record of which native slot each
// connection generation owns. It outlives every session and every native
// generation, so its path is deterministic by design: a bookkeeping record that
// could not be found again after the crash that makes it matter answers
// nothing.
type authLedger struct {
	// mu makes a read and the write it decided one operation. Without it a
	// compare-and-set is two independent syscalls and the entry can move
	// between them.
	mu  sync.Mutex
	dir string
}

// authLedgerRootConfigured reports whether the host supplied a durable ledger
// root at all, which is what separates a surface nobody asked for from one that
// was asked for and could not be prepared.
func authLedgerRootConfigured(options Options) bool {
	return options.ProviderAuthRoot != ""
}

// validateProviderAuthRoot rejects a relative provider-auth root at agent
// construction. An empty root is valid and leaves the surface unadvertised.
func validateProviderAuthRoot(options Options) error {
	root := options.ProviderAuthRoot
	if root == "" || filepath.IsAbs(root) {
		return nil
	}

	return errors.New("provider auth root must be an absolute path")
}

// newAuthLedger resolves and validates the configured durable root.
func newAuthLedger(options Options) (*authLedger, error) {
	if err := validateProviderAuthRoot(options); err != nil {
		return nil, err
	}

	root := options.ProviderAuthRoot

	// The operator-configured root is the directory the host consents to, so it
	// is the one this narrows: a pre-existing root is chmodded rather than left
	// as the host found it.
	if err := ledgerMkdirAll(root, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("create provider auth root: %w", err)
	}

	if err := ledgerChmod(root, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("restrict provider auth root: %w", err)
	}

	dir := filepath.Join(root, authLedgerVendorDir, authLedgerHomeKey(options.Home), authLedgerLeafDir)
	if err := ledgerMkdirAll(dir, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("create provider auth ledger root: %w", err)
	}

	if err := ledgerChmod(dir, authLedgerDirMode); err != nil {
		return nil, fmt.Errorf("restrict provider auth ledger root: %w", err)
	}

	info, err := ledgerStat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect provider auth ledger root: %w", err)
	}

	if !info.IsDir() {
		return nil, errors.New("provider auth ledger root is not a directory")
	}

	probe, err := ledgerCreateTemp(dir, "writable-")
	if err != nil {
		return nil, fmt.Errorf("verify provider auth ledger root is writable: %w", err)
	}

	name := probe.Name()

	return &authLedger{dir: dir}, errors.Join(probe.Close(), ledgerRemove(name))
}

// authLedgerHomeKey scopes the ledger to the agent directory it describes. Two
// agents pointed at different homes describe different slots, so their records
// must not alias.
func authLedgerHomeKey(home string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(home)))

	return hex.EncodeToString(sum[:])[:16]
}

func (l *authLedger) path(providerID string) string {
	sum := sha256.Sum256([]byte(providerID))

	return filepath.Join(l.dir, hex.EncodeToString(sum[:])[:32]+".json")
}

func (l *authLedger) read(providerID string) (authLedgerRecord, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.readEntry(providerID)
}

func (l *authLedger) readEntry(providerID string) (authLedgerRecord, bool, error) {
	contents, err := ledgerReadFile(l.path(providerID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return authLedgerRecord{}, false, nil
		}

		return authLedgerRecord{}, false, fmt.Errorf("read provider auth ledger entry: %w", err)
	}

	var record authLedgerRecord
	if err := json.Unmarshal(contents, &record); err != nil {
		return authLedgerRecord{}, false, fmt.Errorf("decode provider auth ledger entry: %w", err)
	}

	return record, true, nil
}

// write persists a record atomically and durably: a temporary file in the same
// directory, fsynced, renamed over its target, with the directory fsynced
// after. Persisted here means fsynced, never merely written.
func (l *authLedger) write(record authLedgerRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.writeEntry(record)
}

// writeIfCurrent commits a record only while the stored entry still names the
// lineage this record was minted against, and reports whether it did. A leg
// whose native answer outlived its own flow arrives after a supersede has
// minted the provider's next revision or a disconnect has bumped its
// generation, and an unconditional rename would put the closed flow's binding
// back over the one that replaced it — leaving the host holding a generation
// the entry no longer names and a credential no disconnect can ever fence.
func (l *authLedger) writeIfCurrent(record authLedgerRecord) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	current, ok, err := l.readEntry(record.ProviderID)
	if err != nil {
		return false, err
	}

	if ok && (current.ConnectionID != record.ConnectionID ||
		current.Revision != record.Revision ||
		current.BindingGeneration != record.BindingGeneration) {
		return false, nil
	}

	if err := l.writeEntry(record); err != nil {
		return false, err
	}

	return true, nil
}

func (l *authLedger) writeEntry(record authLedgerRecord) error {
	contents, err := ledgerMarshal(record)
	if err != nil {
		return fmt.Errorf("encode provider auth ledger entry: %w", err)
	}

	file, err := ledgerCreateTemp(l.dir, "entry-")
	if err != nil {
		return fmt.Errorf("create provider auth ledger entry: %w", err)
	}

	temp := file.Name()

	if err := writeLedgerFile(file, contents); err != nil {
		return errors.Join(fmt.Errorf("write provider auth ledger entry: %w", err), ledgerRemove(temp))
	}

	if err := ledgerRename(temp, l.path(record.ProviderID)); err != nil {
		return errors.Join(fmt.Errorf("commit provider auth ledger entry: %w", err), ledgerRemove(temp))
	}

	return l.syncDir()
}

func writeLedgerFile(file ledgerFile, contents []byte) error {
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Chmod(authLedgerFileMode); err != nil {
		return errors.Join(err, file.Close())
	}

	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}

	return file.Close()
}

func (l *authLedger) syncDir() error {
	dir, err := ledgerOpen(l.dir)
	if err != nil {
		return fmt.Errorf("open provider auth ledger root: %w", err)
	}

	return errors.Join(dir.Sync(), dir.Close())
}

func (l *authLedger) list() ([]authLedgerRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entries, err := ledgerReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("list provider auth ledger: %w", err)
	}

	records := make([]authLedgerRecord, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		contents, err := ledgerReadFile(filepath.Join(l.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read provider auth ledger entry: %w", err)
		}

		var record authLedgerRecord
		if err := json.Unmarshal(contents, &record); err != nil {
			return nil, fmt.Errorf("decode provider auth ledger entry: %w", err)
		}

		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool { return records[i].ProviderID < records[j].ProviderID })

	return records, nil
}

type authInventoryEntry struct {
	ProviderID        string `json:"providerId"`
	ConnectionID      string `json:"connectionId"`
	Revision          int64  `json:"revision"`
	BindingGeneration int64  `json:"bindingGeneration"`
	ProofSource       string `json:"proofSource"`
}

type authInventoryResult struct {
	Entries []authInventoryEntry `json:"entries"`
}

// inventory reads the ledger and probes the named native slot. The ledger alone
// is never sufficient — an adapter's record of its own intent cannot prove
// residence — and a probe alone proves only that something is resident, not
// that it is the thing this connection installed.
func (p *providerAuth) inventory(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	records, err := p.ledger.list()
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, "", "", "")
	}

	live := make([]authLedgerRecord, 0, len(records))
	providerIDs := make([]string, 0, len(records))

	for _, record := range records {
		if record.State == authLedgerRemoved {
			continue
		}

		live = append(live, record)
		providerIDs = append(providerIDs, record.ProviderID)
	}

	resident, err := p.probeSlots(ctx, session, providerIDs)
	if err != nil {
		return nil, err
	}

	entries := make([]authInventoryEntry, 0, len(live))

	for _, record := range live {
		present := authSlotResident(resident, record.ProviderID)

		entries = append(entries, authInventoryEntry{
			ProviderID:        record.ProviderID,
			ConnectionID:      record.ConnectionID,
			Revision:          record.Revision,
			BindingGeneration: record.BindingGeneration,
			ProofSource:       authProofSource(record.State, present),
		})
	}

	return authInventoryResult{Entries: entries}, nil
}

// probeSlots reads the native credential store for the named providers. An
// empty request performs no native call: there is nothing whose residence the
// answer would depend on.
func (p *providerAuth) probeSlots(ctx context.Context, session *agentSession, providerIDs []string) (map[string]string, error) {
	if len(providerIDs) == 0 {
		return map[string]string{}, nil
	}

	message, err := p.exchange(ctx, session, authBridgeRequest{
		Op:          authOpProbe,
		ProviderIDs: providerIDs,
	})
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, "", "", "")
	}

	return message.Entries, nil
}

// authSlotResident reports whether the probe answered that the provider holds a
// credential. The bridge answers every provider the request named, so residence
// is the entry's value and never the presence of its key — reading the key
// alone would report every provider in the request as resident.
func authSlotResident(entries map[string]string, providerID string) bool {
	return entries[providerID] != ""
}

// authProofSource is the total function of ledger state and native probe. A
// sibling reports exactly the cell the two select and never chooses a value.
func authProofSource(state string, present bool) string {
	if state != authLedgerConfirmed {
		return authProofNotConfirmed
	}

	if present {
		return authProofConfirmedPresent
	}

	return authProofConfirmedAbsent
}
