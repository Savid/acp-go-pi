package pi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The settings.json keys pi reads once at process start to choose the model
// and thinking level a session begins on. pi writes a choice back into the same
// keys wherever one is saved as the default — its own explicit save, and every
// session change on pi before 0.84.3 — so in a durable home, one agent
// directory every session launches against, they are the channel through which
// one session's runtime choice would otherwise decide the next session's
// startup state.
const (
	settingDefaultProvider      = "defaultProvider"
	settingDefaultModel         = "defaultModel"
	settingDefaultThinkingLevel = "defaultThinkingLevel"
)

// startupDefaultKeys is the set of those keys the wrapper reconciles.
var startupDefaultKeys = [...]string{settingDefaultProvider, settingDefaultModel, settingDefaultThinkingLevel}

// Filesystem and timing seams for fault-injection in tests.
var (
	fsMkdir           = os.Mkdir
	settingsLockSleep = time.Sleep
)

const (
	// settingsLockSuffix is pi's lock path for settings.json. pi guards the
	// file with proper-lockfile, whose mutual exclusion is the atomic creation
	// of this sibling directory, so taking it the same way is what keeps a
	// wrapper write and a concurrent session's native write from interleaving
	// into a lost update.
	settingsLockSuffix = ".lock"
	// settingsLockAttempts and settingsLockDelay bound the wait for pi's lock.
	// pi holds it only across one read-modify-write of a small file.
	settingsLockAttempts = 50
	settingsLockDelay    = 20 * time.Millisecond
	// settingsLockStale matches proper-lockfile's own expiry: a lock directory
	// older than this belonged to a process that died holding it, and pi
	// reclaims it on the same terms.
	settingsLockStale = 10 * time.Second
)

// StartupDefaults is one agent directory's operator baseline for the
// settings.json keys pi consults at process start. It is captured before any
// session of this agent has run against the directory, so it holds what the
// operator configured and nothing a session later chose.
type StartupDefaults struct {
	values map[string]json.RawMessage
}

// CaptureStartupDefaults records the operator baseline held in dir's
// settings.json. A missing or empty file is a valid baseline: it means pi picks
// its own defaults. An unreadable or malformed file fails closed, because pi
// would record a load error, run on its own defaults, and say so nowhere the
// wrapper can see.
func CaptureStartupDefaults(dir string) (StartupDefaults, error) {
	settings, err := readAgentSettings(filepath.Join(dir, SettingsFileName))
	if err != nil {
		return StartupDefaults{}, err
	}

	values := make(map[string]json.RawMessage, len(startupDefaultKeys))

	for _, key := range startupDefaultKeys {
		if value, ok := settings[key]; ok {
			values[key] = value
		}
	}

	return StartupDefaults{values: values}, nil
}

// Restore puts dir's settings.json back to the captured baseline for the
// startup keys, leaving every other key pi keeps there untouched. It is the
// same content for every session, so concurrent launches cannot write
// conflicting startup state, and it writes nothing while the file already
// agrees.
func (d StartupDefaults) Restore(dir string) error {
	path := filepath.Join(dir, SettingsFileName)

	settings, err := readAgentSettings(path)
	if err != nil {
		return err
	}

	if !d.drifted(settings) {
		return nil
	}

	unlock, err := lockAgentSettings(path)
	if err != nil {
		return err
	}

	defer unlock()

	// pi may have rewritten the file between the unlocked drift check and the
	// lock, so the write is built from what the lock actually guards.
	settings, err = readAgentSettings(path)
	if err != nil {
		return err
	}

	for _, key := range startupDefaultKeys {
		if value, ok := d.values[key]; ok {
			settings[key] = value

			continue
		}

		delete(settings, key)
	}

	encoded, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pi settings: %w", err)
	}

	if err := fsWriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write pi settings: %w", err)
	}

	return nil
}

// drifted reports whether settings names a startup default other than the
// captured baseline.
func (d StartupDefaults) drifted(settings map[string]json.RawMessage) bool {
	for _, key := range startupDefaultKeys {
		current, present := settings[key]

		baseline, expected := d.values[key]
		if present != expected || !bytes.Equal(current, baseline) {
			return true
		}
	}

	return false
}

// readAgentSettings decodes an agent directory's settings.json into its raw
// keys. A missing or empty file decodes to no keys, matching how pi reads it.
func readAgentSettings(path string) (map[string]json.RawMessage, error) {
	data, err := fsReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]json.RawMessage{}, nil
		}

		return nil, fmt.Errorf("read pi settings: %w", err)
	}

	if len(data) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	var settings map[string]json.RawMessage
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("decode pi settings: %w", err)
	}

	if settings == nil {
		return map[string]json.RawMessage{}, nil
	}

	return settings, nil
}

// lockAgentSettings takes pi's own settings lock and returns its release.
func lockAgentSettings(path string) (func(), error) {
	lock := path + settingsLockSuffix

	for range settingsLockAttempts {
		err := fsMkdir(lock, 0o700)
		if err == nil {
			return func() { _ = fsRemove(lock) }, nil
		}

		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("lock pi settings: %w", err)
		}

		if info, statErr := fsStat(lock); statErr == nil && time.Since(info.ModTime()) > settingsLockStale {
			_ = fsRemove(lock)

			continue
		}

		settingsLockSleep(settingsLockDelay)
	}

	return nil, fmt.Errorf("pi settings lock at %s is held", lock)
}
