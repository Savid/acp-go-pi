package pi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func restoreStartupDefaultsSeams(t *testing.T) {
	t.Helper()

	restoreAgentDirSeams(t)

	mkdir, sleep := fsMkdir, settingsLockSleep

	t.Cleanup(func() { fsMkdir, settingsLockSleep = mkdir, sleep })
}

func writeSettings(t *testing.T, dir string, contents string) string {
	t.Helper()

	path := filepath.Join(dir, SettingsFileName)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	return path
}

func readSettings(t *testing.T, dir string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, SettingsFileName)) // #nosec G304 -- test temp dir.
	require.NoError(t, err)

	return string(data)
}

func TestCaptureStartupDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents *string
		captured map[string]json.RawMessage
		wantErr  string
	}{
		{
			name:     "absent file is pi's own defaults",
			captured: map[string]json.RawMessage{},
		},
		{
			name:     "empty file is pi's own defaults",
			contents: new(""),
			captured: map[string]json.RawMessage{},
		},
		{
			name:     "null document is pi's own defaults",
			contents: new("null"),
			captured: map[string]json.RawMessage{},
		},
		{
			name:     "only the startup keys are captured",
			contents: new(`{"defaultProvider":"openai","defaultModel":"gpt-5","theme":"dark"}`),
			captured: map[string]json.RawMessage{
				"defaultProvider": json.RawMessage(`"openai"`),
				"defaultModel":    json.RawMessage(`"gpt-5"`),
			},
		},
		{
			name:     "malformed settings fail closed",
			contents: new(`{"defaultModel":`),
			wantErr:  "decode pi settings",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			if test.contents != nil {
				writeSettings(t, dir, *test.contents)
			}

			captured, err := CaptureStartupDefaults(dir)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.captured, captured.values)
		})
	}
}

// A launch must start on what the operator configured, so the keys pi persists
// for a session go back to the baseline and every other key stays as pi left
// it.
func TestStartupDefaultsRestoreReconcilesOnlyStartupKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		baseline string
		current  string
		want     string
	}{
		{
			name:     "a session's selection is removed when the operator configured none",
			baseline: `{"theme":"dark"}`,
			current:  `{"theme":"dark","defaultProvider":"openai","defaultModel":"gpt-5","defaultThinkingLevel":"high"}`,
			want:     "{\n  \"theme\": \"dark\"\n}",
		},
		{
			name:     "a session's selection is replaced by the operator's",
			baseline: `{"defaultProvider":"anthropic","defaultModel":"claude"}`,
			current:  `{"defaultProvider":"openai","defaultModel":"gpt-5","quietStartup":true}`,
			want:     "{\n  \"defaultModel\": \"claude\",\n  \"defaultProvider\": \"anthropic\",\n  \"quietStartup\": true\n}",
		},
		{
			name:     "the operator's baseline is rewritten when pi's file is gone",
			baseline: `{"defaultModel":"claude"}`,
			want:     "{\n  \"defaultModel\": \"claude\"\n}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeSettings(t, dir, test.baseline)

			baseline, err := CaptureStartupDefaults(dir)
			require.NoError(t, err)

			require.NoError(t, os.Remove(filepath.Join(dir, SettingsFileName)))

			if test.current != "" {
				writeSettings(t, dir, test.current)
			}

			require.NoError(t, baseline.Restore(dir))
			require.Equal(t, test.want, readSettings(t, dir))
			require.NoDirExists(t, filepath.Join(dir, SettingsFileName+settingsLockSuffix))
		})
	}
}

// Reconciling an unchanged file must not touch it: the steady state of a home
// nobody reconfigured is no writes and no lock traffic at all.
func TestStartupDefaultsRestoreWritesNothingWithoutDrift(t *testing.T) {
	restoreStartupDefaultsSeams(t)

	dir := t.TempDir()
	writeSettings(t, dir, `{"defaultModel":"claude","theme":"dark"}`)

	baseline, err := CaptureStartupDefaults(dir)
	require.NoError(t, err)

	fsWriteFile = func(string, []byte, os.FileMode) error {
		t.Fatal("reconciling an unchanged file wrote to it")

		return nil
	}
	fsMkdir = func(string, os.FileMode) error {
		t.Fatal("reconciling an unchanged file took pi's lock")

		return nil
	}

	require.NoError(t, baseline.Restore(dir))

	// An absent file also agrees with an empty baseline.
	empty := t.TempDir()
	emptyBaseline, err := CaptureStartupDefaults(empty)
	require.NoError(t, err)
	require.NoError(t, emptyBaseline.Restore(empty))
}

// pi guards settings.json with a lock directory it creates atomically. The
// wrapper waits for that lock, reclaims one whose holder died, and refuses to
// write behind a live one.
func TestStartupDefaultsRestoreTakesPiSettingsLock(t *testing.T) {
	restoreStartupDefaultsSeams(t)

	dir := t.TempDir()
	writeSettings(t, dir, `{"defaultModel":"claude"}`)

	baseline, err := CaptureStartupDefaults(dir)
	require.NoError(t, err)

	lock := filepath.Join(dir, SettingsFileName+settingsLockSuffix)
	writeSettings(t, dir, `{"defaultModel":"gpt-5"}`)
	require.NoError(t, os.Mkdir(lock, 0o700))

	waits := 0
	settingsLockSleep = func(time.Duration) {
		waits++

		if waits == 2 {
			require.NoError(t, os.Remove(lock))
		}
	}

	require.NoError(t, baseline.Restore(dir))
	require.Equal(t, 2, waits, "the wrapper must wait for pi rather than write behind it")
	require.Equal(t, "{\n  \"defaultModel\": \"claude\"\n}", readSettings(t, dir))
	require.NoDirExists(t, lock)
}

func TestStartupDefaultsRestoreReclaimsAnExpiredLock(t *testing.T) {
	restoreStartupDefaultsSeams(t)

	dir := t.TempDir()
	writeSettings(t, dir, `{"defaultModel":"claude"}`)

	baseline, err := CaptureStartupDefaults(dir)
	require.NoError(t, err)

	writeSettings(t, dir, `{"defaultModel":"gpt-5"}`)

	lock := filepath.Join(dir, SettingsFileName+settingsLockSuffix)
	require.NoError(t, os.Mkdir(lock, 0o700))

	expired := time.Now().Add(-2 * settingsLockStale)
	require.NoError(t, os.Chtimes(lock, expired, expired))

	settingsLockSleep = func(time.Duration) { t.Fatal("an expired lock must be reclaimed, not waited on") }

	require.NoError(t, baseline.Restore(dir))
	require.Equal(t, "{\n  \"defaultModel\": \"claude\"\n}", readSettings(t, dir))
}

func TestStartupDefaultsRestoreRefusesAHeldLock(t *testing.T) {
	restoreStartupDefaultsSeams(t)

	dir := t.TempDir()
	writeSettings(t, dir, `{"defaultModel":"claude"}`)

	baseline, err := CaptureStartupDefaults(dir)
	require.NoError(t, err)

	writeSettings(t, dir, `{"defaultModel":"gpt-5"}`)
	require.NoError(t, os.Mkdir(filepath.Join(dir, SettingsFileName+settingsLockSuffix), 0o700))

	settingsLockSleep = func(time.Duration) {}

	require.ErrorContains(t, baseline.Restore(dir), "is held")
	require.Equal(t, `{"defaultModel":"gpt-5"}`, readSettings(t, dir))
}

func TestStartupDefaultsRestoreFailures(t *testing.T) {
	dir := t.TempDir()
	writeSettings(t, dir, `{"defaultModel":"claude"}`)

	baseline, err := CaptureStartupDefaults(dir)
	require.NoError(t, err)

	writeSettings(t, dir, `{"defaultModel":"gpt-5"}`)

	tests := []struct {
		name    string
		seams   func(t *testing.T)
		want    string
		invalid bool
	}{
		{
			name: "the drift check cannot read the file",
			seams: func(*testing.T) {
				fsReadFile = func(string) ([]byte, error) { return nil, fmt.Errorf("io error") }
			},
			want: "read pi settings",
		},
		{
			name: "the file changes to something unreadable under the lock",
			seams: func(*testing.T) {
				reads := 0
				underlying := fsReadFile
				fsReadFile = func(path string) ([]byte, error) {
					reads++
					if reads > 1 {
						return nil, fmt.Errorf("io error")
					}

					return underlying(path)
				}
			},
			want: "read pi settings",
		},
		{
			name: "pi's lock cannot be created",
			seams: func(*testing.T) {
				fsMkdir = func(string, os.FileMode) error { return fmt.Errorf("read-only file system") }
			},
			want: "lock pi settings",
		},
		{
			name: "the reconciled file cannot be written",
			seams: func(*testing.T) {
				fsWriteFile = func(string, []byte, os.FileMode) error { return fmt.Errorf("disk full") }
			},
			want: "write pi settings",
		},
		{
			name:    "a baseline value that is not JSON cannot be encoded",
			seams:   func(*testing.T) {},
			want:    "encode pi settings",
			invalid: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreStartupDefaultsSeams(t)
			test.seams(t)

			defaults := baseline
			if test.invalid {
				defaults = StartupDefaults{values: map[string]json.RawMessage{"defaultModel": json.RawMessage("not json")}}
			}

			require.ErrorContains(t, defaults.Restore(dir), test.want)
		})
	}
}
