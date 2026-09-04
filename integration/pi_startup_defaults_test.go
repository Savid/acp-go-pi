//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

// startupDefaultsHome is one durable agent directory driven by a sequence of
// real pi processes, the way a configured home is driven by a sequence of
// sessions.
type startupDefaultsHome struct {
	executable      string
	root            string
	home            string
	baseEnvironment map[string]string
	launches        int
}

func newStartupDefaultsHome(t *testing.T, executable string) *startupDefaultsHome {
	t.Helper()

	runtime := newIntegrationRuntime(t)
	root := runtime.root
	home := filepath.Join(root, "home")

	// pi refuses to switch to a model whose provider has no credential, so the
	// home carries one. It is never spent: this test only reads state and
	// changes selections.
	require.NoError(t, (pi.AgentDir{
		Root:     home,
		AuthJSON: []byte(`{"openai":{"type":"api_key","key":"integration-not-a-real-key"}}`),
	}).Write())

	return &startupDefaultsHome{executable: executable, root: root, home: home, baseEnvironment: runtime.baseEnvironment}
}

// launch starts one pi process against the home and returns its client.
func (h *startupDefaultsHome) launch(t *testing.T, ctx context.Context) *pi.Client {
	t.Helper()

	h.launches++
	sessionDir := filepath.Join(h.root, "sessions", string(rune('a'+h.launches)))
	require.NoError(t, os.MkdirAll(sessionDir, 0o700))

	process, err := pi.StartOrdinaryProcess(ctx, pi.LaunchSpec{
		ExecutablePath:  h.executable,
		AgentDir:        h.home,
		SessionDir:      sessionDir,
		Cwd:             h.root,
		NativeRoot:      h.root,
		BaseEnvironment: h.baseEnvironment,
	})
	require.NoError(t, err)

	client := pi.NewClient(process.Stdin(), process.Stdout())
	require.NoError(t, client.Start(ctx))

	// A model or thinking-level change emits events; an undrained event channel
	// stalls the client's reader and with it every later response.
	go func() {
		for range client.Events() {
		}
	}()

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = process.Shutdown(shutdownCtx)
		_ = process.Close()
		_ = client.Stop()
	})

	return client
}

// selectStartupDefaults records one model and thinking level in the home's
// startup keys, leaving every other key in the file alone. Since pi 0.84.3 a
// session's own selection stays session-scoped, so a selection reaches the
// shared file only where something puts it there deliberately: pi's explicit
// save, or any pi older than that on every session change. The keys are what
// the next launch reads either way, so the drift this reconciliation exists
// for is staged rather than inferred from one pi version's persistence.
func (h *startupDefaultsHome) selectStartupDefaults(t *testing.T, model pi.Model, level string) {
	t.Helper()

	settings := h.settings(t)
	settings["defaultProvider"] = model.Provider
	settings["defaultModel"] = model.ID

	if level != "" {
		settings["defaultThinkingLevel"] = level
	}

	encoded, err := json.Marshal(settings)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(h.home, pi.SettingsFileName), encoded, 0o600))
}

// settings reads what the next pi process launched against the home would read.
func (h *startupDefaultsHome) settings(t *testing.T) map[string]any {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(h.home, pi.SettingsFileName)) // #nosec G304 -- the path is this test's own temp directory.
	if os.IsNotExist(err) {
		return map[string]any{}
	}

	require.NoError(t, err)

	settings := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &settings))

	return settings
}

// TestPiCLIStartupDefaultsSurviveAnotherSession proves the durable-home rule
// against the real CLI: pi persists a session's model and thinking level into
// the one settings.json every session launches against, so without the
// wrapper's reconciliation the next launch starts on the previous session's
// selection. The reconciliation restores the operator baseline captured before
// any session ran, and the next launch starts on that instead.
func TestPiCLIStartupDefaultsSurviveAnotherSession(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	home := newStartupDefaultsHome(t, path)

	baseline, err := pi.CaptureStartupDefaults(home.home)
	require.NoError(t, err)
	require.Empty(t, home.settings(t), "the operator baseline for this home is pi's own defaults")

	first := home.launch(t, ctx)
	operatorState, err := first.GetState(ctx)
	require.NoError(t, err)
	require.NotNil(t, operatorState.Model, "pi must start on a model for this proof to mean anything")

	operatorModel := operatorState.Model.Provider + "/" + operatorState.Model.ID
	operatorLevel := operatorState.ThinkingLevel

	models, err := first.GetAvailableModels(ctx)
	require.NoError(t, err)

	var elsewhere pi.Model

	for _, model := range models {
		if model.Reasoning && model.Provider+"/"+model.ID != operatorModel {
			elsewhere = model

			break
		}
	}

	require.NotEmpty(t, elsewhere.ID, "the catalog must offer a second reasoning model")

	level := pi.ThinkingLevelHigh
	if operatorLevel == level {
		level = pi.ThinkingLevelLow
	}

	_, err = first.SetModel(ctx, elsewhere.Provider, elsewhere.ID)
	require.NoError(t, err)
	require.NoError(t, first.SetThinkingLevel(ctx, level))
	home.selectStartupDefaults(t, elsewhere, level)

	// The hazard, observed in the file every session shares.
	persisted := home.settings(t)
	require.Equal(t, elsewhere.ID, persisted["defaultModel"])
	require.Equal(t, elsewhere.Provider, persisted["defaultProvider"])
	require.Equal(t, level, persisted["defaultThinkingLevel"])

	inheriting := home.launch(t, ctx)
	inheritedState, err := inheriting.GetState(ctx)
	require.NoError(t, err)
	require.NotNil(t, inheritedState.Model)
	require.Equal(t, elsewhere.Provider+"/"+elsewhere.ID,
		inheritedState.Model.Provider+"/"+inheritedState.Model.ID,
		"a launch reads another session's model out of the shared settings.json")
	require.Equal(t, level, inheritedState.ThinkingLevel,
		"a launch reads another session's thinking level out of the shared settings.json")

	require.NoError(t, baseline.Restore(home.home))
	restored := home.settings(t)
	require.NotContains(t, restored, "defaultModel")
	require.NotContains(t, restored, "defaultProvider")
	require.NotContains(t, restored, "defaultThinkingLevel")

	reconciled := home.launch(t, ctx)
	reconciledState, err := reconciled.GetState(ctx)
	require.NoError(t, err)
	require.NotNil(t, reconciledState.Model)
	require.Equal(t, operatorModel, reconciledState.Model.Provider+"/"+reconciledState.Model.ID,
		"a reconciled launch starts on the operator baseline, not another session's model")
	require.Equal(t, operatorLevel, reconciledState.ThinkingLevel,
		"a reconciled launch starts on the operator baseline, not another session's thinking level")
}

// TestPiCLIStartupDefaultsKeepOperatorConfiguration proves the reconciliation
// restores what the operator configured rather than erasing it, and leaves the
// keys pi keeps in the same file for its own purposes alone.
func TestPiCLIStartupDefaultsKeepOperatorConfiguration(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	home := newStartupDefaultsHome(t, path)

	probe := home.launch(t, ctx)
	models, err := probe.GetAvailableModels(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, models)

	operator := models[0]

	var elsewhere pi.Model

	for _, model := range models {
		if model.ID != operator.ID {
			elsewhere = model

			break
		}
	}

	require.NotEmpty(t, elsewhere.ID, "the catalog must offer a second model")

	settingsPath := filepath.Join(home.home, pi.SettingsFileName)
	operatorSettings := map[string]any{
		"defaultProvider": operator.Provider,
		"defaultModel":    operator.ID,
		"quietStartup":    true,
	}
	encoded, err := json.Marshal(operatorSettings)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(settingsPath, encoded, 0o600))

	baseline, err := pi.CaptureStartupDefaults(home.home)
	require.NoError(t, err)

	session := home.launch(t, ctx)
	_, err = session.SetModel(ctx, elsewhere.Provider, elsewhere.ID)
	require.NoError(t, err)
	home.selectStartupDefaults(t, elsewhere, "")
	require.Equal(t, elsewhere.ID, home.settings(t)["defaultModel"])

	require.NoError(t, baseline.Restore(home.home))
	restored := home.settings(t)
	require.Equal(t, operator.ID, restored["defaultModel"])
	require.Equal(t, operator.Provider, restored["defaultProvider"])
	require.Equal(t, true, restored["quietStartup"], "keys the wrapper does not own stay as pi left them")

	reconciled := home.launch(t, ctx)
	state, err := reconciled.GetState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.Model)
	require.Equal(t, operator.Provider+"/"+operator.ID, state.Model.Provider+"/"+state.Model.ID)
}
