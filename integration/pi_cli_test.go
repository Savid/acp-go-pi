//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/pi"
	"github.com/stretchr/testify/require"
)

func TestPiCLIVersionProbe(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime := newIntegrationRuntime(t)
	agentDir := filepath.Join(runtime.root, "probe-agent")
	require.NoError(t, (pi.AgentDir{Root: agentDir}).Write())
	policyHome := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(policyHome, "sentinel"), []byte("unchanged"), 0o600))
	runtime.baseEnvironment["HOME"] = policyHome
	environment := (pi.LaunchSpec{AgentDir: agentDir, BaseEnvironment: runtime.baseEnvironment}).Environ()
	version, err := pi.ProbeOrdinaryVersion(ctx, path, environment)
	require.NoError(t, err)
	require.NotEmpty(t, version)
	require.NoError(t, pi.CheckMinimumVersion(version, pi.DefaultMinimumVersion),
		"installed pi %s is older than the supported minimum %s", version, pi.DefaultMinimumVersion)
	require.DirExists(t, agentDir)
	require.NoDirExists(t, filepath.Join(policyHome, ".pi"))
	entries, err := os.ReadDir(policyHome)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "sentinel", entries[0].Name())
}

// TestPiCLIExplicitSeedResources proves the real CLI honors exact seeded
// extension, skill, and prompt-template paths while ambient discovery stays
// disabled. This is the native reachability contract behind slash commands.
func TestPiCLIExplicitSeedResources(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	runtime := newIntegrationRuntime(t)
	root := runtime.root
	agentDir := filepath.Join(root, "agent")
	sessionDir := filepath.Join(root, "sessions")
	require.NoError(t, os.MkdirAll(sessionDir, 0o700))

	seed := pi.AgentDir{Root: agentDir, SeedFiles: map[string]string{
		"extensions/seed-command.ts": `import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
export default function (pi: ExtensionAPI) {
  pi.registerCommand("seed-command", { description: "Seed command", handler: async () => {} });
}`,
		"skills/seed-skill/SKILL.md": `---
name: seed-skill
description: Deterministic seeded skill
---
Use the deterministic seeded skill.
`,
		"prompts/seed-prompt.md": "Run the deterministic seeded prompt.\n",
	}}
	require.NoError(t, seed.Write())
	resources, err := seed.ExplicitResources()
	require.NoError(t, err)
	_, wrapper, err := pi.CreateSessionResidence(t.TempDir(), agentDir, nil)
	require.NoError(t, err)

	process, err := pi.StartOrdinaryProcess(ctx, pi.LaunchSpec{
		ExecutablePath:      path,
		AgentDir:            agentDir,
		SessionDir:          sessionDir,
		ExtensionPaths:      append(resources.Extensions, wrapper.ExtensionPaths...),
		SkillPaths:          resources.Skills,
		PromptTemplatePaths: resources.PromptTemplates,
		Cwd:                 root,
		NativeRoot:          root,
		BaseEnvironment:     runtime.baseEnvironment,
	})
	require.NoError(t, err)
	client := pi.NewClient(process.Stdin(), process.Stdout())
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = process.Shutdown(shutdownCtx)
		_ = process.Close()
		_ = client.Stop()
	})

	commands, err := client.GetCommands(ctx)
	require.NoError(t, err)
	names := make([]string, 0, len(commands))
	for _, command := range commands {
		names = append(names, command.Name)
	}
	slices.Sort(names)
	// The bridge registers the wrapper-owned provider-auth command natively.
	// It is deliberately absent from the ACP available-commands list, so the
	// native listing is the only place its registration is observable.
	expected := append(expectedBuiltinCommandNames(t, path),
		pi.AuthCommandName, "seed-command", "seed-prompt", "skill:seed-skill")
	slices.Sort(expected)
	require.Equal(t, expected, names)
	require.Empty(t, process.StderrTail())
}

// TestPiCLIBridgeExtensionLoads proves the wrapper-owned bridge extension
// loads in the real binary with the wrapper's exact launch posture: silent
// startup, no commands registered, clean stdin-EOF exit, empty stderr.
func TestPiCLIBridgeExtensionLoads(t *testing.T) {
	path := smokePiPath(t)
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := startHarness(t, ctx, path, true)

	state, err := h.client.GetState(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, state.SessionID)
	require.False(t, state.IsStreaming)

	require.NoError(t, h.client.SetAutoRetry(ctx, false))

	start := time.Now()
	require.NoError(t, h.process.CloseStdin())

	select {
	case <-h.process.Exited():
		require.NoError(t, h.process.WaitErr())
		require.Less(t, time.Since(start), 5*time.Second, "stdin EOF must exit pi immediately")
	case <-time.After(10 * time.Second):
		t.Fatalf("pi did not exit on stdin EOF; stderr: %s", h.process.StderrTail())
	}

	require.Empty(t, h.process.StderrTail(), "startup and shutdown must be silent on stderr")
	require.Zero(t, h.client.DecodeFailures())
}
