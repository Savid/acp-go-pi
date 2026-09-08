package piacp

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func managedHandoffBudget(agent *Agent) *promptImageBudget {
	budget := newPromptImageBudget(agent.imageLimits(), agent.inputHandoffRoot())
	budget.managedHandoff = agent.managedHandoff

	return budget
}

func TestManagedHandoffRejectsCompleteScratchDomain(t *testing.T) {
	for _, relation := range []string{"equal", "ancestor", "descendant"} {
		t.Run(relation, func(t *testing.T) {
			base := t.TempDir()
			scratch := filepath.Join(base, "scratch")
			tree := filepath.Join(scratch, "generation")
			require.NoError(t, os.MkdirAll(tree, 0o700))
			root := scratch
			switch relation {
			case "ancestor":
				root = base
			case "descendant":
				root = tree
			}
			block, _ := handoffFixtureBlock(t, tree, "valid.png", "image/png")
			agent := NewAgent(WithHostAuthority(&edgeHostAuthority{}), WithScratchDir(scratch), WithInputHandoffRoot(root))
			t.Cleanup(func() { _ = agent.Close() })
			require.NoError(t, agent.prepareNativeTree(t.Context(), tree))
			session := &agentSession{agent: agent, id: "session"}
			request := TextPromptRequest(session.id, "turn", "")
			request.Prompt = []acp.ContentBlock{block}
			_, err := session.Prompt(t.Context(), request)
			requireHandoffError(t, err, imageErrorPathNotAllowed, 0, handoffRootUnresolvedMessage)
		})
	}
}

func TestManagedHandoffSurvivesGenerationReclaim(t *testing.T) {
	authority := newDeterministicHostAuthority()
	t.Cleanup(authority.cleanup)
	root := t.TempDir()
	block, png := handoffFixtureBlock(t, root, "valid.png", "image/png")
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath("pi"), WithScratchDir(t.TempDir()),
		WithInputHandoffRoot(root), WithLogger(slog.New(slog.DiscardHandler)))
	t.Cleanup(func() { _ = agent.Close() })
	agent.setConnection(newDirectAgentClient())
	_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
	require.NoError(t, err)
	opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	pinned := agent.managedHandoff.root
	require.NotNil(t, pinned)
	for _, nonce := range []string{"first", "after-reclaim"} {
		request := TextPromptRequest(opened.SessionId, nonce, "")
		request.Prompt = []acp.ContentBlock{block}
		response, promptErr := agent.Prompt(t.Context(), request)
		require.NoError(t, promptErr)
		require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
		require.Same(t, pinned, agent.managedHandoff.root)
	}

	mapped, err := mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
	require.NoError(t, err)
	require.Equal(t, base64.StdEncoding.EncodeToString(png), mapped.Images[0].Data)
	require.NoError(t, agent.Close())
	_, err = pinned.Stat(".")
	require.Error(t, err, "Agent.Close retained its read descriptor")
	_, err = mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
	requireHandoffError(t, err, imageErrorPathNotAllowed, 0, handoffRootUnresolvedMessage)
}

func TestManagedHandoffDiscoveryFreezesBeforeNativeWork(t *testing.T) {
	for _, action := range []string{"failed prepare", "start without prepare"} {
		t.Run(action, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "not-yet-present")
			failure := errors.New("host refused")
			authority := &edgeHostAuthority{
				prepare: func(context.Context, string) error { return failure },
				start:   func(context.Context, NativeRequest) (NativeProcess, error) { return nil, failure },
			}
			agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()), WithInputHandoffRoot(root))
			t.Cleanup(func() { _ = agent.Close() })
			if action == "failed prepare" {
				require.ErrorIs(t, agent.prepareNativeTree(t.Context(), filepath.Join(agent.scratchParent, "generation")), failure)
			} else {
				_, _, err := agent.startAuthorityPiProcess(t.Context(), pi.LaunchSpec{})
				require.ErrorIs(t, err, failure)
			}
			block, _ := handoffFixtureBlock(t, root, "valid.png", "image/png")
			_, err := mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
			requireHandoffError(t, err, imageErrorPathNotAllowed, 0, handoffRootUnresolvedMessage)
		})
	}
}

func TestManagedHandoffPregatesPrecedeRootDiscovery(t *testing.T) {
	for _, verdict := range []string{imageErrorInvalidHandoff, imageErrorInvalidMediaType, imageErrorTooLarge} {
		t.Run(verdict, func(t *testing.T) {
			root := t.TempDir()
			block, png := handoffFixtureBlock(t, root, "valid.png", "image/png")
			switch verdict {
			case imageErrorInvalidHandoff:
				block.Image.Meta = nil
			case imageErrorInvalidMediaType:
				block.Image.MimeType = "image/jpeg;parameter=1"
			case imageErrorTooLarge:
				envelope := handoffEnvelopeFor(png)
				envelope[handoffFieldSizeBytes] = len(png) + 1
				block.Image.Meta[handoffMetaKey] = envelope
			}
			agent := NewAgent(WithHostAuthority(&edgeHostAuthority{}), WithScratchDir(t.TempDir()), WithInputHandoffRoot(root),
				WithImageLimits(ImageLimits{MaxInputBytesPerImage: int64(len(png))}))
			t.Cleanup(func() { _ = agent.Close() })
			_, err := mapPiPrompt(t.Context(), []acp.ContentBlock{block}, managedHandoffBudget(agent))
			requireImageParamError(t, err, verdict, 0)
			require.False(t, agent.managedHandoff.initialized, "pre-gate performed filesystem discovery")
		})
	}
}
