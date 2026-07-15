package piacp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

func TestAgentDirectSurfaceAndVersionChecks(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{MethodId: "native"})
	requireInvalidParams(t, err)
	_, err = agent.Logout(t.Context(), acp.LogoutRequest{})
	require.NoError(t, err)
	_, err = agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})
	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32601, requestError.Code)

	agent.lookPath = func(string) (string, error) { return "/fake/pi", nil }
	agent.probeVersion = func(context.Context, string) (string, error) { return pi.DefaultMinimumVersion, nil }
	require.NoError(t, agent.ensureVersion(t.Context()))
	require.NoError(t, agent.ensureVersion(t.Context()))

	missing := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	missing.lookPath = func(string) (string, error) { return "", errors.New("missing") }
	require.Error(t, missing.ensureVersion(t.Context()))
	probe := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	probe.probeVersion = func(context.Context, string) (string, error) { return "", errors.New("probe") }
	require.Error(t, probe.ensureVersion(t.Context()))
	old := NewAgent(WithExecutablePath("/fake/pi"), WithLogger(slog.New(slog.DiscardHandler)))
	old.probeVersion = func(context.Context, string) (string, error) { return "0.1.0", nil }
	require.Error(t, old.ensureVersion(t.Context()))

	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), "_pi/unknown", nil)
	require.ErrorIs(t, err, errAgentClosed)
	require.NoError(t, agent.Close())
}

func TestStartRealPiProcessRejectsEmptySpec(t *testing.T) {
	_, _, err := startRealPiProcess(t.Context(), pi.LaunchSpec{})
	require.Error(t, err)
}

func TestServeContextAndConnectionBranches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, Serve(ctx, strings.NewReader(""), io.Discard), context.Canceled)
	require.NoError(t, Serve(context.Background(), strings.NewReader(""), io.Discard))

	previous := newServeAgent
	t.Cleanup(func() { newServeAgent = previous })

	started := make(chan struct{})
	newServeAgent = func(opts ...Option) *Agent {
		defer close(started)

		agent := NewAgent(append(opts, WithLogger(slog.New(slog.DiscardHandler)))...)
		process := newFailingCloseProcess()
		process.close = ErrProcessTreeUnproven
		agent.sessions["serve"] = &agentSession{
			agent: agent,
			id:    "serve",
			proc:  process,
			turn:  make(chan struct{}, sessionTurnCapacity),
		}

		return agent
	}

	ctx, cancel = context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		require.NoError(t, input.Close())
		require.NoError(t, inputWriter.Close())
	})
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, input, io.Discard) }()
	<-started
	cancel()
	require.ErrorIs(t, <-errCh, ErrProcessTreeUnproven)
}

func TestAgentCloseJoinsSessionCloseError(t *testing.T) {
	agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)))
	agent.sessions["id"] = &agentSession{
		agent: agent,
		id:    "id",
		proc:  newFailingCloseProcess(),
		turn:  make(chan struct{}, sessionTurnCapacity),
	}
	require.Error(t, agent.Close())
}
