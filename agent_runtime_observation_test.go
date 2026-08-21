package piacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/savid/acp-go-pi/internal/observer"
	"github.com/stretchr/testify/require"
)

func TestRuntimeObservationHooksComposeExactLifetimes(t *testing.T) {
	var releases int
	var processDelta int64
	var snapshot int
	var stage RuntimeStartupStage
	var containment RuntimeContainmentMode
	hooks := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { releases++ }, nil
		},
		ObserveProcess: func(_ context.Context, _ RuntimeProcessKind, delta int64) {
			processDelta += delta
		},
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshot = count
		},
		ObserveStartupStage: func(_ context.Context, _ RuntimeResourceKind, got RuntimeStartupStage, _ time.Duration, _ error) {
			stage = got
		},
		ObserveContainment: func(_ context.Context, got RuntimeContainmentMode) {
			containment = got
		},
	}, observer.New(observer.Config{}), RuntimeContainmentAuthoritative)

	release, err := hooks.AcquireNativeRoot(t.Context(), RuntimeResourceSession)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)

	observeRuntimeProcess(t.Context(), hooks, RuntimeProcessProviderDescendant, 2)
	observeRuntimeProcessSnapshot(t.Context(), hooks, RuntimeProcessProviderDescendant, 3)
	require.NoError(t, observeRuntimeStartupStage(
		t.Context(), hooks, RuntimeResourceRuntime, RuntimeStartupReadiness, time.Now(), nil,
	))
	observeRuntimeContainment(t.Context(), hooks, RuntimeContainmentAuthoritative)
	require.Equal(t, int64(2), processDelta)
	require.Equal(t, 3, snapshot)
	require.Equal(t, RuntimeStartupReadiness, stage)
	require.Equal(t, RuntimeContainmentAuthoritative, containment)

	wantErr := errors.New("full")
	rejected := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, wantErr
		},
	}, observer.New(observer.Config{}), RuntimeContainmentAuthoritative)
	_, err = rejected.ReserveScratchRoot(t.Context(), RuntimeResourcePrompt)
	require.ErrorIs(t, err, wantErr)
}

func TestBestEffortRuntimeObservationPublishesNoProviderSnapshots(t *testing.T) {
	called := 0
	hooks := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { called++ },
	}, observer.New(observer.Config{}), RuntimeContainmentBestEffort)

	observeRuntimeProcessSnapshot(t.Context(), hooks, RuntimeProcessProviderDescendant, 0)
	observeRuntimeProcessSnapshot(t.Context(), hooks, RuntimeProcessProviderDescendant, 9)
	require.Zero(t, called)
}

func TestBoundedRuntimeObservationCancellationEdges(t *testing.T) {
	require.NoError(t, observeRuntimeStartupStage(
		t.Context(), RuntimeResourceHooks{}, RuntimeResourceRuntime, RuntimeStartupReadiness, time.Now(), nil,
	))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, runBoundedHook(cancelled, "cancelled", func() error { return nil }), context.Canceled)

	post := &postStepCancelledContext{Context: t.Context(), never: make(chan struct{})}
	require.ErrorIs(t, runBoundedHook(post, "late", func() error {
		post.cancelled.Store(true)

		return nil
	}), context.Canceled)

	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runBoundedHook(ctx, "blocked", func() error {
			close(entered)
			<-release

			return nil
		})
	}()
	<-entered
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)
}
