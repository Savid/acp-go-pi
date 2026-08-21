package piacp

import (
	"context"
	"errors"
	"sync"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

func acquireNativeRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.AcquireNativeRoot, kind, "native root")
}

func releaseNativeRootWhenComplete(release func(), err error) {
	if release != nil && internalpi.ProcessContainmentComplete(err) {
		release()
	}
}

func reserveScratchRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.ReserveScratchRoot, kind, "scratch root")
}

func acquireRuntimeResource(ctx context.Context, acquire func(context.Context, RuntimeResourceKind) (func(), error), kind RuntimeResourceKind, resource string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if acquire == nil {
		return func() {}, nil
	}

	hookCtx, cancelHook := context.WithTimeout(ctx, sessionSettleTimeout)
	defer cancelHook()

	var release func()

	err := runBoundedHook(hookCtx, "acquire "+resource, func() error {
		acquired, acquireErr := acquire(hookCtx, kind)

		if hookCtx.Err() != nil {
			if acquired != nil {
				acquired()
			}

			return hookCtx.Err()
		}

		if acquireErr != nil {
			return acquireErr
		}

		if acquired == nil {
			return errors.New(resource + " hook returned nil release")
		}

		release = acquired

		return nil
	})
	if err != nil {
		return nil, err
	}

	var once sync.Once

	return func() { once.Do(release) }, nil
}
