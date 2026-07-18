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
	if acquire == nil {
		return func() {}, nil
	}

	release, err := acquire(ctx, kind)
	if err != nil {
		return nil, err
	}

	if release == nil {
		return nil, errors.New(resource + " hook returned nil release")
	}

	var once sync.Once

	return func() { once.Do(release) }, nil
}
