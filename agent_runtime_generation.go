package piacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	internalpi "github.com/savid/acp-go-pi/internal/pi"
)

var (
	runtimeGenerationEnsureScratchParent = ensureScratchParent
	runtimeGenerationAbs                 = filepath.Abs
	runtimeGenerationMkdirTemp           = os.MkdirTemp
	runtimeGenerationChmod               = os.Chmod
	runtimeGenerationRemoveAll           = os.RemoveAll
	runtimeGenerationRandRead            = rand.Read
)

type runtimeGeneration struct {
	root    string
	release func()
	once    sync.Once
	err     error
}

func (a *Agent) createRuntimeGeneration(
	ctx context.Context,
	kind RuntimeResourceKind,
) (internalpi.ContainmentSpec, *runtimeGeneration, error) {
	release, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, kind)
	if err != nil {
		return internalpi.ContainmentSpec{}, nil, err
	}

	parent, err := runtimeGenerationEnsureScratchParent(a.options.ScratchDir)
	if err != nil {
		release()

		return internalpi.ContainmentSpec{}, nil, err
	}

	parent, err = runtimeGenerationAbs(parent)
	if err != nil {
		release()

		return internalpi.ContainmentSpec{}, nil, fmt.Errorf("resolve scratch parent: %w", err)
	}

	root, err := runtimeGenerationMkdirTemp(parent, "acp-go-pi-runtime-*")
	if err != nil {
		release()

		return internalpi.ContainmentSpec{}, nil, fmt.Errorf("create runtime generation root: %w", err)
	}

	if chmodErr := runtimeGenerationChmod(root, 0o700); chmodErr != nil {
		_ = runtimeGenerationRemoveAll(root)

		release()

		return internalpi.ContainmentSpec{}, nil, fmt.Errorf("set runtime generation root mode: %w", chmodErr)
	}

	containment, err := a.containmentSpecForRoot(parent, root, kind)
	if err != nil {
		_ = runtimeGenerationRemoveAll(root)

		release()

		return internalpi.ContainmentSpec{}, nil, err
	}

	return containment, &runtimeGeneration{root: root, release: release}, nil
}

func (a *Agent) containmentSpecForRoot(
	parent string,
	root string,
	kind RuntimeResourceKind,
) (internalpi.ContainmentSpec, error) {
	identity := make([]byte, 16)
	if _, err := runtimeGenerationRandRead(identity); err != nil {
		return internalpi.ContainmentSpec{}, fmt.Errorf("create runtime identity: %w", err)
	}

	absoluteParent, err := runtimeGenerationAbs(parent)
	if err != nil {
		return internalpi.ContainmentSpec{}, fmt.Errorf("resolve scratch parent: %w", err)
	}

	absoluteRoot, err := runtimeGenerationAbs(root)
	if err != nil {
		return internalpi.ContainmentSpec{}, fmt.Errorf("resolve runtime generation root: %w", err)
	}

	return internalpi.ContainmentSpec{
		DarwinBestEffort: a.ContainmentMode() == RuntimeContainmentBestEffort,
		ScratchParent:    absoluteParent,
		GenerationRoot:   absoluteRoot,
		RuntimeID:        hex.EncodeToString(identity),
		LifecycleKind:    string(kind),
		Isolation:        internalProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential, a.options.testOnlyIdentityLockRoot),
	}, nil
}

func internalProcessIsolation(isolation *ProcessIsolation, testOnlyNoCredential bool, testOnlyIdentityLockRoot string) *internalpi.ProcessIsolation {
	if isolation == nil {
		return nil
	}

	base := make(map[string]string, len(isolation.BaseEnvironment))
	for key, value := range isolation.BaseEnvironment {
		base[key] = value
	}

	result := &internalpi.ProcessIsolation{
		UID:                      isolation.UID,
		GID:                      isolation.GID,
		BaseEnvironment:          base,
		TestOnlyNoCredential:     testOnlyNoCredential,
		TestOnlyIdentityLockRoot: testOnlyIdentityLockRoot,
	}

	return result
}

func (g *runtimeGeneration) finalize(containmentErr error) error {
	if g == nil {
		return containmentErr
	}

	g.once.Do(func() {
		if !internalpi.ProcessContainmentComplete(containmentErr) {
			g.err = containmentErr

			return
		}

		g.err = runtimeGenerationRemoveAll(g.root)
		if g.err == nil && g.release != nil {
			g.release()
		}
	})

	return errors.Join(containmentErr, g.err)
}
