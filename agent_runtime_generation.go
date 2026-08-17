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
	runtimeGenerationWriteProbeAgentDir  = func(path string) error {
		return (internalpi.AgentDir{Root: path}).Write()
	}
	runtimeGenerationHandoffNativeTree = handoffGeneratedNativeTree
)

type runtimeGeneration struct {
	root    string
	release func()
	once    sync.Once
	err     error
}

func (g *runtimeGeneration) prepareVersionProbeAgentDir(isolation *ProcessIsolation) (string, error) {
	if g == nil || !filepath.IsAbs(g.root) || filepath.Clean(g.root) != g.root {
		return "", errors.New("version probe runtime generation root is invalid")
	}

	agentDir := filepath.Join(g.root, "probe-agent")
	if err := runtimeGenerationWriteProbeAgentDir(agentDir); err != nil {
		return "", fmt.Errorf("materialize version probe agent directory: %w", err)
	}

	if err := runtimeGenerationHandoffNativeTree(g.root, isolation); err != nil {
		return "", fmt.Errorf("handoff version probe agent directory: %w", err)
	}

	return agentDir, nil
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
		DarwinBestEffort:    a.ContainmentMode() == RuntimeContainmentBestEffort,
		ScratchParent:       absoluteParent,
		GenerationRoot:      absoluteRoot,
		RuntimeID:           hex.EncodeToString(identity),
		LifecycleKind:       string(kind),
		Isolation:           internalProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential, a.options.testOnlyIdentityLockRoot),
		OrdinaryEnvironment: cloneStringMap(a.ordinaryEnvironment),
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
		IdentityLock:             isolation.IdentityLock,
		AuthorityDomain:          isolation.AuthorityDomain,
		StandaloneOwnerID:        isolation.StandaloneOwnerID,
		StandaloneStateRoot:      isolation.StandaloneStateRoot,
	}
	if testOnlyNoCredential {
		result.IdentityLock = nil
		result.AuthorityDomain = nil
		result.StandaloneOwnerID = ""
		result.StandaloneStateRoot = ""
	}

	return result
}

func (a *Agent) nativeOwnershipIsolation() *ProcessIsolation {
	if a == nil || a.options.testOnlyNoCredential {
		return nil
	}

	return a.options.ProcessIsolation
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
