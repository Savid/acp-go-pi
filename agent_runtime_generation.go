package piacp

import (
	"context"
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
	runtimeGenerationWriteProbeAgentDir  = func(path string) error { return (internalpi.AgentDir{Root: path}).Write() }
)

type runtimeGeneration struct {
	agent    *Agent
	root     string
	prepared bool
	once     sync.Once
	err      error
}

func (g *runtimeGeneration) prepareVersionProbeAgentDir(ctx context.Context) (string, error) {
	if g == nil || !filepath.IsAbs(g.root) || filepath.Clean(g.root) != g.root {
		return "", errors.New("version probe runtime generation root is invalid")
	}

	agentDir := filepath.Join(g.root, "probe-agent")

	if err := runtimeGenerationWriteProbeAgentDir(agentDir); err != nil {
		return "", fmt.Errorf("materialize version probe agent directory: %w", err)
	}

	if err := g.agent.prepareNativeTree(ctx, g.root); err != nil {
		return "", fmt.Errorf("prepare version probe agent directory: %w", err)
	}

	g.prepared = g.agent.options.hostAuthoritySupplied

	return agentDir, nil
}

func (a *Agent) createRuntimeGeneration(ctx context.Context) (*runtimeGeneration, error) {
	if closedErr := a.ensureOpen(); closedErr != nil {
		return nil, closedErr
	}

	parent, err := runtimeGenerationEnsureScratchParent(a.scratchParent)
	if err != nil {
		return nil, err
	}

	parent, err = runtimeGenerationAbs(parent)
	if err != nil {
		return nil, fmt.Errorf("resolve scratch parent: %w", err)
	}

	root, err := runtimeGenerationMkdirTemp(parent, "acp-go-pi-runtime-*")
	if err != nil {
		return nil, fmt.Errorf("create runtime generation root: %w", err)
	}

	if err := runtimeGenerationChmod(root, 0o700); err != nil {
		_ = runtimeGenerationRemoveAll(root)

		return nil, fmt.Errorf("set runtime generation root mode: %w", err)
	}

	return &runtimeGeneration{agent: a, root: root}, nil
}

func (g *runtimeGeneration) finalize(ctx context.Context, processErr error) error {
	if g == nil {
		return processErr
	}

	g.once.Do(func() {
		if !nativeContainmentComplete(processErr) {
			g.err = processErr

			return
		}

		if g.prepared {
			if err := g.agent.reclaimNativeTree(ctx, g.root); err != nil {
				g.err = err

				return
			}

			g.prepared = false
		}

		g.err = runtimeGenerationRemoveAll(g.root)
	})

	return errors.Join(processErr, g.err)
}
