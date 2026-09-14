package piacp

import (
	"context"
	"errors"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	configModel        acp.SessionConfigId = "model"
	configThoughtLevel acp.SessionConfigId = "thought_level"

	configTypeSelect = "select"
)

// configOptions renders the session's select options: the model catalog and
// the thinking-level menu, each with its current value.
func (s *session) configOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	model := s.model
	models := append([]pi.Model(nil), s.models...)
	thinkingLevel := s.thinkingLevel
	s.mu.Unlock()

	options := make([]acp.SessionConfigOption, 0, 2)

	if model != "" {
		values := modelSelectOptions(model, models, s.agent.options.ConfiguredModels)
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
			Id:           configModel,
			Name:         "Model",
			Type:         configTypeSelect,
			Category:     new(acp.SessionConfigOptionCategoryModel),
			CurrentValue: acp.SessionConfigValueId(model),
			Options:      acp.SessionConfigSelectOptions{Ungrouped: &values},
		}})
	}

	if thinkingLevel != "" {
		values := thinkingLevelSelectOptions()
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
			Id:           configThoughtLevel,
			Name:         "Thought Level",
			Type:         configTypeSelect,
			Category:     new(acp.SessionConfigOptionCategoryThoughtLevel),
			CurrentValue: acp.SessionConfigValueId(thinkingLevel),
			Options:      acp.SessionConfigSelectOptions{Ungrouped: &values},
		}})
	}

	return options
}

// modelSelectOptions lists the native catalog, then host-listed ids the
// catalog lacks, then the current model when nothing else names it.
func modelSelectOptions(model string, models []pi.Model, hostListed []string) acp.SessionConfigSelectOptionsUngrouped {
	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(models)+len(hostListed)+1)
	seen := make(map[string]struct{}, len(models)+len(hostListed)+1)

	for index := range models {
		info := &models[index]
		if info.Provider == "" || info.ID == "" {
			continue
		}

		if _, ok := seen[info.Ref()]; ok {
			continue
		}

		name := info.Name
		if name == "" {
			name = info.Ref()
		}

		meta := map[string]any{"modelId": info.Ref()}
		if info.ContextWindow > 0 {
			meta["contextWindow"] = info.ContextWindow
		}

		if info.MaxTokens > 0 {
			meta["maxOutputTokens"] = info.MaxTokens
		}

		values = append(values, acp.SessionConfigSelectOption{
			Name:  name,
			Value: acp.SessionConfigValueId(info.Ref()),
			Meta:  map[string]any{vendor: meta},
		})
		seen[info.Ref()] = struct{}{}
	}

	for _, id := range append(hostListed, model) {
		if _, ok := seen[id]; ok {
			continue
		}

		values = append(values, acp.SessionConfigSelectOption{Name: id, Value: acp.SessionConfigValueId(id)})
		seen[id] = struct{}{}
	}

	return values
}

func thinkingLevelSelectOptions() acp.SessionConfigSelectOptionsUngrouped {
	names := map[string]string{
		pi.ThinkingLevelOff: "Off", pi.ThinkingLevelMinimal: "Minimal", pi.ThinkingLevelLow: "Low",
		pi.ThinkingLevelMedium: "Medium", pi.ThinkingLevelHigh: "High", pi.ThinkingLevelXHigh: "Extra High",
		pi.ThinkingLevelMax: "Max",
	}

	levels := pi.ThinkingLevels()
	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(levels))

	for _, level := range levels {
		values = append(values, acp.SessionConfigSelectOption{Name: names[level], Value: acp.SessionConfigValueId(level)})
	}

	return values
}

// setConfigOption applies one select value while no turn is in flight.
func (s *session) setConfigOption(ctx context.Context, configID acp.SessionConfigId, value string) ([]acp.SessionConfigOption, error) {
	if configID != configModel && configID != configThoughtLevel {
		return nil, wire.Unsupported("configId")
	}

	if err := s.admissionError(); err != nil {
		return nil, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return nil, err
	}
	defer release()

	rt, err := s.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}

	switch configID {
	case configModel:
		ref, parseErr := pi.ParseModelRef(value)
		if parseErr != nil {
			return nil, wire.Unsupported("value")
		}

		selected, setErr := rt.client.SetModel(ctx, ref.Provider, ref.ID)
		if setErr != nil {
			var commandErr *pi.CommandError
			if errors.As(setErr, &commandErr) {
				return nil, wire.Unsupported("value")
			}

			return nil, s.transportFailure(ctx, rt, setErr)
		}

		s.mu.Lock()
		s.model = ref.Provider + "/" + selected.ID
		s.contextWindow = selected.ContextWindow
		s.mu.Unlock()
	case configThoughtLevel:
		if value == "" {
			return nil, wire.Unsupported("value")
		}

		if setErr := rt.client.SetThinkingLevel(ctx, value); setErr != nil {
			return nil, s.transportFailure(ctx, rt, setErr)
		}

		// pi acknowledges a level it does not apply; the level read back is
		// the one the session advertises.
		state, stateErr := rt.client.GetState(ctx)
		if stateErr != nil {
			return nil, s.transportFailure(ctx, rt, stateErr)
		}

		s.mu.Lock()
		s.thinkingLevel = state.ThinkingLevel
		s.mu.Unlock()
	}

	if err := s.commitMirror(ctx); err != nil {
		return nil, wire.InternalFailure(vendor, "")
	}

	options := s.configOptions()

	_ = s.emit(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}})

	return options, nil
}
