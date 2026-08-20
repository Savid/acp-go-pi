package piacp

import (
	"context"
	"errors"
	"log/slog"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

// SetSessionMode exists only because github.com/coder/acp-go-sdk's generated
// Agent interface still requires it. Remove this when the upstream SDK drops
// session/set_mode; the local ACP dispatcher intentionally does not route it.
//
// The reserved family literal is refused before the method-not-found verdict.
// This surface is unreachable from the wire but reachable from an embedding Go
// host, and the placement rule is about where the literal may appear rather than
// about which methods this adapter answers: a surface that never carries the
// extension refuses the key rather than ignoring it.
func (a *Agent) SetSessionMode(_ context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if refusal := refuseLifecycleMeta(params.Meta); refusal != nil {
		return acp.SetSessionModeResponse{}, refusal
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

// SetSessionConfigOption handles supported configuration changes.
func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	if refusal := refuseLifecycleMeta(setSessionConfigOptionMeta(params)); refusal != nil {
		return acp.SetSessionConfigOptionResponse{}, refusal
	}

	switch {
	case params.ValueId != nil:
		return a.setSessionConfigValue(ctx, params.ValueId)
	case params.Boolean != nil:
		// Both variants are json:"-", and the union takes this one whenever
		// "value" is absent, so "type" is the only JSON path the caller can
		// correct — including for a request that named no discriminator.
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(jsonFieldType)
	default:
		// No variant at all is unreachable from JSON: the union keys on
		// "value" and takes the boolean form when it is absent, and a "type"
		// it does not recognise fails to decode before reaching here. Only an
		// in-process caller reaches this arm, and it left out "value".
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(acpFieldValue)
	}
}

// setSessionConfigOptionMeta reads whichever variant of the union carries the
// request `_meta`, so the reserved family literal is refused on the mode, model,
// and config surfaces alike.
func setSessionConfigOptionMeta(params acp.SetSessionConfigOptionRequest) map[string]any {
	switch {
	case params.ValueId != nil:
		return params.ValueId.Meta
	case params.Boolean != nil:
		return params.Boolean.Meta
	default:
		return nil
	}
}

func (a *Agent) setSessionConfigValue(
	ctx context.Context,
	params *acp.SetSessionConfigOptionValueId,
) (acp.SetSessionConfigOptionResponse, error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	if poisonErr := session.poisonedError(); poisonErr != nil {
		return acp.SetSessionConfigOptionResponse{}, poisonErr
	}

	switch params.ConfigId {
	case configModel, configThoughtLevel:
	default:
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(acpFieldConfigID)
	}

	releaseTurn, err := session.acquireTurn(ctx)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	defer releaseTurn()

	if poisonErr := session.poisonedError(); poisonErr != nil {
		return acp.SetSessionConfigOptionResponse{}, poisonErr
	}

	switch params.ConfigId {
	case configModel:
		if err := session.applyModelSelection(ctx, string(params.Value)); err != nil {
			return acp.SetSessionConfigOptionResponse{}, err
		}
	case configThoughtLevel:
		if err := session.applyThinkingLevelSelection(ctx, string(params.Value)); err != nil {
			return acp.SetSessionConfigOptionResponse{}, err
		}
	}

	options := sessionConfigOptions(session)
	updates := []acp.SessionUpdate{{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}}}

	if err := session.emitOptionalUpdates(ctx, updates); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

func (s *agentSession) applyModelSelection(ctx context.Context, value string) error {
	ref, err := pi.ParseModelRef(value)
	if err != nil {
		return unsupportedField(acpFieldValue)
	}

	selected, err := s.currentClient().SetModel(ctx, ref.Provider, ref.ID)
	if err != nil {
		var commandErr *pi.CommandError
		if errors.As(err, &commandErr) {
			s.agent.log.ErrorContext(ctx, "pi rejected the selected model", slog.String("message", commandErr.Message))

			return unsupportedField(acpFieldValue)
		}

		return err
	}

	s.mu.Lock()
	s.model = ref.Provider + "/" + selected.ID
	s.contextWindowSize = selected.ContextWindow
	s.mu.Unlock()

	return nil
}

func (s *agentSession) applyThinkingLevelSelection(ctx context.Context, value string) error {
	if value == "" {
		return unsupportedField(acpFieldValue)
	}

	if err := s.currentClient().SetThinkingLevel(ctx, value); err != nil {
		return err
	}

	s.mu.Lock()
	s.thinkingLevel = value
	s.mu.Unlock()

	return nil
}

func sessionConfigOptions(session *agentSession) []acp.SessionConfigOption {
	session.mu.Lock()
	model := session.model
	available := append([]pi.Model(nil), session.availableModels...)
	thinkingLevel := session.thinkingLevel
	session.mu.Unlock()

	options := make([]acp.SessionConfigOption, 0, 2)

	if model != "" {
		if values := modelSelectOptions(model, available); len(values) > 0 {
			options = append(options, acp.SessionConfigOption{
				Select: &acp.SessionConfigOptionSelect{
					Id:           configModel,
					Name:         "Model",
					Category:     configCategory(acp.SessionConfigOptionCategoryModel),
					CurrentValue: acp.SessionConfigValueId(model),
					Options: acp.SessionConfigSelectOptions{
						Ungrouped: &values,
					},
				},
			})
		}
	}

	if thinkingLevel != "" {
		values := thinkingLevelSelectOptions()
		options = append(options, acp.SessionConfigOption{
			Select: &acp.SessionConfigOptionSelect{
				Id:           configThoughtLevel,
				Name:         "Thought Level",
				Category:     configCategory(acp.SessionConfigOptionCategoryThoughtLevel),
				CurrentValue: acp.SessionConfigValueId(thinkingLevel),
				Options: acp.SessionConfigSelectOptions{
					Ungrouped: &values,
				},
			},
		})
	}

	return options
}

func sessionUnstableConfigOptions(session *agentSession) []acp.UnstableSessionConfigOption {
	session.mu.Lock()
	model := session.model
	available := append([]pi.Model(nil), session.availableModels...)
	thinkingLevel := session.thinkingLevel
	session.mu.Unlock()

	options := make([]acp.UnstableSessionConfigOption, 0, 2)

	if model != "" {
		if values := modelSelectOptions(model, available); len(values) > 0 {
			options = append(options, acp.UnstableSessionConfigOption{
				Select: &acp.UnstableSessionConfigOptionSelect{
					Id:           configModel,
					Name:         "Model",
					Type:         configTypeSelect,
					Category:     configCategory(acp.SessionConfigOptionCategoryModel),
					CurrentValue: acp.SessionConfigValueId(model),
					Options: acp.SessionConfigSelectOptions{
						Ungrouped: &values,
					},
				},
			})
		}
	}

	if thinkingLevel != "" {
		values := thinkingLevelSelectOptions()
		options = append(options, acp.UnstableSessionConfigOption{
			Select: &acp.UnstableSessionConfigOptionSelect{
				Id:           configThoughtLevel,
				Name:         "Thought Level",
				Type:         configTypeSelect,
				Category:     configCategory(acp.SessionConfigOptionCategoryThoughtLevel),
				CurrentValue: acp.SessionConfigValueId(thinkingLevel),
				Options: acp.SessionConfigSelectOptions{
					Ungrouped: &values,
				},
			},
		})
	}

	return options
}

func modelSelectOptions(model string, available []pi.Model) acp.SessionConfigSelectOptionsUngrouped {
	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(available)+1)
	seen := make(map[string]struct{}, len(available)+1)

	for index := range available {
		info := &available[index]

		ref := info.Provider + "/" + info.ID
		if info.Provider == "" || info.ID == "" {
			continue
		}

		if _, ok := seen[ref]; ok {
			continue
		}

		values = append(values, acp.SessionConfigSelectOption{
			Name:  modelDisplayName(info),
			Value: acp.SessionConfigValueId(ref),
			Meta:  piModelInfoMeta(info),
		})
		seen[ref] = struct{}{}
	}

	if _, ok := seen[model]; !ok {
		values = append(values, acp.SessionConfigSelectOption{
			Name:  model,
			Value: acp.SessionConfigValueId(model),
		})
	}

	return values
}

func thinkingLevelSelectOptions() acp.SessionConfigSelectOptionsUngrouped {
	levels := pi.ThinkingLevels()
	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(levels))

	for _, level := range levels {
		values = append(values, acp.SessionConfigSelectOption{
			Name:  thinkingLevelDisplayName(level),
			Value: acp.SessionConfigValueId(level),
		})
	}

	return values
}

func thinkingLevelDisplayName(level string) string {
	switch level {
	case pi.ThinkingLevelOff:
		return "Off"
	case pi.ThinkingLevelMinimal:
		return "Minimal"
	case pi.ThinkingLevelLow:
		return "Low"
	case pi.ThinkingLevelMedium:
		return "Medium"
	case pi.ThinkingLevelHigh:
		return "High"
	case pi.ThinkingLevelXHigh:
		return "Extra High"
	case pi.ThinkingLevelMax:
		return "Max"
	default:
		return level
	}
}

func modelDisplayName(info *pi.Model) string {
	if info.Name != "" {
		return info.Name
	}

	return info.Provider + "/" + info.ID
}

// piModelInfoMeta attaches harness-reported model metadata under the model
// select value's _meta.pi; absent metadata is omitted, never zero-filled.
// Model modality data is not published here: the catalog input list feeds
// only the adapter-internal selected-model image gate.
func piModelInfoMeta(info *pi.Model) map[string]any {
	piMeta := make(map[string]any, 3)

	piMeta["modelId"] = info.Provider + "/" + info.ID

	if info.ContextWindow > 0 {
		piMeta["contextWindow"] = info.ContextWindow
	}

	if info.MaxTokens > 0 {
		piMeta["maxOutputTokens"] = info.MaxTokens
	}

	return map[string]any{piMetaKey: piMeta}
}

func configCategory(category acp.SessionConfigOptionCategory) *acp.SessionConfigOptionCategory {
	return &category
}

func selectPositionEncoding(encodings []acp.PositionEncodingKind) acp.PositionEncodingKind {
	for _, encoding := range encodings {
		if encoding == acp.PositionEncodingKindUtf8 {
			return acp.PositionEncodingKindUtf8
		}
	}

	for _, encoding := range encodings {
		if encoding == acp.PositionEncodingKindUtf16 {
			return acp.PositionEncodingKindUtf16
		}
	}

	return acp.PositionEncodingKindUtf16
}
