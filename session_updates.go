package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

var (
	errAgentClosed              = errors.New("ACP agent is closed")
	errACPConnectionNotAttached = errors.New("ACP connection is not attached")
)

const liveSessionTitleMaxRunes = 256

func (s *agentSession) emitUpdates(ctx context.Context, updates []acp.SessionUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	s.agent.observe.ObserveFirstPromptUpdate(ctx)

	s.agent.mu.Lock()
	if s.agent.closed {
		s.agent.mu.Unlock()

		return errAgentClosed
	}

	conn := s.agent.conn
	s.agent.mu.Unlock()

	if conn == nil {
		return errACPConnectionNotAttached
	}

	for _, update := range updates {
		if err := conn.SessionUpdate(ctx, acp.SessionNotification{
			SessionId: s.id,
			Update:    update,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *agentSession) emitOptionalUpdates(ctx context.Context, updates []acp.SessionUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	err := s.emitUpdates(ctx, updates)
	if errors.Is(err, errAgentClosed) || errors.Is(err, errACPConnectionNotAttached) {
		return nil
	}

	return err
}

func (s *agentSession) emitAvailableCommandsUpdate(ctx context.Context, force bool) error {
	current := availableCommandsFromNative(s.commands())

	s.mu.Lock()
	previous := cloneAvailableCommands(s.advertisedCommands)
	s.mu.Unlock()

	var emit []acp.SessionUpdate

	switch {
	case len(current) > 0:
		if !force && availableCommandsEqual(previous, current) {
			return nil
		}

		emit = []acp.SessionUpdate{{
			AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: cloneAvailableCommands(current)},
		}}
	case len(previous) > 0:
		emit = emptyAvailableCommandsUpdate()
	default:
		return nil
	}

	if err := s.emitOptionalUpdates(ctx, emit); err != nil {
		return err
	}

	s.mu.Lock()
	s.advertisedCommands = cloneAvailableCommands(current)
	s.mu.Unlock()

	return nil
}

func (s *agentSession) emitClearAvailableCommandsUpdate(ctx context.Context) error {
	s.mu.Lock()
	previous := len(s.advertisedCommands)
	s.mu.Unlock()

	if previous == 0 {
		return nil
	}

	if err := s.emitOptionalUpdates(ctx, emptyAvailableCommandsUpdate()); err != nil {
		return err
	}

	s.mu.Lock()
	s.advertisedCommands = nil
	s.mu.Unlock()

	return nil
}

func emptyAvailableCommandsUpdate() []acp.SessionUpdate {
	return []acp.SessionUpdate{{
		AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: []acp.AvailableCommand{}},
	}}
}

func (s *agentSession) commands() []pi.SlashCommand {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]pi.SlashCommand(nil), s.availableCommands...)
}

// availableCommandsFromNative converts pi's command catalog into ACP
// available commands, dropping names the shared sanitizer rejects.
func availableCommandsFromNative(commands []pi.SlashCommand) []acp.AvailableCommand {
	available := make([]acp.AvailableCommand, 0, len(commands))

	for _, command := range commands {
		if !validSlashCommandName(command.Name) {
			continue
		}

		available = append(available, acp.AvailableCommand{
			Name:        command.Name,
			Description: command.Description,
		})
	}

	return available
}

// validSlashCommandName is the shared advertisement/routing sanitizer:
// reject empty names, names containing '/', invalid UTF-8, and any Unicode
// whitespace, control, or format rune.
func validSlashCommandName(name string) bool {
	if name == "" || strings.Contains(name, "/") || !utf8.ValidString(name) {
		return false
	}

	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return false
		}
	}

	return true
}

func cloneAvailableCommands(commands []acp.AvailableCommand) []acp.AvailableCommand {
	if len(commands) == 0 {
		return nil
	}

	cloned := make([]acp.AvailableCommand, len(commands))
	for index, command := range commands {
		cloned[index] = command
		if command.Input != nil {
			input := *command.Input
			if input.Unstructured != nil {
				unstructured := *input.Unstructured
				input.Unstructured = &unstructured
			}

			cloned[index].Input = &input
		}
	}

	return cloned
}

func availableCommandsEqual(left []acp.AvailableCommand, right []acp.AvailableCommand) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index].Name != right[index].Name || left[index].Description != right[index].Description {
			return false
		}
	}

	return true
}

func (s *agentSession) poisonedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poisonCause == "" {
		return nil
	}

	return poisonedSessionError(s.poisonCause)
}

func (s *agentSession) poison(ctx context.Context, cause string) error {
	s.mu.Lock()
	if s.poisonCause != "" {
		cause = s.poisonCause
		s.mu.Unlock()

		return poisonedSessionError(cause)
	}

	s.poisonCause = cause
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if s.agent == nil {
		return poisonedSessionError(cause)
	}

	s.agent.log.ErrorContext(ctx, "poison pi session after native invariant violation",
		slog.String(acpFieldSessionID, string(s.id)),
		slog.String("cause", cause),
	)

	if err := s.emitClearAvailableCommandsUpdate(ctx); err != nil {
		s.agent.log.ErrorContext(ctx, "clear available pi commands after poison failed",
			slog.String(acpFieldSessionID, string(s.id)),
			slog.String(jsonFieldError, err.Error()),
		)
	}

	return poisonedSessionError(cause)
}

func poisonedSessionError(cause string) error {
	return acp.NewInternalError(map[string]any{
		jsonFieldError:   "session poisoned",
		jsonFieldMessage: cause,
	})
}

func (s *agentSession) emitLiveSessionInfoUpdate(ctx context.Context, prompt []acp.ContentBlock) error {
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	update := acp.SessionSessionInfoUpdate{UpdatedAt: &updatedAt}
	title := liveSessionTitleFromPrompt(prompt)

	s.mu.Lock()

	s.updatedAt = updatedAt
	if s.title == "" && title != "" {
		s.title = title
		update.Title = &title
	}
	s.mu.Unlock()

	return s.emitOptionalUpdates(ctx, []acp.SessionUpdate{{SessionInfoUpdate: &update}})
}

func (s *agentSession) sessionInfo(id acp.SessionId) acp.SessionInfo {
	s.mu.Lock()
	title := s.title
	updatedAt := s.updatedAt
	s.mu.Unlock()

	if title == "" {
		title = string(id)
	}

	info := acp.SessionInfo{
		SessionId:             id,
		Title:                 &title,
		Cwd:                   s.cwd,
		AdditionalDirectories: append([]string(nil), s.additionalDirectories...),
	}
	if updatedAt != "" {
		info.UpdatedAt = &updatedAt
	}

	return info
}

func liveSessionTitleFromPrompt(prompt []acp.ContentBlock) string {
	for _, block := range prompt {
		if block.Text == nil {
			continue
		}

		if title := normalizeLiveSessionTitle(block.Text.Text); title != "" {
			return title
		}
	}

	return ""
}

func normalizeLiveSessionTitle(text string) string {
	title := strings.Join(strings.Fields(text), " ")
	if title == "" {
		return ""
	}

	if utf8.RuneCountInString(title) <= liveSessionTitleMaxRunes {
		return title
	}

	runes := []rune(title)

	return strings.TrimSpace(string(runes[:liveSessionTitleMaxRunes-3])) + "..."
}

// emitRawPiEvent emits one live raw-event notification when raw events are
// enabled. It never returns an error and never aborts the caller's turn: raw
// events are non-authoritative debug output, so oversized/unserializable
// events become markers and emit failures are recorded on the internal
// observer hook.
func (s *agentSession) emitRawPiEvent(ctx context.Context, raw []byte) {
	if !s.rawMessages.Enabled() || len(raw) == 0 {
		return
	}

	s.agent.mu.Lock()
	if s.agent.closed || s.agent.conn == nil {
		s.agent.mu.Unlock()

		return
	}

	conn := s.agent.conn
	s.agent.mu.Unlock()

	s.mu.Lock()
	s.rawEventSequence++
	sequence := s.rawEventSequence
	s.mu.Unlock()

	payload := map[string]any{
		acpFieldSessionID:     s.id,
		rawEventFieldSequence: sequence,
		rawEventFieldSource:   rawEventSourceValue,
		rawEventFieldEvent:    json.RawMessage(raw),
	}

	if marker, replaced := rawEventMarker(payload); replaced {
		payload[rawEventFieldEvent] = marker
	}

	if err := conn.NotifyExtension(ctx, RawEventMethod, payload); err != nil {
		s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
	}
}
