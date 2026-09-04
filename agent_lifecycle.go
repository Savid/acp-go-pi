package piacp

import (
	"encoding/json"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

// lifecycleMetaKey is the family-reserved literal the session lifecycle
// extension rides under on every surface that carries it.
const lifecycleMetaKey = lifecycle.MetaKey

func (a *Agent) provenLifecycleFacts() lifecycle.Negotiated {
	proven := lifecycle.Negotiated{
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}

	if a.options.hostAuthoritySupplied {
		proven.AuthoritativeQuiescence = true
		proven.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return proven
}

// negotiateLifecycle reads the host's offer and records the answer for the
// connection. An absent offer leaves the key omitted from the response and the
// extension dormant for every session on that connection.
func (a *Agent) negotiateLifecycle(meta map[string]any) (map[string]any, error) {
	present, refusal := lifecycle.DecodeCapability(meta)
	if refusal != nil {
		return nil, unsupportedField(refusal.Field)
	}

	answer := lifecycle.Negotiated{}
	if present {
		answer = a.provenLifecycleFacts()
		answer.Version = lifecycle.Version
	}

	a.mu.Lock()
	a.lifecycle = answer
	a.mu.Unlock()

	if !present {
		return map[string]any{}, nil
	}

	return map[string]any{lifecycleMetaKey: answer.Advertisement()}, nil
}

// lifecycleNegotiated reports the answer this connection gave.
func (a *Agent) lifecycleNegotiated() lifecycle.Negotiated {
	if a == nil {
		return lifecycle.Negotiated{}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

// refuseLifecycleMeta rejects the family literal on a surface that never
// carries it. A family literal is never a foreign namespace and never a no-op,
// so the refusal names the exact member path.
func refuseLifecycleMeta(meta map[string]any) error {
	if _, present := meta[lifecycleMetaKey]; !present {
		return nil
	}

	return unsupportedField(lifecycle.MetaPath)
}

// refuseLifecycleRawMeta applies the same rule to an extension call whose
// parameters are still undecoded, so the key is refused before the method is
// resolved and before any leg's own validation or refusal runs. The refusal
// does not depend on which method carried the key: a family literal is never a
// foreign namespace, and a method this adapter does not define is not licence
// to ignore one.
func refuseLifecycleRawMeta(params json.RawMessage) error {
	var envelope struct {
		Meta map[string]json.RawMessage `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	if err := json.Unmarshal(params, &envelope); err != nil {
		return nil //nolint:nilerr // The route's decoder reports malformed JSON.
	}

	if _, present := envelope.Meta[lifecycleMetaKey]; !present {
		return nil
	}

	return unsupportedField(lifecycle.MetaPath)
}

// lifecyclePromptCorrelation reads the submission identity a prompt carries.
// Route validation runs first, so a prompt never reports two rejections and the
// order of two failures is never implementation-defined.
func (a *Agent) lifecyclePromptCorrelation(meta map[string]any) (lifecycle.Submission, error) {
	submission, refusal := lifecycle.DecodePromptCorrelation(meta, a.lifecycleNegotiated())
	if refusal != nil {
		return lifecycle.Submission{}, unsupportedField(refusal.Field)
	}

	return submission, nil
}
