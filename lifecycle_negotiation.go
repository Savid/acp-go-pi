package piacp

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/lifecycle"
)

// lifecycleMetaKey is the family-reserved literal the session lifecycle
// extension rides under on every surface that carries it.
const lifecycleMetaKey = lifecycle.MetaKey

var lifecycleRandRead = rand.Read

// provenLifecycleFacts resolves the answer for the active configuration from
// the same code path that enforces containment, never from a compiled-in
// constant.
//
//   - `updatesOutsidePrompt` is true because the session's process-generation
//     outbox routes every native event whether or not a prompt is in flight,
//     and the stream opens on the establishing response rather than inside a
//     prompt.
//   - `authoritativeQuiescence` is true only where session close proves
//     whole-tree vacancy: the Linux supervised boundary enumerates its own
//     tree, and every other boundary signals a process group without being
//     able to state what remained.
//   - `activityKinds` is empty because no native event carries a background
//     activity entity: the agent and turn brackets, the queue update, the
//     compaction and auto-retry pairs, and the tool events are all foreground.
func (a *Agent) provenLifecycleFacts() lifecycle.Negotiated {
	proven := lifecycle.Negotiated{
		UpdatesOutsidePrompt: true,
		ActivityKinds:        []lifecycle.ActivityKind{},
	}

	if a.ContainmentMode() == RuntimeContainmentAuthoritative {
		proven.AuthoritativeQuiescence = true
		proven.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return proven
}

// negotiateLifecycle reads the host's offer and records the answer for the
// connection. An absent offer leaves the key omitted from the response and the
// extension dormant for every session on that connection.
func (a *Agent) negotiateLifecycle(meta map[string]any) (map[string]any, error) {
	offer, present, refusal := lifecycle.DecodeOffer(meta)
	if refusal != nil {
		return nil, unsupportedField(refusal.Field)
	}

	answer, common := lifecycle.Negotiated{}, false
	if present {
		answer, common = offer.Answer(a.provenLifecycleFacts())
	}

	a.mu.Lock()
	a.lifecycle = answer
	a.mu.Unlock()

	if !common {
		return nil, nil
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

// refuseLifecycleRawMeta applies the same rule to an extension leg whose
// parameters are still undecoded, so the key is refused before the leg's own
// validation or refusal runs.
func refuseLifecycleRawMeta(params json.RawMessage) error {
	var envelope struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}

	if err := json.Unmarshal(params, &envelope); err != nil {
		return nil
	}

	if _, present := envelope.Meta[lifecycleMetaKey]; !present {
		return nil
	}

	return unsupportedField(lifecycle.MetaPath)
}

// lifecycleGuardedExtensionMethods are the extension legs this adapter defines.
// A method it does not define stays a method-not-found rather than becoming an
// invalid-params answer about a key no route was ever going to read.
var lifecycleGuardedExtensionMethods = map[string]struct{}{
	ForkSessionMethod:    {},
	AuthMethodsMethod:    {},
	AuthAuthorizeMethod:  {},
	AuthCallbackMethod:   {},
	AuthStatusMethod:     {},
	AuthCancelMethod:     {},
	AuthInventoryMethod:  {},
	AuthDisconnectMethod: {},
}

// refuseLifecycleExtensionMeta rejects the family literal on one of this
// adapter's extension legs, configured or not.
func refuseLifecycleExtensionMeta(method string, params json.RawMessage) error {
	if _, guarded := lifecycleGuardedExtensionMethods[method]; !guarded {
		return nil
	}

	return refuseLifecycleRawMeta(params)
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

// lifecycleActionMeta renders the action correlation value the agent stamps on
// every permission and elicitation it emits while the extension is negotiated.
// The action id is lifecycle identity only: it never routes or authorizes the
// callback, which stays the reserved route envelope's job.
func lifecycleActionMeta(streamID, actionID string, owner lifecycle.Owner) map[string]any {
	return map[string]any{lifecycleMetaKey: map[string]any{
		"version":  lifecycle.Version,
		"streamId": streamID,
		"action": map[string]any{
			"actionId": actionID,
			"owner":    map[string]any{"type": string(owner.Type), "id": owner.ID},
		},
	}}
}

// newLifecycleID mints one opaque lifecycle identifier. The identities this
// adapter mints are adapter provenance: they name a stream, cycle, turn, or
// action inside one incarnation and are never derived from a native id or from
// the route nonce.
func newLifecycleID(prefix string) (string, error) {
	var data [16]byte
	if _, err := lifecycleRandRead(data[:]); err != nil {
		return "", err
	}

	return prefix + "-" + hex.EncodeToString(data[:]), nil
}

// lifecycleNotificationMeta is the envelope carrier's `_meta`. The envelope
// rides the notification, never the update object's own per-entity metadata.
func lifecycleNotificationMeta(envelope map[string]any) map[string]any {
	return map[string]any{lifecycleMetaKey: envelope}
}

// lifecycleCarrier is the identity-only session_info_update every envelope
// rides. It sets no title and no timestamp, so carrying an envelope mutates no
// state a client reduces.
func lifecycleCarrier() acp.SessionUpdate {
	return acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}
}
