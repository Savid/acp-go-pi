package piacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-pi/internal/pi"
)

// authHarness stands one agent, one live session, and a scripted bridge in for
// the pi child. Every /acp-auth command the broker sends is decoded here and
// answered with the marker dialogs the real extension would raise.
type authHarness struct {
	t       *testing.T
	agent   *Agent
	broker  *providerAuth
	session *agentSession
	client  *stubPiClient
	root    string
	home    string

	mu      sync.Mutex
	dialogs int
	bridge  func(ctx context.Context, request pi.AuthRequest) error
	answers map[string]*authAnswer
}

// authAnswer records the single response the broker writes back for one dialog,
// so several waiters can observe it.
type authAnswer struct {
	done  chan struct{}
	once  sync.Once
	value pi.UIResponse
}

func newAuthHarness(t *testing.T, opts ...Option) *authHarness {
	t.Helper()

	client := newStubPiClient()
	client.state.SessionID = "auth-session"

	harness := &authHarness{
		t:       t,
		client:  client,
		root:    t.TempDir(),
		home:    t.TempDir(),
		answers: make(map[string]*authAnswer),
	}

	client.respondFunc = harness.recordAnswer

	base := make([]Option, 0, 2+len(opts))
	base = append(base, WithProviderAuthRoot(harness.root), WithHome(harness.home))
	agent := newStubClientAgent(t, client, append(base, opts...)...)

	session, err := agent.startAndStoreSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.NoError(t, err)

	harness.agent = agent
	harness.broker = agent.providerAuth
	harness.session = session

	require.NotNil(t, harness.broker)

	client.promptFunc = func(ctx context.Context, message string) error {
		request, err := decodeAuthCommand(message)
		if err != nil {
			return err
		}

		harness.mu.Lock()
		bridge := harness.bridge
		harness.mu.Unlock()

		if bridge == nil {
			return nil
		}

		return bridge(ctx, request)
	}

	t.Cleanup(func() { _ = session.Close(context.WithoutCancel(t.Context())) })

	return harness
}

func decodeAuthCommand(message string) (pi.AuthRequest, error) {
	payload, found := strings.CutPrefix(message, "/"+pi.AuthCommandName+" ")
	if !found {
		return pi.AuthRequest{}, errors.New("not an auth command")
	}

	var request pi.AuthRequest

	return request, json.Unmarshal([]byte(payload), &request)
}

// scriptBridge installs the fake bridge's reply to one /acp-auth command.
func (h *authHarness) scriptBridge(reply func(ctx context.Context, request pi.AuthRequest) error) {
	h.mu.Lock()
	h.bridge = reply
	h.mu.Unlock()
}

// deliver raises one bridge dialog, lets the broker route it, and reports the
// dialog id so a test can wait for the answer the broker wrote back.
func (h *authHarness) deliver(ctx context.Context, message pi.AuthMessage) string {
	request := h.dialogRequest(message)
	h.broker.handleAuthDialog(ctx, h.session, request)

	return request.ID
}

func (h *authHarness) dialogRequest(message pi.AuthMessage) pi.UIRequest {
	encoded, err := json.Marshal(message)
	require.NoError(h.t, err)

	h.mu.Lock()
	h.dialogs++
	id := fmt.Sprintf("dialog-%d", h.dialogs)
	h.answers[id] = &authAnswer{done: make(chan struct{})}
	h.mu.Unlock()

	return pi.UIRequest{ID: id, Method: uiMethodSelect, Title: pi.AuthTitleMarker + string(encoded)}
}

func (h *authHarness) recordAnswer(response pi.UIResponse) {
	h.mu.Lock()
	answer := h.answers[response.ID]
	h.mu.Unlock()

	if answer == nil {
		return
	}

	answer.once.Do(func() {
		answer.value = response
		close(answer.done)
	})
}

// awaitAnswer blocks until the broker answers the named dialog.
func (h *authHarness) awaitAnswer(dialogID string) pi.UIResponse {
	h.mu.Lock()
	answer := h.answers[dialogID]
	h.mu.Unlock()

	require.NotNil(h.t, answer)

	select {
	case <-answer.done:
		return answer.value
	case <-time.After(10 * time.Second):
		require.FailNow(h.t, "dialog was never answered", dialogID)

		return pi.UIResponse{}
	}
}

func (h *authHarness) responses() []pi.UIResponse {
	h.client.mu.Lock()
	defer h.client.mu.Unlock()

	return append([]pi.UIResponse(nil), h.client.responses...)
}

func (h *authHarness) lastResponse() pi.UIResponse {
	responses := h.responses()
	require.NotEmpty(h.t, responses)

	return responses[len(responses)-1]
}

// call invokes one provider-auth leg through the agent's extension dispatcher,
// which is the only route a host has.
func (h *authHarness) call(ctx context.Context, method string, params any) (any, error) {
	encoded, err := json.Marshal(params)
	require.NoError(h.t, err)

	return h.agent.HandleExtensionMethod(ctx, method, encoded)
}

// seedCatalog runs the methods leg against a scripted provider enumeration and
// returns the generation that names it.
func (h *authHarness) seedCatalog(ctx context.Context, providers []pi.AuthProvider) string {
	h.scriptBridge(func(ctx context.Context, request pi.AuthRequest) error {
		require.Equal(h.t, pi.AuthOpCatalog, request.Op)
		h.deliver(ctx, pi.AuthMessage{ID: request.ID, Kind: pi.AuthKindCatalog, Providers: providers})

		return nil
	})

	result, err := h.call(ctx, AuthMethodsMethod, map[string]any{authFieldSessionID: string(h.session.id)})
	require.NoError(h.t, err)

	return methodsResult(h.t, result).Generation
}

func methodsResult(t *testing.T, value any) authMethodsResult {
	t.Helper()

	result, ok := value.(authMethodsResult)
	require.True(t, ok)

	return result
}

func inventoryResult(t *testing.T, value any) authInventoryResult {
	t.Helper()

	result, ok := value.(authInventoryResult)
	require.True(t, ok)

	return result
}

func defaultAuthProviders() []pi.AuthProvider {
	return []pi.AuthProvider{
		{ID: "anthropic", Name: "Anthropic", OAuth: &pi.AuthOAuthEntry{Name: "Anthropic (Claude Pro/Max)"}},
		{ID: "openai", Name: "OpenAI", API: &pi.AuthAPIEntry{Name: "OpenAI API key"}},
	}
}

func requireAuthFailed(t *testing.T, err error, cause string) {
	t.Helper()

	var requestError *acp.RequestError
	require.ErrorAs(t, err, &requestError)
	require.Equal(t, -32000, requestError.Code)

	data, ok := requestError.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, authFailedErrorTag, data[jsonFieldError])
	require.Equal(t, cause, data["cause"])
}

func TestNewProviderAuthRequiresBothPreconditions(t *testing.T) {
	t.Parallel()

	client := newStubPiClient()

	require.Nil(t, newStubClientAgent(t, client).providerAuth)
	require.Nil(t, newStubClientAgent(t, client, WithProviderAuthRoot(t.TempDir())).providerAuth)
	require.Nil(t, newStubClientAgent(t, client, WithHome(t.TempDir())).providerAuth)
	require.NotNil(t, newStubClientAgent(t, client, WithProviderAuthRoot(t.TempDir()), WithHome(t.TempDir())).providerAuth)
}

// TestNewProviderAuthUnusableRootStaysUnadvertised pins the family rule that a
// root asked for and not preparable leaves the surface absent, exactly as an
// unset one does.
func TestNewProviderAuthUnusableRootStaysUnadvertised(t *testing.T) {
	t.Parallel()

	agent := newStubClientAgent(t, newStubPiClient(), WithProviderAuthRoot("relative/root"), WithHome(t.TempDir()))
	require.Nil(t, agent.providerAuth)
}

func TestAuthCapabilityAdvertisesSevenLegs(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	resp, err := harness.agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	piMeta, ok := resp.AgentCapabilities.Meta[piMetaKey].(map[string]any)
	require.True(t, ok)

	capability, ok := piMeta[providerAuthCapabilityKey].(map[string]any)
	require.True(t, ok)

	require.Equal(t, authMethodNames(), capability[providerAuthMethodsField])
	require.Len(t, capability, 1, "no injectionKey: pi accepts no injection")
	require.Len(t, authMethodNames(), 7)
}

func TestAuthCapabilityAbsentWithoutRoot(t *testing.T) {
	t.Parallel()

	agent := newStubClientAgent(t, newStubPiClient())

	resp, err := agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	piMeta, ok := resp.AgentCapabilities.Meta[piMetaKey].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, piMeta, providerAuthCapabilityKey)
}

// TestAuthLegsUnadvertisedReturnMethodNotFound pins that an absent surface
// answers nothing rather than answering closed.
func TestAuthLegsUnadvertisedReturnMethodNotFound(t *testing.T) {
	t.Parallel()

	agent := newStubClientAgent(t, newStubPiClient())

	for _, method := range authMethodNames() {
		_, err := agent.HandleExtensionMethod(t.Context(), method, json.RawMessage(`{}`))

		var requestError *acp.RequestError
		require.ErrorAs(t, err, &requestError)
		require.Equal(t, -32601, requestError.Code, method)
	}
}

func TestAuthExtensionMethodIgnoresForeignMethod(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	result, handled, err := harness.agent.handleAuthExtensionMethod(t.Context(), "_pi/session/fork", nil)
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestAuthCauseRetryable(t *testing.T) {
	t.Parallel()

	for _, cause := range []string{authCauseTransport, authCauseProcess, authCauseTimeout} {
		require.True(t, authCauseRetryable(cause), cause)
	}

	for _, cause := range []string{
		authCauseNativeVeto, authCauseProviderRefused, authCauseHarvestFailed,
		authCauseUnsupportedVariant, authCauseFlowExpired, authCauseFlowState,
		authCauseFlowCancelled, authCausePolicy,
	} {
		require.False(t, authCauseRetryable(cause), cause)
	}
}

func TestAuthFailedErrorShape(t *testing.T) {
	t.Parallel()

	failure := &authFailedError{cause: authCauseTransport, providerID: "p", method: "m", flowID: "f"}
	require.Equal(t, "pi_auth_failed: transport", failure.Error())

	data, ok := failure.requestError().Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, authFailedErrorTag, data[jsonFieldError])
	require.Equal(t, authCauseTransport, data["cause"])
	require.Equal(t, true, data["retryable"])
	require.Equal(t, "p", data[authFieldProviderID])
	require.Equal(t, "m", data[authFieldMethod])
	require.Equal(t, "f", data[authFieldFlowID])

	bare, ok := (&authFailedError{cause: authCausePolicy}).requestError().Data.(map[string]any)
	require.True(t, ok)
	require.NotContains(t, bare, authFieldProviderID)
	require.NotContains(t, bare, authFieldMethod)
	require.NotContains(t, bare, authFieldFlowID)
}

// TestAuthFlowTransition pins the normative cause-to-transition table, including
// the three causes that must consume nothing.
func TestAuthFlowTransition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cause    string
		inFlight bool
		state    string
		reason   string
	}{
		{authCauseNativeVeto, false, authStateFailed, authReasonNativeVeto},
		{authCauseUnsupportedVariant, false, authStateFailed, authReasonNativeVeto},
		{authCauseProviderRefused, false, authStateFailed, authReasonProviderRefused},
		{authCauseTransport, false, authStateFailed, authReasonTransport},
		{authCauseTransport, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseProcess, false, authStateFailed, authReasonProcess},
		{authCauseProcess, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseTimeout, false, authStateFailed, authReasonTransport},
		{authCauseTimeout, true, authStateFailed, authReasonAcceptanceUnknown},
		{authCauseHarvestFailed, false, authStateFailed, authReasonHarvestFailed},
		{authCauseFlowExpired, false, authStateExpired, authReasonDeadline},
		{authCausePolicy, false, "", ""},
		{authCauseFlowState, true, "", ""},
		{authCauseFlowCancelled, false, "", ""},
	}

	for _, testCase := range cases {
		state, reason := authFlowTransition(testCase.cause, testCase.inFlight)
		require.Equal(t, testCase.state, state, testCase.cause)
		require.Equal(t, testCase.reason, reason, testCase.cause)
	}
}

// TestAuthParamFieldsRejectsClosedObjectViolations pins the closed-object rule:
// encoding/json alone would let a duplicate key silently win.
func TestAuthParamFieldsRejectsClosedObjectViolations(t *testing.T) {
	t.Parallel()

	for _, raw := range map[string]string{
		"not an object":  `["a"]`,
		"unknown field":  `{"sessionId":"s","extra":1}`,
		"duplicate":      `{"sessionId":"s","sessionId":"t"}`,
		"malformed body": `{"sessionId":`,
		"trailing":       `{"sessionId":"s"} {}`,
		"unterminated":   `{"sessionId":"s"`,
	} {
		_, err := authParamFields(json.RawMessage(raw), authFieldSessionID)
		requireInvalidParams(t, err)
	}

	fields, err := authParamFields(json.RawMessage(`{"sessionId":"s"}`), authFieldSessionID)
	require.NoError(t, err)
	require.Len(t, fields, 1)
}

func TestAuthFieldDecoders(t *testing.T) {
	t.Parallel()

	fields := map[string]json.RawMessage{
		"str":   json.RawMessage(`"value"`),
		"empty": json.RawMessage(`""`),
		"bad":   json.RawMessage(`5`),
		"num":   json.RawMessage(`7`),
	}

	value, err := authRequiredString(fields, "str")
	require.NoError(t, err)
	require.Equal(t, "value", value)

	_, err = authRequiredString(fields, "empty")
	requireInvalidParams(t, err)
	_, err = authRequiredString(fields, "missing")
	requireInvalidParams(t, err)

	empty, err := authString(fields, "empty")
	require.NoError(t, err)
	require.Empty(t, empty)
	_, err = authString(fields, "missing")
	requireInvalidParams(t, err)
	_, err = authString(fields, "bad")
	requireInvalidParams(t, err)

	number, err := authRequiredInt64(fields, "num")
	require.NoError(t, err)
	require.Equal(t, int64(7), number)
	_, err = authRequiredInt64(fields, "missing")
	requireInvalidParams(t, err)
	_, err = authRequiredInt64(fields, "str")
	requireInvalidParams(t, err)
}

func TestAuthSessionRejectsUnknownSession(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	_, err := harness.broker.authSession("missing")
	require.Error(t, err)
}

func TestAuthGoSafeRecoversPanic(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)

	done := make(chan struct{})
	harness.broker.goSafe("test", func() {
		defer close(done)

		panic("boom")
	})

	<-done
}

// TestDurableHomeReplacesGeneratedAgentDir pins that a configured home is what
// pi sees as its agent directory, which is what its cross-process credential
// lock is keyed on.
func TestDurableHomeReplacesGeneratedAgentDir(t *testing.T) {
	t.Parallel()

	harness := newAuthHarness(t)
	require.Equal(t, harness.home, harness.session.launch.AgentDir)

	// A relaunch keeps the same directory: copying it forward would strand the
	// credential store the harness refreshes under its own lock.
	spec, err := harness.session.nextRuntimeLaunch(harness.session.launch, "")
	require.NoError(t, err)
	require.Equal(t, harness.home, spec.AgentDir)
}

func TestDurableHomeReportsCreationFailure(t *testing.T) {
	agent := newStubClientAgent(t, newStubPiClient(), WithHome("/srv/pi-home"))

	original := materializeMkdirAll
	materializeMkdirAll = func(path string, mode os.FileMode) error {
		if path == "/srv/pi-home" {
			return errors.New("mkdir")
		}

		return original(path, mode)
	}

	t.Cleanup(func() { materializeMkdirAll = original })

	agent.versionChecked = true

	_, err := agent.startSession(t.Context(), sessionStart{Cwd: "/cwd"})
	require.Error(t, err)
}

func TestDurableHomeReportsRelaunchCreationFailure(t *testing.T) {
	harness := newAuthHarness(t)

	original := materializeMkdirAll
	materializeMkdirAll = func(path string, mode os.FileMode) error {
		if path == harness.home {
			return errors.New("mkdir")
		}

		return original(path, mode)
	}

	t.Cleanup(func() { materializeMkdirAll = original })

	_, err := harness.session.nextRuntimeLaunch(harness.session.launch, "")
	require.Error(t, err)
}

// TestAuthCommandIsNotAdvertised pins that the wrapper-owned bridge command is
// plumbing, not a command a host or a model may invoke.
func TestAuthCommandIsNotAdvertised(t *testing.T) {
	t.Parallel()

	advertised := availableCommandsFromNative([]pi.SlashCommand{
		{Name: pi.AuthCommandName, Description: "ACP provider-auth bridge"},
		{Name: "compact"},
	})

	require.Equal(t, []acp.AvailableCommand{{Name: "compact"}}, advertised)
}

// TestAuthParamFieldsRejectsMalformedKey pins that a body whose key position is
// not a string is rejected before any field is read.
func TestAuthParamFieldsRejectsMalformedKey(t *testing.T) {
	t.Parallel()

	_, err := authParamFields(json.RawMessage(`{true}`), authFieldSessionID)
	requireInvalidParams(t, err)
}
