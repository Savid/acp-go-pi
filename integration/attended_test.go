//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	piacp "github.com/savid/acp-go-pi"
	"github.com/stretchr/testify/require"
)

const envRunAttended = "ACP_GO_PI_RUN_ATTENDED"

// requireRunAttended gates the tier and resolves the pi binary it drives. Once
// the gate is set a missing CLI fails rather than skips: the operator set it
// intending to spend a quarter of an hour at a real login prompt, and a
// sub-second skip scrolls past them in the -v stream.
func requireRunAttended(t *testing.T) string {
	t.Helper()
	requireRunIntegration(t)

	if os.Getenv(envRunAttended) != "1" {
		t.Skipf("set %s=1 to run provider-auth flows a human must approve", envRunAttended)
	}

	path, err := resolvePiPath()
	if err != nil {
		t.Fatalf("%s=1 requires the pi CLI (%v); install pi or set %s", envRunAttended, err, envHarnessPath)
	}

	return path
}

// attendedPrompt asks the operator for one value on the terminal. The tier
// fails rather than skips once its gate is set: an attended suite that quietly
// went green without a human answering is worse than a red one.
func attendedPrompt(t *testing.T, question string) string {
	t.Helper()

	terminal, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("%s=1 requires a terminal a human can answer on: %v", envRunAttended, err)
	}

	t.Cleanup(func() { _ = terminal.Close() })

	if _, err := fmt.Fprintf(terminal, "\n%s\n> ", question); err != nil {
		t.Fatalf("write the attended prompt: %v", err)
	}

	answer, err := bufio.NewReader(terminal).ReadString('\n')
	if err != nil {
		t.Fatalf("read the attended answer: %v", err)
	}

	answer = strings.TrimSpace(answer)
	if answer == "" {
		t.Fatal("the attended answer was empty")
	}

	return answer
}

// TestAttendedProviderAuthOAuthFlow drives a real provider login end to end.
// The owner opens the presented URL, approves in a browser, and pastes the code
// back; nothing here can be automated, which is the whole reason this tier is
// separate from the unattended ones.
func TestAttendedProviderAuthOAuthFlow(t *testing.T) {
	piPath := requireRunAttended(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	home := t.TempDir()
	agent := startAgentBinary(t, ctx,
		"-path", piPath,
		"-scratch-dir", t.TempDir(),
		"-home", home,
		"-provider-auth-root", t.TempDir(),
	)

	conn := acp.NewClientSideConnection(&recordingClient{}, agent.stdin, agent.stdout)

	initialized, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	piMeta, ok := initialized.AgentCapabilities.Meta["pi"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, piMeta, "providerAuth", "stderr: %s", agent.stderrString())

	session, err := conn.NewSession(ctx, piacp.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "stderr: %s", agent.stderrString())

	sessionID := string(session.SessionId)

	var methods attendedMethodsWire
	attendedCall(t, ctx, conn, piacp.AuthMethodsMethod, map[string]any{"sessionId": sessionID}, &methods)

	providerID := attendedPrompt(t, fmt.Sprintf("provider to log into (oauth methods: %s)", strings.Join(attendedOAuthProviders(methods), ", ")))

	var authorization attendedAuthorizeWire

	attendedCall(t, ctx, conn, piacp.AuthAuthorizeMethod, map[string]any{
		"sessionId":          sessionID,
		"providerId":         providerID,
		"connectionId":       "attended-connection",
		"methodsGeneration":  methods.Generation,
		"method":             "oauth",
		"authorizeRequestId": "attended-request",
	}, &authorization)

	require.NotEmpty(t, authorization.URL)
	require.Equal(t, "callback", authorization.Interaction, "this tier drives the paste-back shape")

	code := attendedPrompt(t, fmt.Sprintf("open %s\n%s\npaste the authorization code or redirect URL", authorization.URL, authorization.Message))

	attendedCall(t, ctx, conn, piacp.AuthCallbackMethod, map[string]any{
		"sessionId": sessionID, "providerId": providerID,
		"method": "oauth", "flowId": authorization.FlowID, "input": code,
	}, nil)

	var status attendedStatusWire
	attendedCall(t, ctx, conn, piacp.AuthStatusMethod, map[string]any{
		"sessionId": sessionID, "providerId": providerID, "flowId": authorization.FlowID,
	}, &status)
	require.Equal(t, "authenticated", status.State)

	var inventory attendedInventoryWire
	attendedCall(t, ctx, conn, piacp.AuthInventoryMethod, map[string]any{"sessionId": sessionID}, &inventory)
	require.Len(t, inventory.Entries, 1)
	require.Equal(t, "confirmed_present", inventory.Entries[0].ProofSource)

	attendedCall(t, ctx, conn, piacp.AuthDisconnectMethod, map[string]any{
		"sessionId": sessionID, "providerId": providerID,
		"connectionId": "attended-connection", "bindingGeneration": 1,
	}, nil)

	var afterRemoval attendedInventoryWire
	attendedCall(t, ctx, conn, piacp.AuthInventoryMethod, map[string]any{"sessionId": sessionID}, &afterRemoval)
	require.Empty(t, afterRemoval.Entries)
}

func attendedOAuthProviders(methods attendedMethodsWire) []string {
	names := make([]string, 0, len(methods.Providers))

	for providerID, entries := range methods.Providers {
		for _, entry := range entries {
			if entry.Type == "oauth" {
				names = append(names, providerID)
			}
		}
	}

	return names
}

type attendedMethodsWire struct {
	Providers  map[string][]attendedMethodWire `json:"providers"`
	Generation string                          `json:"generation"`
}

type attendedMethodWire struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

type attendedAuthorizeWire struct {
	Interaction   string `json:"interaction"`
	URL           string `json:"url"`
	Message       string `json:"message"`
	UserCode      string `json:"userCode"`
	CallbackInput string `json:"callbackInput"`
	FlowID        string `json:"flowId"`
	FlowExpiresAt int64  `json:"flowExpiresAt"`
}

type attendedStatusWire struct {
	FlowID string `json:"flowId"`
	State  string `json:"state"`
	Reason string `json:"reason"`
}

type attendedInventoryWire struct {
	Entries []struct {
		ProviderID  string `json:"providerId"`
		ProofSource string `json:"proofSource"`
	} `json:"entries"`
}

func attendedCall(t *testing.T, ctx context.Context, conn *acp.ClientSideConnection, method string, params any, out any) {
	t.Helper()

	raw, err := conn.CallExtension(ctx, method, params)
	require.NoError(t, err, method)

	if out != nil {
		require.NoError(t, json.Unmarshal(raw, out), method)
	}
}
