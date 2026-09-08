package piacp

import (
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func TestInvalidConstructionPrecedesNativeMaterialization(t *testing.T) {
	for _, option := range []Option{
		WithDefaultModel("invalid-model"),
		WithEnv(map[string]string{"NODE_OPTIONS": "forbidden"}),
	} {
		root := t.TempDir()
		home := filepath.Join(root, "native")
		ledger := filepath.Join(root, "ledger")
		agent := NewAgent(WithLogger(slog.New(slog.DiscardHandler)), WithHome(home), WithProviderAuthRoot(ledger), option)
		_, err := agent.Initialize(t.Context(), defaultInitializeRequest())
		requireClosedInternalError(t, err, invalidOptionsError)
		_, err = agent.NewSession(t.Context(), NewSessionRequest(root))
		requireClosedInternalError(t, err, invalidOptionsError)
		_, err = agent.LoadSession(t.Context(), LoadSessionRequest(validSessionUUID, root))
		requireClosedInternalError(t, err, invalidOptionsError)
		_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest(validSessionUUID, root))
		requireClosedInternalError(t, err, invalidOptionsError)
		_, err = agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, forkRaw(t, ForkSessionRequest(validSessionUUID, root)))
		requireClosedInternalError(t, err, invalidOptionsError)
		require.NoDirExists(t, home)
		require.NoDirExists(t, ledger)
		require.NoError(t, agent.Close())
	}
}

func TestPathValidationHelpers(t *testing.T) {
	require.Error(t, validateRequiredAbsolutePath("cwd", ""))
	require.Error(t, validateRequiredAbsolutePath("cwd", "relative"))
	require.NoError(t, validateRequiredAbsolutePath("cwd", absTestPath("absolute")))
	require.NoError(t, validateOptionalAbsolutePath("cwd", nil))
	empty := ""
	require.NoError(t, validateOptionalAbsolutePath("cwd", &empty))
	relative := "relative"
	require.Error(t, validateOptionalAbsolutePath("cwd", &relative))
	absolute := absTestPath("absolute")
	require.NoError(t, validateOptionalAbsolutePath("cwd", &absolute))
	require.Error(t, validateAbsolutePaths("paths", []string{""}))
	require.Error(t, validateAbsolutePaths("paths", []string{"relative"}))
	require.NoError(t, validateAbsolutePaths("paths", []string{absTestPath("one"), absTestPath("two")}))
	require.Error(t, validateSessionStartPaths("", nil))
	require.Error(t, validateSessionStartPaths(testCwd, []string{"relative"}))
	require.NoError(t, validateSessionStartPaths(testCwd, []string{absTestPath("also")}))
}

func TestConfigurationAndAdmissionEdges(t *testing.T) {
	_, err := resolveSessionConfiguration(PiOptions{}, sessionConfigurationPresence{}, sessionConfigurationRecord{
		Env: map[string]string{}, ExtraPathDirs: []string{"relative"},
	})
	require.Error(t, err)

	agent := NewAgent()
	agent.options.ProviderAuthRoot = "/provider-auth"
	agent.options.hostAuthoritySupplied = true
	require.NoError(t, configureProviderAuth(agent))

	agent.markNativeTreeBusy("")
	require.Empty(t, agent.nativeBusyRoots)
	path, err := agent.resolveExecutablePath()
	require.NoError(t, err)
	require.Equal(t, rawEventSourceValue, path)
}

// TestRelativeCwdRefusedOnEverySessionStartSurface pins the family's uniform
// relative-`cwd` rejection on the wire, on every surface that takes one. The
// refusal is the two-key unsupported shape naming `cwd` — never a pi token and
// never a message-shaped data object — and it lands before any native process
// or store entry exists.
func TestRelativeCwdRefusedOnEverySessionStartSurface(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testContainmentOption())

	_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{Cwd: "relative"})
	requireRefusal(t, valUnsupported, jsonFieldCwd, err)

	_, err = agent.LoadSession(t.Context(), acp.LoadSessionRequest{
		SessionId: acp.SessionId(validSessionUUID), Cwd: "relative",
	})
	requireRefusal(t, valUnsupported, jsonFieldCwd, err)

	_, err = agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{
		SessionId: acp.SessionId(validSessionUUID), Cwd: "relative",
	})
	requireRefusal(t, valUnsupported, jsonFieldCwd, err)

	forked := ForkSessionRequest(forkParentID, "relative")
	_, err = agent.HandleExtensionMethod(t.Context(), ForkSessionMethod, forkRaw(t, forked))
	requireRefusal(t, valUnsupported, jsonFieldCwd, err)

	// The same absolute path passes the gate, so the refusals above are the
	// path rule rather than an unconditional failure.
	require.NoError(t, validateSessionStartPaths(t.TempDir(), nil))
}

// TestExtensionParamsRefusedAsAWhole pins the uniform extension-params rule: a
// `_pi/*` request whose params cannot be decoded, or fail validation as a
// whole, is refused naming `params`.
func TestExtensionParamsRefusedAsAWhole(t *testing.T) {
	t.Parallel()

	client := newStubPiClient()
	agent := newStubClientAgent(t, client, WithProviderAuthRoot(t.TempDir()), WithHome(t.TempDir()))
	require.NotNil(t, agent.providerAuth)

	for _, method := range []string{
		ForkSessionMethod,
		AuthMethodsMethod,
		AuthAuthorizeMethod,
		AuthCallbackMethod,
		AuthStatusMethod,
		AuthCancelMethod,
		AuthInventoryMethod,
		AuthDisconnectMethod,
	} {
		for _, params := range []string{`[]`, `"text"`, `{`, `{} {}`} {
			_, err := agent.HandleExtensionMethod(t.Context(), method, json.RawMessage(params))
			requireRefusal(t, valUnsupported, jsonFieldParams, err)
		}
	}
}
