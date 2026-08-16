package pi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustCreateResidence(t *testing.T, agentDir string, mcp *MCPConfig) (*SessionResidence, SessionResidenceFiles) {
	t.Helper()

	residence, files, err := CreateSessionResidence(agentDir, mcp)
	require.NoError(t, err)

	return residence, files
}

func TestSessionResidencePublishesWrapperExtensions(t *testing.T) {
	t.Parallel()

	t.Run("bridge only", func(t *testing.T) {
		t.Parallel()

		residence, files := mustCreateResidence(t, filepath.Join(t.TempDir(), "agent"), nil)
		require.Equal(t, []string{
			filepath.Join(residence.Root(), BridgeExtensionFileName),
			filepath.Join(residence.Root(), PathExtensionFileName),
		}, files.ExtensionPaths)
		require.Empty(t, files.MCPConfigPath)

		bridge, err := os.ReadFile(files.ExtensionPaths[0]) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, bridgeExtensionSource, bridge)
		require.Contains(t, string(bridge), PermissionTitleMarker)
		require.Contains(t, string(bridge), EnvPermissionMode)
		require.Contains(t, string(bridge), `pi.on("message_end"`)
		require.Contains(t, string(bridge), "acpMessageId: randomUUID()")
		require.Contains(t, string(bridge), `name: QUESTION_TOOL`)
		require.Contains(t, string(bridge), `ctx.ui.input`)
		require.Contains(t, string(bridge), `ctx.ui.select`)
		require.Contains(t, string(bridge), `event.toolName === QUESTION_TOOL`)
		require.Contains(t, string(bridge), `toolCallId: event.toolCallId`)
		pathExtension, err := os.ReadFile(files.ExtensionPaths[1]) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, pathExtensionSource, pathExtension)
		require.Contains(t, string(pathExtension), EnvExtraPathDirs)
		require.Contains(t, string(pathExtension), `pi.on("user_bash"`)
		require.Contains(t, string(pathExtension), "createBashTool")
		require.NoFileExists(t, filepath.Join(residence.Root(), MCPExtensionFileName))
	})

	t.Run("bridge and mcp", func(t *testing.T) {
		t.Parallel()

		config := MCPConfig{Servers: []MCPServer{
			{
				Name:    "calc",
				Type:    "stdio",
				Command: "python3",
				Args:    []string{"/srv/server.py"},
				Env:     map[string]string{"MCP_TEST_ENV": "v"},
			},
			{
				Name:    "web",
				Type:    "http",
				URL:     "http://127.0.0.1:8973/mcp",
				Headers: map[string]string{"Authorization": "Bearer x"},
			},
		}}

		residence, files := mustCreateResidence(t, filepath.Join(t.TempDir(), "agent"), &config)
		require.Equal(t, []string{
			filepath.Join(residence.Root(), BridgeExtensionFileName),
			filepath.Join(residence.Root(), PathExtensionFileName),
			filepath.Join(residence.Root(), MCPExtensionFileName),
		}, files.ExtensionPaths)
		require.Equal(t, filepath.Join(residence.Root(), MCPConfigFileName), files.MCPConfigPath)

		mcp, err := os.ReadFile(files.ExtensionPaths[2]) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, mcpExtensionSource, mcp)
		require.Contains(t, string(mcp), EnvMCPConfig)
		require.Contains(t, string(mcp), "mcp__${server.name}__${tool.name}")

		data, err := os.ReadFile(files.MCPConfigPath) // #nosec G304 -- test temp dir.
		require.NoError(t, err)

		var decoded MCPConfig

		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, config, decoded)
	})
}

// TestConcurrentSessionResidencesCannotObserveEachOther proves the property a
// shared durable home used to break: two sessions writing MCP config into one
// agent directory each address their own file, so neither can read or replace
// the other's servers, arguments, environment, or credential-bearing headers.
func TestConcurrentSessionResidencesCannotObserveEachOther(t *testing.T) {
	t.Parallel()

	agentDir := t.TempDir()
	firstConfig := MCPConfig{Servers: []MCPServer{{
		Name: "first", Type: "http", URL: "http://127.0.0.1:1/mcp",
		Headers: map[string]string{"Authorization": "Bearer first-secret"},
	}}}
	secondConfig := MCPConfig{Servers: []MCPServer{{
		Name: "second", Type: "http", URL: "http://127.0.0.1:2/mcp",
		Headers: map[string]string{"Authorization": "Bearer second-secret"},
	}}}

	first, firstFiles := mustCreateResidence(t, agentDir, &firstConfig)
	second, secondFiles := mustCreateResidence(t, agentDir, &secondConfig)

	require.NotEqual(t, first.Root(), second.Root())
	require.NotEqual(t, firstFiles.MCPConfigPath, secondFiles.MCPConfigPath)
	require.NotEqual(t, firstFiles.ExtensionPaths, secondFiles.ExtensionPaths)

	firstData, err := os.ReadFile(firstFiles.MCPConfigPath) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.NotContains(t, string(firstData), "second-secret")

	var decoded MCPConfig

	require.NoError(t, json.Unmarshal(firstData, &decoded))
	require.Equal(t, firstConfig, decoded)

	// Removing one session's residence leaves every other session's intact.
	require.NoError(t, first.Remove())
	require.NoFileExists(t, firstFiles.MCPConfigPath)
	require.FileExists(t, secondFiles.MCPConfigPath)
	require.NoError(t, second.Remove())
	require.NoError(t, (*SessionResidence)(nil).Remove())
}

// TestSessionResidenceFilesArePublishedOnce proves a residence file is
// read-only and cannot be rewritten, so a pi child can never be relaunched from
// bytes a later write substituted under it.
func TestSessionResidenceFilesArePublishedOnce(t *testing.T) {
	t.Parallel()

	residence, files := mustCreateResidence(t, t.TempDir(), &MCPConfig{})

	info, err := os.Stat(files.MCPConfigPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o400), info.Mode().Perm())

	_, err = residence.publish(MCPConfigFileName, []byte("replacement"))
	require.ErrorContains(t, err, "publish session residence file")
	require.NoFileExists(t, files.MCPConfigPath+residenceStagingSuffix)

	published, err := os.ReadFile(files.MCPConfigPath) // #nosec G304 -- test temp dir.
	require.NoError(t, err)
	require.NotContains(t, string(published), "replacement")
}

func TestSessionResidencePublishFaultInjection(t *testing.T) {
	tests := []struct {
		name   string
		failOn string
	}{
		{name: "bridge extension", failOn: BridgeExtensionFileName},
		{name: "path extension", failOn: PathExtensionFileName},
		{name: "mcp extension", failOn: MCPExtensionFileName},
		{name: "mcp config", failOn: MCPConfigFileName},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreAgentDirSeams(t)

			realWrite := fsWriteFile
			fsWriteFile = func(path string, data []byte, perm os.FileMode) error {
				if filepath.Base(path) == test.failOn+residenceStagingSuffix {
					return os.ErrPermission
				}

				return realWrite(path, data, perm)
			}

			agentDir := t.TempDir()
			_, _, err := CreateSessionResidence(agentDir, &MCPConfig{})
			require.ErrorContains(t, err, "stage session residence file")
			require.ErrorContains(t, err, test.failOn)

			// A refused residence leaves nothing behind in a shared agent
			// directory.
			entries, readErr := os.ReadDir(filepath.Join(agentDir, sessionResidenceDir))
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
}

func TestSessionResidenceCreateAndRemoveFailures(t *testing.T) {
	restoreAgentDirSeams(t)

	fsMkdirAll = func(string, os.FileMode) error { return os.ErrPermission }

	_, _, err := CreateSessionResidence(t.TempDir(), nil)
	require.ErrorContains(t, err, "create session residence root")

	fsMkdirAll = os.MkdirAll
	fsMkdirTemp = func(string, string) (string, error) { return "", os.ErrPermission }

	_, _, err = CreateSessionResidence(t.TempDir(), nil)
	require.ErrorContains(t, err, "create session residence")

	fsMkdirTemp = os.MkdirTemp
	residence, _ := mustCreateResidence(t, t.TempDir(), nil)

	fsRemoveAll = func(string) error { return os.ErrPermission }
	require.ErrorContains(t, residence.Remove(), "remove session residence")

	fsRemoveAll = os.RemoveAll
	fsRemove = func(string) error { return os.ErrPermission }

	_, _, err = CreateSessionResidence(t.TempDir(), nil)
	require.ErrorContains(t, err, "clean session residence staging")

	fsRemove = os.Remove
	fsLink = func(string, string) error { return os.ErrPermission }

	_, _, err = CreateSessionResidence(t.TempDir(), nil)
	require.ErrorContains(t, err, "publish session residence file")
}

func TestSessionResidenceMCPConfigEncodeFailure(t *testing.T) {
	realMarshal := marshalMCPConfig
	t.Cleanup(func() { marshalMCPConfig = realMarshal })

	marshalMCPConfig = func(any, string, string) ([]byte, error) {
		return nil, os.ErrInvalid
	}

	_, _, err := CreateSessionResidence(t.TempDir(), &MCPConfig{})
	require.ErrorContains(t, err, "encode mcp config")
}

func TestParsePermissionTitle(t *testing.T) {
	t.Parallel()

	t.Run("valid marker payload", func(t *testing.T) {
		t.Parallel()

		prompt := PermissionPrompt{
			ToolCallID: "native-call-42",
			ToolName:   "bash",
			Input:      json.RawMessage(`{"command":"ls -la"}`),
		}
		payload, err := json.Marshal(prompt)
		require.NoError(t, err)

		parsed, ok := ParsePermissionTitle(PermissionTitleMarker + string(payload))
		require.True(t, ok)
		require.Equal(t, "native-call-42", parsed.ToolCallID)
		require.Equal(t, "bash", parsed.ToolName)
		require.JSONEq(t, `{"command":"ls -la"}`, string(parsed.Input))
	})

	t.Run("ordinary dialog title", func(t *testing.T) {
		t.Parallel()

		_, ok := ParsePermissionTitle("Clear session?")
		require.False(t, ok)
	})

	for _, testCase := range []struct {
		name    string
		payload string
	}{
		{name: "invalid json", payload: "not json"},
		{name: "missing native id", payload: `{"toolName":"bash"}`},
		{name: "empty native id", payload: `{"toolCallId":"","toolName":"bash"}`},
		{name: "blank native id", payload: `{"toolCallId":"  ","toolName":"bash"}`},
		{name: "malformed native id", payload: `{"toolCallId":7,"toolName":"bash"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, ok := ParsePermissionTitle(PermissionTitleMarker + testCase.payload)
			require.False(t, ok)
		})
	}
}

func TestProviderAuthCommandEncodingAndTitleParsing(t *testing.T) {
	request := AuthRequest{ID: "flow-1", Op: AuthOpCatalog, ProviderID: "anthropic", ProviderIDs: []string{"one"}}
	command := EncodeAuthCommand(request)
	require.Equal(t, "/"+AuthCommandName+` {"id":"flow-1","op":"catalog","providerId":"anthropic","providerIds":["one"]}`, command)

	message := AuthMessage{ID: "flow-1", Kind: AuthKindResult}
	payload, err := json.Marshal(message)
	require.NoError(t, err)
	parsed, ok := ParseAuthTitle(AuthTitleMarker + string(payload))
	require.True(t, ok)
	require.Equal(t, message, parsed)

	for _, title := range []string{
		"ordinary title",
		AuthTitleMarker + "{",
		AuthTitleMarker + `{"id":"","kind":"result"}`,
		AuthTitleMarker + `{"id":"flow","kind":" "}`,
	} {
		_, ok = ParseAuthTitle(title)
		require.False(t, ok, title)
	}
}
