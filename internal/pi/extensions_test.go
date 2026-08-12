package pi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteExtensions(t *testing.T) {
	t.Parallel()

	t.Run("bridge only", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "agent")

		paths, err := WriteExtensions(dir, false)
		require.NoError(t, err)
		require.Equal(t, []string{
			filepath.Join(dir, BridgeExtensionFileName),
			filepath.Join(dir, PathExtensionFileName),
		}, paths)

		bridge, err := os.ReadFile(paths[0]) // #nosec G304 -- test temp dir.
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
		pathExtension, err := os.ReadFile(paths[1]) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, pathExtensionSource, pathExtension)
		require.Contains(t, string(pathExtension), EnvExtraPathDirs)
		require.Contains(t, string(pathExtension), `pi.on("user_bash"`)
		require.Contains(t, string(pathExtension), "createBashTool")
		require.NoFileExists(t, filepath.Join(dir, MCPExtensionFileName))
	})

	t.Run("bridge and mcp", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "agent")

		paths, err := WriteExtensions(dir, true)
		require.NoError(t, err)
		require.Equal(t, []string{
			filepath.Join(dir, BridgeExtensionFileName),
			filepath.Join(dir, PathExtensionFileName),
			filepath.Join(dir, MCPExtensionFileName),
		}, paths)

		mcp, err := os.ReadFile(paths[2]) // #nosec G304 -- test temp dir.
		require.NoError(t, err)
		require.Equal(t, mcpExtensionSource, mcp)
		require.Contains(t, string(mcp), EnvMCPConfig)
		require.Contains(t, string(mcp), "mcp__${server.name}__${tool.name}")
	})
}

func TestWriteExtensionsFaultInjection(t *testing.T) {
	tests := []struct {
		name    string
		failOn  string
		wantErr string
	}{
		{name: "bridge write failure", failOn: BridgeExtensionFileName, wantErr: "write bridge extension"},
		{name: "path write failure", failOn: PathExtensionFileName, wantErr: "write path extension"},
		{name: "mcp write failure", failOn: MCPExtensionFileName, wantErr: "write mcp extension"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreAgentDirSeams(t)

			realWrite := fsWriteFile
			fsWriteFile = func(path string, data []byte, perm os.FileMode) error {
				if filepath.Base(path) == test.failOn {
					return os.ErrPermission
				}

				return realWrite(path, data, perm)
			}

			_, err := WriteExtensions(t.TempDir(), true)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestWriteExtensionsMkdirFailure(t *testing.T) {
	restoreAgentDirSeams(t)

	fsMkdirAll = func(string, os.FileMode) error { return os.ErrPermission }

	_, err := WriteExtensions(t.TempDir(), false)
	require.ErrorContains(t, err, "create extension directory")
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

func TestWriteMCPConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

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

	path, err := WriteMCPConfig(dir, config)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, MCPConfigFileName), path)

	data, err := os.ReadFile(path) // #nosec G304 -- test temp dir.
	require.NoError(t, err)

	var decoded MCPConfig

	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, config, decoded)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteMCPConfigWriteFailure(t *testing.T) {
	restoreAgentDirSeams(t)

	fsWriteFile = func(string, []byte, os.FileMode) error { return os.ErrPermission }

	_, err := WriteMCPConfig(t.TempDir(), MCPConfig{})
	require.ErrorContains(t, err, "write mcp config")
}

func TestWriteMCPConfigEncodeFailure(t *testing.T) {
	realMarshal := marshalMCPConfig
	t.Cleanup(func() { marshalMCPConfig = realMarshal })

	marshalMCPConfig = func(any, string, string) ([]byte, error) {
		return nil, os.ErrInvalid
	}

	_, err := WriteMCPConfig(t.TempDir(), MCPConfig{})
	require.ErrorContains(t, err, "encode mcp config")
}
