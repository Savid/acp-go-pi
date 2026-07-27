package pi

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

//go:embed ext/acp-bridge.ts
var bridgeExtensionSource []byte

//go:embed ext/acp-mcp.ts
var mcpExtensionSource []byte

var marshalMCPConfig = json.MarshalIndent

const (
	// BridgeExtensionFileName is the wrapper-owned permission bridge
	// extension written into each per-session agent directory.
	BridgeExtensionFileName = "acp-bridge.ts"
	// MCPExtensionFileName is the wrapper-owned MCP client extension written
	// when the session declares MCP servers.
	MCPExtensionFileName = "acp-mcp.ts"
	// MCPConfigFileName is the per-session MCP server config consumed by the
	// MCP extension.
	MCPConfigFileName = "acp-mcp-config.json"

	// EnvPermissionMode selects the bridge permission mode for one pi child.
	EnvPermissionMode = "ACP_GO_PI_PERMISSION"
	// EnvMCPConfig carries the MCP config path for one pi child.
	EnvMCPConfig = "ACP_GO_PI_MCP_CONFIG"

	// PermissionModeAsk raises a permission dialog for every tool call.
	PermissionModeAsk = "ask"
	// PermissionModeAllow auto-allows every tool call without a dialog.
	PermissionModeAllow = "allow"

	// PermissionTitleMarker prefixes the select-dialog title the bridge
	// extension raises for a tool-call permission request. The remainder of
	// the title is the JSON-encoded PermissionPrompt.
	PermissionTitleMarker = "acp-go-pi:permission:"

	// PermissionOptionAllow is the select option that allows the tool call.
	PermissionOptionAllow = "allow"
	// PermissionOptionDeny is the select option that blocks the tool call.
	PermissionOptionDeny = "deny"

	// AuthTitleMarker prefixes every dialog title the bridge extension raises
	// for a provider-auth exchange. The remainder is the JSON-encoded
	// AuthMessage.
	AuthTitleMarker = "acp-go-pi:auth:"

	// AuthCommandName is the wrapper-owned slash command that carries one
	// provider-auth request into the bridge extension. It is never advertised
	// as an ACP available command.
	AuthCommandName = "acp-auth"

	// AuthAck answers a bridge dialog that only reports.
	AuthAck = "ok"
)

// Provider-auth request operations the bridge extension implements.
const (
	AuthOpCatalog = "catalog"
	AuthOpProbe   = "probe"
	AuthOpLogin   = "login"
	AuthOpRemove  = "remove"
)

// Provider-auth message kinds the bridge extension reports.
const (
	AuthKindCatalog = "catalog"
	AuthKindProbe   = "probe"
	AuthKindEvent   = "event"
	AuthKindPrompt  = "prompt"
	AuthKindResult  = "result"
)

// Native login method discriminators carried on an AuthRequest.
const (
	AuthMethodOAuth = "oauth"
	AuthMethodAPI   = "api"
)

// AuthRequest is one provider-auth request encoded into the /acp-auth
// command argument.
type AuthRequest struct {
	ID          string   `json:"id"`
	Op          string   `json:"op"`
	ProviderID  string   `json:"providerId,omitempty"`
	Method      string   `json:"method,omitempty"`
	ProviderIDs []string `json:"providerIds,omitempty"`
}

// AuthProvider is one entry of the executed native provider catalog.
type AuthProvider struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	OAuth *AuthOAuthEntry `json:"oauth"`
	API   *AuthAPIEntry   `json:"api"`
}

// AuthOAuthEntry is a provider's native OAuth login method.
type AuthOAuthEntry struct {
	Name       string `json:"name"`
	LoginLabel string `json:"loginLabel,omitempty"`
}

// AuthAPIEntry is a provider's native api-key login method.
type AuthAPIEntry struct {
	Name string `json:"name"`
}

// AuthNativeEvent is one native login presentation event.
type AuthNativeEvent struct {
	Type            string `json:"type"`
	Message         string `json:"message,omitempty"`
	URL             string `json:"url,omitempty"`
	Instructions    string `json:"instructions,omitempty"`
	UserCode        string `json:"userCode,omitempty"`
	VerificationURI string `json:"verificationUri,omitempty"`
	IntervalSeconds int64  `json:"intervalSeconds,omitempty"`
	ExpiresIn       int64  `json:"expiresInSeconds,omitempty"`
}

// AuthMessage is the payload the bridge extension encodes into a marker
// dialog title.
type AuthMessage struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Prompt   string `json:"prompt,omitempty"`
	Message  string `json:"message,omitempty"`
	OK       bool   `json:"ok,omitempty"`
	Cause    string `json:"cause,omitempty"`
	Expires  int64  `json:"expires,omitempty"`
	CredType string `json:"credentialType,omitempty"`

	Providers []AuthProvider    `json:"providers,omitempty"`
	Entries   map[string]string `json:"entries,omitempty"`
	Event     *AuthNativeEvent  `json:"event,omitempty"`
}

// Native prompt kinds the bridge relays from one login flow.
const (
	AuthPromptText       = "text"
	AuthPromptSecret     = "secret"
	AuthPromptSelect     = "select"
	AuthPromptManualCode = "manual_code"
)

// EncodeAuthCommand renders one provider-auth request as the prompt text that
// invokes the bridge command.
func EncodeAuthCommand(request AuthRequest) string {
	// AuthRequest is strings and a string slice, so encoding cannot fail.
	payload, _ := json.Marshal(request)

	return "/" + AuthCommandName + " " + string(payload)
}

// ParseAuthTitle recognizes a bridge provider-auth dialog title and decodes
// its payload. It reports false for every other dialog.
func ParseAuthTitle(title string) (AuthMessage, bool) {
	payload, found := strings.CutPrefix(title, AuthTitleMarker)
	if !found {
		return AuthMessage{}, false
	}

	var message AuthMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		return AuthMessage{}, false
	}

	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.Kind) == "" {
		return AuthMessage{}, false
	}

	return message, true
}

// WriteExtensions writes the wrapper-owned extensions into dir and returns
// their absolute paths in -e load order. The MCP extension is written only
// when includeMCP is set.
func WriteExtensions(dir string, includeMCP bool) ([]string, error) {
	if err := fsMkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create extension directory: %w", err)
	}

	bridgePath := filepath.Join(dir, BridgeExtensionFileName)
	if err := fsWriteFile(bridgePath, bridgeExtensionSource, 0o600); err != nil {
		return nil, fmt.Errorf("write bridge extension: %w", err)
	}

	paths := []string{bridgePath}

	if includeMCP {
		mcpPath := filepath.Join(dir, MCPExtensionFileName)
		if err := fsWriteFile(mcpPath, mcpExtensionSource, 0o600); err != nil {
			return nil, fmt.Errorf("write mcp extension: %w", err)
		}

		paths = append(paths, mcpPath)
	}

	return paths, nil
}

// PermissionPrompt is the payload the bridge extension encodes into a
// permission select-dialog title.
type PermissionPrompt struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Input      json.RawMessage `json:"input,omitempty"`
}

// ParsePermissionTitle recognizes a bridge permission dialog title and
// decodes its payload. It reports false for ordinary extension dialogs.
func ParsePermissionTitle(title string) (PermissionPrompt, bool) {
	payload, found := strings.CutPrefix(title, PermissionTitleMarker)
	if !found {
		return PermissionPrompt{}, false
	}

	var prompt PermissionPrompt
	if err := json.Unmarshal([]byte(payload), &prompt); err != nil {
		return PermissionPrompt{}, false
	}

	if strings.TrimSpace(prompt.ToolCallID) == "" {
		return PermissionPrompt{}, false
	}

	return prompt, true
}

// MCPServer is one entry of the per-session MCP config consumed by the MCP
// extension.
type MCPServer struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Command string            `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// MCPConfig is the per-session MCP server config consumed by the MCP
// extension.
type MCPConfig struct {
	Servers []MCPServer `json:"servers"`
}

// WriteMCPConfig writes the per-session MCP config into dir and returns its
// path for EnvMCPConfig.
func WriteMCPConfig(dir string, config MCPConfig) (string, error) {
	encoded, err := marshalMCPConfig(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode mcp config: %w", err)
	}

	path := filepath.Join(dir, MCPConfigFileName)
	if err := fsWriteFile(path, append(encoded, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write mcp config: %w", err)
	}

	return path, nil
}
