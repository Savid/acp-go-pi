package pi

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

//go:embed ext/acp-bridge.ts
var bridgeExtensionSource []byte

//go:embed ext/acp-mcp.ts
var mcpExtensionSource []byte

//go:embed ext/acp-path.ts
var pathExtensionSource []byte

var marshalMCPConfig = json.MarshalIndent

const (
	// BridgeExtensionFileName is the wrapper-owned permission bridge
	// extension published in the shared source cache.
	BridgeExtensionFileName = "acp-bridge.ts"
	// MCPExtensionFileName is the wrapper-owned MCP client extension written
	// when the session declares MCP servers.
	MCPExtensionFileName = "acp-mcp.ts"
	// PathExtensionFileName is the wrapper-owned native-shell PATH extension.
	PathExtensionFileName = "acp-path.ts"
	// MCPConfigFileName is the per-session MCP server config consumed by the
	// MCP extension.
	MCPConfigFileName = "acp-mcp-config.json"

	// sessionResidenceDir is the agent-directory subtree holding one residence
	// per session. A durable agent directory is shared by every session the
	// adapter runs, so nothing a session writes may live at a fixed path
	// inside it.
	sessionResidenceDir = ".acp-session"

	// residenceStagingSuffix names the carrier an immutable residence file is
	// written through before it is published under its final name.
	residenceStagingSuffix = ".staging"

	// EnvPermissionMode selects the bridge permission mode for one pi child.
	EnvPermissionMode = "ACP_GO_PI_PERMISSION"
	// EnvMCPConfig carries the MCP config path for one pi child.
	EnvMCPConfig = "ACP_GO_PI_MCP_CONFIG"
	// EnvExtraPathDirs carries the ordered native-shell PATH prefixes for one
	// pi child, encoded with the platform path-list separator.
	EnvExtraPathDirs = "ACP_GO_PI_EXTRA_PATH_DIRS"

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
	// AuthOpQuota reads account observations inside the native credential scope.
	AuthOpQuota = "quota"
)

// Provider-auth message kinds the bridge extension reports.
const (
	AuthKindCatalog = "catalog"
	AuthKindProbe   = "probe"
	AuthKindEvent   = "event"
	AuthKindPrompt  = "prompt"
	AuthKindResult  = "result"
	// AuthKindCancel names the one dialog the wrapper leaves unanswered while a
	// native login runs. Answering it aborts that login.
	AuthKindCancel = "cancel"
	// AuthKindQuota and AuthKindQuotaCancel are values-free quota bridge dialogs.
	AuthKindQuota       = "quota"
	AuthKindQuotaCancel = "quota_cancel"
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

	Providers []AuthProvider `json:"providers,omitempty"`
	// Entries answers one probe: for every provider the request named, the type
	// of the credential stored for it, or the empty string where nothing is
	// stored. The answer is total, so the value carries residence and the key
	// carries only the address.
	Entries map[string]string `json:"entries,omitempty"`
	Event   *AuthNativeEvent  `json:"event,omitempty"`
	// Options carries a native select prompt's option ids, which is the only
	// thing that tells the adapter which branch of a login-variant choice it
	// can broker.
	Options []string `json:"options,omitempty"`
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

// SessionResidence is one session's private subtree inside a pi agent
// directory. Sessions can share a single agent directory when a durable home is
// configured, and the files a session writes there carry its MCP stdio
// commands, arguments, environment, and credential-bearing HTTP headers, so
// every session gets a residence of its own and every file in it is published
// exactly once and left read-only.
type SessionResidence struct {
	root string
}

// SessionResidenceFiles are the launch paths one session's residence provides.
type SessionResidenceFiles struct {
	// ExtensionPaths are the wrapper-owned extensions in -e load order.
	ExtensionPaths []string
	// MCPConfigPath is the session's MCP config for EnvMCPConfig, empty when
	// the session declared no MCP servers.
	MCPConfigPath string
}

// CreateSessionResidence creates one session's residence under agentDir and
// publishes everything it holds. The extension sources are the same bytes for
// every session, so they live in the content-addressed store under extRoot
// rather than here: a path minted per session would make pi compile them again
// on every launch. What stays is the session's own MCP config. The residence
// is complete when it returns, so nothing ever writes into it again.
func CreateSessionResidence(
	extRoot string, agentDir string, mcp *MCPConfig,
) (*SessionResidence, SessionResidenceFiles, error) {
	shared, err := publishSharedExtensions(extRoot)
	if err != nil {
		return nil, SessionResidenceFiles{}, err
	}

	parent := filepath.Join(agentDir, sessionResidenceDir)
	if mkdirErr := fsMkdirAll(parent, 0o700); mkdirErr != nil {
		return nil, SessionResidenceFiles{}, fmt.Errorf("create session residence root: %w", mkdirErr)
	}

	root, err := fsMkdirTemp(parent, "session-*")
	if err != nil {
		return nil, SessionResidenceFiles{}, fmt.Errorf("create session residence: %w", err)
	}

	residence := &SessionResidence{root: root}

	files, err := residence.publishAll(shared, mcp)
	if err != nil {
		return nil, SessionResidenceFiles{}, errors.Join(err, residence.Remove())
	}

	return residence, files, nil
}

// Root is the residence directory holding this session's files.
func (r *SessionResidence) Root() string {
	return r.root
}

// Remove deletes this session's residence, and never anything else in the
// agent directory it may share with other sessions.
func (r *SessionResidence) Remove() error {
	if r == nil {
		return nil
	}

	if err := fsRemoveAll(r.root); err != nil {
		return fmt.Errorf("remove session residence: %w", err)
	}

	return nil
}

// publish writes one immutable residence file. Contents land in a staging
// carrier and are linked onto the final name, so a reader never observes a
// partial file and a name that already exists fails the write instead of
// replacing a file a running pi child was launched from.
func (r *SessionResidence) publish(name string, contents []byte) (string, error) {
	path := filepath.Join(r.root, name)
	staging := path + residenceStagingSuffix

	if err := fsWriteFile(staging, contents, 0o400); err != nil {
		return "", fmt.Errorf("stage session residence file %q: %w", name, err)
	}

	if err := fsLink(staging, path); err != nil {
		_ = fsRemove(staging)

		return "", fmt.Errorf("publish session residence file %q: %w", name, err)
	}

	if err := fsRemove(staging); err != nil {
		return "", fmt.Errorf("clean session residence staging %q: %w", name, err)
	}

	return path, nil
}

func (r *SessionResidence) publishAll(shared sharedExtensionPaths, mcp *MCPConfig) (SessionResidenceFiles, error) {
	files := SessionResidenceFiles{ExtensionPaths: []string{shared.bridge, shared.path}}

	if mcp == nil {
		return files, nil
	}

	files.ExtensionPaths = append(files.ExtensionPaths, shared.mcp)

	encoded, err := marshalMCPConfig(*mcp, "", "  ")
	if err != nil {
		return SessionResidenceFiles{}, fmt.Errorf("encode mcp config: %w", err)
	}

	files.MCPConfigPath, err = r.publish(MCPConfigFileName, append(encoded, '\n'))
	if err != nil {
		return SessionResidenceFiles{}, err
	}

	return files, nil
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
