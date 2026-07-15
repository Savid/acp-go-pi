package piacp

import (
	"context"
	"encoding/json"

	"github.com/coder/acp-go-sdk"
)

// configModel is the session config option id carrying the model selector.
const configModel acp.SessionConfigId = "model"

// SessionRequestOption configures embedded-Go ACP session lifecycle requests.
type SessionRequestOption func(*sessionRequestConfig)

type sessionRequestConfig struct {
	additionalDirectories []string
	mcpServers            []acp.McpServer
	meta                  map[string]any
}

// NewSessionRequest constructs a session/new request with ACP-required empty
// slices initialized for embedded Go callers.
func NewSessionRequest(cwd string, opts ...SessionRequestOption) acp.NewSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.NewSessionRequest{
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// LoadSessionRequest constructs a session/load request with ACP-required empty
// slices initialized for embedded Go callers.
func LoadSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.LoadSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.LoadSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// ResumeSessionRequest constructs a session/resume request.
func ResumeSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.ResumeSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.ResumeSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            config.stableMCPServers(),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// ForkSessionRequest constructs params for the pi fork extension method.
func ForkSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.UnstableForkSessionRequest {
	config := newSessionRequestConfig(opts...)

	return acp.UnstableForkSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            unstableMCPServersFromStable(config.stableMCPServers()),
		AdditionalDirectories: config.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(config.meta),
	}
}

// CallForkSession calls the pi fork extension method and decodes the SDK payload shape.
func CallForkSession(
	ctx context.Context,
	conn *acp.ClientSideConnection,
	params acp.UnstableForkSessionRequest,
) (acp.UnstableForkSessionResponse, error) {
	raw, err := conn.CallExtension(ctx, ForkSessionMethod, params)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	var resp acp.UnstableForkSessionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	return resp, nil
}

// WithSessionMCPServers sets MCP servers for a session lifecycle request.
func WithSessionMCPServers(servers ...acp.McpServer) SessionRequestOption {
	cloned := cloneMCPServers(servers)

	return func(config *sessionRequestConfig) {
		config.mcpServers = cloneMCPServers(cloned)
	}
}

// WithSessionAdditionalDirectories sets additional workspace directories for a
// session lifecycle request.
func WithSessionAdditionalDirectories(paths ...string) SessionRequestOption {
	cloned := append([]string(nil), paths...)

	return func(config *sessionRequestConfig) {
		config.additionalDirectories = append([]string(nil), cloned...)
	}
}

// WithSessionMeta merges metadata into a session lifecycle request.
func WithSessionMeta(meta map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(meta)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned)
	}
}

// WithSessionPiOptions merges pi-specific options into a session lifecycle
// request's _meta.pi.options object.
func WithSessionPiOptions(options PiOptions) SessionRequestOption {
	cloned := clonePiOptions(options)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, cloned.Meta())
	}
}

// WithSessionOutputSchema sets JSON Schema structured output for a session
// lifecycle request. pi has no native structured-output surface, so sessions
// carrying it fail closed at session start.
func WithSessionOutputSchema(schema map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(schema)

	return func(config *sessionRequestConfig) {
		config.meta = mergeAnyMap(config.meta, PiOptions{OutputSchema: cloned}.Meta())
	}
}

// WithSessionRawEvents toggles raw pi event emission for a session lifecycle request.
func WithSessionRawEvents(enabled bool) SessionRequestOption {
	return func(config *sessionRequestConfig) {
		if config.meta == nil {
			config.meta = map[string]any{}
		}

		piMeta := ensureMetaMap(config.meta, piMetaKey)
		piMeta[metaRawEventKey] = map[string]any{metaRawEventEnabledKey: enabled}
		config.meta[piMetaKey] = piMeta
	}
}

func newSessionRequestConfig(opts ...SessionRequestOption) sessionRequestConfig {
	config := sessionRequestConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	return config
}

func (config sessionRequestConfig) stableMCPServers() []acp.McpServer {
	if config.mcpServers == nil {
		return []acp.McpServer{}
	}

	return cloneMCPServers(config.mcpServers)
}

func (config sessionRequestConfig) additionalDirectoriesClone() []string {
	return append([]string(nil), config.additionalDirectories...)
}

// PromptRequest constructs a session/prompt request with a non-nil prompt
// slice for embedded Go callers.
func PromptRequest(sessionID acp.SessionId, turnNonce string, blocks ...acp.ContentBlock) acp.PromptRequest {
	return acp.PromptRequest{
		SessionId: sessionID,
		Meta:      turnRouteMeta(turnNonce),
		Prompt:    append([]acp.ContentBlock{}, blocks...),
	}
}

// TextPromptRequest constructs a session/prompt request containing one text
// content block.
func TextPromptRequest(sessionID acp.SessionId, turnNonce, text string) acp.PromptRequest {
	return PromptRequest(sessionID, turnNonce, acp.TextBlock(text))
}

// CancelRequest builds an active-turn cancellation carrying the mandatory route nonce.
func CancelRequest(sessionID acp.SessionId, turnNonce string) acp.CancelNotification {
	return acp.CancelNotification{SessionId: sessionID, Meta: turnRouteMeta(turnNonce)}
}

// SetConfigOptionRequest constructs a value-id session/set_config_option request.
func SetConfigOptionRequest(
	sessionID acp.SessionId,
	configID acp.SessionConfigId,
	value acp.SessionConfigValueId,
) acp.SetSessionConfigOptionRequest {
	return acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			SessionId: sessionID,
			ConfigId:  configID,
			Value:     value,
		},
	}
}

// SetModelRequest constructs a model selector update request. Models are
// addressed as "provider/id".
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}

// DeleteSessionRequest constructs a session/delete request.
func DeleteSessionRequest(sessionID acp.SessionId) acp.UnstableDeleteSessionRequest {
	return acp.UnstableDeleteSessionRequest{SessionId: sessionID}
}

// StdioMCPServer constructs an ACP stdio MCP server declaration.
func StdioMCPServer(name string, command string, args []string, env map[string]string) acp.McpServer {
	variables := make([]acp.EnvVariable, 0, len(env))
	for key, value := range env {
		variables = append(variables, acp.EnvVariable{Name: key, Value: value})
	}

	return acp.McpServer{
		Stdio: &acp.McpServerStdio{
			Name:    name,
			Command: command,
			Args:    append([]string(nil), args...),
			Env:     variables,
		},
	}
}

// HTTPMCPServer constructs an ACP HTTP MCP server declaration.
func HTTPMCPServer(name string, url string, headers map[string]string) acp.McpServer {
	values := make([]acp.HttpHeader, 0, len(headers))
	for key, value := range headers {
		values = append(values, acp.HttpHeader{Name: key, Value: value})
	}

	return acp.McpServer{
		Http: &acp.McpServerHttpInline{
			Name:    name,
			Url:     url,
			Headers: values,
		},
	}
}

// ListSessionsRequestOption configures embedded-Go session/list requests.
type ListSessionsRequestOption func(*acp.ListSessionsRequest)

// ListSessionsRequest constructs a session/list request.
func ListSessionsRequest(opts ...ListSessionsRequestOption) acp.ListSessionsRequest {
	var req acp.ListSessionsRequest
	for _, opt := range opts {
		opt(&req)
	}

	return req
}

// WithListSessionsCwd filters session/list by cwd.
func WithListSessionsCwd(cwd string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		value := cwd
		req.Cwd = &value
	}
}

// WithListSessionsCursor sets the cursor for session/list pagination.
func WithListSessionsCursor(cursor string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		value := cursor
		req.Cursor = &value
	}
}

// WithListSessionsMeta sets metadata on a session/list request.
func WithListSessionsMeta(meta map[string]any) ListSessionsRequestOption {
	cloned := cloneAnyMap(meta)

	return func(req *acp.ListSessionsRequest) {
		req.Meta = mergeAnyMap(req.Meta, cloned)
	}
}

// PiOption configures PiOptions values.
type PiOption func(*PiOptions)

// NewPiOptions constructs PiOptions from functional options.
func NewPiOptions(opts ...PiOption) PiOptions {
	options := PiOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return clonePiOptions(options)
}

// WithPiModel configures the initial pi model as "provider/id".
func WithPiModel(model string) PiOption {
	return func(options *PiOptions) {
		options.Model = model
	}
}

// WithPiEnv configures pi session environment overrides.
func WithPiEnv(env map[string]string) PiOption {
	cloned := cloneStringMap(env)

	return func(options *PiOptions) {
		options.Env = cloneStringMap(cloned)
	}
}

// WithPiOutputSchema configures JSON Schema structured output. pi has no
// native structured-output surface, so sessions carrying it fail closed at
// session start.
func WithPiOutputSchema(schema map[string]any) PiOption {
	cloned := cloneAnyMap(schema)

	return func(options *PiOptions) {
		options.OutputSchema = cloneAnyMap(cloned)
	}
}

// WithPiThinkingLevel configures the pi reasoning level for the session.
func WithPiThinkingLevel(level string) PiOption {
	return func(options *PiOptions) {
		options.ThinkingLevel = level
	}
}

// WithPiPermission configures the adapter permission mode for the session:
// "ask" or "allow".
func WithPiPermission(mode string) PiOption {
	return func(options *PiOptions) {
		options.Permission = mode
	}
}

// WithPiAutoRetry opts the session in to pi's native automatic retry of
// transient provider errors. Off by default: without it a native failure
// surfaces once, immediately, with the real cause.
func WithPiAutoRetry(enabled bool) PiOption {
	return func(options *PiOptions) {
		options.AutoRetry = enabled
	}
}

func clonePiOptions(options PiOptions) PiOptions {
	cloned := options
	cloned.Env = cloneStringMap(options.Env)
	cloned.OutputSchema = cloneAnyMap(options.OutputSchema)

	return cloned
}

func mergeAnyMap(base map[string]any, overlay map[string]any) map[string]any {
	result := cloneAnyMap(base)
	if result == nil {
		result = map[string]any{}
	}

	for key, value := range overlay {
		if valueMap, ok := value.(map[string]any); ok {
			if existingMap, ok := result[key].(map[string]any); ok {
				result[key] = mergeAnyMap(existingMap, valueMap)

				continue
			}
		}

		result[key] = cloneAny(value)
	}

	return result
}

func ensureMetaMap(meta map[string]any, key string) map[string]any {
	current, _ := meta[key].(map[string]any)
	if current == nil {
		current = map[string]any{}
	} else {
		current = cloneAnyMap(current)
	}

	meta[key] = current

	return current
}

func cloneMCPServers(servers []acp.McpServer) []acp.McpServer {
	if servers == nil {
		return nil
	}

	cloned := make([]acp.McpServer, len(servers))
	for index, server := range servers {
		cloned[index] = cloneMCPServer(server)
	}

	return cloned
}

func cloneMCPServer(server acp.McpServer) acp.McpServer {
	switch {
	case server.Http != nil:
		value := *server.Http
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Http: &value}
	case server.Sse != nil:
		value := *server.Sse
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Sse: &value}
	case server.Acp != nil:
		value := *server.Acp
		value.Meta = cloneAnyMap(value.Meta)

		return acp.McpServer{Acp: &value}
	case server.Stdio != nil:
		value := cloneMCPServerStdio(server.Stdio)

		return acp.McpServer{Stdio: value}
	default:
		return acp.McpServer{}
	}
}

func cloneMCPServerStdio(server *acp.McpServerStdio) *acp.McpServerStdio {
	if server == nil {
		return nil
	}

	value := *server
	value.Meta = cloneAnyMap(value.Meta)
	value.Args = append([]string(nil), value.Args...)
	value.Env = cloneEnvVariables(value.Env)

	return &value
}

func cloneHTTPHeaders(headers []acp.HttpHeader) []acp.HttpHeader {
	if headers == nil {
		return nil
	}

	cloned := make([]acp.HttpHeader, len(headers))
	for index, header := range headers {
		cloned[index] = header
		cloned[index].Meta = cloneAnyMap(header.Meta)
	}

	return cloned
}

func cloneEnvVariables(env []acp.EnvVariable) []acp.EnvVariable {
	if env == nil {
		return nil
	}

	cloned := make([]acp.EnvVariable, len(env))
	for index, variable := range env {
		cloned[index] = variable
		cloned[index].Meta = cloneAnyMap(variable.Meta)
	}

	return cloned
}

func unstableMCPServersFromStable(servers []acp.McpServer) []acp.UnstableMcpServer {
	if servers == nil {
		return nil
	}

	cloned := make([]acp.UnstableMcpServer, len(servers))
	for index, server := range servers {
		cloned[index] = unstableMCPServerFromStable(server)
	}

	return cloned
}

func unstableMCPServerFromStable(server acp.McpServer) acp.UnstableMcpServer {
	switch {
	case server.Http != nil:
		value := acp.UnstableMcpServerHttp{
			Meta:    cloneAnyMap(server.Http.Meta),
			Headers: cloneHTTPHeaders(server.Http.Headers),
			Name:    server.Http.Name,
			Type:    server.Http.Type,
			Url:     server.Http.Url,
		}

		return acp.UnstableMcpServer{Http: &value}
	case server.Sse != nil:
		value := acp.UnstableMcpServerSse{
			Meta:    cloneAnyMap(server.Sse.Meta),
			Headers: cloneHTTPHeaders(server.Sse.Headers),
			Name:    server.Sse.Name,
			Type:    server.Sse.Type,
			Url:     server.Sse.Url,
		}

		return acp.UnstableMcpServer{Sse: &value}
	case server.Acp != nil:
		value := acp.UnstableMcpServerAcpInline{
			Meta: cloneAnyMap(server.Acp.Meta),
			Id:   acp.UnstableMcpServerAcpId(server.Acp.Id),
			Name: server.Acp.Name,
			Type: server.Acp.Type,
		}

		return acp.UnstableMcpServer{Acp: &value}
	case server.Stdio != nil:
		return acp.UnstableMcpServer{Stdio: cloneMCPServerStdio(server.Stdio)}
	default:
		return acp.UnstableMcpServer{}
	}
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}

	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = cloneAny(value)
	}

	return cloned
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneAny(item)
		}

		return cloned
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}
