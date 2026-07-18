package piacp

import (
	"context"
	"encoding/json"

	"github.com/coder/acp-go-sdk"
)

// envAgentVersion carries the adapter version into the pi child so the MCP
// extension reports it as its MCP clientInfo version.
const envAgentVersion = "ACP_GO_PI_VERSION"

// emptyForkSessionError rejects forking a session with no conversation
// entries: pi's native clone fails on an empty session.
func emptyForkSessionError() *acp.RequestError {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: "cannot fork an empty session",
		jsonFieldField: acpFieldSessionID,
	})
}

// handleForkSession implements the _pi/session/fork extension method: the
// parent's mirrored rows hydrate a fresh pi process, whose native clone mints
// the child session id.
func (a *Agent) handleForkSession(
	ctx context.Context,
	raw json.RawMessage,
) (acp.UnstableForkSessionResponse, error) {
	var params acp.UnstableForkSessionRequest
	if err := json.Unmarshal(raw, &params); err != nil {
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	if err := params.Validate(); err != nil {
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	metaOptions, err := piOptionsFromMeta(params.Meta)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, lifecycleMetaError(err)
	}

	additionalDirectories := sessionAdditionalDirectories(params.AdditionalDirectories)
	if validationErr := validateSessionStartPaths(params.Cwd, additionalDirectories); validationErr != nil {
		return acp.UnstableForkSessionResponse{}, validationErr
	}

	mcpServers := stableMCPServers(params.McpServers)

	if a.isDeleted(params.SessionId) {
		return acp.UnstableForkSessionResponse{}, unknownSessionError()
	}

	// An active parent's latest turns may not be committed yet; commit them
	// so the fork hydrates the parent's current history.
	parent, parentErr := a.session(params.SessionId)
	if parentErr == nil {
		if commitErr := parent.commitMirror(ctx); commitErr != nil {
			return acp.UnstableForkSessionResponse{}, commitErr
		}
	}

	entries, err := a.loadStoreEntries(ctx, a.sessionStore(), SessionKey{SessionID: string(params.SessionId)})
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	if len(entries) == 0 {
		// An active session with nothing mirrored yet exists but has no
		// conversation to clone; only an id that is neither active nor stored
		// is unknown.
		if parent != nil {
			return acp.UnstableForkSessionResponse{}, emptyForkSessionError()
		}

		return acp.UnstableForkSessionResponse{}, unknownSessionError()
	}

	if !storeSessionHasContent(entries) {
		return acp.UnstableForkSessionResponse{}, emptyForkSessionError()
	}

	session, err := a.startAndStoreSession(ctx, sessionStart{
		Cwd:                   params.Cwd,
		AdditionalDirectories: additionalDirectories,
		McpServers:            mcpServers,
		ResumeID:              string(params.SessionId),
		HydrateEntries:        entries,
		ForkSession:           true,
		MetaOptions:           metaOptions,
		RawMessages:           rawMessageConfigFromMeta(params.Meta),
	})
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	session.emitCurrentUsageUpdate(ctx)

	return acp.UnstableForkSessionResponse{
		SessionId:     session.id,
		ConfigOptions: sessionUnstableConfigOptions(session),
	}, nil
}

// stableMCPServers converts the fork request's unstable MCP declarations into
// the stable shape shared by every session-start path.
func stableMCPServers(servers []acp.UnstableMcpServer) []acp.McpServer {
	converted := make([]acp.McpServer, 0, len(servers))

	for _, server := range servers {
		switch {
		case server.Http != nil:
			converted = append(converted, acp.McpServer{Http: &acp.McpServerHttpInline{
				Meta:    cloneAnyMap(server.Http.Meta),
				Headers: server.Http.Headers,
				Name:    server.Http.Name,
				Type:    server.Http.Type,
				Url:     server.Http.Url,
			}})
		case server.Sse != nil:
			converted = append(converted, acp.McpServer{Sse: &acp.McpServerSseInline{
				Meta:    cloneAnyMap(server.Sse.Meta),
				Headers: server.Sse.Headers,
				Name:    server.Sse.Name,
				Type:    server.Sse.Type,
				Url:     server.Sse.Url,
			}})
		case server.Acp != nil:
			converted = append(converted, acp.McpServer{Acp: &acp.McpServerAcpInline{
				Meta: cloneAnyMap(server.Acp.Meta),
				Id:   acp.McpServerAcpId(server.Acp.Id),
				Name: server.Acp.Name,
				Type: server.Acp.Type,
			}})
		case server.Stdio != nil:
			stdio := *server.Stdio
			converted = append(converted, acp.McpServer{Stdio: &stdio})
		default:
			converted = append(converted, acp.McpServer{})
		}
	}

	return converted
}
