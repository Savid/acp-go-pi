package piacp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	piacp "github.com/savid/acp-go-pi"
)

func ExampleNewPiOptions() {
	options := piacp.NewPiOptions(
		piacp.WithPiModel("provider/model"),
		piacp.WithPiThinkingLevel("high"),
		piacp.WithPiPermission("ask"),
	)

	fmt.Println(options.Model)
	fmt.Println(options.ThinkingLevel)
	fmt.Println(options.Permission)
	// Output:
	// provider/model
	// high
	// ask
}

func ExampleNewSessionRequest() {
	request := piacp.NewSessionRequest(
		"/workspace",
		piacp.WithSessionAdditionalDirectories("/shared"),
		piacp.WithSessionRawEvents(true),
	)

	fmt.Println(request.Cwd)
	fmt.Println(request.AdditionalDirectories[0])
	// Output:
	// /workspace
	// /shared
}

// ExampleServe_initialize embeds the agent over a pair of pipes — the same
// wiring a host uses for stdio — and reads the capabilities the handshake
// advertises.
func ExampleServe_initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()

	defer clientToAgentReader.Close()
	defer clientToAgentWriter.Close()
	defer agentToClientReader.Close()
	defer agentToClientWriter.Close()

	done := make(chan error, 1)

	go func() {
		done <- piacp.Serve(ctx, clientToAgentReader, agentToClientWriter)
	}()

	_, _ = fmt.Fprintln(clientToAgentWriter,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)

	line, _ := bufio.NewReader(agentToClientReader).ReadString('\n')

	cancel()
	_ = clientToAgentWriter.Close()
	<-done

	var response struct {
		Result struct {
			AuthMethods       []any `json:"authMethods"`
			AgentCapabilities struct {
				LoadSession     bool `json:"loadSession"`
				McpCapabilities struct {
					Http bool `json:"http"`
				} `json:"mcpCapabilities"`
				PromptCapabilities struct {
					Image bool `json:"image"`
				} `json:"promptCapabilities"`
				SessionCapabilities map[string]any `json:"sessionCapabilities"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}

	_ = json.Unmarshal([]byte(line), &response)

	capabilities := response.Result.AgentCapabilities

	fmt.Println(len(response.Result.AuthMethods) == 0)
	fmt.Println(capabilities.LoadSession)
	fmt.Println(capabilities.McpCapabilities.Http)
	fmt.Println(capabilities.PromptCapabilities.Image)

	names := make([]string, 0, len(capabilities.SessionCapabilities))
	for name := range capabilities.SessionCapabilities {
		names = append(names, name)
	}

	sort.Strings(names)
	fmt.Println(names)
	// Output:
	// true
	// true
	// true
	// true
	// [additionalDirectories close delete list resume]
}
