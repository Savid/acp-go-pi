package piacp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

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

// ExampleServe_initialize embeds the agent over a pair of pipes, the same
// wiring a host uses for stdio, and reads the capabilities the handshake
// advertises.
func ExampleServe_initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()

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
				LoadSession         bool           `json:"loadSession"`
				SessionCapabilities map[string]any `json:"sessionCapabilities"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}

	_ = json.Unmarshal([]byte(line), &response)

	fmt.Println(len(response.Result.AuthMethods))
	fmt.Println(response.Result.AgentCapabilities.LoadSession)
	fmt.Println(len(response.Result.AgentCapabilities.SessionCapabilities))
	// Output:
	// 0
	// true
	// 5
}
