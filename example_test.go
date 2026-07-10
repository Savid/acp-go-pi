package piacp_test

import (
	"fmt"

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
