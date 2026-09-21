package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scriptedPi answers commands on stdin with canned stdout records.
type scriptedPi struct {
	t       *testing.T
	stdin   *io.PipeReader
	stdout  *io.PipeWriter
	answers func(command map[string]any) []string
}

func newScriptedPi(t *testing.T, answers func(map[string]any) []string) (*Client, *scriptedPi) {
	t.Helper()

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	scripted := &scriptedPi{t: t, stdin: stdinReader, stdout: stdoutWriter, answers: answers}

	go scripted.serve()

	client := NewClient(stdinWriter, stdoutReader)
	require.NoError(t, client.Start(context.Background()))
	require.Error(t, client.Start(context.Background()))

	return client, scripted
}

func (s *scriptedPi) serve() {
	scanner := bufio.NewScanner(s.stdin)

	for scanner.Scan() {
		var command map[string]any

		_ = json.Unmarshal(scanner.Bytes(), &command)

		for _, line := range s.answers(command) {
			_, _ = io.WriteString(s.stdout, line+"\n")
		}
	}

	_ = s.stdout.Close()
}

// drain consumes both streams and reports when the read loop has ended.
func drain(client *Client) <-chan struct{} {
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		for range client.Events() {
		}
	}()

	go func() {
		for range client.UIRequests() {
		}
	}()

	return stopped
}

func TestClientCallCorrelatesResponses(t *testing.T) {
	t.Parallel()

	client, scripted := newScriptedPi(t, func(command map[string]any) []string {
		id, _ := command["id"].(string)

		switch command["type"] {
		case "get_state":
			return []string{`{"type":"agent_start"}`, `{"type":"response","id":"` + id + `","command":"get_state","success":true,"data":{"sessionId":"s","thinkingLevel":"low"}}`}
		case "set_model":
			return []string{`{"type":"response","id":"` + id + `","command":"set_model","success":false,"error":"nope"}`}
		case "get_commands":
			return []string{`{"type":"response","id":"` + id + `","command":"get_commands","success":true}`}
		default:
			return []string{`{"type":"response","id":"stray","command":"x","success":true}`, `{"type":"response","id":"` + id + `","command":"` + fmt.Sprint(command["type"]) + `","success":true}`}
		}
	})

	events := make(chan Event, 8)
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		for event := range client.Events() {
			events <- event
		}
	}()

	go func() {
		for range client.UIRequests() {
		}
	}()

	state, err := client.GetState(context.Background())
	require.NoError(t, err)
	require.Equal(t, "s", state.SessionID)
	require.IsType(t, AgentStartEvent{}, <-events)

	_, err = client.SetModel(context.Background(), "a", "b")

	var commandErr *CommandError

	require.ErrorAs(t, err, &commandErr)
	require.Equal(t, "nope", commandErr.Message)

	_, err = client.GetCommands(context.Background())
	require.ErrorContains(t, err, "returned no data")

	require.NoError(t, client.Prompt(context.Background(), "hi", []ImageContent{NewImageContent("d", "image/png")}))
	require.NoError(t, client.Abort(context.Background()))
	require.NoError(t, client.SetThinkingLevel(context.Background(), "high"))
	require.NoError(t, client.SetAutoRetry(context.Background(), true))

	_, err = client.Call(context.Background(), map[string]any{})
	require.Error(t, err)

	require.NoError(t, client.RespondUI(UIResponse{ID: "u", Cancelled: true}))

	_ = scripted.stdin.Close()
	_ = scripted.stdout.Close()
	<-stopped
	require.NoError(t, client.Err())
}

func TestClientContextCancelAndClose(t *testing.T) {
	t.Parallel()

	client, scripted := newScriptedPi(t, func(map[string]any) []string { return nil })
	stopped := drain(client)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.GetState(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	_ = scripted.stdout.Close()
	<-stopped
	require.NoError(t, client.Err())

	_, err = client.GetState(context.Background())
	require.ErrorIs(t, err, ErrTransportClosed)
}

func TestClientStructuralFailure(t *testing.T) {
	t.Parallel()

	client, scripted := newScriptedPi(t, func(map[string]any) []string { return []string{"{not json"} })
	stopped := drain(client)

	_, err := client.GetState(context.Background())
	require.ErrorIs(t, err, ErrTransportClosed)
	require.ErrorIs(t, client.Err(), ErrJSONLStructural)

	<-stopped
	_ = scripted.stdin.Close()
}
