package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testHarness wires a Client to fake pi stdin/stdout streams.
type testHarness struct {
	client   *Client
	commands *bufio.Scanner
	stdout   *io.PipeWriter
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	stdinRead, stdinWrite := io.Pipe()
	stdoutRead, stdoutWrite := io.Pipe()

	client := NewClient(stdinWrite, stdoutRead)
	require.NoError(t, client.Start(t.Context()))

	scanner := bufio.NewScanner(stdinRead)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)

	t.Cleanup(func() {
		_ = stdoutWrite.Close()
		_ = stdinWrite.Close()
		_ = client.Stop()
	})

	return &testHarness{client: client, commands: scanner, stdout: stdoutWrite}
}

// nextCommand reads one JSONL command the client wrote to pi stdin.
func (h *testHarness) nextCommand(t *testing.T) map[string]any {
	t.Helper()

	require.True(t, h.commands.Scan(), "expected a command line")

	var fields map[string]any

	require.NoError(t, json.Unmarshal(h.commands.Bytes(), &fields))

	return fields
}

// emit writes one JSONL record on the fake pi stdout.
func (h *testHarness) emit(t *testing.T, line string) {
	t.Helper()

	_, err := h.stdout.Write([]byte(line + "\n"))
	require.NoError(t, err)
}

// respond acks the given command id.
func (h *testHarness) respond(t *testing.T, id string, command string, body string) {
	t.Helper()

	line := `{"id":"` + id + `","type":"response","command":"` + command + `","success":true` + body + `}`
	h.emit(t, line)
}

func TestClientCallCorrelation(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	type result struct {
		state SessionState
		err   error
	}

	first := make(chan result, 1)
	second := make(chan result, 1)

	go func() {
		state, err := harness.client.GetState(t.Context())
		first <- result{state: state, err: err}
	}()

	firstCommand := harness.nextCommand(t)
	require.Equal(t, "get_state", firstCommand["type"])

	firstID, ok := firstCommand["id"].(string)
	require.True(t, ok)

	go func() {
		state, err := harness.client.GetState(t.Context())
		second <- result{state: state, err: err}
	}()

	secondCommand := harness.nextCommand(t)

	secondID, ok := secondCommand["id"].(string)
	require.True(t, ok)
	require.NotEqual(t, firstID, secondID)

	// Answer out of order: the second command first.
	harness.respond(t, secondID, "get_state", `,"data":{"sessionId":"second"}`)
	harness.respond(t, firstID, "get_state", `,"data":{"sessionId":"first"}`)

	secondResult := <-second
	require.NoError(t, secondResult.err)
	require.Equal(t, "second", secondResult.state.SessionID)

	firstResult := <-first
	require.NoError(t, firstResult.err)
	require.Equal(t, "first", firstResult.state.SessionID)
}

func TestClientEventsBeforeResponseBarrier(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	done := make(chan error, 1)

	go func() {
		done <- harness.client.Abort(t.Context())
	}()

	command := harness.nextCommand(t)
	require.Equal(t, "abort", command["type"])

	id, ok := command["id"].(string)
	require.True(t, ok)

	// pi emits the terminal events before the abort ack. Both lines go out in
	// one write so the pipe write cannot block on the undelivered event.
	harness.emit(t, `{"type":"agent_settled"}`+"\n"+
		`{"id":"`+id+`","type":"response","command":"abort","success":true}`)

	// The response cannot resolve until the event is consumed: delivery is
	// synchronous in stream order.
	select {
	case <-done:
		t.Fatal("abort resolved before the preceding event was consumed")
	case <-time.After(50 * time.Millisecond):
	}

	event := <-harness.client.Events()
	require.Equal(t, "agent_settled", event.Kind())
	require.NoError(t, <-done)
}

func TestClientUIRequestRoundTrip(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	harness.emit(t, `{"type":"extension_ui_request","id":"uuid-1","method":"select",`+
		`"title":"acp-go-pi:permission:{\"toolName\":\"bash\",\"input\":{\"command\":\"ls\"}}",`+
		`"options":["allow","deny"]}`)

	request := <-harness.client.UIRequests()
	require.Equal(t, "uuid-1", request.ID)

	prompt, ok := ParsePermissionTitle(request.Title)
	require.True(t, ok)
	require.Equal(t, "bash", prompt.ToolName)

	// RespondUI blocks on the synchronous pipe until the fake reads it.
	respondErr := make(chan error, 1)

	go func() {
		respondErr <- harness.client.RespondUI(UIValueResponse(request.ID, "allow"))
	}()

	response := harness.nextCommand(t)
	require.NoError(t, <-respondErr)
	require.Equal(t, "extension_ui_response", response["type"])
	require.Equal(t, "uuid-1", response["id"])
	require.Equal(t, "allow", response["value"])
}

func TestClientSkipsMalformedLines(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	harness.emit(t, `{"type":`)
	harness.emit(t, `not json at all`)
	harness.emit(t, `   `)
	harness.emit(t, `{"type":"agent_start"}`)

	event := <-harness.client.Events()
	require.Equal(t, "agent_start", event.Kind())
	require.Equal(t, uint64(2), harness.client.DecodeFailures())
}

func TestClientStrayResponsesAreCounted(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	harness.emit(t, `{"type":"response","command":"parse","success":false,"error":"Failed to parse command"}`)
	harness.emit(t, `{"type":"agent_start"}`)

	event := <-harness.client.Events()
	require.Equal(t, "agent_start", event.Kind())
	require.Equal(t, uint64(1), harness.client.StrayResponses())
}

func TestClientTransportEOFFailsPending(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	done := make(chan error, 1)

	go func() {
		_, err := harness.client.Call(t.Context(), map[string]any{"type": "get_state"})
		done <- err
	}()

	harness.nextCommand(t)
	require.NoError(t, harness.stdout.Close())

	err := <-done
	require.ErrorIs(t, err, ErrTransportClosed)

	<-harness.client.Done()
	require.NoError(t, harness.client.Stop())

	// Calls after close fail immediately.
	_, err = harness.client.Call(t.Context(), map[string]any{"type": "abort"})
	require.ErrorIs(t, err, ErrTransportClosed)
}

func TestClientReadErrorIsReported(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	require.NoError(t, harness.stdout.CloseWithError(io.ErrUnexpectedEOF))

	<-harness.client.Done()
	require.ErrorIs(t, harness.client.Err(), io.ErrUnexpectedEOF)
	require.ErrorIs(t, harness.client.Stop(), io.ErrUnexpectedEOF)
}

func TestClientCallContextCancel(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)

	go func() {
		_, err := harness.client.Call(ctx, map[string]any{"type": "get_state"})
		done <- err
	}()

	harness.nextCommand(t)
	cancel()

	require.ErrorIs(t, <-done, context.Canceled)
}

func TestClientCallRequiresType(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	_, err := harness.client.Call(t.Context(), map[string]any{"message": "hi"})
	require.ErrorContains(t, err, "requires a type field")
}

func TestClientDoubleStartFails(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)
	require.ErrorContains(t, harness.client.Start(t.Context()), "already started")
}

func TestClientStartContextCancelStopsLoop(t *testing.T) {
	t.Parallel()

	stdinRead, stdinWrite := io.Pipe()
	stdoutRead, stdoutWrite := io.Pipe()

	t.Cleanup(func() {
		_ = stdinRead.Close()
		_ = stdoutWrite.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	client := NewClient(stdinWrite, stdoutRead)
	require.NoError(t, client.Start(ctx))

	// Emit an event nobody consumes: the loop blocks delivering it, then the
	// context cancel releases the loop.
	_, err := stdoutWrite.Write([]byte(`{"type":"agent_start"}` + "\n"))
	require.NoError(t, err)

	cancel()

	<-client.Done()
	require.NoError(t, stdoutRead.Close())
	require.ErrorIs(t, client.Stop(), context.Canceled)
}

func TestClientWriteFailures(t *testing.T) {
	t.Parallel()

	t.Run("unencodable payload", func(t *testing.T) {
		t.Parallel()

		harness := newTestHarness(t)

		_, err := harness.client.Call(t.Context(), map[string]any{
			"type": "prompt",
			"bad":  func() {},
		})
		require.ErrorContains(t, err, "encode jsonl record")
	})

	t.Run("closed stdin fails the write", func(t *testing.T) {
		t.Parallel()

		stdinRead, stdinWrite := io.Pipe()
		stdoutRead, stdoutWrite := io.Pipe()

		client := NewClient(stdinWrite, stdoutRead)
		require.NoError(t, client.Start(t.Context()))

		t.Cleanup(func() {
			_ = stdoutWrite.Close()
			_ = client.Stop()
		})

		require.NoError(t, stdinRead.Close())

		_, err := client.Call(t.Context(), map[string]any{"type": "get_state"})
		require.ErrorContains(t, err, "write get_state command")

		// The typed wrappers surface the same write failure.
		require.ErrorContains(t, client.Abort(t.Context()), "write abort command")

		_, err = client.GetState(t.Context())
		require.ErrorContains(t, err, "write get_state command")

		require.ErrorContains(t, client.RespondUI(UICancelResponse("id")), "write extension ui response")
	})
}

func TestClientClosedErrorCarriesReadFailure(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	done := make(chan error, 1)

	go func() {
		_, err := harness.client.Call(t.Context(), map[string]any{"type": "get_state"})
		done <- err
	}()

	harness.nextCommand(t)
	require.NoError(t, harness.stdout.CloseWithError(io.ErrUnexpectedEOF))

	err := <-done
	require.ErrorIs(t, err, ErrTransportClosed)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestClientCommandErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		invoke func(client *Client) error
	}{
		{
			name:   "simple command failure",
			invoke: func(client *Client) error { return client.Abort(t.Context()) },
		},
		{
			name: "cancellable command failure",
			invoke: func(client *Client) error {
				_, err := client.Clone(t.Context())

				return err
			},
		},
		{
			name: "list models failure",
			invoke: func(client *Client) error {
				_, err := client.GetAvailableModels(t.Context())

				return err
			},
		},
		{
			name: "list commands failure",
			invoke: func(client *Client) error {
				_, err := client.GetCommands(t.Context())

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := scriptCall(t, func(t *testing.T, harness *testHarness) {
				t.Helper()

				command := harness.nextCommand(t)
				commandType, ok := command["type"].(string)
				require.True(t, ok)
				harness.emit(t, `{"id":"`+commandID(t, command)+`","type":"response","command":"`+
					commandType+`","success":false,"error":"Session file is empty"}`)
			}, test.invoke)

			var commandErr *CommandError

			require.ErrorAs(t, err, &commandErr)
			require.Equal(t, "Session file is empty", commandErr.Message)
		})
	}
}

func TestClientRespondUIKeepsExplicitType(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	respondErr := make(chan error, 1)

	go func() {
		respondErr <- harness.client.RespondUI(UIConfirmResponse("id-1", false))
	}()

	response := harness.nextCommand(t)
	require.NoError(t, <-respondErr)
	require.Equal(t, "extension_ui_response", response["type"])
	require.Equal(t, false, response["confirmed"])
}

func TestClientRespondUIDefaultsType(t *testing.T) {
	t.Parallel()

	harness := newTestHarness(t)

	respondErr := make(chan error, 1)

	go func() {
		respondErr <- harness.client.RespondUI(UIResponse{ID: "id-2", Cancelled: true})
	}()

	response := harness.nextCommand(t)
	require.NoError(t, <-respondErr)
	require.Equal(t, "extension_ui_response", response["type"])
	require.Equal(t, true, response["cancelled"])
}

func TestClientContextCancelDuringUIRequestDelivery(t *testing.T) {
	t.Parallel()

	stdinRead, stdinWrite := io.Pipe()
	stdoutRead, stdoutWrite := io.Pipe()

	t.Cleanup(func() {
		_ = stdinRead.Close()
		_ = stdoutWrite.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	client := NewClient(stdinWrite, stdoutRead)
	require.NoError(t, client.Start(ctx))

	// Emit a ui request nobody consumes: the loop blocks delivering it, then
	// the context cancel releases the loop.
	_, err := stdoutWrite.Write([]byte(`{"type":"extension_ui_request","id":"u1","method":"input","title":"t"}` + "\n"))
	require.NoError(t, err)

	cancel()

	<-client.Done()
	require.NoError(t, stdoutRead.Close())
	require.ErrorIs(t, client.Err(), context.Canceled)
}
