package piacp

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestTransportOrdersHooksAfterResponse(t *testing.T) {
	t.Parallel()

	input := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"s2","cwd":"/w"}}` + "\n" +
		`{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"_meta":{"acp-go.dev/lifecycle":{"version":1}}}}` + "\n" +
		`{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"s1","_meta":{"acp-go.dev/lifecycle":{"version":1.0}}}}` + "\n"

	var output bytes.Buffer

	tr := newTransport(bytes.NewBufferString(input), &output)
	consumed, err := io.ReadAll(tr.reader())
	require.NoError(t, err)
	require.Equal(t, input, string(consumed))

	require.JSONEq(t, `{"_meta":{"acp-go.dev/lifecycle":{"version":1}}}`, string(tr.takeRaw(rawKeyInitialize)))
	require.Nil(t, tr.takeRaw(rawKeyInitialize))

	raw := tr.takeRawPrompt("s1", map[string]any{wire.LifecycleKey: map[string]any{"version": float64(1)}})
	require.Contains(t, string(raw), `"version":1.0`)
	require.Nil(t, tr.takeRawPrompt("s1", nil))

	ran := make(chan acp.SessionId, 2)
	tr.registerHook("s1", func(context.Context) { ran <- "s1" })
	tr.registerHook("s2", func(context.Context) { ran <- "s2" })

	writer := tr.writer()
	_, err = writer.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s1"}}` + "\n"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionId("s1"), <-ran)

	_, err = writer.Write([]byte(`{"jsonrpc":"2.0","id":2,"error":{"code":1}}` + "\n"))
	require.NoError(t, err)

	select {
	case id := <-ran:
		t.Fatalf("hook %s ran after an error response", id)
	case <-time.After(50 * time.Millisecond):
	}

	require.Contains(t, output.String(), `"sessionId":"s1"`)
}

func TestTransportReleasesRequestWrite(t *testing.T) {
	t.Parallel()

	tr := newTransport(bytes.NewBuffer(nil), io.Discard)
	written := tr.awaitRequestWrite("act-1")

	frame := `{"jsonrpc":"2.0","id":9,"method":"session/request_permission","params":{"_meta":{"acp-go.dev/lifecycle":{"action":{"actionId":"act-1"}}}}}` + "\n"
	_, err := tr.writer().Write([]byte(frame[:10]))
	require.NoError(t, err)

	select {
	case <-written:
		t.Fatal("released before the frame completed")
	default:
	}

	_, err = tr.writer().Write([]byte(frame[10:]))
	require.NoError(t, err)

	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("request write not released")
	}
}
