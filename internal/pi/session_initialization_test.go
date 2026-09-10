package pi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitialSessionReadsBridgePrefix(t *testing.T) {
	t.Parallel()
	h := newTestHarness(t)
	h.emit(t, `{"type":"extension_ui_request","id":"init","method":"notify","message":"acp-go-pi:session-initialization:[{\"type\":\"session\",\"version\":3,\"id\":\"native-id\"}]"}`)
	rows, err := h.client.InitialSession(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.JSONEq(t, `{"type":"session","version":3,"id":"native-id"}`, string(rows[0]))
}

func TestInitialSessionFailsWhenUnavailable(t *testing.T) {
	t.Parallel()
	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()

		h := newTestHarness(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := h.client.InitialSession(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("malformed", func(t *testing.T) {
		t.Parallel()

		h := newTestHarness(t)
		h.emit(t, `{"type":"extension_ui_request","id":"init","method":"notify","message":"acp-go-pi:session-initialization:invalid"}`)
		_, err := h.client.InitialSession(t.Context())
		require.ErrorIs(t, err, ErrTransportClosed)
		require.ErrorIs(t, h.client.Err(), ErrJSONLStructural)
	})
}
