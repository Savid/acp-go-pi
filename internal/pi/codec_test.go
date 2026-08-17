package pi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineReaderFraming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "lf delimited records",
			input: "{\"a\":1}\n{\"b\":2}\n",
			want:  []string{`{"a":1}`, `{"b":2}`},
		},
		{
			name:  "crlf stripped",
			input: "{\"a\":1}\r\n{\"b\":2}\r\n",
			want:  []string{`{"a":1}`, `{"b":2}`},
		},
		{
			name:  "unicode separators are not delimiters",
			input: "{\"text\":\"a b c\"}\n",
			want:  []string{"{\"text\":\"a b c\"}"},
		},
		{
			name:  "final unterminated record",
			input: "{\"a\":1}\n{\"b\":2}",
			want:  []string{`{"a":1}`, `{"b":2}`},
		},
		{
			name:  "record larger than the reader buffer",
			input: `{"text":"` + strings.Repeat("x", 256<<10) + `"}` + "\n",
			want:  []string{`{"text":"` + strings.Repeat("x", 256<<10) + `"}`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reader := NewLineReader(strings.NewReader(test.input))

			got := make([]string, 0, len(test.want))

			for {
				line, err := reader.Next()
				if len(line) > 0 {
					got = append(got, string(line))
				}

				if err != nil {
					require.ErrorContains(t, err, "EOF")

					break
				}
			}

			require.Equal(t, test.want, got)
		})
	}
}

func TestLineReaderRejectsOversizeRecord(t *testing.T) {
	t.Parallel()

	reader := NewLineReader(strings.NewReader(strings.Repeat("x", maxLineBytes+1)))
	line, err := reader.Next()
	require.Nil(t, line)
	require.ErrorContains(t, err, "jsonl record exceeds")
}

func TestDecodeMessageClassification(t *testing.T) {
	t.Parallel()

	t.Run("response", func(t *testing.T) {
		t.Parallel()

		line := `{"id":"acp-1","type":"response","command":"get_state","success":true,"data":{"sessionId":"abc"}}`

		message, err := DecodeMessage([]byte(line))
		require.NoError(t, err)
		require.Equal(t, MessageKindResponse, message.Kind)
		require.Equal(t, "acp-1", message.Response.ID)
		require.Equal(t, "get_state", message.Response.Command)
		require.True(t, message.Response.Success)
		require.NoError(t, message.Response.Err())
		require.JSONEq(t, line, string(message.Response.RawJSON()))
	})

	t.Run("failed response yields command error", func(t *testing.T) {
		t.Parallel()

		line := `{"type":"response","command":"set_model","success":false,"error":"Model not found: nope/missing"}`

		message, err := DecodeMessage([]byte(line))
		require.NoError(t, err)

		commandErr := message.Response.Err()
		require.Error(t, commandErr)

		var typed *CommandError

		require.ErrorAs(t, commandErr, &typed)
		require.Equal(t, "set_model", typed.Command)
		require.Equal(t, "Model not found: nope/missing", typed.Message)
		require.Contains(t, commandErr.Error(), "Model not found")
	})

	t.Run("extension ui request", func(t *testing.T) {
		t.Parallel()

		line := `{"type":"extension_ui_request","id":"uuid-1","method":"select",` +
			`"title":"Allow?","options":["allow","deny"],"timeout":10000}`

		message, err := DecodeMessage([]byte(line))
		require.NoError(t, err)
		require.Equal(t, MessageKindUIRequest, message.Kind)
		require.Equal(t, "uuid-1", message.UIRequest.ID)
		require.Equal(t, "select", message.UIRequest.Method)
		require.Equal(t, []string{"allow", "deny"}, message.UIRequest.Options)
		require.NotNil(t, message.UIRequest.TimeoutMs)
		require.InDelta(t, 10000.0, *message.UIRequest.TimeoutMs, 0)
		require.True(t, message.UIRequest.IsDialog())
		require.JSONEq(t, line, string(message.UIRequest.RawJSON()))
	})

	t.Run("fire and forget ui request is not a dialog", func(t *testing.T) {
		t.Parallel()

		line := `{"type":"extension_ui_request","id":"uuid-2","method":"notify","message":"hi","notifyType":"info"}`

		message, err := DecodeMessage([]byte(line))
		require.NoError(t, err)
		require.False(t, message.UIRequest.IsDialog())
		require.Equal(t, "info", message.UIRequest.NotifyType)
	})

	t.Run("event", func(t *testing.T) {
		t.Parallel()

		message, err := DecodeMessage([]byte(`{"type":"agent_settled"}`))
		require.NoError(t, err)
		require.Equal(t, MessageKindEvent, message.Kind)
		require.Equal(t, "agent_settled", message.Event.Kind())
	})

	t.Run("malformed json", func(t *testing.T) {
		t.Parallel()

		_, err := DecodeMessage([]byte(`{"type":`))
		require.Error(t, err)
	})

	t.Run("malformed response body", func(t *testing.T) {
		t.Parallel()

		_, err := DecodeMessage([]byte(`{"type":"response","success":"nope"}`))
		require.Error(t, err)
	})

	t.Run("malformed ui request body", func(t *testing.T) {
		t.Parallel()

		_, err := DecodeMessage([]byte(`{"type":"extension_ui_request","options":"nope"}`))
		require.Error(t, err)
	})
}

func TestUIResponseBuilders(t *testing.T) {
	t.Parallel()

	value := UIValueResponse("id-1", "allow")
	require.Equal(t, "extension_ui_response", value.Type)
	require.Equal(t, "id-1", value.ID)
	require.NotNil(t, value.Value)
	require.Equal(t, "allow", *value.Value)

	confirmed := UIConfirmResponse("id-2", true)
	require.NotNil(t, confirmed.Confirmed)
	require.True(t, *confirmed.Confirmed)

	cancelled := UICancelResponse("id-3")
	require.True(t, cancelled.Cancelled)
	require.Nil(t, cancelled.Value)
	require.Nil(t, cancelled.Confirmed)
}
