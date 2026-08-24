package pi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseModelRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		model   string
		want    ModelRef
		wantErr bool
	}{
		{
			name:  "provider and id",
			model: "openai/gpt-4o",
			want:  ModelRef{Provider: "openai", ID: "gpt-4o"},
		},
		{
			name:  "id may contain slashes",
			model: "openrouter/meta/llama-3",
			want:  ModelRef{Provider: "openrouter", ID: "meta/llama-3"},
		},
		{name: "missing slash", model: "gpt-4o", wantErr: true},
		{name: "empty provider", model: "/id", wantErr: true},
		{name: "empty id", model: "provider/", wantErr: true},
		{name: "empty", model: "", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ref, err := ParseModelRef(test.model)
			if test.wantErr {
				require.ErrorContains(t, err, "provider/id")

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, ref)
			require.Equal(t, test.model, ref.String())
		})
	}
}

func TestThinkingLevels(t *testing.T) {
	t.Parallel()

	levels := ThinkingLevels()
	require.Equal(t, []string{"off", "minimal", "low", "medium", "high", "xhigh", "max"}, levels)

	// The returned slice is a copy.
	levels[0] = "mutated"
	require.Equal(t, "off", ThinkingLevels()[0])
}
