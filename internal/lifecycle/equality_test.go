package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLifecycleNumberEqualityIsExactAndNonExpanding pins the number predicate
// this extension compares under: spelling is not content, sign is content
// except at zero, and a literal whose decimal expansion no machine holds is
// decided from its normalized triple rather than by expanding it.
func TestLifecycleNumberEqualityIsExactAndNonExpanding(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		left  string
		right string
		equal bool
	}{
		"integer against the same integer with a fraction part": {left: "1", right: "1.0", equal: true},
		"exponent against the expanded integer":                 {left: "1e2", right: "100", equal: true},
		"fraction scaled by an exponent":                        {left: "1.5e1", right: "15", equal: true},
		"trailing zeros in the coefficient":                     {left: "-1", right: "-1.000", equal: true},
		"every zero is the same zero":                           {left: "-0", right: "0.0e999", equal: true},
		"case of the exponent marker":                           {left: "1e999999999", right: "1E999999999", equal: true},
		"explicitly signed exponent":                            {left: "1e+2", right: "1e2", equal: true},
		"sign is content away from zero":                        {left: "-1", right: "1", equal: false},
		"integers a double would merge":                         {left: "1234567890123456788", right: "1234567890123456789", equal: false},
		"exponents that differ":                                 {left: "1e999999999", right: "1e999999998", equal: false},
		"zero against a value":                                  {left: "0", right: "1", equal: false},
		"negative exponents":                                    {left: "1e-3", right: "0.001", equal: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.equal, numbersEqual(json.Number(test.left), json.Number(test.right)))
			require.Equal(t, test.equal, numbersEqual(json.Number(test.right), json.Number(test.left)))
		})
	}
}

// TestLifecycleValueEqualityComparesDecodedForms pins the structural half of the
// predicate: key order and whitespace are not differences, and a value of a
// different shape never equals one of another.
func TestLifecycleValueEqualityComparesDecodedForms(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		left  string
		right string
		equal bool
	}{
		"key order and whitespace":  {left: `{"a":1,"b":[1,2]}`, right: "{\n \"b\" : [1, 2],\n \"a\": 1.0}", equal: true},
		"array element differs":     {left: `[1,2]`, right: `[1,3]`, equal: false},
		"array length differs":      {left: `[1,2]`, right: `[1]`, equal: false},
		"array against an object":   {left: `[1]`, right: `{"0":1}`, equal: false},
		"object length differs":     {left: `{"a":1}`, right: `{"a":1,"b":2}`, equal: false},
		"object key differs":        {left: `{"a":1}`, right: `{"b":1}`, equal: false},
		"number against a string":   {left: `1`, right: `"1"`, equal: false},
		"scalars and null":          {left: `{"a":null,"b":true,"c":"x"}`, right: `{"c":"x","b":true,"a":null}`, equal: true},
		"nested arrays of numbers":  {left: `[[1e1],[2]]`, right: `[[10],[2.0]]`, equal: true},
		"nested difference in deep": {left: `[[1e1],[2]]`, right: `[[10],[3]]`, equal: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			left, err := decodeValue(json.RawMessage(test.left))
			require.NoError(t, err)

			right, err := decodeValue(json.RawMessage(test.right))
			require.NoError(t, err)

			require.Equal(t, test.equal, valueEqual(left, right))
			require.Equal(t, test.equal, valueEqual(right, left))
		})
	}
}

// TestProgressEqualityTreatsAbsenceAsNotAValue pins that a record which never
// carried progress differs from any delivery that carries one, so the terminal
// restatement rule cannot read absence as agreement.
func TestProgressEqualityTreatsAbsenceAsNotAValue(t *testing.T) {
	t.Parallel()

	require.False(t, progressEqual(json.RawMessage(`{"stage":"final"}`), nil))
	require.True(t, progressEqual(json.RawMessage(`{"stage":"final","n":1}`), json.RawMessage(`{"n":1.0,"stage":"final"}`)))
	require.False(t, progressEqual(json.RawMessage(`{"stage":"final"}`), json.RawMessage(`{"stage":"done"}`)))
}

// TestHugeExponentLiteralsDecodeWithoutExpansion pins that a number the decoder
// would have to materialize to reject is decoded, retained, and compared as its
// lexeme. A decoder collapsing numbers onto doubles refuses this frame outright.
func TestHugeExponentLiteralsDecodeWithoutExpansion(t *testing.T) {
	t.Parallel()

	value, err := decodeValue(json.RawMessage(`{"n":1e999999999}`))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"n": json.Number("1e999999999")}, value)

	_, err = decodeValue(json.RawMessage(`{`))
	require.Error(t, err)
}
