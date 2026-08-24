package lifecycle

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func offerMeta(versions any) map[string]any {
	return map[string]any{MetaKey: map[string]any{fieldVersions: versions}}
}

// TestDecodeOfferReportsAnAbsentOffer pins that the host asking for nothing is
// not a refusal.
func TestDecodeOfferReportsAnAbsentOffer(t *testing.T) {
	t.Parallel()

	offer, offered, refusal := DecodeOffer(nil)
	require.Nil(t, refusal)
	require.False(t, offered)
	require.Empty(t, offer.Versions)
}

// TestDecodeOfferStrictness pins that every refusal names the exact member path,
// and that versions is validated on every offer whatever the version.
func TestDecodeOfferStrictness(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"non-object", map[string]any{MetaKey: []any{1.0}}, MetaPath},
		{"unknown member", map[string]any{MetaKey: map[string]any{
			fieldVersions: []any{1.0}, "activityKinds": []any{},
		}}, MetaPath + ".activityKinds"},
		{"missing versions", map[string]any{MetaKey: map[string]any{}}, MetaPath + ".versions"},
		{"empty versions", offerMeta([]any{}), MetaPath + ".versions"},
		{"non-array versions", offerMeta(1.0), MetaPath + ".versions"},
		{"fractional version", offerMeta([]any{1.5}), MetaPath + ".versions"},
		{"unrepresentable version", offerMeta([]any{1e300}), MetaPath + ".versions"},
		{"unrepresentable negative version", offerMeta([]any{-1e300}), MetaPath + ".versions"},
		{"string version", offerMeta([]any{"1"}), MetaPath + ".versions"},
		{"unparsable number", offerMeta([]any{json.Number("one")}), MetaPath + ".versions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, offered, refusal := DecodeOffer(tc.meta)
			require.False(t, offered)
			require.NotNil(t, refusal)
			require.Equal(t, tc.field, refusal.Field)
			require.Equal(t, "unsupported "+tc.field, refusal.Error())
		})
	}
}

// TestOfferAnswerIntersects pins that the answer is the common set, that only the
// answer is ordered, and that an empty intersection answers nothing at all.
func TestOfferAnswerIntersects(t *testing.T) {
	t.Parallel()

	proven := Negotiated{ActivityKinds: []ActivityKind{}}

	answer, common := Offer{Versions: []int{1, 1, 2}}.Answer(proven)
	require.True(t, common)
	require.Equal(t, []int{1}, answer.Versions, "the intersection is duplicate-free")

	answer, common = Offer{Versions: []int{2, 1}}.Answer(proven)
	require.True(t, common)
	require.Equal(t, []int{1}, answer.Versions, "an unordered offer is intersected, not refused")

	_, common = Offer{Versions: []int{2, 3}}.Answer(proven)
	require.False(t, common)
}

// TestDecodeOfferReadsEveryIntegerSpelling pins that a decoded wire float64, an
// embedding host's int, and a number-preserving decoder's json.Number are the
// same offered version.
func TestDecodeOfferReadsEveryIntegerSpelling(t *testing.T) {
	t.Parallel()

	for _, versions := range []any{[]any{1.0}, []any{1}, []any{json.Number("1")}} {
		offer, offered, refusal := DecodeOffer(offerMeta(versions))
		require.Nil(t, refusal)
		require.True(t, offered)
		require.Equal(t, []int{1}, offer.Versions)
	}
}

// TestIntegerValueBoundsTheTypeItReadsInto pins the boundary of every integer
// spelling against the range the value is read into rather than against int64.
// A build whose int is narrower is the case the rule exists for: a value that
// fits an int64 and not the target names no value of that type, so it is
// refused instead of being narrowed to a different number.
func TestIntegerValueBoundsTheTypeItReadsInto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  any
		low  int64
		high int64
		want int64
		ok   bool
	}{
		{"float at the ceiling", float64(math.MaxInt32), math.MinInt32, math.MaxInt32, math.MaxInt32, true},
		{"float one past the ceiling", float64(math.MaxInt32) + 1, math.MinInt32, math.MaxInt32, 0, false},
		{"float at the floor", float64(math.MinInt32), math.MinInt32, math.MaxInt32, math.MinInt32, true},
		{"float one past the floor", float64(math.MinInt32) - 1, math.MinInt32, math.MaxInt32, 0, false},
		{"float past a narrower ceiling than int64", float64(1 << 40), math.MinInt32, math.MaxInt32, 0, false},
		{"float past every ceiling", 1e300, math.MinInt64, math.MaxInt64, 0, false},
		{"float past every floor", -1e300, math.MinInt64, math.MaxInt64, 0, false},
		{"fractional float", 1.5, math.MinInt64, math.MaxInt64, 0, false},
		{"largest float64 an int64 holds", float64(math.MaxInt64 - 1023), math.MinInt64, math.MaxInt64, math.MaxInt64 - 1023, true},
		{"host int at the ceiling", 10, -10, 10, 10, true},
		{"host int past the ceiling", 11, -10, 10, 0, false},
		{"host int past the floor", -11, -10, 10, 0, false},
		{"number at the ceiling", json.Number("2147483647"), math.MinInt32, math.MaxInt32, math.MaxInt32, true},
		{"number past the ceiling", json.Number("2147483648"), math.MinInt32, math.MaxInt32, 0, false},
		{"number past the floor", json.Number("-2147483649"), math.MinInt32, math.MaxInt32, 0, false},
		{"number past every ceiling", json.Number("9223372036854775808"), math.MinInt64, math.MaxInt64, 0, false},
		{"unparsable number", json.Number("one"), math.MinInt64, math.MaxInt64, 0, false},
		{"not a number at all", "1", math.MinInt64, math.MaxInt64, 0, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value, ok := integerInRange(test.raw, test.low, test.high)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, value)
		})
	}

	// The platform's own bounds are the ones integerValue reads into, so an
	// accepted value survives the narrowing unchanged.
	platform := []struct {
		raw  any
		want int
	}{
		{float64(math.MaxInt32), math.MaxInt32},
		{math.MaxInt, math.MaxInt},
		{json.Number("2147483647"), math.MaxInt32},
	}

	for _, test := range platform {
		value, ok := integerValue(test.raw)
		require.True(t, ok)
		require.Equal(t, test.want, value)
	}

	_, ok := integerValue(float64(math.MaxInt) + 1)
	require.False(t, ok, "a float past the platform ceiling was narrowed instead of refused")
}

func correlationMeta(value any) map[string]any {
	return map[string]any{MetaKey: value}
}

// TestDecodePromptCorrelationRequiresTheKeyWhileNegotiated pins both halves of
// the presence rule.
func TestDecodePromptCorrelationRequiresTheKeyWhileNegotiated(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Versions: []int{Version}}

	submission, refusal := DecodePromptCorrelation(nil, Negotiated{})
	require.Nil(t, refusal)
	require.Equal(t, Submission{}, submission)

	_, refusal = DecodePromptCorrelation(correlationMeta(map[string]any{}), Negotiated{})
	require.NotNil(t, refusal)
	require.Equal(t, MetaPath, refusal.Field)

	_, refusal = DecodePromptCorrelation(nil, negotiated)
	require.NotNil(t, refusal)
	require.Equal(t, MetaPath, refusal.Field)
}

// TestDecodePromptCorrelationStrictness pins the value's fixed shape: two members
// on the object, three inside the submission, and bounded opaque handles.
func TestDecodePromptCorrelationStrictness(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Versions: []int{Version}}
	submission := map[string]any{"submissionId": "sub-1", "clientNonce": "non-1"}

	for _, tc := range []struct {
		name  string
		value any
		field string
	}{
		{"non-object", "v1", MetaPath},
		{"unknown member", map[string]any{"version": 1.0, "submission": submission, "streamId": "x"}, MetaPath + ".streamId"},
		{"missing version", map[string]any{"submission": submission}, MetaPath + ".version"},
		{"fractional version", map[string]any{"version": 1.5, "submission": submission}, MetaPath + ".version"},
		{"unrepresentable version", map[string]any{"version": 1e300, "submission": submission}, MetaPath + ".version"},
		{"unsupported version", map[string]any{"version": 2.0, "submission": submission}, MetaPath + ".version"},
		{"missing submission", map[string]any{"version": 1.0}, MetaPath + ".submission"},
		{"unknown submission member", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "turnId": "t",
		}}, MetaPath + ".submission.turnId"},
		{"missing submission id", map[string]any{"version": 1.0, "submission": map[string]any{
			"clientNonce": "non-1",
		}}, MetaPath + ".submission.submissionId"},
		{"empty submission id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "", "clientNonce": "non-1",
		}}, MetaPath + ".submission.submissionId"},
		{"non-string nonce", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": 1.0,
		}}, MetaPath + ".submission.clientNonce"},
		{"over-bound run id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "runId": strings.Repeat("r", IdentifierBound+1),
		}}, MetaPath + ".submission.runId"},
		{"empty run id", map[string]any{"version": 1.0, "submission": map[string]any{
			"submissionId": "sub-1", "clientNonce": "non-1", "runId": "",
		}}, MetaPath + ".submission.runId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, refusal := DecodePromptCorrelation(correlationMeta(tc.value), negotiated)
			require.NotNil(t, refusal)
			require.Equal(t, tc.field, refusal.Field)
		})
	}
}

// TestDecodePromptCorrelationReadsTheWholeSubmission pins that an optional run id
// is read when present and left empty when absent.
func TestDecodePromptCorrelationReadsTheWholeSubmission(t *testing.T) {
	t.Parallel()

	negotiated := Negotiated{Versions: []int{Version}}

	submission, refusal := DecodePromptCorrelation(correlationMeta(map[string]any{
		"version":    1.0,
		"submission": map[string]any{"submissionId": "sub-1", "clientNonce": "non-1", "runId": "run-1"},
	}), negotiated)
	require.Nil(t, refusal)
	require.Equal(t, Submission{SubmissionID: "sub-1", ClientNonce: "non-1", RunID: "run-1"}, submission)

	submission, refusal = DecodePromptCorrelation(correlationMeta(map[string]any{
		"version":    1,
		"submission": map[string]any{"submissionId": "sub-1", "clientNonce": "non-1"},
	}), negotiated)
	require.Nil(t, refusal)
	require.Equal(t, Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, submission)
}
