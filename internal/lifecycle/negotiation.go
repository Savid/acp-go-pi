package lifecycle

import (
	"encoding/json"
	"math"
)

// MetaPath is the request path a rejection names. Negotiation and correlation
// values are rejected as invalid params rather than as stream violations,
// because they are read before any stream exists.
const MetaPath = `_meta["` + MetaKey + `"]`

// ParamError refuses a negotiation or correlation value. It names the exact
// member path so a host can tell which value it got wrong, and it is the one
// family literal this adapter validates on `initialize` itself.
type ParamError struct {
	// Field is the full request path, from MetaPath down to the offending
	// member.
	Field string
	// Missing reports that the key the contract requires was absent rather
	// than present and refused. The two verdicts are distinct: a host reading
	// the missing verdict adds the key, and a host reading the unsupported
	// verdict on the same bare path stops sending it on that surface.
	Missing bool
}

// Error implements error.
func (e *ParamError) Error() string {
	if e.Missing {
		return "missing " + e.Field
	}

	return "unsupported " + e.Field
}

func paramError(members ...string) *ParamError {
	field := MetaPath
	for _, member := range members {
		field += "." + member
	}

	return &ParamError{Field: field}
}

// missingParamError refuses the reserved key a surface required and the host
// omitted. It never names a member: nothing below an absent key exists.
func missingParamError() *ParamError {
	return &ParamError{Field: MetaPath, Missing: true}
}

// DecodeCapability reads the capability from `InitializeRequest._meta`. An absent value is
// reported as not present rather than as a refusal: the host asked for nothing,
// and the answer, every envelope, and every correlation read are then omitted for
// the whole connection.
func DecodeCapability(meta map[string]any) (bool, *ParamError) {
	raw, present := meta[MetaKey]
	if !present {
		return false, nil
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return false, paramError()
	}

	for key := range fields {
		if key != fieldVersion {
			return false, paramError(key)
		}
	}

	version, ok := integerValue(fields[fieldVersion])
	if !ok || version != Version {
		return false, paramError(fieldVersion)
	}

	return true, nil
}

// Submission names one accepted client prompt. The client nonce is the host's own
// input identity: it is distinct from every JSON-RPC message id, and neither is
// ever substituted for the other.
type Submission struct {
	SubmissionID string
	ClientNonce  string
	RunID        string
}

// DecodePromptCorrelation reads the value a `session/prompt` carries while
// version 1 is negotiated. The key is required when negotiated and forbidden when
// not, and either way the verdict is reached before the prompt is dispatched, so
// no frame is written to the harness.
func DecodePromptCorrelation(meta map[string]any, negotiated Negotiated) (Submission, *ParamError) {
	raw, present := meta[MetaKey]

	switch {
	case !negotiated.Present() && present:
		return Submission{}, paramError()
	case !negotiated.Present():
		return Submission{}, nil
	case !present:
		return Submission{}, missingParamError()
	}

	fields, ok := raw.(map[string]any)
	if !ok {
		return Submission{}, paramError()
	}

	for key := range fields {
		if key != fieldVersion && key != fieldSubmission {
			return Submission{}, paramError(key)
		}
	}

	if refusal := checkCorrelationVersion(fields, negotiated); refusal != nil {
		return Submission{}, refusal
	}

	return decodeSubmission(fields[fieldSubmission])
}

func checkCorrelationVersion(fields map[string]any, negotiated Negotiated) *ParamError {
	version, ok := integerValue(fields[fieldVersion])
	if !ok || version != Version || !negotiated.Present() {
		return paramError(fieldVersion)
	}

	return nil
}

// integerValue reads one JSON integer. A decoded wire value arrives as a float64
// and an embedding Go host writes an int, so both are the same integer; a
// fractional value is neither.
//
// Integrality alone does not make a float64 an integer here, and neither does
// fitting in an int64. The bounds are the bounds of the type the value is read
// into, so a magnitude past them is refused rather than narrowed to whatever
// that build's out-of-range conversion happens to produce — which is why the
// range is passed in rather than assumed, and why `1e300` is refused on every
// build and `1e10` on one whose int is 32 bits.
func integerValue(raw any) (int, bool) {
	value, ok := integerInRange(raw, math.MinInt, math.MaxInt)

	return int(value), ok
}

// integerInRange reads one JSON integer and refuses every value outside the
// inclusive bounds it is given.
func integerInRange(raw any, low int64, high int64) (int64, bool) {
	switch value := raw.(type) {
	case float64:
		if value != math.Trunc(value) || value < float64(low) || value >= float64(high)+1 {
			return 0, false
		}

		return int64(value), true
	case int:
		if int64(value) < low || int64(value) > high {
			return 0, false
		}

		return int64(value), true
	case json.Number:
		number, err := value.Int64()
		if err != nil || number < low || number > high {
			return 0, false
		}

		return number, true
	default:
		return 0, false
	}
}

func decodeSubmission(raw any) (Submission, *ParamError) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return Submission{}, paramError(fieldSubmission)
	}

	for key := range fields {
		if key != fieldSubmissionID && key != fieldClientNonce && key != fieldRunID {
			return Submission{}, paramError(fieldSubmission, key)
		}
	}

	submission := Submission{}

	for _, member := range []struct {
		key      string
		target   *string
		required bool
	}{
		{fieldSubmissionID, &submission.SubmissionID, true},
		{fieldClientNonce, &submission.ClientNonce, true},
		{fieldRunID, &submission.RunID, false},
	} {
		value, refusal := correlationIdentifier(fields, member.key, member.required)
		if refusal != nil {
			return Submission{}, refusal
		}

		*member.target = value
	}

	return submission, nil
}

// correlationIdentifier reads one opaque handle. An identifier is a correlation
// handle, not a payload: it is bounded, and it is never empty — an optional one
// is omitted rather than emptied, so a member present carrying the empty string
// is malformed rather than absent.
func correlationIdentifier(fields map[string]any, key string, required bool) (string, *ParamError) {
	raw, present := fields[key]
	if !present {
		if required {
			return "", paramError(fieldSubmission, key)
		}

		return "", nil
	}

	value, ok := raw.(string)
	if !ok || value == "" || len(value) > IdentifierBound {
		return "", paramError(fieldSubmission, key)
	}

	return value, nil
}
