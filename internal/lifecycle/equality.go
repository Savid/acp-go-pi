package lifecycle

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
)

// Lifecycle value equality is the single comparison this extension uses wherever
// it asks whether two deliveries say the same thing: whole-notification equality
// for a retransmission, member-wise equality for a terminal restatement. Two
// values are equal when their decoded forms are deeply equal — key order and
// insignificant whitespace are not differences — and JSON numbers compare as
// exact mathematical values.
//
// That number rule is deliberately stricter than RFC 8785, which collapses
// numbers onto doubles: a consumer that certifies a boundary has to decide
// equality losslessly, so nothing in this file ever sees a float64. The scope is
// this extension alone; the byte-exact fences elsewhere in the family contract
// stay byte-exact and never route through here.

// decodeValue reads one JSON document into the form value equality compares.
// Numbers are retained as their lexemes, which is what makes the comparison
// lossless and what lets a literal whose expansion no machine holds decode at
// all.
func decodeValue(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err //nolint:wrapcheck // The caller names the refusal this decode reports.
	}

	return value, nil
}

// valueEqual decides lifecycle value equality over two decoded documents.
func valueEqual(left, right any) bool {
	switch typed := left.(type) {
	case json.Number:
		other, ok := right.(json.Number)

		return ok && numbersEqual(typed, other)
	case map[string]any:
		return objectsEqual(typed, right)
	case []any:
		return arraysEqual(typed, right)
	default:
		return left == right
	}
}

func objectsEqual(left map[string]any, right any) bool {
	other, ok := right.(map[string]any)
	if !ok || len(left) != len(other) {
		return false
	}

	for key, value := range left {
		counterpart, present := other[key]
		if !present || !valueEqual(value, counterpart) {
			return false
		}
	}

	return true
}

func arraysEqual(left []any, right any) bool {
	other, ok := right.([]any)
	if !ok || len(left) != len(other) {
		return false
	}

	for index := range left {
		if !valueEqual(left[index], other[index]) {
			return false
		}
	}

	return true
}

// progressEqual decides whether a carried progress object says what the reduced
// record already holds. A record that never carried progress differs from any
// delivery that carries one: absence is not a value.
func progressEqual(carried, recorded json.RawMessage) bool {
	if recorded == nil {
		return false
	}

	// Both documents came off the wire through the object check in the decoder,
	// so each decodes.
	decodedCarried, _ := decodeValue(carried)
	decodedRecorded, _ := decodeValue(recorded)

	return valueEqual(decodedCarried, decodedRecorded)
}

// decimal is one JSON number in the normalized form the comparison uses: a sign,
// a coefficient stripped of leading and trailing zeros, and the exponent that
// scales it. Every zero normalizes to the same empty coefficient, so -0 equals 0.
type decimal struct {
	negative bool
	digits   string
	exponent *big.Int
}

// numbersEqual compares two number lexemes as exact mathematical values without
// ever materializing them: the normalized triples decide the answer, so a
// twelve-byte literal with a nine-digit exponent costs what its bytes cost and
// two nineteen-digit integers a double would merge stay distinct.
func numbersEqual(left, right json.Number) bool {
	first, second := normalizeNumber(left.String()), normalizeNumber(right.String())

	if first.digits == "" || second.digits == "" {
		return first.digits == second.digits
	}

	return first.negative == second.negative &&
		first.digits == second.digits &&
		first.exponent.Cmp(second.exponent) == 0
}

// normalizeNumber reduces one lexeme to its decimal triple. The lexeme is a
// well-formed JSON number, because the decoder that produced it refused
// everything else.
func normalizeNumber(lexeme string) decimal {
	normalized := decimal{exponent: new(big.Int)}

	if strings.HasPrefix(lexeme, "-") {
		normalized.negative = true
		lexeme = lexeme[1:]
	}

	significand, exponent := lexeme, ""
	if index := strings.IndexAny(lexeme, "eE"); index >= 0 {
		significand, exponent = lexeme[:index], lexeme[index+1:]
	}

	digits := significand

	if index := strings.IndexByte(significand, '.'); index >= 0 {
		digits = significand[:index] + significand[index+1:]
		normalized.exponent.SetInt64(int64(index + 1 - len(significand)))
	}

	if exponent != "" {
		// The stated exponent scales the coefficient rather than expanding it, so
		// an arbitrarily large one stays arithmetic on one small integer.
		stated := new(big.Int)
		stated.SetString(exponent, 10)
		normalized.exponent.Add(normalized.exponent, stated)
	}

	digits = strings.TrimLeft(digits, "0")
	normalized.digits = strings.TrimRight(digits, "0")
	normalized.exponent.Add(normalized.exponent, big.NewInt(int64(len(digits)-len(normalized.digits))))

	return normalized
}
