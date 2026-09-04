//go:build windows

package piacp

// Windows reports the same path as simply not there: a component that is a file
// rather than a directory makes the whole name unresolvable, so the mapper sees
// a missing file. The refusal is the same; only which of the two closed
// verdicts the platform hands it differs.
const (
	handoffThroughFileError   = imageErrorMissingFile
	handoffThroughFileMessage = handoffFileAbsentMessage
)
