//go:build integration

package integration

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// attendedStdinProbeChildMarker puts a re-executed test binary into
// stdin-recording mode, with the recording path following it. Both travel in
// argv after the test flag terminator rather than in the environment: a new
// operator-visible variable is exactly what the family's enumerated
// integration namespace does not admit.
const attendedStdinProbeChildMarker = "acp-go-pi-record-child-stdin"

var errFailingWriter = errors.New("prompt stream closed")

// failingWriter stands for a prompt stream the operator cannot see, such as a
// closed terminal.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errFailingWriter }

// blockedReader never produces a byte and never ends. It stands for an operator
// who walked away from the terminal.
type blockedReader struct {
	released chan struct{}
}

func (r *blockedReader) Read([]byte) (int, error) {
	<-r.released

	return 0, io.EOF
}

type singleReaderProbe struct {
	released   chan struct{}
	overlapped chan struct{}
	once       sync.Once
	active     atomic.Int32
}

func (r *singleReaderProbe) Read([]byte) (int, error) {
	if r.active.Add(1) > 1 {
		r.once.Do(func() { close(r.overlapped) })
	}
	defer r.active.Add(-1)

	<-r.released

	return 0, io.EOF
}

// TestAttendedConsoleReadsTheAnswerFromSuppliedInput pins the carrier: the
// answer is whatever arrives on the stream the console was handed, so an
// operator piping a code in is answering the same way a terminal does.
func TestAttendedConsoleReadsTheAnswerFromSuppliedInput(t *testing.T) {
	var prompt bytes.Buffer
	console := newAttendedConsole(&prompt, strings.NewReader("  ABC-123  \n"))

	answer, err := console.ask("paste the authorization code", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "ABC-123", answer)
	require.Contains(t, prompt.String(), "paste the authorization code")
}

// TestAttendedConsoleNeverEchoesTheAnswer keeps the one credential this tier
// handles off the stream a run captures.
func TestAttendedConsoleNeverEchoesTheAnswer(t *testing.T) {
	const secret = "sk-canary-authorization-code"

	var prompt bytes.Buffer
	console := newAttendedConsole(&prompt, strings.NewReader(secret+"\n"))

	answer, err := console.ask("paste the authorization code", time.Minute)
	require.NoError(t, err)
	require.Equal(t, secret, answer)
	require.NotContains(t, prompt.String(), secret)
}

// TestAttendedConsoleKeepsUnreadInputForTheNextQuestion is why the console owns
// one retained reader: a fresh buffered reader per question drops everything
// the operator typed past the first newline, and the second question then
// blocks on input that was already consumed.
func TestAttendedConsoleKeepsUnreadInputForTheNextQuestion(t *testing.T) {
	var prompt bytes.Buffer
	console := newAttendedConsole(&prompt, strings.NewReader("anthropic\nABC-123\n"))

	provider, err := console.ask("provider", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "anthropic", provider)

	code, err := console.ask("code", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "ABC-123", code)
}

// TestAttendedConsoleAcceptsAFinalUnterminatedAnswer accepts the last line a
// pipe delivers without a trailing newline. Refusing it would fail a run the
// operator actually answered.
func TestAttendedConsoleAcceptsAFinalUnterminatedAnswer(t *testing.T) {
	var prompt bytes.Buffer
	console := newAttendedConsole(&prompt, strings.NewReader("ABC-123"))

	answer, err := console.ask("code", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "ABC-123", answer)
}

// TestAttendedConsoleFailsWithoutAnAnswer covers the two shapes of "nobody
// answered". Both must be errors: a tier that treats silence as success is the
// failure mode the attended gate exists to prevent.
func TestAttendedConsoleFailsWithoutAnAnswer(t *testing.T) {
	for name, input := range map[string]string{
		"closed input": "",
		"blank line":   "   \n",
	} {
		t.Run(name, func(t *testing.T) {
			var prompt bytes.Buffer
			console := newAttendedConsole(&prompt, strings.NewReader(input))

			_, err := console.ask("code", time.Minute)
			require.ErrorIs(t, err, errAttendedNoAnswer)
		})
	}
}

// TestAttendedConsoleFailsWhenNobodyAnswersInTime proves the deadline is real,
// so an abandoned attended run ends instead of parking a CI slot forever.
func TestAttendedConsoleFailsWhenNobodyAnswersInTime(t *testing.T) {
	reader := &blockedReader{released: make(chan struct{})}
	t.Cleanup(func() { close(reader.released) })

	var prompt bytes.Buffer
	console := newAttendedConsole(&prompt, reader)

	_, err := console.ask("code", 10*time.Millisecond)
	require.ErrorIs(t, err, errAttendedTimedOut)
}

func TestAttendedConsoleTimeoutKeepsOneInputReader(t *testing.T) {
	reader := &singleReaderProbe{released: make(chan struct{}), overlapped: make(chan struct{})}
	t.Cleanup(func() { close(reader.released) })

	console := newAttendedConsole(io.Discard, reader)
	_, err := console.ask("first", 10*time.Millisecond)
	require.ErrorIs(t, err, errAttendedTimedOut)
	_, err = console.ask("second", 10*time.Millisecond)
	require.ErrorIs(t, err, errAttendedTimedOut)

	select {
	case <-reader.overlapped:
		t.Fatal("attended console started concurrent readers after a timeout")
	default:
	}
}

// TestAttendedConsoleLeavesChildStdinIntact is the regression for the attended
// console being constructed at package init. This binary re-executes itself as
// the fake pi harness, and that child decodes pi's JSONL request stream from
// stdin; a console built at init started a buffered stdin reader in every such
// child and swallowed whole records before the harness saw them. The child here
// records every byte of its stdin, so any init-time reader shows up as a short
// or gap-ridden recording.
func TestAttendedConsoleLeavesChildStdinIntact(t *testing.T) {
	requireRunIntegration(t)

	recorded := filepath.Join(t.TempDir(), "child-stdin")

	// The payload deliberately exceeds the pipe buffer, so the parent's writer
	// blocks and the child has to read across a long window. A payload that
	// fits in the buffer can be drained whole by the child's first read before
	// an init-time reader is ever scheduled, which hides the defect.
	var stream bytes.Buffer
	for seq := range 65536 {
		fmt.Fprintf(&stream, "{\"type\":\"request\",\"seq\":%d}\n", seq)
	}

	command := exec.Command(os.Args[0], // #nosec G204 -- re-executes this fixed test helper.
		"-test.run=^TestAttendedStdinProbeChild$",
		"--", attendedStdinProbeChildMarker, recorded,
	)
	command.Stdin = bytes.NewReader(stream.Bytes())

	output, err := command.CombinedOutput()
	require.NoError(t, err, "child: %s", output)

	delivered, err := os.ReadFile(recorded)
	require.NoError(t, err)
	require.Equal(t, stream.Len(), len(delivered),
		"package init consumed stdin the re-executed child was meant to read")
	require.True(t, bytes.Equal(stream.Bytes(), delivered),
		"the bytes the re-executed child read are not the bytes the parent wrote")
}

// TestAttendedStdinProbeChild is the child half of the regression above. It is
// inert in a normal run and only becomes a stdin recorder when the parent
// passes the marker and a recording path in argv.
func TestAttendedStdinProbeChild(t *testing.T) {
	marker := slices.Index(os.Args, attendedStdinProbeChildMarker)
	if marker < 0 || marker+1 >= len(os.Args) {
		return
	}
	target := os.Args[marker+1]

	delivered, err := io.ReadAll(os.Stdin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(target, delivered, 0o600))
}

// TestAttendedConsoleReportsAnUnwritablePrompt fails the ask rather than
// reading an answer to a question the operator never saw.
func TestAttendedConsoleReportsAnUnwritablePrompt(t *testing.T) {
	console := newAttendedConsole(failingWriter{}, strings.NewReader("ABC-123\n"))

	_, err := console.ask("code", time.Minute)
	require.ErrorIs(t, err, errFailingWriter)
}
