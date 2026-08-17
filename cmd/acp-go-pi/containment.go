package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
)

const (
	containmentOutputWarning   = "PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed"
	containmentCommandDiagnose = "diagnose"
	containmentCommandCleanup  = "cleanup"
	containmentBestEffort      = "best_effort"
	containmentVendor          = "pi"
)

var containmentDiagnoseCommand = containmentDiagnose
var containmentCleanupCommand = containmentCleanup

type containmentDiagnoseRecord struct {
	RuntimeID      string `json:"runtime_id"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	State          string `json:"state"`
	GenerationRoot string `json:"generation_root"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	CorrelatedPIDs []int  `json:"correlated_pids"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	AmbiguousPIDs  []int  `json:"ambiguous_pids"`  //nolint:tagliatelle // Operator JSON contract uses snake_case.
}

type containmentDiagnoseOutput struct {
	Vendor        string                      `json:"vendor"`
	Containment   string                      `json:"containment"`
	ScratchParent string                      `json:"scratch_parent"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	Warning       string                      `json:"warning"`
	Records       []containmentDiagnoseRecord `json:"records"`
}

type containmentCleanupOutput struct {
	Vendor                  string `json:"vendor"`
	Containment             string `json:"containment"`
	ScratchParent           string `json:"scratch_parent"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	Warning                 string `json:"warning"`
	RuntimeID               string `json:"runtime_id"`                //nolint:tagliatelle // Operator JSON contract uses snake_case.
	GenerationRoot          string `json:"generation_root"`           //nolint:tagliatelle // Operator JSON contract uses snake_case.
	TermSignalledPIDs       []int  `json:"term_signaled_pids"`        //nolint:tagliatelle // Operator JSON contract uses snake_case.
	KillSignalledPIDs       []int  `json:"kill_signaled_pids"`        //nolint:tagliatelle // Operator JSON contract uses snake_case.
	RemainingCorrelatedPIDs []int  `json:"remaining_correlated_pids"` //nolint:tagliatelle // Operator JSON contract uses snake_case.
	AmbiguousPIDs           []int  `json:"ambiguous_pids"`            //nolint:tagliatelle // Operator JSON contract uses snake_case.
	RootRemoved             bool   `json:"root_removed"`              //nolint:tagliatelle // Operator JSON contract uses snake_case.
	ResultReady             bool   `json:"-"`
}

func runContainmentCommand(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "acp-go-pi: containment requires diagnose or cleanup")

		return 2
	}

	usageError := func(message string) int {
		_, _ = fmt.Fprintln(stderr, "acp-go-pi: "+message)

		return 2
	}

	var (
		value       any
		err         error
		emitPartial bool
	)

	switch args[0] {
	case containmentCommandDiagnose:
		flags := flag.NewFlagSet("acp-go-pi containment diagnose", flag.ContinueOnError)
		flags.SetOutput(stderr)

		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the pi containment registry")
		if parseErr := flags.Parse(args[1:]); parseErr != nil {
			return 2
		}

		switch {
		case flags.NArg() != 0:
			return usageError("containment diagnose accepts no positional arguments")
		case !validContainmentScratchDir(*scratchDir):
			return usageError("containment diagnose requires -scratch-dir")
		default:
			value, err = containmentDiagnoseCommand(*scratchDir)
		}
	case containmentCommandCleanup:
		flags := flag.NewFlagSet("acp-go-pi containment cleanup", flag.ContinueOnError)
		flags.SetOutput(stderr)
		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the pi containment registry")
		runtimeID := flags.String("runtime-id", "", "operator-selected runtime id")

		force := flags.Bool("force", false, "signal correlated processes despite PID-reuse and collateral-signalling risk, then remove the selected generation root")
		if parseErr := flags.Parse(args[1:]); parseErr != nil {
			return 2
		}

		switch {
		case flags.NArg() != 0:
			return usageError("containment cleanup accepts no positional arguments")
		case *runtimeID == "":
			return usageError("containment cleanup requires -runtime-id")
		case !validContainmentScratchDir(*scratchDir):
			return usageError("containment cleanup requires -scratch-dir")
		case !validContainmentRuntimeID(*runtimeID):
			return usageError("containment cleanup runtime id must be 128-bit lowercase hex")
		case !*force:
			return usageError("containment cleanup requires -force")
		default:
			output, cleanupErr := containmentCleanupCommand(*scratchDir, *runtimeID, *force)
			value, err = output, cleanupErr
			emitPartial = cleanupErr != nil && output.ResultReady
		}
	default:
		return usageError(fmt.Sprintf("unknown containment command %q", args[0]))
	}

	if err != nil {
		if emitPartial {
			if encodeErr := json.NewEncoder(stdout).Encode(value); encodeErr != nil {
				_, _ = fmt.Fprintf(stderr, "acp-go-pi: encode containment result: %v\n", encodeErr)

				return 1
			}
		}

		_, _ = fmt.Fprintf(stderr, "acp-go-pi: %v\n", err)

		return 1
	}

	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-pi: encode containment result: %v\n", err)

		return 1
	}

	return 0
}

func validContainmentScratchDir(path string) bool {
	return strings.TrimSpace(path) != "" && !strings.ContainsRune(path, '\x00')
}

func validContainmentRuntimeID(runtimeID string) bool {
	if len(runtimeID) != 32 || runtimeID != strings.ToLower(runtimeID) {
		return false
	}

	for _, value := range runtimeID {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return false
		}
	}

	return true
}
