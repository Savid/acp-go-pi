package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"

	piacp "github.com/savid/acp-go-pi"
)

// seedFileFlag collects repeatable -seed-file <relpath>=<hostpath> values,
// reading each host file's contents into a map keyed by the relative path.
type seedFileFlag struct {
	files map[string]string
}

func (s *seedFileFlag) String() string {
	if s == nil || len(s.files) == 0 {
		return ""
	}

	names := make([]string, 0, len(s.files))
	for name := range s.files {
		names = append(names, name)
	}

	slices.Sort(names)

	return strings.Join(names, ",")
}

func (s *seedFileFlag) Set(value string) error {
	relPath, hostPath, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("invalid -seed-file %q: expected <relpath>=<hostpath>", value)
	}

	relPath = strings.TrimSpace(relPath)
	hostPath = strings.TrimSpace(hostPath)

	if relPath == "" || hostPath == "" {
		return fmt.Errorf("invalid -seed-file %q: expected <relpath>=<hostpath>", value)
	}

	contents, err := os.ReadFile(hostPath)
	if err != nil {
		return fmt.Errorf("read seed file %q: %w", hostPath, err)
	}

	if s.files == nil {
		s.files = make(map[string]string)
	}

	s.files[relPath] = string(contents)

	return nil
}

var serve = piacp.Serve
var exit = os.Exit
var shutdownOpenTelemetry = shutdownTelemetry
var agentVersion = version

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("acp-go-pi", flag.ContinueOnError)
	flags.SetOutput(stderr)

	piPath := flags.String("path", "", "path to pi CLI")
	piHome := flags.String("home", "", "durable per-instance PI_CODING_AGENT_DIR; empty gives each session an ephemeral one")
	scratchDir := flags.String("scratch-dir", "", "parent directory for ephemeral session scratch; empty means the system temp directory")
	providerAuthRoot := flags.String("provider-auth-root", "", "durable root for the values-free provider-auth ledger; empty leaves provider auth unadvertised")
	model := flags.String("model", "", "default pi model as provider/id")
	seedFiles := &seedFileFlag{}
	flags.Var(seedFiles, "seed-file", "seed file written into each session's pi agent dir as <relpath>=<hostpath>; repeatable")
	debug := flags.Bool("debug", false, "write debug logs to stderr")
	printVersion := flags.Bool("version", false, "print adapter version and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	version := agentVersion()

	if *printVersion {
		_, _ = fmt.Fprintln(stdout, version)

		return 0
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if *debug {
		logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	telemetry, err := configureTelemetry(ctx, logger, version)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-pi: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.logger

	signals := forwardedSignals()
	receivedSignals := make(chan os.Signal, 1)

	// NotifyContext cancels serving on a signal; this channel preserves the
	// actual signal value so the process can return the conventional exit code.
	signal.Notify(receivedSignals, signals...)
	defer signal.Stop(receivedSignals)

	ctx, stop := signal.NotifyContext(ctx, signals...)
	defer stop()

	serveOptions := make([]piacp.Option, 0, 10+len(telemetry.options))

	serveOptions = append(serveOptions,
		piacp.WithAgentVersion(version),
		piacp.WithExecutablePath(*piPath),
		piacp.WithHome(*piHome),
		piacp.WithScratchDir(*scratchDir),
		piacp.WithProviderAuthRoot(*providerAuthRoot),
		piacp.WithDefaultModel(*model),
		piacp.WithLogger(logger),
	)

	if len(seedFiles.files) > 0 {
		serveOptions = append(serveOptions, piacp.WithSeedFiles(seedFiles.files))
	}

	serveOptions = append(serveOptions, telemetry.options...)

	serveErr := serve(ctx, stdin, stdout, serveOptions...)
	shutdownErr := shutdownOpenTelemetry(context.Background(), telemetry.shutdown)

	if serveErr != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-pi: %v\n", serveErr)

		return 1
	}

	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-pi: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	if sig := pendingSignal(receivedSignals); sig != nil {
		return signalCode(sig)
	}

	return 0
}

func pendingSignal(signals <-chan os.Signal) os.Signal {
	select {
	case sig := <-signals:
		return sig
	default:
		return nil
	}
}
