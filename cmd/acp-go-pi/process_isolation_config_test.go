package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	piacp "github.com/savid/acp-go-pi"
)

const testProcessIsolationConfigPath = "/test/process-isolation.json"

func stubProcessIsolationConfig(t *testing.T) {
	t.Helper()

	original := processIsolationConfigLoader
	processIsolationConfigLoader = func(path string) (processIsolationConfig, error) {
		if path != testProcessIsolationConfigPath {
			t.Fatalf("process isolation config path = %q", path)
		}

		return processIsolationConfig{
			UID:                 20001,
			GID:                 20001,
			BaseEnvironment:     map[string]string{"PATH": "/usr/bin", "HOME": "/var/empty/acp", "USER": "acp", "LOGNAME": "acp"},
			StandaloneOwnerID:   "test-owner",
			StandaloneStateRoot: "/var/empty/acp",
		}, nil
	}
	t.Cleanup(func() { processIsolationConfigLoader = original })
}

func isolatedArgs(args ...string) []string {
	return append([]string{"-" + processIsolationConfigFlag, testProcessIsolationConfigPath}, args...)
}

func TestDecodeProcessIsolationConfigStrict(t *testing.T) {
	config, err := decodeProcessIsolationConfig([]byte(`{"uid":20001,"gid":20002,"baseEnvironment":{"PATH":"/usr/bin"},"inheritEnvironment":["AMP_API_KEY"],"standaloneOwnerId":"deployment-a","standaloneStateRoot":"/var/lib/acp"}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.UID != 20001 || config.GID != 20002 || config.BaseEnvironment["PATH"] != "/usr/bin" || len(config.InheritEnvironment) != 1 {
		t.Fatalf("decoded config = %#v", config)
	}

	for _, document := range []string{
		`{"uid":1,"gid":2,"baseEnvironment":{},"unknown":true}`,
		`{"uid":1,"gid":2,"baseEnvironment":{}} {}`,
		`{} @`,
		`{1:2}`,
		`[@]`,
		`{"uid":"invalid","gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"uid":2,"gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"gid":2,"baseEnvironment":{"PATH":"/bin","PATH":"/usr/bin"}}`,
		``,
	} {
		if _, err := decodeProcessIsolationConfig([]byte(document)); err == nil {
			t.Fatalf("decode unexpectedly accepted %q", document)
		}
	}
}

func TestRunWithoutProcessIsolationConfigUsesOrdinaryMode(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)

	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		t.Fatal("ordinary mode loaded an isolation policy")

		return processIsolationConfig{}, nil
	}
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...piacp.Option) error {
		options := piacp.Options{}
		for _, opt := range opts {
			opt(&options)
		}
		if options.ProcessIsolation != nil {
			t.Fatalf("ordinary mode ProcessIsolation = %#v", options.ProcessIsolation)
		}

		return nil
	}

	if code := run(t.Context(), nil, strings.NewReader(""), &strings.Builder{}, &strings.Builder{}); code != 0 {
		t.Fatalf("run code = %d", code)
	}
}

func TestRunWithExplicitProcessIsolationConfigIsFailClosed(t *testing.T) {
	disableTelemetry(t)
	restoreMainSeams(t)

	original := processIsolationConfigLoader
	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		return processIsolationConfig{}, errors.New("invalid policy")
	}
	t.Cleanup(func() { processIsolationConfigLoader = original })
	serve = func(context.Context, io.Reader, io.Writer, ...piacp.Option) error {
		t.Fatal("serve called after explicit policy load failed")

		return nil
	}

	var stderr strings.Builder
	code := run(
		t.Context(), []string{"-" + processIsolationConfigFlag, "/invalid-policy"},
		strings.NewReader(""), &strings.Builder{}, &stderr,
	)
	if code != 1 || !strings.Contains(stderr.String(), "process isolation: invalid policy") {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}
}
