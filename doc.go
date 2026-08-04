// Package piacp exposes the local pi coding agent CLI as an Agent Client
// Protocol agent.
//
// Most hosts run the agent over a pair of JSON-RPC streams using [Serve].
// Serve starts one pi RPC-mode process per ACP session, maps ACP requests
// into pi JSONL RPC commands, and streams ACP session updates back to the
// client. Hosts must complete ACP initialization before issuing session or
// other agent methods.
//
// Hosts should use [Serve] for the JSON-RPC transport. Provider
// authentication remains owned by the operator: credentials are injected
// into each isolated per-session pi agent directory from options and
// environment, never brokered through ACP auth methods.
//
// Hosts that need durable remote resume can provide [WithSessionStore]. A
// session store receives pi session JSONL mirror rows, can back
// session/list, and can hydrate a stored session file into an isolated pi
// session directory for session/load or session/resume when local native
// state is absent.
//
// Hosts that need adapter telemetry can provide OpenTelemetry providers with
// [WithTracerProvider] and [WithMeterProvider]. The package never configures
// global OpenTelemetry providers; the acp-go-pi binary handles env-based
// exporter setup for command-line use. Caller-supplied providers remain
// owned by the caller, including ForceFlush and Shutdown.
//
// Linux provides authoritative native process containment. Windows refuses
// native launch because it cannot apply the mandatory Unix UID/GID isolation.
// Darwin fails native startup closed unless [WithDarwinBestEffortContainment] is
// supplied; that mode reaps the direct child and observes only the captured
// original process group, so it does not establish escaped-descendant absence.
package piacp
