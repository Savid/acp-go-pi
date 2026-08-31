// Package piacp exposes the local pi coding agent CLI as an Agent Client
// Protocol agent.
//
// Most hosts run the agent over a pair of JSON-RPC streams using [Serve].
// Serve starts one pi RPC-mode process per ACP session, maps ACP requests
// into pi JSONL RPC commands, and streams ACP session updates back to the
// client. Hosts must complete ACP initialization before issuing session or
// other agent methods.
//
// Hosts should use [Serve] for the JSON-RPC transport. Credentials may be
// injected explicitly, or ordinary-mode hosts may configure [WithHome] and
// [WithProviderAuthRoot] to expose Pi's seven provider-auth extension legs.
// Brokered credentials remain in Pi's durable native home; the adapter's
// ledger contains binding metadata but no credential values.
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
// Omitting [WithHostAuthority] runs Pi ordinarily as the adapter identity.
// Supplying [WithHostAuthority] routes every native launch and private native
// tree through the borrowed host boundary and fails closed if that authority
// becomes unavailable.
package piacp
