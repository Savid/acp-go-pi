// Package piacp exposes the pi coding agent CLI as an Agent Client Protocol
// agent.
//
// Most hosts run the agent over a pair of JSON-RPC streams using [Serve].
// Serve starts one `pi --mode rpc` process per ACP session, maps ACP requests
// onto pi's JSONL RPC, and streams ACP session updates back to the client.
// pi inherits the adapter's environment and keeps its sessions in its own
// home, so a session started over ACP can be continued natively with
// `pi --resume` after the adapter closes.
//
// Hosts that need durable remote resume provide [WithSessionStore]. The
// store mirrors pi's session JSONL rows and backs session/list, session/load,
// and session/resume when the native file is absent.
//
// Hosts that need adapter telemetry provide OpenTelemetry providers with
// [WithTracerProvider] and [WithMeterProvider]; the package never configures
// global providers.
package piacp
