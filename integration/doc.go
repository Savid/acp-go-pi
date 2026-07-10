//go:build integration

// Package integration contains opt-in integration coverage for the pi ACP
// wrapper.
//
// Run with both the integration build tag and ACP_GO_PI_RUN_INTEGRATION=1.
// The smoke tier drives the real local pi binary without spending model
// tokens and a deterministic fake pi harness; the live tier additionally
// requires ACP_GO_PI_RUN_LIVE_TOKENS=1 and provider credentials, and may
// spend tokens.
package integration
