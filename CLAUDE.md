# CLAUDE.md

This file provides Claude Code-specific project guidance.

@AGENTS.md

## Claude Code Notes

- When updating integration coverage, use the real local `pi` CLI for the
  agent process. Local helper processes are only for deterministic MCP server
  endpoints.
- Verify uncertain pi RPC behavior against the real binary in a throwaway
  `PI_CODING_AGENT_DIR` under a temp directory; `pi --mode rpc` answers
  `get_state`/`get_commands` without credentials.
