# Resume From File

This example reads mirrored pi session JSONL rows into a `SessionStore`, loads
the session through ACP so previous interactions are replayed, then sends one
no-tools smoke-test prompt in-process.
It denies tool permissions by default so a copied session cannot silently run
commands while you are checking resume behavior.

Use it with rows previously captured from the adapter's session store:

```sh
go run ./examples/resume-from-file \
  -file ./transcript.jsonl \
  -auth-file "$HOME/.pi/agent/auth.json"
```

The first pi session header row carries `id` and `cwd`; when present,
`-session` and `-cwd` are inferred from it. Loading uses normal ACP
`session/load`, and the prompt uses normal ACP `session/prompt`.

Pass `-prompt "..."` to change the smoke-test turn, `-path` to point at a
specific `pi` CLI, and `-scratch-dir` to set the parent root for isolated pi
session state. `-auth-file` explicitly seeds credentials into that isolated session.
