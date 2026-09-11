# Minimal Client

Launches `acp-go-pi` as a subprocess, creates one session in the current
directory, sends one prompt, and prints the streamed answer. Every permission
request is allowed once.

```sh
go run ./examples/minimal-client "Reply with a short hello from ACP"
```

A local `pi` must be installed and authenticated; the session inherits your
environment and pi's home exactly as running `pi` in this directory would.
