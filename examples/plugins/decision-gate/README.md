# Decision Gate

Decision Gate is a Multica plugin example that packages one MCP tool and one
reusable skill. It keeps the small decision model outside the agent process and
starts it only when `decision` is called.

## Multica mapping

The manifest contributes an agent-triggered MCP hook and a `decision-gate`
skill. Multica discovers the MCP server's `decision` tool and requires an
administrator to approve it before an agent can call it. That approval is the
shared-package safety boundary; the plugin does not silently grant a new remote
tool to every workspace.

The MCP server is the small always-on process. Its backend manager:

- defaults to Laya;
- serializes concurrent calls so one backend is spawned at most once;
- reuses the resident backend for subsequent calls;
- keeps at most one backend resident;
- stops it after 60 seconds without a request;
- retries a failed backend once; and
- returns `{ "status": "unavailable", "fallback": "codex" }` after the retry.

The server logs backend name, lifecycle events, latency, and failure class. It
does not log state, questions, options, or full model output.

## Run the server

Node.js 20 or newer is required. The Laya package downloads its ONNX bundle on
first use, so install it in this directory:

```bash
npm install
```

The published manifest intentionally uses an HTTPS example host. For a real
installation, deploy this server at the URL in
`multica.plugin.json` (or publish a manifest copy with the URL and matching
`net:` scope for your service). Generate a development certificate for local
testing:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout dev-key.pem -out dev-cert.pem \
  -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1"
```

```bash
DECISION_GATE_TOKEN=replace-me \
TLS_CERT=dev-cert.pem TLS_KEY=dev-key.pem \
node server/main.mjs
```

For a local-only smoke test, `MCP_ALLOW_HTTP=1` starts plain HTTP. Do not use
that mode for a published plugin; the Multica manifest contract requires an
HTTPS MCP endpoint.

Install the plugin in Multica, set its `decision_credential` to the same token,
discover the `decision` tool, and approve it. The bundled skill is installed by
the plugin and can be attached to the agents that should use it.

## Backend selection

`DECISION_GATE_DEFAULT_BACKEND` defaults to `laya`. The request may select
`auto`, `laya`, `von`, `llama_cpp`, or `decider`; all backend commands and URLs
come from server environment variables, never from tool input.

```text
DECISION_GATE_IDLE_TIMEOUT_SECONDS=60
DECISION_GATE_STARTUP_TIMEOUT_SECONDS=30
DECISION_GATE_SHUTDOWN_GRACE_SECONDS=3
# Set to 0 only when an external supervisor owns backend cleanup.
DECISION_GATE_KILL_ON_MCP_EXIT=1

# Optional Laya bundle/cache locations.
LAYA_MODEL_DIR=/models/laya
LAYA_CACHE=/var/cache/receptron-laya

# Optional service adapters. *_ARGS_JSON must be a JSON string array.
VON_COMMAND=von
VON_ARGS_JSON=["serve","--port","18080"]
LLAMA_CPP_MODEL=/models/qwen-small.gguf
LLAMA_CPP_ARGS_JSON=["-m","/models/qwen-small.gguf","--host","127.0.0.1","--port","18081"]
DECIDER_COMMAND=decider
DECIDER_ARGS_JSON=["serve","--port","18082"]
```

Von, llama.cpp, and Decider are replaceable adapters, not escalation policy.
The skill remains the safety boundary: confidence is evidence, and the agent
must make final architectural, root-cause, merge, and blocker judgments itself.
