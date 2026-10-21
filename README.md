# ccstitch

See what every Claude Code session did, on your own machine: each turn's model calls and tool calls with timing, tokens and a list-price cost estimate, and the `claude -p` sessions those turns launched, nested under the tool call that launched them.

ccstitch is a small local collector for Claude Code's native OpenTelemetry spans. It stores them in one SQLite file, repairs the traces the CLI currently breaks, serves a built-in viewer, and can forward the repaired traces to Phoenix, Jaeger, Tempo or Langfuse.

- Python standard library only (3.10+). No dependencies, no build step, no CDN.
- One process bound to 127.0.0.1, one SQLite file, 7-day retention by default.
- Maps every span onto the OpenTelemetry GenAI semantic conventions.

## The problem

Claude Code can export a span tree per turn (`claude_code.interaction` → `llm_request` / `tool` → `tool.execution` ...). Point it at Jaeger or Phoenix and two things go wrong:

1. **Orphaned headless turns: [anthropics/claude-code#99607](https://github.com/anthropics/claude-code/issues/99607).** Headless (`claude -p`) turns intermittently lose the `claude_code.interaction` root span. Every `llm_request` and `tool` span of that turn then arrives as a parentless root **in its own trace**, and the inbound `TRACEPARENT` is ignored. One turn becomes a dozen one-span traces. Measured on CLI 2.1.283: 2 of 5 headless runs.
2. **Agents that launch agents are separate processes.** A `claude -p` started from another session's Bash tool is its own process. Its only link to the tool call that started it is the `TRACEPARENT` environment variable, and when (1) hits the child, that link is gone too.

The raw export is therefore unusable for anything that runs agents from agents: scripts, CI jobs, orchestrators, cron.

## 60-second quickstart

```bash
pipx install git+https://github.com/jddavenportOpen/ccstitch   # or: pip install git+https://...
ccstitch install --dry-run      # shows the exact settings.json diff; writes nothing
ccstitch install                # adds the env block + SessionStart hook to ~/.claude/settings.json
ccstitch serve                  # collector + viewer on http://127.0.0.1:4328/
```

Start a **new** Claude Code session (running sessions keep the environment they started with), then open <http://127.0.0.1:4328/>. `ccstitch status` checks both halves: is the collector answering, and are the settings in place.

To keep it running, use your service manager. A systemd user unit:

```ini
# ~/.config/systemd/user/ccstitch.service
[Unit]
Description=ccstitch trace collector

[Service]
ExecStart=%h/.local/bin/ccstitch serve
Restart=on-failure

[Install]
WantedBy=default.target
```

`systemctl --user enable --now ccstitch`. On macOS, a LaunchAgent with `ProgramArguments` = `[<path to ccstitch>, serve]` and `KeepAlive` = true does the same.

## What `ccstitch install` writes

Only the traces signal is pointed at ccstitch, so an existing metrics or logs pipeline on `OTEL_EXPORTER_OTLP_ENDPOINT` keeps working:

```json
{
  "env": {
    "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
    "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1",
    "OTEL_TRACES_EXPORTER": "otlp",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4328/v1/traces",
    "OTEL_METRICS_EXPORTER": "none",
    "OTEL_LOGS_EXPORTER": "none",
    "CCSTITCH_MANAGED": "CLAUDE_CODE_ENABLE_TELEMETRY,..."
  },
  "hooks": {
    "SessionStart": [
      {"hooks": [{"type": "command", "command": "/path/to/python -m ccstitch hook", "timeout": 5, "async": true}]}
    ]
  }
}
```

- **Merges, never clobbers.** A key that already holds a different value is reported as a conflict and nothing is written unless you pass `--force`. `OTEL_METRICS_EXPORTER` / `OTEL_LOGS_EXPORTER` are only set when absent. Your other hooks and settings are untouched.
- **`--uninstall` is the exact inverse.** It removes the keys listed in `CCSTITCH_MANAGED` (only while they still hold ccstitch's value) and the ccstitch hook entry, and nothing else.
- The original file is backed up once to `settings.json.bak-ccstitch` and replaced atomically. An unreadable settings file is refused, never overwritten.
- `--settings PATH` targets another file (a project's `.claude/settings.json`, or `$CLAUDE_CONFIG_DIR/settings.json`, which is also the default when that variable is set). `--port N` changes the port in both the endpoint and the hook.
- **Why `http/json`:** [anthropics/claude-code#97805](https://github.com/anthropics/claude-code/issues/97805), headless sessions silently drop gRPC exports. The collector answers protobuf with a 415 that names this fix.

## How the repair works

Every span the CLI sends carries `session.id`. The SessionStart hook records, for each session, the `TRACEPARENT` it was started with (plus its working directory and an optional agent name). That is enough to rebuild the tree:

```
 what Claude Code sent                         what ccstitch serves
 ─────────────────────                         ────────────────────
 trace A  interaction (parent session P)       trace A
            ├ llm_request                        session P
            └ tool: Bash "claude -p ..."           interaction
                 └ tool.execution  ◄─┐               ├ llm_request
                                     │               └ tool: Bash "claude -p ..."
 trace B  llm_request   (session C)  │                    └ tool.execution
 trace C  tool: Read    (session C)  │                         └ session C   (repaired)
 trace D  llm_request   (session C)  │                              ├ llm_request
          no interaction: #99607     │                              ├ tool: Read
                                     │                              └ llm_request
 hook: session C started with ───────┘
       TRACEPARENT = A / tool.execution
```

The stitcher runs every 20 seconds and recomputes a day's window from scratch, because late arrivals change answers: a parent's tool span is exported only after the child it launched exits, and a turn's interaction span arrives after its children.

- A native trace that contains an interaction span keeps its own id. A healthy turn is served exactly as Claude Code sent it.
- A native trace without one (a turn #99607 broke) is grouped under its session, `s:<session id>`.
- A session whose `TRACEPARENT` names a span in another trace is folded into that trace, under that span.
- If the parent span never arrives (its process was killed), the child waits 10 minutes, then folds in by the trace id its `TRACEPARENT` named, so siblings launched by the same parent still land together.
- Link cycles are cut; chains are followed at most 32 deep.

## The viewer and the API

`http://127.0.0.1:4328/` lists traces with totals (model calls, tool calls, tokens, prompt cache hit rate, list-price estimate, how many sessions were repaired). `/t/<id>` shows one trace as a collapsible tree with a waterfall, and the full attributes of the selected span: the CLI's own keys and the GenAI-convention keys side by side. It is one HTML file inside the package; it works offline and says so plainly when there is no data yet or when the collector stops answering.

The same data as JSON:

| Endpoint | |
|---|---|
| `GET /api/overview?hours=24` | totals, by agent, top tools, models |
| `GET /api/traces?hours=24&limit=50&agent=x&errors=1` | trace list |
| `GET /api/traces/<id>` | one trace as a flat node list (`parent` links) |
| `GET /healthz` | liveness, counters, last stitch pass, forwarder state |
| `POST /v1/traces` | OTLP/HTTP JSON ingest |
| `POST /v1/links` | SessionStart records |

`ccstitch export <id> [--out t.json]` writes one repaired trace as an OTLP/HTTP JSON `ExportTraceServiceRequest`.

## OpenTelemetry GenAI semantic conventions

Every span the API or the exporter serves also carries the [GenAI semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai) (Development stability), mapped in one file (`semconv.py`) and checked against `gen-ai-dev/1.42.0-dev`. The raw CLI counters stay on every span.

| Claude Code | GenAI convention |
|---|---|
| `claude_code.llm_request` | `gen_ai.operation.name=chat`, span name `chat {model}`, kind CLIENT |
| `claude_code.tool` | `execute_tool`, span name `execute_tool {tool}` |
| `claude_code.interaction` | `invoke_agent`, span name `invoke_agent {agent}` |
| a turn #99607 broke | a synthetic `invoke_agent` span parents the orphans (export only) |
| a Python job (`ccstitch.context`) | a synthetic `invoke_workflow {name}` root span |
| `model` | `gen_ai.request.model`; `gen_ai.response.model` on a served call |
| `request_id` / `stop_reason` / `effort` | `gen_ai.response.id` / `gen_ai.response.finish_reasons` / `gen_ai.request.reasoning.level` |
| `ttft_ms` | `gen_ai.response.time_to_first_chunk` (seconds) |
| `input_tokens + cache_read_tokens + cache_creation_tokens` | `gen_ai.usage.input_tokens` (Anthropic's `input_tokens` excludes cache; the convention's total includes it) |
| `cache_read_tokens` / `cache_creation_tokens` | `gen_ai.usage.cache_read.input_tokens` / `gen_ai.usage.cache_write.input_tokens` |
| `output_tokens` | `gen_ai.usage.output_tokens` |
| `tool_name` / `tool_use_id` | `gen_ai.tool.name`, `gen_ai.tool.type` (`extension` for `mcp__*`) / `gen_ai.tool.call.id` |
| `session.id` | `gen_ai.conversation.id` |
| the session's agent | `gen_ai.agent.name`, and `service.name` on export |
| `error_class` / `status_code` on a failure | `error.type` (never the error message) |

## Forwarding repaired traces to Phoenix, Jaeger, Tempo or Langfuse

Pointing Claude Code straight at a backend gives it the broken traces. The forwarder sends the repaired ones instead: stitched parentage, synthetic roots, `service.name` = agent, `gen_ai.*` attributes. It is off unless the collector's environment sets an endpoint.

Arize Phoenix, for example:

```bash
pip install arize-phoenix && phoenix serve            # UI + OTLP on http://127.0.0.1:6006
CCSTITCH_FORWARD_ENDPOINT=http://127.0.0.1:6006 ccstitch serve
```

| Backend | `CCSTITCH_FORWARD_ENDPOINT` | Notes |
|---|---|---|
| Arize Phoenix | `http://127.0.0.1:6006` | accepts protobuf only, which is the default |
| Jaeger (OTLP enabled) | `http://127.0.0.1:4318` | |
| Grafana Tempo / OTel Collector | `http://<host>:4318` | |
| Langfuse | `https://<host>/api/public/otel` | `CCSTITCH_FORWARD_HEADERS=Authorization=Basic%20<base64 pk:sk>` |

A base URL gets `/v1/traces` appended; a full `.../v1/traces` URL is used as is. Headers use the `OTEL_EXPORTER_OTLP_HEADERS` format (comma-separated `key=value`, percent-encoded). `CCSTITCH_FORWARD_PROTOCOL` is `http/protobuf` (default, encoded with the standard library) or `http/json`.

How it behaves:

- A trace is sent once it has been quiet for 12 minutes (`CCSTITCH_FORWARD_SETTLE_S`, never less than the stitcher's 10-minute grace plus a minute), so an orphan child has folded into its parent before anything leaves.
- If a trace grows later, only what is new is sent, including every span of a child session the stitcher folded in since.
- It never touches ingest: its own thread, its own SQLite connection, a queue bounded by traces (256) and spans (50,000), batches of up to 2,000 spans, retries with backoff (1 s, 2 s, 4 s; a 4xx other than 408/429 is not retried). Under pressure it drops and counts; `/healthz` shows `forward` (sent, retries, dropped, last error).

## Python jobs that launch `claude -p`

A script that calls `claude -p` five times produces five unrelated traces, because each child starts with no `TRACEPARENT`. `ccstitch.context` gives the job one trace:

```python
import os, subprocess
from ccstitch import context

env = dict(os.environ, CCSTITCH_AGENT="nightly-review")
env.update(context.child_env(env))        # TRACEPARENT (+ OTEL_RESOURCE_ATTRIBUTES ccstitch.agent=...)
subprocess.run(["claude", "-p", "..."], env=env)
```

- An inherited, well-formed `TRACEPARENT` passes through untouched, so a script started from a Claude session's Bash tool hangs under that tool call.
- Otherwise the process mints one context for all its calls (rotated every 30 minutes, `CCSTITCH_JOB_MAX_S`, so a daemon does not become one multi-day trace), or one per `with context.unit(agent=..., workflow=...):` block when the script knows its units of work.
- The job is recorded once at the collector and shown as an `invoke_workflow` root; it never sends a span of its own.
- `CCSTITCH_AGENT` also names the session through a resource attribute, so sessions with hooks disabled are still attributed.

## Privacy

- **Content stays redacted.** Claude Code redacts prompts and tool content unless you opt in with `OTEL_LOG_*` variables, and ccstitch never sets them. What is stored is span names, timings, token counts, model ids, tool names, and the first word of Bash commands (`bash_argv0`) as the CLI reports it.
- **Identity attributes are dropped at ingest:** `user.email`, `user.id`, `user.account_uuid`, `user.account_id`, `organization.id`, `terminal.type`.
- **Everything stays local.** One SQLite file under `$XDG_STATE_HOME/ccstitch` (default `~/.local/state/ccstitch`), and a collector bound to 127.0.0.1. Reads answer only to a local `Host` header, so a web page cannot read your traces through DNS rebinding. The forwarder is the only path off the machine, and it is off unless you set an endpoint.
- The session records do include each session's working directory, which is shown in the viewer.

## Configuration

| Variable | Default | |
|---|---|---|
| `CCSTITCH_PORT` | `4328` | collector port (4317/4318 are left free for a real OTLP collector) |
| `CCSTITCH_DIR` | `$XDG_STATE_HOME/ccstitch` | state directory |
| `CCSTITCH_DB` | `<dir>/traces.db` | SQLite file |
| `CCSTITCH_RETENTION_DAYS` | `7` | older data is pruned hourly |
| `CCSTITCH_MAX_DB_MB` | `1536` | then the oldest data goes first |
| `CCSTITCH_PRICES` | built-in table | JSON, `{"opus": [in, out, cache_write, cache_read], ...}` USD per million tokens |
| `CCSTITCH_AGENT` | | names a session's agent (hook and resource attribute) |
| `CCSTITCH_HOOK_DISABLED` | | `1` turns the hook into a no-op |
| `CCSTITCH_JOB_CONTEXT` | on | `0` stops `ccstitch.context` minting job traces |
| `CCSTITCH_FORWARD_*` | off | see forwarding |

Off switches: `touch <state dir>/OFF` makes the collector acknowledge and discard spans; `ccstitch install --uninstall` stops new sessions exporting. A dead collector costs Claude Code nothing: measured on 2.1.283, `claude -p` exits 0 and prints nothing extra, and the hook spools its record (capped at 5 MB) for the collector to replay when it starts.

## Limitations

- **It depends on a beta.** The span tree only exists with `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1`. Span and attribute names can change in any CLI release; the strings ccstitch relies on are listed in `otlp.CLI_LITERALS`. Verified against Claude Code 2.1.283.
- **The repair is per session, not per turn.** When #99607 drops the interaction span, nothing says which turn an orphan belonged to, so all of a broken session's orphans hang under one synthetic session node in time order.
- **Linking a child needs its SessionStart hook.** A child session that runs with hooks disabled is attributed through `CCSTITCH_AGENT` if set, but if #99607 also broke it, it cannot be placed under its parent's tool call.
- **A healthy trace keeps its foreign parent.** A session started under a tool call that this store never received (for example, from a session that exports somewhere else) is shown as a root here, and its exported root span still names that missing parent.
- **OTLP/HTTP JSON in only.** No protobuf or gRPC ingest (Claude Code's JSON exporter is the one that works headless).
- **Costs are estimates.** List prices per model family; a subscription plan is not billed per token, older models in a family were priced differently, and a model family not in the table counts as $0.
- **Single machine, single user.** No authentication beyond the loopback bind and the Host check; any local process can post spans.
- **Forwarding is best effort.** Bounded queues, retries, then drop-and-count. It is not a durable export pipeline.
- Tested on macOS and Linux. Windows is untested. Not published on PyPI yet.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[test]"
pytest
```

The tests are hermetic: a temp store, ephemeral ports, no `claude`, no network beyond 127.0.0.1. `tests/conftest.py` points the store at a temp dir before any module is imported, so a test run cannot touch a real store.

## License

MIT
