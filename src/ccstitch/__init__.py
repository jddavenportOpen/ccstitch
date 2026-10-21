"""ccstitch: a local trace store for Claude Code's native OpenTelemetry spans.

Claude Code exports a span tree per turn when CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1
and OTEL_TRACES_EXPORTER=otlp: claude_code.interaction -> llm_request / tool
(-> tool.execution, tool.blocked_on_user) / hook / mcp.rpc / subagent.spawn. This
package receives those spans on a small local collector, keeps them in SQLite, and
repairs the trees the CLI breaks:

* anthropics/claude-code#99607: headless turns intermittently drop the interaction
  span, so every llm_request/tool span arrives as a parentless root in its own trace.
* a `claude -p` launched by another session is a separate process; its link to the
  launching tool call only survives through TRACEPARENT.

Repair key: every span carries `session.id`, and a SessionStart hook records the
TRACEPARENT each session was started with. A "trace" here is therefore a tree of
SESSIONS (one root session and everything it launched), with each session's spans
under it.

Standard library only. Modules:

    config     paths, port, retention, the attribute drop list
    otlp       OTLP/HTTP JSON -> span rows (+ CLI_LITERALS, the CLI strings relied on)
    store      SQLite schema, ingest, retention, and the read queries
    stitch     resolves each session's parent session and root
    collector  the HTTP receiver and the built-in viewer (`ccstitch serve`)
    hook       the SessionStart link recorder (`ccstitch hook`)
    semconv    the one mapping onto the OpenTelemetry GenAI semantic conventions
    context    W3C trace context for Python jobs that launch claude -p
    export     one repaired trace as OTLP/HTTP JSON (`ccstitch export`)
    forward    optional forwarder of repaired traces to any OTLP backend
    install    writes the settings.json env block + SessionStart hook (`ccstitch install`)
"""
__version__ = "0.1.0"
