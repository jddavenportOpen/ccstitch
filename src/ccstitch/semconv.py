"""Claude Code spans -> OpenTelemetry GenAI semantic conventions. The ONE place that
knows the convention's names.

The GenAI conventions are still Development stability: names move between releases
(`gen_ai.system` became `gen_ai.provider.name`; the cache counters became
`gen_ai.usage.cache_read.input_tokens` / `cache_write`). Keeping every name here
means a rename is a one-file change, and SEMCONV_VERSION says which release this
mapping was checked against.

Verified 2026-10-16 against open-telemetry/semantic-conventions-genai @ f3c6d0c9
(model/gen-ai/registry.yaml + spans.yaml; manifest schema_url
https://opentelemetry.io/schemas/gen-ai-dev/1.42.0-dev, built on core semconv 1.44.0):

  span                       gen_ai.operation.name   exported span name
  claude_code.llm_request    chat                    "chat {gen_ai.request.model}"    kind CLIENT
  claude_code.tool           execute_tool            "execute_tool {gen_ai.tool.name}"
  claude_code.interaction    invoke_agent            "invoke_agent {gen_ai.agent.name}"
  (synthetic) session root   invoke_agent            only where #99607 dropped the interaction
  (synthetic) job root       invoke_workflow         "invoke_workflow {gen_ai.workflow.name}"

Anthropic-specific rule (anthropic.gen_ai.client.inference): Anthropic's
`input_tokens` EXCLUDES cached tokens, so
  gen_ai.usage.input_tokens = input_tokens + cache_read + cache_creation.
The raw Claude Code counters stay on the span untouched; the store's cost math reads
those, never the gen_ai.* totals.

Stdlib only, no I/O.
"""
from __future__ import annotations

from typing import Any, Optional

SEMCONV_VERSION = "gen-ai-dev/1.42.0-dev"
SCHEMA_URL = "https://opentelemetry.io/schemas/gen-ai-dev/1.42.0-dev"
SOURCE_REF = "open-telemetry/semantic-conventions-genai@f3c6d0c9bb66"

PROVIDER = "anthropic"

# attribute names (one place)
OPERATION = "gen_ai.operation.name"
PROVIDER_NAME = "gen_ai.provider.name"
SYSTEM_DEPRECATED = "gen_ai.system"          # deprecated for PROVIDER_NAME; still read by older backends
REQUEST_MODEL = "gen_ai.request.model"
RESPONSE_MODEL = "gen_ai.response.model"
RESPONSE_ID = "gen_ai.response.id"
FINISH_REASONS = "gen_ai.response.finish_reasons"
TTFC = "gen_ai.response.time_to_first_chunk"   # seconds (double)
REASONING_LEVEL = "gen_ai.request.reasoning.level"
USAGE_IN = "gen_ai.usage.input_tokens"
USAGE_OUT = "gen_ai.usage.output_tokens"
USAGE_CACHE_READ = "gen_ai.usage.cache_read.input_tokens"
USAGE_CACHE_WRITE = "gen_ai.usage.cache_write.input_tokens"
TOOL_NAME = "gen_ai.tool.name"
TOOL_CALL_ID = "gen_ai.tool.call.id"
TOOL_TYPE = "gen_ai.tool.type"
AGENT_NAME = "gen_ai.agent.name"
CONVERSATION_ID = "gen_ai.conversation.id"
WORKFLOW_NAME = "gen_ai.workflow.name"
ERROR_TYPE = "error.type"

OP_CHAT = "chat"
OP_TOOL = "execute_tool"
OP_AGENT = "invoke_agent"
OP_WORKFLOW = "invoke_workflow"

# OTLP span kinds (opentelemetry-proto trace.proto SpanKind)
KIND_INTERNAL = 1
KIND_CLIENT = 3

_OPS = {
    "claude_code.llm_request": OP_CHAT,
    "claude_code.tool": OP_TOOL,
    "claude_code.interaction": OP_AGENT,
}


def operation(name: str) -> Optional[str]:
    return _OPS.get(name)


def _int(x: Any) -> Optional[int]:
    if isinstance(x, bool):
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def error_type(name: str, a: dict[str, Any], status_code: Optional[int] = None) -> Optional[str]:
    """Low-cardinality error identifier, or None when the span did not fail.
    The CLI's `error_class` (rate_limit, auth_error, ...) first, then the HTTP status,
    then the convention's fallback `_OTHER`. Never the error message itself."""
    failed = status_code == 2 or a.get("success") is False
    if not failed:
        return None
    ec = a.get("error_class")
    if isinstance(ec, str) and ec.strip():
        return ec.strip()[:64]
    sc = _int(a.get("status_code"))
    if sc:
        return str(sc)
    if name == "claude_code.tool.execution" or name == "claude_code.tool":
        return "tool_error"
    return "_OTHER"


def span_attrs(name: str, a: dict[str, Any], *, agent: Optional[str] = None,
               status_code: Optional[int] = None) -> dict[str, Any]:
    """The gen_ai.* (and error.type) attributes for one Claude Code span. Additive:
    never returns a key whose value is None, never touches the input."""
    out: dict[str, Any] = {}
    op = operation(name)
    if op:
        out[OPERATION] = op
    sid = a.get("session.id")
    if isinstance(sid, str) and sid:
        out[CONVERSATION_ID] = sid
    if agent:
        out[AGENT_NAME] = agent

    if op == OP_CHAT:
        out[PROVIDER_NAME] = PROVIDER
        out[SYSTEM_DEPRECATED] = PROVIDER
        model = a.get(REQUEST_MODEL) or a.get("model")
        if model:
            out[REQUEST_MODEL] = model
            if a.get("success") is not False:
                # The CLI reports one model; on a served call that is the model that answered.
                out[RESPONSE_MODEL] = model
        rid = a.get(RESPONSE_ID) or a.get("request_id")
        if rid:
            out[RESPONSE_ID] = rid
        fr = a.get(FINISH_REASONS)
        if isinstance(fr, list) and fr:
            out[FINISH_REASONS] = [str(x) for x in fr]
        elif a.get("stop_reason"):
            out[FINISH_REASONS] = [str(a["stop_reason"])]
        if a.get("effort"):
            out[REASONING_LEVEL] = str(a["effort"])
        ttft = a.get("ttft_ms")
        if isinstance(ttft, (int, float)) and not isinstance(ttft, bool):
            out[TTFC] = round(float(ttft) / 1000.0, 3)
        tin, tout = _int(a.get("input_tokens")), _int(a.get("output_tokens"))
        cr, cw = _int(a.get("cache_read_tokens")), _int(a.get("cache_creation_tokens"))
        if any(v is not None for v in (tin, cr, cw)):
            out[USAGE_IN] = (tin or 0) + (cr or 0) + (cw or 0)
        if tout is not None:
            out[USAGE_OUT] = tout
        if cr is not None:
            out[USAGE_CACHE_READ] = cr
        if cw is not None:
            out[USAGE_CACHE_WRITE] = cw
    elif op == OP_TOOL:
        tool = a.get("tool_name")
        if tool:
            out[TOOL_NAME] = tool
            # Every Claude Code tool runs client-side; an MCP tool calls out to a server.
            out[TOOL_TYPE] = "extension" if str(tool).startswith("mcp__") else "function"
    elif op == OP_AGENT:
        out[PROVIDER_NAME] = PROVIDER

    if name in ("claude_code.tool", "claude_code.tool.execution"):
        cid = a.get(TOOL_CALL_ID) or a.get("tool_use_id")
        if cid:
            out[TOOL_CALL_ID] = cid
    et = error_type(name, a, status_code)
    if et:
        out[ERROR_TYPE] = et
    return out


def span_name(name: str, a: dict[str, Any], *, agent: Optional[str] = None) -> str:
    """The convention's span name; the CLI's own name for anything unmapped."""
    op = operation(name)
    if op == OP_CHAT:
        model = a.get(REQUEST_MODEL) or a.get("model")
        return f"{op} {model}" if model else op
    if op == OP_TOOL:
        tool = a.get("tool_name")
        return f"{op} {tool}" if tool else op
    if op == OP_AGENT:
        return f"{op} {agent}" if agent else op
    return name


def span_kind(name: str) -> int:
    return KIND_CLIENT if operation(name) == OP_CHAT else KIND_INTERNAL


def agent_span_attrs(agent: Optional[str], session_id: Optional[str]) -> dict[str, Any]:
    """A synthetic invoke_agent span standing in for a turn #99607 dropped."""
    out: dict[str, Any] = {OPERATION: OP_AGENT, PROVIDER_NAME: PROVIDER}
    if agent:
        out[AGENT_NAME] = agent
    if session_id:
        out[CONVERSATION_ID] = session_id
    return out


def workflow_span_attrs(workflow: Optional[str], agent: Optional[str]) -> dict[str, Any]:
    """A synthetic invoke_workflow span: one Python job and the claude calls it made."""
    out: dict[str, Any] = {OPERATION: OP_WORKFLOW}
    if workflow:
        out[WORKFLOW_NAME] = workflow
    if agent:
        out[AGENT_NAME] = agent
    return out
