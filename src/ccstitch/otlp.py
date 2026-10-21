"""OTLP/HTTP JSON trace payloads -> flat span rows.

Only the JSON encoding is accepted (OTEL_EXPORTER_OTLP_PROTOCOL=http/json). Protobuf
would need a dependency, and gRPC is broken for headless sessions anyway
(anthropics/claude-code#97805 silently drops them).
"""
from __future__ import annotations

import json
import os
from typing import Any, Iterable

from ccstitch import config

# The CLI strings this package reads. If a Claude Code release renames one of them,
# the viewer goes silently empty, so check a new CLI version against this list
# (`strings $(which claude) | grep -F <literal>` for each) before trusting it.
CLI_LITERALS = (
    "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA", "OTEL_TRACES_EXPORTER", "TRACEPARENT",
    "claude_code.interaction", "claude_code.llm_request", "claude_code.tool",
    "claude_code.tool.execution", "claude_code.subagent.spawn",
    "session.id", "tool_name", "cache_read_tokens", "cache_creation_tokens", "ttft_ms",
    # read by semconv.py (error.type) and context.py (resource attribution)
    "error_class", "gen_ai.tool.call.id", "OTEL_RESOURCE_ATTRIBUTES",
)

# Resource attribute a spawner puts in the child's OTEL_RESOURCE_ATTRIBUTES
# (context.py). It names the agent even when the session's hooks are disabled
# (`claude -p --settings` with disableAllHooks), so no SessionStart record exists.
RESOURCE_AGENT = "ccstitch.agent"

SPAN_INTERACTION = "claude_code.interaction"
SPAN_LLM = "claude_code.llm_request"
SPAN_TOOL = "claude_code.tool"

# List-price USD per million tokens by model family: (input, output, cache write,
# cache read). An ESTIMATE of work volume, never a bill: a subscription plan is not
# charged per token, and older models in a family were priced differently. Override
# with CCSTITCH_PRICES='{"opus": [5, 25, 6.25, 0.5], ...}'. A model whose family is
# not in the table costs 0.
PRICING = {
    "opus": (5.00, 25.00, 6.25, 0.50),
    "sonnet": (3.00, 15.00, 3.75, 0.30),
    "haiku": (1.00, 5.00, 1.25, 0.10),
}


def _prices() -> dict[str, tuple[float, float, float, float]]:
    raw = os.environ.get("CCSTITCH_PRICES")
    if not raw:
        return PRICING
    try:
        extra = {str(k).lower(): tuple(float(x) for x in v) for k, v in json.loads(raw).items()}
    except (ValueError, TypeError, AttributeError):
        return PRICING
    return {**PRICING, **{k: v for k, v in extra.items() if len(v) == 4}}


def _value(v: dict[str, Any]) -> Any:
    if not isinstance(v, dict):
        return None
    if "stringValue" in v:
        return v["stringValue"]
    if "intValue" in v:
        try:
            return int(v["intValue"])
        except (TypeError, ValueError):
            return None
    if "doubleValue" in v:
        return v["doubleValue"]
    if "boolValue" in v:
        return bool(v["boolValue"])
    if "arrayValue" in v:
        return [_value(x) for x in (v["arrayValue"] or {}).get("values") or []]
    if "kvlistValue" in v:
        return _attrs((v["kvlistValue"] or {}).get("values") or [])
    return None


def _attrs(items: Iterable[dict[str, Any]], drop: frozenset[str] = frozenset()) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for a in items or []:
        k = a.get("key")
        if isinstance(k, str) and k not in drop:
            out[k] = _value(a.get("value") or {})
    return out


def _int(x: Any) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def parse_traces(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten an ExportTraceServiceRequest. Spans missing an id are skipped."""
    rows: list[dict[str, Any]] = []
    for rs in payload.get("resourceSpans") or []:
        res = _attrs((rs.get("resource") or {}).get("attributes") or [])
        service = res.get("service.name")
        res_agent = res.get(RESOURCE_AGENT)
        res_agent = str(res_agent)[:80] if isinstance(res_agent, str) and res_agent.strip() else None
        for ss in rs.get("scopeSpans") or []:
            for s in ss.get("spans") or []:
                trace_id, span_id = s.get("traceId"), s.get("spanId")
                if not trace_id or not span_id:
                    continue
                attrs = _attrs(s.get("attributes") or [], config.DROP_ATTRS)
                rows.append({
                    "trace_id": str(trace_id),
                    "span_id": str(span_id),
                    "parent_span_id": s.get("parentSpanId") or None,
                    "name": str(s.get("name") or "?"),
                    "start_ns": _int(s.get("startTimeUnixNano")),
                    "end_ns": _int(s.get("endTimeUnixNano")),
                    "status_code": _int((s.get("status") or {}).get("code")) or 0,
                    "session_id": attrs.get("session.id"),
                    "attrs": attrs,
                    "service": service,
                    "resource_agent": res_agent,
                })
    return rows


def model_family(model: str | None, prices: dict[str, Any] | None = None) -> str | None:
    m = (model or "").lower()
    for fam in (prices or _prices()):
        if fam in m:
            return fam
    return None


def usd_equiv(attrs: dict[str, Any]) -> float:
    prices = _prices()
    fam = model_family(attrs.get("model") or attrs.get("gen_ai.request.model"), prices)
    if fam is None:
        return 0.0
    pin, pout, pcw, pcr = prices[fam]
    n = lambda k: float(attrs.get(k) or 0)  # noqa: E731
    return (n("input_tokens") * pin + n("output_tokens") * pout
            + n("cache_creation_tokens") * pcw + n("cache_read_tokens") * pcr) / 1e6


def is_error(row: dict[str, Any]) -> bool:
    return row.get("status_code") == 2 or row["attrs"].get("success") is False


def parse_traceparent(tp: str | None) -> tuple[str, str] | None:
    """W3C traceparent -> (trace_id, span_id), or None if malformed."""
    parts = (tp or "").strip().split("-")
    if len(parts) != 4 or len(parts[1]) != 32 or len(parts[2]) != 16:
        return None
    try:
        int(parts[1], 16), int(parts[2], 16)
    except ValueError:
        return None
    if set(parts[1]) == {"0"} or set(parts[2]) == {"0"}:
        return None
    return parts[1], parts[2]
