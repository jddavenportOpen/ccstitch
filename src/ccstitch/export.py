"""One repaired trace as a standard OTLP/HTTP JSON ExportTraceServiceRequest.

    ccstitch export <trace_id> [--out file.json] [--db path]

What "repaired" means in the output, so any OTLP backend (Arize Phoenix, Jaeger,
Grafana Tempo, Langfuse) shows the tree the built-in viewer shows:

* one trace id for the whole logical trace. A native id is kept; a logical id that
  is not one (a #99607 session bucket, "s:<session>") gets a stable id derived from
  it (sha256), so re-exports land on the same trace;
* every span's parentSpanId is the STITCHED parent: a broken turn's orphans hang
  under a synthetic `invoke_agent <agent>` span standing in for the dropped
  interaction; a child `claude -p` hangs under the tool call (or job) that launched
  it; a job trace (context.py) gets a synthetic `invoke_workflow <workflow>` root
  whose span id is the one its children already name as parent;
* resource `service.name` = the agent that ran the session (else `claude-code`);
* span names and gen_ai.* attributes follow the GenAI conventions (semconv.py);
  the CLI's own attributes are kept, the CLI span name moves to
  `claude_code.span.name`, and identity attributes never reach it (dropped at ingest).

Pure read of the store (read-only connection from the CLI).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

from ccstitch import __version__, config, semconv, store

SCOPE = {"name": "ccstitch", "version": __version__}
DEFAULT_SERVICE = "claude-code"
# Never leaves the machine, even from a store written before ingest dropped it.
DROP_ON_EXPORT = frozenset({"user.account_uuid"})
_HEX = set("0123456789abcdef")


def _is_hex(s: str, n: int) -> bool:
    return len(s) == n and set(s.lower()) <= _HEX and set(s) != {"0"}


def otel_trace_id(logical_id: str) -> str:
    if _is_hex(logical_id, 32):
        return logical_id.lower()
    return hashlib.sha256(f"ccstitch-trace:{logical_id}".encode()).hexdigest()[:32]


def _synthetic_span_id(*parts: str) -> str:
    return hashlib.sha256(("ccstitch-span:" + "|".join(parts)).encode()).hexdigest()[:16]


def any_value(v: Any) -> Optional[dict[str, Any]]:
    """Python value -> OTLP JSON AnyValue (None for None)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [x for x in (any_value(i) for i in v) if x is not None]}}
    if isinstance(v, dict):
        return {"kvlistValue": {"values": kv_list(v)}}
    return {"stringValue": str(v)}


def kv_list(d: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for k, v in d.items():
        av = any_value(v)
        if av is not None:
            out.append({"key": str(k), "value": av})
    return out


def build(conn, logical_id: str, *, max_spans: int = config.FORWARD_MAX_TRACE_SPANS,
          skip_span_ids: Iterable[str] = (), since_received_ns: Optional[int] = None,
          include_traces: Iterable[str] = ()) -> Optional[dict[str, Any]]:
    """{"payload": ExportTraceServiceRequest, "span_ids": [...], "n_spans": int} for one
    logical trace, or None when the store has no such trace.

    skip_span_ids / since_received_ns / include_traces let the forwarder send only
    what it has not sent before: real spans received after the last export, every
    span of a native trace that joined this logical trace since then (a child the
    stitcher folded in late: its spans arrived long ago but were never sent under
    this trace), and synthetic spans whose ids it has not used."""
    t = store.get_trace(conn, logical_id, max_spans=max_spans, raw=True)
    if t is None:
        return None
    tid = otel_trace_id(logical_id)
    skip = set(skip_span_ids)
    joined = set(include_traces)
    nodes = {n["id"]: n for n in t["nodes"]}
    t0 = int(t["started_ns"])

    sess_agent = {n["session_id"]: (n["detail"].get("agent") or None)
                  for n in t["nodes"] if n["kind"] == "session"}
    # A session node whose direct children are all interactions needs no synthetic
    # span: the CLI's own invoke_agent (the interaction) is there. Any other direct
    # child is an orphan #99607 left behind, and needs the stand-in.
    direct: dict[str, set[str]] = {}
    for n in t["nodes"]:
        if n["parent"] and n["parent"].startswith("s:"):
            direct.setdefault(n["parent"], set()).add(n["kind"])
    healthy = {n["session_id"] for n in t["nodes"]
               if n["kind"] == "session" and direct.get(n["id"]) == {"interaction"}}

    span_id_of: dict[str, Optional[str]] = {}

    def sid_for(nid: Optional[str]) -> Optional[str]:
        """The exported span id standing for node nid (walking through collapsed
        session nodes), or None for a root."""
        seen = 0
        while nid is not None and seen < 64:
            seen += 1
            if nid in span_id_of:
                return span_id_of[nid]
            n = nodes.get(nid)
            if n is None:
                return None
            if n["kind"] == "session" and n["session_id"] in healthy:
                nid = n["parent"]
                continue
            return span_id_of.setdefault(nid, _node_span_id(n, logical_id))
        return None

    groups: dict[str, list[dict[str, Any]]] = {}
    sent: list[str] = []
    synthetic: list[str] = []
    real = 0
    for n in t["nodes"]:
        if n["kind"] == "session" and n["session_id"] in healthy:
            continue
        my_id = sid_for(n["id"])
        if my_id is None:
            continue
        parent = sid_for(n["parent"])
        raw = n.get("raw")
        if raw is not None:
            if (since_received_ns is not None and (raw.get("received_ns") or 0) <= since_received_ns
                    and raw.get("trace_id") not in joined):
                continue
            span = _real_span(n, raw, tid, parent, sess_agent.get(n["session_id"]))
            service = sess_agent.get(n["session_id"]) or DEFAULT_SERVICE
            real += 1
        else:
            if my_id in skip:
                continue
            span = _synthetic_span(n, tid, parent, t0, logical_id)
            synthetic.append(my_id)
            if n["kind"] in ("session", "job"):
                service = n["detail"].get("agent")
            else:
                service = t.get("agent") if t.get("agent") != "unattributed" else None
            service = service or DEFAULT_SERVICE
        groups.setdefault(service, []).append(span)
        sent.append(my_id)
    payload = {"resourceSpans": [
        {"resource": {"attributes": kv_list({"service.name": svc, "service.namespace": "claude-code"})},
         "scopeSpans": [{"scope": SCOPE, "schemaUrl": semconv.SCHEMA_URL, "spans": spans}]}
        for svc, spans in sorted(groups.items())]}
    return {"payload": payload, "span_ids": sent, "synthetic_ids": synthetic,
            "n_spans": int(t["spans"]), "real_spans": real, "trace_id": tid,
            "truncated": bool(t["truncated"])}


def _node_span_id(n: dict[str, Any], logical_id: str) -> str:
    if n.get("raw") is not None or n["kind"] == "job":
        return n["id"]               # real span, or the job's own (referenced) span id
    return _synthetic_span_id(logical_id, n["id"])


def _status(err: bool, error_type: Optional[str], code: Optional[int]) -> dict[str, Any]:
    if err:
        return {"code": 2, "message": error_type or "_OTHER"}
    return {"code": 1} if code == 1 else {"code": 0}


def _real_span(n: dict[str, Any], raw: dict[str, Any], tid: str, parent: Optional[str],
               agent: Optional[str]) -> dict[str, Any]:
    a = {k: v for k, v in (raw.get("attrs") or {}).items() if k not in DROP_ON_EXPORT}
    gen = semconv.span_attrs(n["name"], a, agent=agent, status_code=raw.get("status_code"))
    attrs = {**a, **gen, "claude_code.span.name": n["name"]}
    start = raw.get("start_ns") or 0
    span = {
        "traceId": tid, "spanId": raw["span_id"], "name": semconv.span_name(n["name"], a, agent=agent),
        "kind": semconv.span_kind(n["name"]),
        "startTimeUnixNano": str(start), "endTimeUnixNano": str(raw.get("end_ns") or start),
        "attributes": kv_list(attrs),
        "status": _status(bool(raw.get("err")), gen.get(semconv.ERROR_TYPE), raw.get("status_code")),
    }
    if parent:
        span["parentSpanId"] = parent
    return span


def _synthetic_span(n: dict[str, Any], tid: str, parent: Optional[str], t0: int,
                    logical_id: str) -> dict[str, Any]:
    start = t0 + int(float(n.get("start_ms") or 0) * 1e6)
    end = start + int(float(n.get("dur_ms") or 0) * 1e6)
    d = n.get("detail") or {}
    if n["kind"] == "session":
        agent = d.get("agent")
        attrs = semconv.agent_span_attrs(agent, n["session_id"])
        name = f"{semconv.OP_AGENT} {agent}" if agent else semconv.OP_AGENT
    elif n["kind"] == "job":
        attrs = semconv.workflow_span_attrs(d.get("workflow"), d.get("agent"))
        if d.get("pid"):
            attrs["process.pid"] = int(d["pid"])
        wf = d.get("workflow") or d.get("agent")
        name = f"{semconv.OP_WORKFLOW} {wf}" if wf else semconv.OP_WORKFLOW
    else:
        attrs, name = {}, n.get("label") or "group"
    attrs["ccstitch.synthetic"] = True
    attrs["ccstitch.logical_trace_id"] = logical_id
    span = {"traceId": tid, "spanId": _node_span_id(n, logical_id), "name": name,
            "kind": semconv.KIND_INTERNAL, "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(end), "attributes": kv_list(attrs), "status": {"code": 0}}
    if parent:
        span["parentSpanId"] = parent
    return span


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="ccstitch export",
                                 description="Dump one repaired trace as OTLP/HTTP JSON.")
    ap.add_argument("trace_id", help="logical trace id, as /api/traces lists it (hex or s:<session>)")
    ap.add_argument("--out", help="write to this file instead of stdout")
    ap.add_argument("--db", help=f"trace store (default {config.db_path()})")
    args = ap.parse_args(argv)
    db = Path(args.db) if args.db else config.db_path()
    if not db.exists():
        print(f"no trace store at {db}", file=sys.stderr)
        return 2
    conn = store.connect(db, readonly=True)
    try:
        res = build(conn, args.trace_id.strip())
    finally:
        conn.close()
    if res is None:
        print(f"no trace {args.trace_id!r} in {db}", file=sys.stderr)
        return 1
    text = json.dumps(res["payload"], indent=2)
    if args.out:
        Path(args.out).write_text(text + "\n")
        print(f"wrote {len(res['span_ids'])} spans (trace {res['trace_id']}) to {args.out}",
              file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
