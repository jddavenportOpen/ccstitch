"""ccstitch, industry-standard layer: the OpenTelemetry GenAI mapping
(semconv.py), resource-attribute attribution, the job trace context (context.py),
job-rooted and parentless stitching, the OTLP exporter (export.py) and the forwarder
(forward.py) against a fake local receiver. Hermetic: tmp SQLite, ephemeral ports,
no claude, no network beyond 127.0.0.1."""
from __future__ import annotations

import contextvars
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ccstitch import (collector, config, context, export, forward, hook, otlp, semconv,
                      stitch, store)

NOW = time.time_ns()


def _attr(k, v):
    if isinstance(v, bool):
        return {"key": k, "value": {"boolValue": v}}
    if isinstance(v, int):
        return {"key": k, "value": {"intValue": str(v)}}
    if isinstance(v, list):
        return {"key": k, "value": {"arrayValue": {"values": [{"stringValue": x} for x in v]}}}
    return {"key": k, "value": {"stringValue": v}}


def span(trace, sid, name, *, parent=None, session="S1", start=0, dur=10, status=0, **attrs):
    a = {"session.id": session, "user.account_uuid": "acct-1", **attrs}
    return {"traceId": trace, "spanId": sid, "parentSpanId": parent or "", "name": name,
            "startTimeUnixNano": str(NOW + start * 10**6),
            "endTimeUnixNano": str(NOW + (start + dur) * 10**6),
            "status": {"code": status}, "attributes": [_attr(k, v) for k, v in a.items()]}


def payload(*spans, resource=None):
    res = {"service.name": "claude-code", **(resource or {})}
    return {"resourceSpans": [{"resource": {"attributes": [_attr(k, v) for k, v in res.items()]},
                               "scopeSpans": [{"spans": list(spans)}]}]}


def T(n):
    return f"{n:032x}"


def S(n):
    return f"{n:016x}"


def ingest(conn, *spans, resource=None):
    return store.ingest(conn, otlp.parse_traces(payload(*spans, resource=resource)))


LLM = dict(model="claude-opus-5-5", input_tokens=10, output_tokens=100, cache_read_tokens=1000,
           cache_creation_tokens=50, success=True, ttft_ms=1500, effort="high",
           stop_reason="end_turn", request_id="req_1")


def healthy_turn(trace, session="S1", base=1, root_parent=None, start=0):
    i, m, t, x = S(base), S(base + 1), S(base + 2), S(base + 3)
    return [span(trace, i, otlp.SPAN_INTERACTION, parent=root_parent, session=session,
                 start=start, dur=100),
            span(trace, m, otlp.SPAN_LLM, parent=i, session=session, start=start + 1, dur=50, **LLM),
            span(trace, t, otlp.SPAN_TOOL, parent=i, session=session, start=start + 60, dur=30,
                 tool_name="Bash", tool_use_id="toolu_1"),
            span(trace, x, "claude_code.tool.execution", parent=t, session=session,
                 start=start + 61, dur=28, success=True, tool_use_id="toolu_1")], x


# ── semconv ─────────────────────────────────────────────────────────────────

def test_llm_span_maps_to_genai_chat_with_anthropic_token_rule():
    a = {"session.id": "S", **LLM}
    g = semconv.span_attrs(otlp.SPAN_LLM, a, agent="planner")
    assert g["gen_ai.operation.name"] == "chat"
    assert g["gen_ai.provider.name"] == "anthropic" and g["gen_ai.system"] == "anthropic"
    assert g["gen_ai.request.model"] == g["gen_ai.response.model"] == "claude-opus-5-5"
    # Anthropic input_tokens excludes cache: the convention's total includes both.
    assert g["gen_ai.usage.input_tokens"] == 10 + 1000 + 50
    assert g["gen_ai.usage.cache_read.input_tokens"] == 1000
    assert g["gen_ai.usage.cache_write.input_tokens"] == 50
    assert g["gen_ai.usage.output_tokens"] == 100
    assert g["gen_ai.response.finish_reasons"] == ["end_turn"]
    assert g["gen_ai.response.time_to_first_chunk"] == 1.5
    assert g["gen_ai.request.reasoning.level"] == "high"
    assert g["gen_ai.response.id"] == "req_1"
    assert g["gen_ai.conversation.id"] == "S" and g["gen_ai.agent.name"] == "planner"
    assert "error.type" not in g
    assert "input_tokens" not in g and a["input_tokens"] == 10      # raw counters untouched
    assert semconv.span_name(otlp.SPAN_LLM, a) == "chat claude-opus-5-5"
    assert semconv.span_kind(otlp.SPAN_LLM) == semconv.KIND_CLIENT


def test_failed_llm_span_gets_low_cardinality_error_type():
    a = {"model": "claude-haiku-4-5", "success": False, "status_code": 429,
         "error": "This request would exceed your account's rate limit.", "error_class": "rate_limit"}
    g = semconv.span_attrs(otlp.SPAN_LLM, a, status_code=2)
    assert g["error.type"] == "rate_limit" and "gen_ai.response.model" not in g
    assert semconv.error_type(otlp.SPAN_LLM, {"success": False, "status_code": 503}) == "503"
    assert semconv.error_type(otlp.SPAN_LLM, {}, status_code=2) == "_OTHER"


def test_tool_and_interaction_spans_map_to_execute_tool_and_invoke_agent():
    g = semconv.span_attrs(otlp.SPAN_TOOL, {"tool_name": "Bash", "gen_ai.tool.call.id": "toolu_9"})
    assert (g["gen_ai.operation.name"], g["gen_ai.tool.name"], g["gen_ai.tool.call.id"],
            g["gen_ai.tool.type"]) == ("execute_tool", "Bash", "toolu_9", "function")
    assert semconv.span_attrs(otlp.SPAN_TOOL, {"tool_name": "mcp__weather__forecast"})["gen_ai.tool.type"] == "extension"
    assert semconv.span_name(otlp.SPAN_TOOL, {"tool_name": "Read"}) == "execute_tool Read"
    ex = semconv.span_attrs("claude_code.tool.execution", {"tool_use_id": "toolu_9", "success": False})
    assert "gen_ai.operation.name" not in ex and ex["gen_ai.tool.call.id"] == "toolu_9"
    assert ex["error.type"] == "tool_error"
    ia = semconv.span_attrs(otlp.SPAN_INTERACTION, {"session.id": "S"}, agent="lead")
    assert ia["gen_ai.operation.name"] == "invoke_agent" and ia["gen_ai.agent.name"] == "lead"
    assert semconv.span_name(otlp.SPAN_INTERACTION, {}, agent="lead") == "invoke_agent lead"


def test_trace_detail_carries_genai_attributes_additively():
    conn = store.connect()
    kids, _ = healthy_turn(T(1))
    ingest(conn, *kids)
    store.record_link(conn, {"session_id": "S1", "agent": "planner"})
    stitch.run(conn)
    t = store.get_trace(conn, T(1))
    llm = next(n for n in t["nodes"] if n["kind"] == "llm")
    assert llm["detail"]["input_tokens"] == 10                       # the CLI key is still there
    assert llm["detail"]["gen_ai.usage.input_tokens"] == 1060
    assert llm["detail"]["gen_ai.agent.name"] == "planner"
    assert "raw" not in llm                                          # internal fields stay internal


# ── attribution through the resource ────────────────────────────────────────

def test_resource_agent_attributes_a_hookless_session():
    conn = store.connect()
    ingest(conn, span(T(1), S(1), otlp.SPAN_LLM, session="H", **LLM),
           resource={otlp.RESOURCE_AGENT: "evaluator"})
    r = conn.execute("SELECT agent, attribution FROM sessions WHERE session_id='H'").fetchone()
    assert (r["agent"], r["attribution"]) == ("evaluator", "resource")


def test_unresolved_hook_record_is_completed_by_resource_and_hook_answer_wins():
    conn = store.connect()
    store.record_link(conn, {"session_id": "U", "agent": None, "attribution": "unresolved", "cwd": "/tmp"})
    ingest(conn, span(T(1), S(1), otlp.SPAN_LLM, session="U"), resource={otlp.RESOURCE_AGENT: "evaluator"})
    r = conn.execute("SELECT agent, attribution, cwd FROM sessions WHERE session_id='U'").fetchone()
    assert (r["agent"], r["attribution"], r["cwd"]) == ("evaluator", "resource", "/tmp")
    store.record_link(conn, {"session_id": "E", "agent": "planner", "attribution": "env"})
    ingest(conn, span(T(2), S(2), otlp.SPAN_LLM, session="E"), resource={otlp.RESOURCE_AGENT: "evaluator"})
    r = conn.execute("SELECT agent, attribution FROM sessions WHERE session_id='E'").fetchone()
    assert (r["agent"], r["attribution"]) == ("planner", "env")


# ── job trace context ───────────────────────────────────────────────────────

def test_inherited_traceparent_is_propagated_untouched():
    tp = f"00-{T(7)}-{S(7)}-01"
    env = {"TRACEPARENT": tp}
    assert "TRACEPARENT" not in context.child_env(env)
    assert context.job_traceparent(env) == tp


def test_one_process_one_trace_then_rotation(monkeypatch):
    recs = []
    monkeypatch.setattr(context, "_record", recs.append)
    a = context.child_env({})["TRACEPARENT"]
    b = context.child_env({})["TRACEPARENT"]
    assert a == b and otlp.parse_traceparent(a) and len(recs) == 1
    assert recs[0]["kind"] == "job" and recs[0]["pid"] == os.getpid()
    assert (recs[0]["trace_id"], recs[0]["span_id"]) == otlp.parse_traceparent(a)
    real = time.time
    monkeypatch.setattr(context.time, "time", lambda: real() + config.job_max_s() + 1)
    c = context.child_env({})["TRACEPARENT"]
    assert c != a and len(recs) == 2          # a daemon does not become one multi-day trace


def test_malformed_inherited_traceparent_is_replaced_and_fork_remints(monkeypatch):
    monkeypatch.setattr(context, "_record", lambda rec: None)
    a = context.child_env({"TRACEPARENT": "garbage"})["TRACEPARENT"]
    assert otlp.parse_traceparent(a)
    monkeypatch.setattr(context.os, "getpid", lambda: 999_999)
    assert context.child_env({})["TRACEPARENT"] != a


def test_kill_switch_and_never_raises(monkeypatch):
    assert context.child_env({"CCSTITCH_JOB_CONTEXT": "0"}) == {}
    tp = f"00-{T(7)}-{S(7)}-01"
    assert context.job_traceparent({"CCSTITCH_JOB_CONTEXT": "0", "TRACEPARENT": tp}) == tp

    def boom(env=None):
        raise RuntimeError("x")
    monkeypatch.setattr(context, "job_traceparent", boom)
    assert context.child_env({}) == {} and context.shell_exports({}) == ""


def test_resource_attributes_merge_and_replace_agent(monkeypatch):
    monkeypatch.setattr(context, "_record", lambda rec: None)
    out = context.child_env({"CCSTITCH_AGENT": "evaluator",
                             "OTEL_RESOURCE_ATTRIBUTES": "team=x,ccstitch.agent=old"})
    assert out["OTEL_RESOURCE_ATTRIBUTES"] == "team=x,ccstitch.agent=evaluator"
    assert "OTEL_RESOURCE_ATTRIBUTES" not in context.child_env({})


def test_job_record_reaches_the_collector(monkeypatch):
    got, done = [], threading.Event()

    def fake_deliver(rec):
        got.append(rec)
        done.set()
        return True
    monkeypatch.setenv("CCSTITCH_JOB_RECORD_IN_TESTS", "1")
    monkeypatch.setattr(hook, "deliver", fake_deliver)
    context.child_env({"CCSTITCH_AGENT": "nightly"})
    assert done.wait(5) and got[0]["agent"] == "nightly" and got[0]["kind"] == "job"
    conn = store.connect()
    assert store.record_link(conn, got[0])                  # the collector's /v1/links door
    assert conn.execute("SELECT agent FROM jobs").fetchone()[0] == "nightly"
    assert not store.record_link(conn, {"kind": "job", "trace_id": "zz", "span_id": "1"})


def test_job_records_are_never_written_from_a_test_run(monkeypatch):
    monkeypatch.delenv("CCSTITCH_JOB_RECORD_IN_TESTS", raising=False)
    called = []
    monkeypatch.setattr(hook, "deliver", lambda rec: called.append(rec))
    context.child_env({})
    time.sleep(0.05)
    assert called == []


def test_hook_spool_stops_growing_past_the_cap(monkeypatch):
    monkeypatch.setenv("CCSTITCH_PORT", "1")
    monkeypatch.setattr(config, "MAX_SPOOL_BYTES", 10)
    hook.deliver({"session_id": "a" * 20})
    size = config.spool_path().stat().st_size
    hook.deliver({"session_id": "b"})
    assert config.spool_path().stat().st_size == size


# ── job-rooted and parentless stitching ─────────────────────────────────────

def _job(conn, trace, span_id, agent="nightly", workflow="nightly_report"):
    assert store.record_link(conn, {"kind": "job", "trace_id": trace, "span_id": span_id,
                                    "agent": agent, "workflow": workflow, "pid": 42,
                                    "started_ns": NOW - 10**6})


def test_job_is_the_root_of_its_claude_calls():
    """A cron job calls claude twice: one healthy turn (joins the job trace natively)
    and one broken by #99607 (folds in through its SessionStart TRACEPARENT)."""
    conn = store.connect()
    job_tp = f"00-{T(100)}-{S(100)}-01"
    _job(conn, T(100), S(100))
    kids, _ = healthy_turn(T(100), session="A", root_parent=S(100))
    ingest(conn, *kids)
    store.record_link(conn, {"session_id": "A", "agent": "nightly", "traceparent": job_tp})
    store.record_link(conn, {"session_id": "B", "agent": "nightly", "traceparent": job_tp})
    ingest(conn, span(T(201), S(201), otlp.SPAN_LLM, session="B", start=200, **LLM),
           span(T(202), S(202), otlp.SPAN_LLM, session="B", start=300, **LLM))
    stitch.run(conn)
    assert {r[0] for r in conn.execute("SELECT logical_id FROM ntraces")} == {T(100)}
    t = store.get_trace(conn, T(100))
    by_id = {n["id"]: n for n in t["nodes"]}
    job = by_id[S(100)]
    assert job["kind"] == "job" and job["parent"] is None and job["label"] == "nightly"
    assert job["detail"]["gen_ai.operation.name"] == "invoke_workflow"
    assert by_id["s:A"]["parent"] == S(100) and by_id["s:B"]["parent"] == S(100)
    assert by_id[S(1)]["parent"] == "s:A" and by_id[S(201)]["parent"] == "s:B"
    assert "root" not in by_id and t["agent"] == "nightly" and t["sessions"] == 2
    listed = store.list_traces(conn, 0)
    assert [x["id"] for x in listed] == [T(100)] and listed[0]["agent"] == "nightly"


def test_missing_root_span_still_groups_siblings_after_grace():
    """The parent span never arrives (killed process, lost job record): siblings fold
    into the trace id their TRACEPARENT named once they have been quiet a while."""
    conn = store.connect()
    tp = f"00-{T(300)}-{S(300)}-01"
    old = -(stitch.LINK_GRACE_S + 120) * 1000
    for sess, n in (("C1", 1), ("C2", 2)):
        store.record_link(conn, {"session_id": sess, "traceparent": tp, "started_ns": NOW + old * 10**6})
        ingest(conn, span(T(300 + n), S(300 + n), otlp.SPAN_LLM, session=sess, start=old, **LLM))
    stitch.run(conn)
    assert {r[0] for r in conn.execute("SELECT logical_id FROM ntraces")} == {T(300)}
    t = store.get_trace(conn, T(300))
    by_id = {n["id"]: n for n in t["nodes"]}
    assert by_id["root"]["parent"] is None
    assert by_id["s:C1"]["parent"] == "root" and by_id["s:C2"]["parent"] == "root"


def test_old_database_without_jobs_table_still_reads(tmp_path):
    db = tmp_path / "old.db"
    conn = store.connect(db)
    kids, _ = healthy_turn(T(1))
    ingest(conn, *kids)
    stitch.run(conn)
    conn.execute("DROP TABLE jobs")
    conn.commit()
    ro = store.connect(db, readonly=True)
    assert store.get_trace(ro, T(1))["spans"] == 4
    assert store.list_traces(ro, 0)[0]["id"] == T(1)


# ── export ──────────────────────────────────────────────────────────────────

def _spans_of(p):
    out = {}
    for rs in p["resourceSpans"]:
        svc = next(a["value"]["stringValue"] for a in rs["resource"]["attributes"]
                   if a["key"] == "service.name")
        for ss in rs["scopeSpans"]:
            for s in ss["spans"]:
                attrs = {a["key"]: next(iter(a["value"].values())) for a in s["attributes"]}
                out[s["spanId"]] = {**s, "attrs": attrs, "service": svc}
    return out


def test_export_broken_turn_gets_synthetic_invoke_agent_root():
    conn = store.connect()
    ingest(conn, span(T(11), S(1), otlp.SPAN_LLM, session="B", **LLM),
           span(T(12), S(2), otlp.SPAN_TOOL, session="B", start=20, tool_name="Read"),
           span(T(12), S(3), "claude_code.tool.execution", parent=S(2), session="B", start=21))
    store.record_link(conn, {"session_id": "B", "agent": "researcher"})
    stitch.run(conn)
    res = export.build(conn, "s:B")
    sp = _spans_of(res["payload"])
    assert len(res["trace_id"]) == 32 and res["trace_id"] == export.otel_trace_id("s:B")
    assert {s["traceId"] for s in sp.values()} == {res["trace_id"]}
    root = [s for s in sp.values() if "parentSpanId" not in s]
    assert len(root) == 1 and root[0]["name"] == "invoke_agent researcher"
    assert root[0]["attrs"]["gen_ai.operation.name"] == "invoke_agent"
    assert root[0]["attrs"]["ccstitch.synthetic"] is True
    assert sp[S(1)]["parentSpanId"] == root[0]["spanId"] and sp[S(3)]["parentSpanId"] == S(2)
    llm = sp[S(1)]
    assert llm["name"] == "chat claude-opus-5-5" and llm["kind"] == semconv.KIND_CLIENT
    assert llm["attrs"]["gen_ai.usage.input_tokens"] == "1060"           # intValue is a string
    assert llm["attrs"]["claude_code.span.name"] == otlp.SPAN_LLM
    assert "user.account_uuid" not in llm["attrs"] and llm["service"] == "researcher"
    assert res["synthetic_ids"] == [root[0]["spanId"]]


def test_export_healthy_turn_keeps_native_ids_and_adds_nothing():
    conn = store.connect()
    kids, _ = healthy_turn(T(1))
    ingest(conn, *kids)
    stitch.run(conn)
    res = export.build(conn, T(1))
    sp = _spans_of(res["payload"])
    assert set(sp) == {S(1), S(2), S(3), S(4)} and res["trace_id"] == T(1)
    assert "parentSpanId" not in sp[S(1)] and sp[S(2)]["parentSpanId"] == S(1)
    assert sp[S(1)]["name"] == "invoke_agent" and sp[S(3)]["name"] == "execute_tool Bash"
    assert sp[S(1)]["service"] == export.DEFAULT_SERVICE


def test_export_job_trace_root_uses_the_span_id_children_name():
    conn = store.connect()
    _job(conn, T(100), S(100))
    kids, _ = healthy_turn(T(100), session="A", root_parent=S(100))
    ingest(conn, *kids)
    store.record_link(conn, {"session_id": "A", "agent": "nightly",
                             "traceparent": f"00-{T(100)}-{S(100)}-01"})
    stitch.run(conn)
    sp = _spans_of(export.build(conn, T(100))["payload"])
    assert sp[S(100)]["name"] == "invoke_workflow nightly_report"
    assert "parentSpanId" not in sp[S(100)] and sp[S(1)]["parentSpanId"] == S(100)
    assert sp[S(100)]["attrs"]["process.pid"] == "42"


def test_export_cli_writes_a_file(tmp_path, capsys):
    conn = store.connect()
    kids, _ = healthy_turn(T(1))
    ingest(conn, *kids)
    stitch.run(conn)
    conn.close()
    out = tmp_path / "t.json"
    assert export.main([T(1), "--out", str(out), "--db", str(config.db_path())]) == 0
    assert len(_spans_of(json.loads(out.read_text()))) == 4
    assert export.main(["nope", "--db", str(config.db_path())]) == 1


# ── forwarder ───────────────────────────────────────────────────────────────

class Receiver:
    def __init__(self, codes=()):
        self.codes = list(codes)
        self.bodies: list[dict] = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length") or 0))
                code = outer.codes.pop(0) if outer.codes else 200
                if code == 200:
                    ctype = self.headers.get("content-type") or ""
                    outer.bodies.append({"path": self.path, "auth": self.headers.get("authorization"),
                                         "ctype": ctype, "raw": body,
                                         "json": json.loads(body) if "json" in ctype else None})
                self.send_response(code)
                self.send_header("content-length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def close(self):
        self.httpd.shutdown()


@pytest.fixture
def receiver():
    r = Receiver()
    yield r
    r.close()


def _settled(conn):
    """A broken session from 20 min ago, already quiet."""
    old = -20 * 60 * 1000
    ingest(conn, span(T(11), S(1), otlp.SPAN_LLM, session="B", start=old, **LLM),
           span(T(12), S(2), otlp.SPAN_TOOL, session="B", start=old + 20, tool_name="Read"))
    stitch.run(conn)
    conn.execute("UPDATE ntraces SET updated_ns = ?", (NOW - 20 * 60 * 10**9,))
    conn.commit()


def _drain(fw):
    while True:
        b = fw._next_batch() if not fw.q.empty() else None
        if b is None:
            return
        fw.send(b)


def test_forwarder_sends_repaired_trace_once_then_only_new_spans(receiver):
    conn = store.connect()
    _settled(conn)
    fw = forward.Forwarder(receiver.url + "/v1/traces", headers={"Authorization": "Basic x"},
                           settle_s=60, backfill_s=86400, backoff_s=0, protocol="http/json")
    assert fw.collect(conn) == 1
    _drain(fw)
    assert len(receiver.bodies) == 1 and receiver.bodies[0]["path"] == "/v1/traces"
    assert receiver.bodies[0]["auth"] == "Basic x"
    sent = _spans_of(receiver.bodies[0]["json"])
    assert len(sent) == 3                                   # 2 real + the synthetic session root
    assert fw.collect(conn) == 0                            # nothing new: nothing re-sent
    # a late span lands in the same logical trace
    ingest(conn, span(T(13), S(3), otlp.SPAN_LLM, session="B", start=-19 * 60 * 1000, **LLM))
    stitch.run(conn)
    conn.execute("UPDATE ntraces SET updated_ns = ?", (NOW - 20 * 60 * 10**9,))
    conn.commit()
    assert fw.collect(conn) == 1
    _drain(fw)
    assert set(_spans_of(receiver.bodies[1]["json"])) == {S(3)}
    h = fw.health()
    assert h["spans_sent"] == 4 and h["dropped_spans"] == 0 and h["enabled"]


def test_forwarder_waits_for_a_trace_to_settle(receiver):
    conn = store.connect()
    kids, _ = healthy_turn(T(1))
    ingest(conn, *kids)                                    # just arrived
    stitch.run(conn)
    fw = forward.Forwarder(receiver.url, settle_s=300, backfill_s=86400)
    assert fw.collect(conn) == 0


def test_forwarder_retries_transient_failures_with_backoff():
    r = Receiver(codes=[503, 429])
    try:
        fw = forward.Forwarder(r.url, backoff_s=0)
        assert fw.send({"payload": {"resourceSpans": []}, "spans": 1})
        assert fw.stats["retries"] == 2 and fw.stats["batches_sent"] == 1
    finally:
        r.close()


def test_forwarder_does_not_retry_a_rejected_batch_and_counts_the_drop():
    r = Receiver(codes=[400])
    try:
        fw = forward.Forwarder(r.url, backoff_s=0)
        assert not fw.send({"payload": {"resourceSpans": []}, "spans": 7})
        assert fw.stats["retries"] == 0 and fw.stats["failed_batches"] == 1
        assert fw.stats["dropped_spans"] == 7 and fw.stats["last_error"] == "HTTP 400"
    finally:
        r.close()


def test_forwarder_unreachable_endpoint_drops_after_attempts():
    fw = forward.Forwarder("http://127.0.0.1:1/v1/traces", backoff_s=0, max_attempts=3, timeout_s=1)
    assert not fw.send({"payload": {"resourceSpans": []}, "spans": 2})
    assert fw.stats["retries"] == 2 and fw.stats["dropped_spans"] == 2


def test_forwarder_full_queue_drops_instead_of_blocking():
    conn = store.connect()
    _settled(conn)
    ingest(conn, span(T(21), S(21), otlp.SPAN_LLM, session="Z", start=-20 * 60 * 1000, **LLM))
    stitch.run(conn)
    conn.execute("UPDATE ntraces SET updated_ns = ?", (NOW - 20 * 60 * 10**9,))
    conn.commit()
    fw = forward.Forwarder("http://127.0.0.1:1", queue_max=1, settle_s=60, backfill_s=86400)
    t0 = time.monotonic()
    assert fw.collect(conn) == 1
    assert time.monotonic() - t0 < 2
    assert fw.stats["dropped_traces"] == 1 and fw.health()["queued"] == 1


def test_forwarder_thread_end_to_end(receiver):
    conn = store.connect()
    _settled(conn)
    fw = forward.Forwarder(receiver.url, settle_s=60, backfill_s=86400, backoff_s=0, protocol="http/json")
    fw.start()
    try:
        fw.collect(conn)
        deadline = time.time() + 5
        while not receiver.bodies and time.time() < deadline:
            time.sleep(0.05)
        assert receiver.bodies
    finally:
        fw.close()


def test_forwarder_collects_on_its_own_connection_without_the_collector_lock(receiver, monkeypatch):
    conn = store.connect()
    _settled(conn)
    conn.close()
    monkeypatch.setattr(config, "STITCH_INTERVAL_S", 0.05)
    state = collector.State()
    fw = forward.Forwarder(receiver.url, settle_s=60, backfill_s=86400, backoff_s=0, protocol="http/json")
    with state.lock:                       # ingest's lock held the whole time: no effect
        fw.start(config.db_path())
        try:
            deadline = time.time() + 5
            while not receiver.bodies and time.time() < deadline:
                time.sleep(0.05)
        finally:
            fw.close()
    assert receiver.bodies and len(_spans_of(receiver.bodies[0]["json"])) == 3


def test_forward_config_shapes(monkeypatch):
    monkeypatch.setenv("CCSTITCH_FORWARD_ENDPOINT", "http://127.0.0.1:6006")
    assert config.forward_endpoint() == "http://127.0.0.1:6006/v1/traces"
    monkeypatch.setenv("CCSTITCH_FORWARD_ENDPOINT", "https://x.example/api/public/otel/v1/traces")
    assert config.forward_endpoint().endswith("/otel/v1/traces")
    monkeypatch.setenv("CCSTITCH_FORWARD_ENDPOINT", "ftp://nope")
    assert config.forward_endpoint() is None and forward.from_env() is None
    monkeypatch.setenv("CCSTITCH_FORWARD_HEADERS", "Authorization=Basic%20abc,x-team=a")
    assert config.forward_headers() == {"Authorization": "Basic abc", "x-team": "a"}


# ── collector ───────────────────────────────────────────────────────────────

def test_send_swallows_a_client_hangup_and_counts_it():
    state = collector.State()
    Handler = collector.make_handler(state)
    h = Handler.__new__(Handler)
    h.send_response = h.send_header = h.end_headers = lambda *a, **k: None

    class Gone:
        def write(self, b):
            raise BrokenPipeError(32, "Broken pipe")
    h.wfile = Gone()
    h._send(200, {})
    assert state.client_gone == 1 and h.close_connection
    hz = collector.health(state)
    assert hz["client_gone"] == 1 and hz["forward"] == {"enabled": False}


def test_cli_literals_name_what_semconv_reads():
    for lit in ("error_class", "gen_ai.tool.call.id", "OTEL_RESOURCE_ATTRIBUTES"):
        assert lit in otlp.CLI_LITERALS


# ── regressions ──────────────────────────────────────────────────


def test_hanging_collector_does_not_delay_child_env(monkeypatch):
    """The job record is posted from a daemon thread: a collector that accepts and
    never answers must cost the spawn nothing."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)                                  # accepts the connect, never replies
    try:
        monkeypatch.setenv("CCSTITCH_PORT", str(srv.getsockname()[1]))
        monkeypatch.setenv("CCSTITCH_JOB_RECORD_IN_TESTS", "1")
        t0 = time.monotonic()
        out = context.child_env({})
        assert time.monotonic() - t0 < 0.1 and otlp.parse_traceparent(out["TRACEPARENT"])
    finally:
        srv.close()


def test_resource_attribute_value_is_percent_encoded():
    got = context._resource_attrs("team=a", "odd,agent=x y")
    assert got == "team=a,ccstitch.agent=odd%2Cagent%3Dx%20y"
    assert got.count(",") == 1 and got.split(",")[1].count("=") == 1


def test_each_spawned_run_is_its_own_unit_labelled_by_its_agent(monkeypatch):
    recs = []
    monkeypatch.setattr(context, "_record", recs.append)
    a = context.shell_exports({}, fresh=True, agent="nightly", workflow="run:aaaa")
    b = context.shell_exports({}, fresh=True, agent="planner", workflow="run:bbbb")
    assert a != b and a.startswith("export TRACEPARENT=00-")
    assert [(r["agent"], r["workflow"]) for r in recs] == [("nightly", "run:aaaa"), ("planner", "run:bbbb")]
    tp = f"00-{T(9)}-{S(9)}-01"                   # launched from a traced tool call: hang under it
    assert context.shell_exports({"TRACEPARENT": tp}, fresh=True, agent="x") == f"export TRACEPARENT={tp}; "
    assert len(recs) == 2


def test_unit_scopes_claude_calls_and_crosses_copied_contexts(monkeypatch):
    recs = []
    monkeypatch.setattr(context, "_record", recs.append)
    process_tp = context.job_traceparent({})
    with context.unit(agent="evals", workflow="case-1") as tp:
        assert tp != process_tp and context.child_env({})["TRACEPARENT"] == tp
        with context.unit(agent="inner") as inner:
            assert inner == tp                     # nested units reuse the outer one
        got = []
        ctx = contextvars.copy_context()
        th = threading.Thread(target=lambda: got.append(ctx.run(context.job_traceparent, {})))
        th.start()
        th.join()
        assert got == [tp]
    assert context.job_traceparent({}) == process_tp
    assert recs[-1]["workflow"] == "case-1"


def test_fork_resets_the_lock_and_the_context(monkeypatch):
    monkeypatch.setattr(context, "_record", lambda rec: None)
    before = context.job_traceparent({})
    held = context._LOCK
    held.acquire()                                  # another thread held it at fork time
    try:
        context._after_fork_in_child()
        after = context.job_traceparent({})         # must not deadlock
        assert after != before and context._LOCK is not held
    finally:
        held.release()


def test_job_without_agent_keeps_the_sessions_agent():
    conn = store.connect()
    _job(conn, T(100), S(100), agent=None, workflow="queue_worker")
    kids, _ = healthy_turn(T(100), session="A", root_parent=S(100))
    ingest(conn, *kids)
    store.record_link(conn, {"session_id": "A", "agent": "nightly",
                             "traceparent": f"00-{T(100)}-{S(100)}-01"})
    stitch.run(conn)
    assert store.list_traces(conn, 0)[0]["agent"] == "nightly"
    t = store.get_trace(conn, T(100))
    assert t["agent"] == "nightly"
    assert next(n for n in t["nodes"] if n["kind"] == "job")["label"] == "queue_worker"


def test_child_inside_grace_window_stays_on_its_own():
    conn = store.connect()
    store.record_link(conn, {"session_id": "C", "traceparent": f"00-{T(300)}-{S(300)}-01"})
    ingest(conn, span(T(301), S(301), otlp.SPAN_LLM, session="C", **LLM))   # recent
    stitch.run(conn)
    assert conn.execute("SELECT logical_id FROM ntraces").fetchone()[0] == "s:C"


def test_siblings_of_different_parents_never_merge():
    conn = store.connect()
    old = -(stitch.LINK_GRACE_S + 120) * 1000
    for sess, n, parent in (("C1", 1, T(300)), ("C2", 2, T(400))):
        store.record_link(conn, {"session_id": sess, "traceparent": f"00-{parent}-{S(300)}-01",
                                 "started_ns": NOW + old * 10**6})
        ingest(conn, span(T(500 + n), S(500 + n), otlp.SPAN_LLM, session=sess, start=old, **LLM))
    stitch.run(conn)
    got = dict(conn.execute("SELECT session_id, logical_id FROM ntraces").fetchall())
    assert got == {"C1": T(300), "C2": T(400)}


def test_forward_settle_window_always_exceeds_the_stitch_grace(monkeypatch):
    monkeypatch.setenv("CCSTITCH_FORWARD_SETTLE_S", "30")
    assert config.forward_settle_s() >= stitch.LINK_GRACE_S + 60
    monkeypatch.delenv("CCSTITCH_FORWARD_SETTLE_S")
    assert forward.Forwarder("http://127.0.0.1:1").settle_s > stitch.LINK_GRACE_S


def _age(conn):
    conn.execute("UPDATE ntraces SET updated_ns = ?", (NOW - 30 * 60 * 10**9,))
    conn.commit()


def test_child_folded_in_after_export_is_resent_whole_under_the_parent(receiver):
    """The parent was already forwarded when a late link folds a child in. The child's
    spans arrived long before that export; they must still go out, under the parent's
    trace and the launching tool call, not only the placeholder."""
    conn = store.connect()
    old = -30 * 60 * 1000
    kids, exec_span = healthy_turn(T(1), session="P", start=old)
    ingest(conn, *kids)
    ingest(conn, span(T(21), S(31), otlp.SPAN_LLM, session="C", start=old + 62, dur=5, **LLM),
           span(T(22), S(32), otlp.SPAN_LLM, session="C", start=old + 70, dur=5, **LLM))
    stitch.run(conn)
    _age(conn)
    fw = forward.Forwarder(receiver.url, settle_s=60, backfill_s=86400, backoff_s=0, protocol="http/json")
    fw.collect(conn)
    _drain(fw)
    first = {}
    for b in receiver.bodies:
        first.update(_spans_of(b["json"]))
    assert first[S(1)]["traceId"] == T(1)
    # the late link (a spooled SessionStart record replayed after a collector restart)
    store.record_link(conn, {"session_id": "C", "agent": "researcher",
                             "traceparent": f"00-{T(1)}-{exec_span}-01"})
    stitch.run(conn)
    _age(conn)
    n_before = len(receiver.bodies)
    fw.collect(conn)
    _drain(fw)
    last = {}
    for b in receiver.bodies[n_before:]:
        last.update(_spans_of(b["json"]))
    assert {S(31), S(32)} <= set(last)
    assert {s["traceId"] for s in last.values()} == {T(1)}
    placeholder = last[S(31)]["parentSpanId"]
    assert last[placeholder]["parentSpanId"] == exec_span
    assert last[placeholder]["name"] == "invoke_agent researcher"
    assert not ({S(1), S(2), S(3), S(4)} & set(last))      # the parent's own spans are not resent


def test_forward_truncation_is_counted_and_never_recorded_as_sent(receiver):
    conn = store.connect()
    _settled(conn)
    fw = forward.Forwarder(receiver.url, settle_s=60, backfill_s=86400, backoff_s=0,
                           max_trace_spans=1, protocol="http/json")
    assert fw.collect(conn) == 1
    r = conn.execute("SELECT n_spans, sent_spans FROM exports").fetchone()
    assert (r["n_spans"], r["sent_spans"]) == (2, 1)
    assert fw.stats["truncated_traces"] == 1 and fw.stats["truncated_spans"] == 1
    assert fw.collect(conn) == 0                           # no rebuild loop


def test_forward_queue_is_bounded_by_spans():
    fw = forward.Forwarder("http://127.0.0.1:1", queue_max=100, queue_max_spans=5)
    assert fw._enqueue({"resourceSpans": []}, 4)
    assert not fw._enqueue({"resourceSpans": []}, 4)       # 8 > 5 spans: dropped, not queued
    assert fw._enqueue({"resourceSpans": []}, 1)
    assert fw.health()["queued_spans"] == 5
    fw._next_batch()
    assert fw.health()["queued_spans"] == 0
    big = forward.Forwarder("http://127.0.0.1:1", queue_max_spans=5)
    assert big._enqueue({"resourceSpans": []}, 50)         # one oversize trace may go alone
