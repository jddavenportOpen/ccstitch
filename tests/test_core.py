"""ccstitch core: OTLP parse, ingest, the #99607 repair, cross-process links,
retention, the collector's HTTP surface, and the SessionStart hook. Hermetic: a tmp
SQLite file and a collector on an ephemeral port; no claude, no network."""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from ccstitch import collector, config, hook, otlp, stitch, store

NOW = time.time_ns()


def _attr(k, v):
    if isinstance(v, bool):
        return {"key": k, "value": {"boolValue": v}}
    if isinstance(v, int):
        return {"key": k, "value": {"intValue": str(v)}}
    return {"key": k, "value": {"stringValue": v}}


def span(trace, sid, name, *, parent=None, session="S1", start=0, dur=10, **attrs):
    a = {"session.id": session, "user.email": "someone@example.com", "user.id": "u",
         "user.account_uuid": "00000000-0000-4000-8000-000000000000", "organization.id": "org",
         "terminal.type": "tty", **attrs}
    return {"traceId": trace, "spanId": sid, "parentSpanId": parent or "", "name": name,
            "startTimeUnixNano": str(NOW + start * 10**6),
            "endTimeUnixNano": str(NOW + (start + dur) * 10**6),
            "status": {"code": 0}, "attributes": [_attr(k, v) for k, v in a.items()]}


def payload(*spans):
    return {"resourceSpans": [{"resource": {"attributes": [_attr("service.name", "claude-code")]},
                               "scopeSpans": [{"spans": list(spans)}]}]}


def T(n):
    return f"{n:032x}"


def S(n):
    return f"{n:016x}"


def healthy_turn(trace, session="S1", base=1):
    """interaction -> llm, tool -> tool.execution; interaction exported LAST."""
    i, l, t, x = S(base), S(base + 1), S(base + 2), S(base + 3)
    kids = [span(trace, l, otlp.SPAN_LLM, parent=i, session=session, start=1, dur=50,
                 model="claude-opus-5-5", input_tokens=10, output_tokens=100,
                 cache_read_tokens=1000, cache_creation_tokens=0, success=True),
            span(trace, t, otlp.SPAN_TOOL, parent=i, session=session, start=60, dur=30,
                 tool_name="Bash", bash_argv0="claude"),
            span(trace, x, "claude_code.tool.execution", parent=t, session=session, start=61,
                 dur=28, success=True)]
    inter = span(trace, i, otlp.SPAN_INTERACTION, session=session, start=0, dur=100)
    return kids, inter, x


def test_parse_drops_identity_attrs_and_reads_types():
    rows = otlp.parse_traces(payload(span(T(1), S(1), otlp.SPAN_LLM, model="m", input_tokens=5,
                                          success=False)))
    assert len(rows) == 1
    a = rows[0]["attrs"]
    assert "user.email" not in a and "user.id" not in a
    assert set(a) & config.DROP_ATTRS == set()
    assert a["input_tokens"] == 5 and a["success"] is False
    assert rows[0]["session_id"] == "S1" and rows[0]["parent_span_id"] is None
    assert otlp.is_error(rows[0])


def test_traceparent_parse():
    assert otlp.parse_traceparent("00-" + T(5) + "-" + S(7) + "-01") == (T(5), S(7))
    assert otlp.parse_traceparent("garbage") is None
    assert otlp.parse_traceparent("00-" + "0" * 32 + "-" + S(7) + "-01") is None


def test_usd_equiv_uses_family_rates():
    a = {"model": "claude-opus-5-5", "input_tokens": 1_000_000, "output_tokens": 0}
    assert otlp.usd_equiv(a) == pytest.approx(5.0)
    assert otlp.usd_equiv({"model": "unknown-x", "input_tokens": 10**6}) == 0.0


def test_ingest_is_idempotent_and_aggregates():
    conn = store.connect()
    kids, inter, _ = healthy_turn(T(1))
    rows = otlp.parse_traces(payload(*kids, inter))
    assert store.ingest(conn, rows) == 4
    assert store.ingest(conn, rows) == 0          # exporter retry: no double count
    n = conn.execute("SELECT * FROM ntraces WHERE trace_id=?", (T(1),)).fetchone()
    assert (n["n_spans"], n["n_llm"], n["n_tools"], n["has_interaction"]) == (4, 1, 1, 1)
    assert n["cache_read_tokens"] == 1000


def test_healthy_turn_logical_follows_late_interaction():
    conn = store.connect()
    kids, inter, _ = healthy_turn(T(1))
    store.ingest(conn, otlp.parse_traces(payload(*kids)))     # children arrive first
    stitch.run(conn)
    assert conn.execute("SELECT logical_id FROM ntraces").fetchone()[0] == "s:S1"
    store.ingest(conn, otlp.parse_traces(payload(inter)))     # the root arrives last
    stitch.run(conn)
    assert conn.execute("SELECT logical_id FROM ntraces").fetchone()[0] == T(1)
    t = store.get_trace(conn, T(1))
    roots = [n for n in t["nodes"] if n["parent"] == "s:S1"]
    assert [n["kind"] for n in roots] == ["interaction"] and not t["repaired"]


def test_broken_turn_is_grouped_under_its_session():
    """#99607: no interaction; every llm/tool span is a parentless root in its own trace."""
    conn = store.connect()
    store.ingest(conn, otlp.parse_traces(payload(
        span(T(11), S(1), otlp.SPAN_LLM, session="B", start=0, model="claude-haiku-4-5"),
        span(T(12), S(2), otlp.SPAN_TOOL, session="B", start=20, tool_name="Read"),
        span(T(12), S(3), "claude_code.tool.execution", parent=S(2), session="B", start=21),
        span(T(13), S(4), otlp.SPAN_LLM, session="B", start=40, model="claude-haiku-4-5"))))
    stitch.run(conn)
    assert {r[0] for r in conn.execute("SELECT logical_id FROM ntraces")} == {"s:B"}
    listed = store.list_traces(conn, 0)
    assert len(listed) == 1 and listed[0]["repaired"] and listed[0]["llm"] == 2
    t = store.get_trace(conn, "s:B")
    by_id = {n["id"]: n for n in t["nodes"]}
    assert by_id["s:B"]["parent"] is None
    assert by_id[S(1)]["parent"] == "s:B" and by_id[S(3)]["parent"] == S(2)


def test_broken_child_folds_into_the_parent_tool_call():
    """A child `claude -p` launched from the parent's Bash tool, itself broken by #99607:
    the SessionStart link (its TRACEPARENT) is the only thing tying it to the parent."""
    conn = store.connect()
    kids, inter, exec_span = healthy_turn(T(1), session="P")
    store.ingest(conn, otlp.parse_traces(payload(*kids, inter)))
    store.record_link(conn, {"session_id": "C", "agent": "researcher",
                             "traceparent": f"00-{T(1)}-{exec_span}-01"})
    store.ingest(conn, otlp.parse_traces(payload(
        span(T(21), S(31), otlp.SPAN_LLM, session="C", start=65, dur=5, model="claude-sonnet-5"),
        span(T(22), S(32), otlp.SPAN_LLM, session="C", start=71, dur=5, model="claude-sonnet-5"))))
    stitch.run(conn)
    assert {r[0] for r in conn.execute("SELECT logical_id FROM ntraces")} == {T(1)}
    t = store.get_trace(conn, T(1))
    by_id = {n["id"]: n for n in t["nodes"]}
    assert by_id["s:C"]["parent"] == exec_span and by_id["s:C"]["label"] == "researcher"
    assert by_id[S(31)]["parent"] == "s:C"
    assert t["sessions"] == 2 and t["repaired"]
    assert [x["id"] for x in store.list_traces(conn, 0)] == [T(1)]


def test_link_before_parent_span_resolves_later():
    conn = store.connect()
    kids, inter, exec_span = healthy_turn(T(1), session="P")
    store.record_link(conn, {"session_id": "C", "traceparent": f"00-{T(1)}-{exec_span}-01"})
    store.ingest(conn, otlp.parse_traces(payload(span(T(21), S(31), otlp.SPAN_LLM, session="C"))))
    stitch.run(conn)
    assert conn.execute("SELECT logical_id FROM ntraces WHERE trace_id=?", (T(21),)).fetchone()[0] == "s:C"
    store.ingest(conn, otlp.parse_traces(payload(*kids, inter)))   # parent exports after child exits
    stitch.run(conn)
    assert conn.execute("SELECT logical_id FROM ntraces WHERE trace_id=?", (T(21),)).fetchone()[0] == T(1)


def test_link_cycle_cannot_loop():
    conn = store.connect()
    store.ingest(conn, otlp.parse_traces(payload(span(T(1), S(1), otlp.SPAN_LLM, session="A"),
                                                 span(T(2), S(2), otlp.SPAN_LLM, session="B"))))
    store.record_link(conn, {"session_id": "A", "traceparent": f"00-{T(2)}-{S(2)}-01"})
    store.record_link(conn, {"session_id": "B", "traceparent": f"00-{T(1)}-{S(1)}-01"})
    res = stitch.run(conn)                      # must terminate
    assert res["edges"] == 2


def test_first_link_wins_on_resume():
    conn = store.connect()
    store.record_link(conn, {"session_id": "X", "agent": "planner", "traceparent": f"00-{T(1)}-{S(1)}-01"})
    store.record_link(conn, {"session_id": "X", "agent": "other", "source": "resume"})
    r = conn.execute("SELECT agent, link_span_id, source FROM sessions WHERE session_id='X'").fetchone()
    assert (r["agent"], r["link_span_id"], r["source"]) == ("planner", S(1), "resume")


def test_overview_counts_repairs_and_tools():
    conn = store.connect()
    kids, inter, _ = healthy_turn(T(1), session="P")
    store.ingest(conn, otlp.parse_traces(payload(*kids, inter)))
    old = -10 * 60 * 1000   # settled: ten minutes ago
    store.ingest(conn, otlp.parse_traces(payload(span(T(9), S(90), otlp.SPAN_LLM, session="B",
                                                      start=old, model="claude-haiku-4-5"))))
    store.ingest(conn, otlp.parse_traces(payload(span(T(8), S(80), otlp.SPAN_INTERACTION,
                                                      session="H", start=old))))
    stitch.run(conn)
    ov = store.overview(conn, 0)
    assert ov["llm_calls"] == 2 and ov["tool_calls"] == 1
    assert ov["sessions_repaired"] == 1 and ov["sessions_settled"] == 2
    assert ov["top_tools"][0]["tool"] == "Bash"


def test_prune_drops_old_rows_and_respects_size(tmp_path):
    conn = store.connect()
    store.ingest(conn, otlp.parse_traces(payload(span(T(1), S(1), otlp.SPAN_LLM,
                                                      start=-10 * 86400 * 1000))))
    store.ingest(conn, otlp.parse_traces(payload(span(T(2), S(2), otlp.SPAN_LLM))))
    out = store.prune(conn, retention_days=7, max_bytes=10**9)
    assert out["spans"] == 1
    assert [r[0] for r in conn.execute("SELECT trace_id FROM spans")] == [T(2)]


# ── collector HTTP ──────────────────────────────────────────────────────────

@pytest.fixture
def server():
    state = collector.State()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), collector.make_handler(state))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}", state
    httpd.shutdown()


def _post(url, body, ctype="application/json"):
    req = urllib.request.Request(url, data=body, headers={"content-type": ctype}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read())


def test_collector_accepts_json_and_serves_reads(server):
    base, state = server
    kids, inter, _ = healthy_turn(T(1))
    code, _ = _post(base + "/v1/traces", json.dumps(payload(*kids, inter)).encode())
    assert code == 200 and state.received == 4
    assert _post(base + "/v1/links", json.dumps({"session_id": "S1", "agent": "lead"}).encode())[0] == 200
    stitch.run(state.conn)
    assert _get(base + "/healthz")["ok"]
    traces = _get(base + "/api/traces?hours=1")["traces"]
    assert traces[0]["id"] == T(1) and traces[0]["agent"] == "lead"
    assert _get(base + f"/api/traces/{T(1)}")["spans"] == 4
    assert _get(base + "/api/overview?hours=1")["llm_calls"] == 1


def test_collector_refuses_protobuf_with_the_fix(server):
    base, _ = server
    code, body = _post(base + "/v1/traces", b"\x0a\x00", ctype="application/x-protobuf")
    assert code == 415 and "http/json" in body["error"]


def test_collector_off_switch_discards(server):
    base, state = server
    config.off_path().parent.mkdir(parents=True, exist_ok=True)
    config.off_path().touch()
    kids, inter, _ = healthy_turn(T(1))
    code, body = _post(base + "/v1/traces", json.dumps(payload(*kids, inter)).encode())
    assert code == 200 and "discarded" in body and state.received == 0


def test_logs_and_metrics_are_swallowed(server):
    base, _ = server
    assert _post(base + "/v1/logs", b"{}")[0] == 200
    assert _post(base + "/v1/metrics", b"{}")[0] == 200


# ── hook ────────────────────────────────────────────────────────────────────

def test_hook_record_prefers_explicit_agent():
    rec = hook.build_record({"session_id": "a", "cwd": "/x", "source": "startup"},
                            {"CCSTITCH_AGENT": "batch", "TRACEPARENT": "tp"},
                            attribute=lambda cwd: ("never", "cwd"))
    assert (rec["agent"], rec["attribution"], rec["traceparent"]) == ("batch", "env", "tp")


def test_hook_attribution_failure_is_not_fatal():
    def boom(cwd):
        raise RuntimeError("x")
    rec = hook.build_record({"session_id": "a"}, {}, attribute=boom)
    assert rec["agent"] is None and rec["attribution"] == "error"


def test_hook_spools_when_collector_down(monkeypatch):
    monkeypatch.setenv("CCSTITCH_PORT", "1")         # nothing listens there
    assert hook.deliver({"session_id": "z"}) is False
    lines = config.spool_path().read_text().splitlines()
    assert json.loads(lines[0])["session_id"] == "z"
    state = collector.State()
    assert collector.drain_spool(state) == 1
    assert not config.spool_path().exists()
    assert state.conn.execute("SELECT COUNT(*) FROM sessions WHERE session_id='z'").fetchone()[0] == 1


def test_healthy_child_session_gets_a_labelled_node_and_counts():
    """A healthy child turn carries the parent's trace id and names the launching
    tool.execution as its parent: it must still appear as its own labelled session,
    and the list and drill-in must agree on the session count."""
    conn = store.connect()
    kids, inter, exec_span = healthy_turn(T(1), session="P")
    child_inter = span(T(1), S(40), otlp.SPAN_INTERACTION, parent=exec_span, session="C", start=62, dur=20)
    child_llm = span(T(1), S(41), otlp.SPAN_LLM, parent=S(40), session="C", start=63, dur=10,
                     model="claude-sonnet-5")
    store.ingest(conn, otlp.parse_traces(payload(*kids, inter, child_inter, child_llm)))
    store.record_link(conn, {"session_id": "C", "agent": "researcher"})
    stitch.run(conn)
    t = store.get_trace(conn, T(1))
    by_id = {n["id"]: n for n in t["nodes"]}
    assert by_id["s:C"]["parent"] == exec_span and by_id["s:C"]["label"] == "researcher"
    assert by_id[S(40)]["parent"] == "s:C" and by_id[S(41)]["parent"] == S(40)
    listed = store.list_traces(conn, 0)
    assert listed[0]["sessions"] == t["sessions"] == 2
    assert store.overview(conn, 0)["sessions"] == 2      # the child shares the parent's native trace
