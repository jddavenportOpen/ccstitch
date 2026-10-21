"""Assign every native trace its logical (dashboard) trace id.

    base(t)    = t.trace_id         if the native trace holds an interaction span
               = "s:<session_id>"   otherwise (a turn #99607 broke into orphan roots)
    edge       "s:<child>" -> base(native trace of the child's TRACEPARENT span)
    logical(t) = follow edges from base(t)

A healthy child never needs an edge: its interaction carries the parent's trace id,
so it lands in the parent's native trace by itself. The edge exists for broken
children, whose spans all arrived as parentless roots.

Job traces (context.py): a Python job's TRACEPARENT names a span that is never
exported; the jobs table holds it instead. A child linked to a job folds into the
job's trace id directly (a job is a root, it is never "broken").

A parent span that never arrives (its process was killed, the job record was lost)
must not strand the child forever: once the child has been quiet for LINK_GRACE_S
it folds into the trace id its TRACEPARENT named, so siblings launched by the same
parent still land together.

Every pass recomputes the whole recent window instead of patching incrementally.
Late arrivals change answers: a parent's tool span is exported only after the child
it launched has exited, and a turn's interaction span arrives after its children. A
full recompute over a day of rows costs milliseconds and cannot go stale.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from ccstitch import config
from ccstitch.store import now_ns

MAX_CHAIN = 32
# How long a child waits for its parent's span before folding by trace id alone. A
# parent's tool span is exported seconds after the child exits, so this only fires
# when the parent span is genuinely gone.
LINK_GRACE_S = config.LINK_GRACE_S


def _base(trace_id: str, session_id: Optional[str], has_interaction: int) -> str:
    if has_interaction or not session_id:
        return trace_id
    return f"s:{session_id}"


def run(conn: sqlite3.Connection, *, horizon_s: Optional[int] = None) -> dict[str, Any]:
    since = now_ns() - int((horizon_s or config.RESOLVE_HORIZON_S) * 1e9)

    # 1. cache the native trace of each session's TRACEPARENT span, once it exists
    resolved_links = 0
    pending = conn.execute(
        "SELECT session_id, link_span_id FROM sessions WHERE link_span_id IS NOT NULL"
        " AND link_native IS NULL AND COALESCE(last_seen_ns, started_ns, 0) >= ?", (since,)).fetchall()
    with conn:
        for r in pending:
            hit = conn.execute("SELECT trace_id FROM spans WHERE span_id = ? LIMIT 1",
                               (r["link_span_id"],)).fetchone()
            if not hit:
                hit = conn.execute("SELECT trace_id FROM jobs WHERE span_id = ? LIMIT 1",
                                   (r["link_span_id"],)).fetchone()
            if hit:
                conn.execute("UPDATE sessions SET link_native = ? WHERE session_id = ?",
                             (hit["trace_id"], r["session_id"]))
                resolved_links += 1

    # 2. edges: a broken child's session bucket points at its parent's base
    natives: dict[str, tuple[Optional[str], int]] = {}

    def native(tid: str) -> Optional[tuple[Optional[str], int]]:
        if tid not in natives:
            row = conn.execute("SELECT session_id, has_interaction FROM ntraces WHERE trace_id = ?",
                               (tid,)).fetchone()
            natives[tid] = (row["session_id"], row["has_interaction"]) if row else (None, -1)
        return None if natives[tid][1] == -1 else natives[tid]

    job_traces = {r[0] for r in conn.execute("SELECT DISTINCT trace_id FROM jobs")}
    grace = now_ns() - int(LINK_GRACE_S * 1e9)
    edges: dict[str, str] = {}
    for r in conn.execute(
            "SELECT session_id, link_native, link_trace_id,"
            " COALESCE(last_seen_ns, started_ns, 0) AS seen FROM sessions"
            " WHERE (link_native IS NOT NULL OR link_trace_id IS NOT NULL)"
            " AND COALESCE(last_seen_ns, started_ns, 0) >= ?", (since,)):
        if r["link_native"] in job_traces:
            target = r["link_native"]
        elif r["link_native"]:
            parent = native(r["link_native"])
            if parent is None:
                continue
            target = _base(r["link_native"], parent[0], parent[1])
        elif r["seen"] < grace:
            target = r["link_trace_id"]       # the parent span never arrived
        else:
            continue
        bucket = f"s:{r['session_id']}"
        if target != bucket:
            edges[bucket] = target

    def final(x: str) -> str:
        seen = {x}
        for _ in range(MAX_CHAIN):
            nxt = edges.get(x)
            if nxt is None or nxt in seen:
                return x
            seen.add(nxt)
            x = nxt
        return x

    # 3. recompute the window (plus anything flagged dirty that fell outside it)
    changed = 0
    rows = conn.execute(
        "SELECT trace_id, session_id, has_interaction, logical_id FROM ntraces"
        " WHERE last_ns >= ? OR dirty = 1 OR logical_id IS NULL", (since,)).fetchall()
    with conn:
        for r in rows:
            want = final(_base(r["trace_id"], r["session_id"], r["has_interaction"]))
            if want != r["logical_id"]:
                conn.execute("UPDATE ntraces SET logical_id = ?, dirty = 0 WHERE trace_id = ?",
                             (want, r["trace_id"]))
                changed += 1
        conn.execute("UPDATE ntraces SET dirty = 0 WHERE dirty = 1")
    return {"window": len(rows), "changed": changed, "edges": len(edges),
            "links_resolved": resolved_links}
