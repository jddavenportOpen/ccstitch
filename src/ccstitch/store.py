"""SQLite trace store: schema, ingest, retention, and the read queries.

Three levels:

  spans     one row per OTLP span, as the CLI sent it (identity attrs dropped).
  ntraces   one row per NATIVE trace id, with running totals kept at ingest.
            A healthy turn is one native trace rooted at claude_code.interaction.
            A turn broken by anthropics/claude-code#99607 is many: every
            llm_request/tool span is its own parentless trace.
  logical   what the dashboard calls a trace. ntraces.logical_id is set by stitch.py:
            a native trace that holds an interaction keeps its own id; one that
            does not is grouped under its session ("s:<session_id>"); and a child
            session launched from another session's tool call is folded into the
            trace that tool call belongs to.

sessions holds what the SessionStart hook knew: who ran it (agent), where (cwd),
and the TRACEPARENT it was started with (link_trace_id / link_span_id). A session
whose hooks are disabled is still named by the `ccstitch.agent` resource attribute
its launcher set (attribution 'resource').

jobs holds one row per Python job trace context (context.py): a process that ran
claude with no inherited TRACEPARENT minted one, and its span id is the parent every
claude call it made points at. The job is the root of that trace; it never sends a
span of its own, so the read side synthesizes it from this row.

exports remembers what the forwarder (forward.py) already sent, per logical trace.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, Optional

from ccstitch import config, otlp, semconv

SCHEMA = """
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL, span_id TEXT NOT NULL, parent_span_id TEXT,
    session_id TEXT, name TEXT NOT NULL, start_ns INTEGER, end_ns INTEGER,
    status_code INTEGER, err INTEGER DEFAULT 0, usd REAL DEFAULT 0,
    attrs TEXT, received_ns INTEGER,
    PRIMARY KEY (trace_id, span_id));
CREATE INDEX IF NOT EXISTS spans_span ON spans(span_id);
CREATE INDEX IF NOT EXISTS spans_start ON spans(start_ns);
CREATE INDEX IF NOT EXISTS spans_name_start ON spans(name, start_ns);

CREATE TABLE IF NOT EXISTS ntraces (
    trace_id TEXT PRIMARY KEY, session_id TEXT, logical_id TEXT,
    first_ns INTEGER, last_ns INTEGER,
    n_spans INTEGER DEFAULT 0, n_llm INTEGER DEFAULT 0, n_tools INTEGER DEFAULT 0,
    n_errors INTEGER DEFAULT 0, input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
    cache_creation_tokens INTEGER DEFAULT 0, usd REAL DEFAULT 0,
    has_interaction INTEGER DEFAULT 0, dirty INTEGER DEFAULT 1, updated_ns INTEGER);
CREATE INDEX IF NOT EXISTS ntraces_logical ON ntraces(logical_id);
CREATE INDEX IF NOT EXISTS ntraces_last ON ntraces(last_ns);
CREATE INDEX IF NOT EXISTS ntraces_session ON ntraces(session_id);
CREATE INDEX IF NOT EXISTS ntraces_dirty ON ntraces(dirty);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY, agent TEXT, attribution TEXT, cwd TEXT,
    source TEXT, started_ns INTEGER, last_seen_ns INTEGER,
    link_trace_id TEXT, link_span_id TEXT, link_native TEXT);
CREATE INDEX IF NOT EXISTS sessions_last ON sessions(last_seen_ns);

CREATE TABLE IF NOT EXISTS jobs (
    span_id TEXT PRIMARY KEY, trace_id TEXT NOT NULL, agent TEXT, workflow TEXT,
    pid INTEGER, started_ns INTEGER, received_ns INTEGER);
CREATE INDEX IF NOT EXISTS jobs_trace ON jobs(trace_id);

CREATE TABLE IF NOT EXISTS exports (
    logical_id TEXT PRIMARY KEY, n_spans INTEGER, sent_spans INTEGER, exported_ns INTEGER,
    synthetic TEXT, natives TEXT);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

DETAIL_KEYS = ("model", "input_tokens", "output_tokens", "cache_read_tokens",
               "cache_creation_tokens", "ttft_ms", "stop_reason", "success", "attempt",
               "tool_name", "bash_argv0", "bash_command_class", "decision", "source",
               "interaction.sequence", "parent.source", "query_source_safe",
               "llm_request.context", "duration_ms", "speed")


def now_ns() -> int:
    return time.time_ns()


def connect(path: Optional[Path] = None, *, readonly: bool = False) -> sqlite3.Connection:
    p = Path(path or config.db_path())
    if readonly:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5)
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        fresh = not p.exists()
        conn = sqlite3.connect(str(p), timeout=10, check_same_thread=False)
        if fresh:
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(SCHEMA)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


# ── ingest ──────────────────────────────────────────────────────────────────

def ingest(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    """Insert spans (idempotent on (trace_id, span_id)) and roll their totals into
    ntraces. Returns the number of NEW spans."""
    t = now_ns()
    agg: dict[str, dict[str, Any]] = {}
    seen_sessions: dict[str, int] = {}
    res_agents: dict[str, str] = {}
    new = 0
    with conn:
        for r in rows:
            a = r["attrs"]
            err = 1 if otlp.is_error(r) else 0
            usd = otlp.usd_equiv(a) if r["name"] == otlp.SPAN_LLM else 0.0
            cur = conn.execute(
                "INSERT OR IGNORE INTO spans (trace_id, span_id, parent_span_id, session_id,"
                " name, start_ns, end_ns, status_code, err, usd, attrs, received_ns)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (r["trace_id"], r["span_id"], r["parent_span_id"], r["session_id"], r["name"],
                 r["start_ns"], r["end_ns"], r["status_code"], err, usd,
                 json.dumps(a, separators=(",", ":"), default=str), t))
            if cur.rowcount != 1:
                continue
            new += 1
            g = agg.setdefault(r["trace_id"], {
                "session_id": r["session_id"], "first": None, "last": None, "n": 0, "llm": 0,
                "tools": 0, "err": 0, "in": 0, "out": 0, "cr": 0, "cw": 0, "usd": 0.0, "inter": 0})
            g["session_id"] = g["session_id"] or r["session_id"]
            s, e = r["start_ns"], r["end_ns"] or r["start_ns"]
            if s is not None:
                g["first"] = s if g["first"] is None else min(g["first"], s)
            if e is not None:
                g["last"] = e if g["last"] is None else max(g["last"], e)
            g["n"] += 1
            g["err"] += err
            g["usd"] += usd
            if r["name"] == otlp.SPAN_LLM:
                g["llm"] += 1
                g["in"] += int(a.get("input_tokens") or 0)
                g["out"] += int(a.get("output_tokens") or 0)
                g["cr"] += int(a.get("cache_read_tokens") or 0)
                g["cw"] += int(a.get("cache_creation_tokens") or 0)
            elif r["name"] == otlp.SPAN_TOOL:
                g["tools"] += 1
            elif r["name"] == otlp.SPAN_INTERACTION:
                g["inter"] = 1
            if r["session_id"] and e:
                seen_sessions[r["session_id"]] = max(seen_sessions.get(r["session_id"], 0), e)
            if r["session_id"] and r.get("resource_agent"):
                res_agents.setdefault(r["session_id"], r["resource_agent"])
        for tid, g in agg.items():
            conn.execute(
                "INSERT INTO ntraces (trace_id, session_id, first_ns, last_ns, n_spans, n_llm,"
                " n_tools, n_errors, input_tokens, output_tokens, cache_read_tokens,"
                " cache_creation_tokens, usd, has_interaction, dirty, updated_ns)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)"
                " ON CONFLICT(trace_id) DO UPDATE SET"
                " session_id=COALESCE(ntraces.session_id, excluded.session_id),"
                " first_ns=MIN(COALESCE(ntraces.first_ns, excluded.first_ns), COALESCE(excluded.first_ns, ntraces.first_ns)),"
                " last_ns=MAX(COALESCE(ntraces.last_ns, 0), COALESCE(excluded.last_ns, 0)),"
                " n_spans=ntraces.n_spans+excluded.n_spans, n_llm=ntraces.n_llm+excluded.n_llm,"
                " n_tools=ntraces.n_tools+excluded.n_tools, n_errors=ntraces.n_errors+excluded.n_errors,"
                " input_tokens=ntraces.input_tokens+excluded.input_tokens,"
                " output_tokens=ntraces.output_tokens+excluded.output_tokens,"
                " cache_read_tokens=ntraces.cache_read_tokens+excluded.cache_read_tokens,"
                " cache_creation_tokens=ntraces.cache_creation_tokens+excluded.cache_creation_tokens,"
                " usd=ntraces.usd+excluded.usd,"
                " has_interaction=MAX(ntraces.has_interaction, excluded.has_interaction),"
                " dirty=1, updated_ns=excluded.updated_ns",
                (tid, g["session_id"], g["first"], g["last"], g["n"], g["llm"], g["tools"],
                 g["err"], g["in"], g["out"], g["cr"], g["cw"], g["usd"], g["inter"], t))
        for sid, last in seen_sessions.items():
            ra = res_agents.get(sid)
            conn.execute(
                "INSERT INTO sessions (session_id, agent, attribution, last_seen_ns) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(session_id) DO UPDATE SET"
                f" {_AGENT_FILL},"
                " last_seen_ns=MAX(COALESCE(sessions.last_seen_ns, 0), excluded.last_seen_ns)",
                (sid, ra, "resource" if ra else None, last))
    return new


# Agent and attribution move together: a row that has no agent yet takes the
# incoming pair (an 'unresolved' hook record is completed by a later resource
# attribute, and vice versa); a row that has one keeps it (first answer wins).
_AGENT_FILL = (
    "attribution=CASE WHEN sessions.agent IS NULL AND excluded.agent IS NOT NULL"
    " THEN excluded.attribution ELSE COALESCE(sessions.attribution, excluded.attribution) END,"
    " agent=COALESCE(sessions.agent, excluded.agent)")


def record_link(conn: sqlite3.Connection, rec: dict[str, Any]) -> bool:
    """Store one SessionStart record. The first non-empty value of each field wins:
    a resumed session keeps the agent and parent it was born with. A record with
    kind 'job' is a job trace context (context.py) and goes to record_job."""
    if rec.get("kind") == "job":
        return record_job(conn, rec)
    sid = str(rec.get("session_id") or "").strip()
    if not sid or len(sid) > 128:
        return False
    tp = otlp.parse_traceparent(rec.get("traceparent"))
    started = rec.get("started_ns") or now_ns()
    with conn:
        conn.execute(
            "INSERT INTO sessions (session_id, agent, attribution, cwd, source, started_ns,"
            " last_seen_ns, link_trace_id, link_span_id) VALUES (?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(session_id) DO UPDATE SET"
            f" {_AGENT_FILL},"
            " cwd=COALESCE(sessions.cwd, excluded.cwd),"
            " source=COALESCE(sessions.source, excluded.source),"
            " started_ns=MIN(COALESCE(sessions.started_ns, excluded.started_ns), excluded.started_ns),"
            " last_seen_ns=MAX(COALESCE(sessions.last_seen_ns, 0), excluded.last_seen_ns),"
            " link_trace_id=COALESCE(sessions.link_trace_id, excluded.link_trace_id),"
            " link_span_id=COALESCE(sessions.link_span_id, excluded.link_span_id)",
            (sid, _clip(rec.get("agent"), 80), _clip(rec.get("attribution"), 80),
             _clip(rec.get("cwd"), 300), _clip(rec.get("source"), 40), started, started,
             tp[0] if tp else None, tp[1] if tp else None))
    return True


def record_job(conn: sqlite3.Connection, rec: dict[str, Any]) -> bool:
    """Store one job trace context. Ids must be W3C-shaped hex; anything else is
    refused (this endpoint is reachable by any local process)."""
    tp = otlp.parse_traceparent(f"00-{rec.get('trace_id')}-{rec.get('span_id')}-01")
    if tp is None:
        return False
    try:
        pid = int(rec.get("pid")) if rec.get("pid") is not None else None
        started = int(rec.get("started_ns") or now_ns())
    except (TypeError, ValueError):
        return False
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO jobs (span_id, trace_id, agent, workflow, pid, started_ns,"
            " received_ns) VALUES (?,?,?,?,?,?,?)",
            (tp[1], tp[0], _clip(rec.get("agent"), 80), _clip(rec.get("workflow"), 120), pid,
             started, now_ns()))
    return True


def jobs_by(conn: sqlite3.Connection, *, trace_ids: Iterable[str] = (),
            span_ids: Iterable[str] = ()) -> list[dict[str, Any]]:
    """Job rows matching any of the given trace or span ids. A database written
    before the jobs table existed (a read-only connection never creates tables) answers [] instead of raising."""
    t = list(dict.fromkeys(x for x in trace_ids if x))[:500]
    sp = list(dict.fromkeys(x for x in span_ids if x))[:500]
    if not t and not sp:
        return []
    clauses, args = [], []
    if t:
        clauses.append(f"trace_id IN ({','.join('?' * len(t))})")
        args += t
    if sp:
        clauses.append(f"span_id IN ({','.join('?' * len(sp))})")
        args += sp
    try:
        return [dict(r) for r in conn.execute(
            f"SELECT * FROM jobs WHERE {' OR '.join(clauses)} ORDER BY started_ns", args)]
    except sqlite3.OperationalError:
        return []


def _clip(v: Any, n: int) -> Optional[str]:
    return str(v)[:n] if v not in (None, "") else None


# ── retention ───────────────────────────────────────────────────────────────

def prune(conn: sqlite3.Connection, *, retention_days: Optional[float] = None,
          max_bytes: Optional[int] = None, db_file: Optional[Path] = None) -> dict[str, int]:
    """Drop data older than the retention window, then oldest-first until the file
    fits under max_bytes. Bounded work per call so the collector never stalls."""
    days = retention_days if retention_days is not None else config.retention_days()
    cap = max_bytes if max_bytes is not None else config.max_db_bytes()
    cutoff = now_ns() - int(days * 86400 * 1e9)
    out = {"spans": 0, "ntraces": 0}

    def _drop_before(ts: int) -> None:
        with conn:
            out["spans"] += conn.execute("DELETE FROM spans WHERE start_ns < ?", (ts,)).rowcount
            out["ntraces"] += conn.execute("DELETE FROM ntraces WHERE last_ns < ?", (ts,)).rowcount
            conn.execute("DELETE FROM sessions WHERE COALESCE(last_seen_ns, started_ns, 0) < ?", (ts,))
            conn.execute("DELETE FROM jobs WHERE started_ns < ?", (ts,))
            conn.execute("DELETE FROM exports WHERE exported_ns < ?", (ts,))

    _drop_before(cutoff)
    f = Path(db_file or config.db_path())
    for _ in range(8):
        try:
            size = f.stat().st_size
        except OSError:
            break
        if size <= cap:
            break
        oldest = conn.execute("SELECT MIN(start_ns) FROM spans").fetchone()[0]
        newest = conn.execute("SELECT MAX(start_ns) FROM spans").fetchone()[0]
        if oldest is None or newest is None or newest <= oldest:
            break
        _drop_before(oldest + (newest - oldest) // 8)   # shed the oldest eighth
        conn.execute("PRAGMA incremental_vacuum")
    conn.execute("PRAGMA incremental_vacuum(2000)")
    return out


# ── reads ───────────────────────────────────────────────────────────────────

def _short(model: Optional[str]) -> str:
    m = (model or "").replace("claude-", "")
    return m.split("-2025")[0].split("-2026")[0] or "?"


def overview(conn: sqlite3.Connection, since_ns: int) -> dict[str, Any]:
    tot = conn.execute(
        "SELECT COUNT(DISTINCT logical_id) traces,"
        " COALESCE(SUM(n_llm),0) llm, COALESCE(SUM(n_tools),0) tools,"
        " COALESCE(SUM(n_errors),0) errors, COALESCE(SUM(usd),0) usd,"
        " COALESCE(SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens),0) tokens,"
        " COALESCE(SUM(cache_read_tokens),0) cr,"
        " COALESCE(SUM(input_tokens+cache_read_tokens+cache_creation_tokens),0) prompt_tokens"
        " FROM ntraces WHERE last_ns >= ?", (since_ns,)).fetchone()
    # Sessions from the spans: a healthy child session shares its parent's native
    # trace, so ntraces.session_id (the first one seen) undercounts.
    n_sessions = conn.execute("SELECT COUNT(DISTINCT session_id) FROM spans WHERE start_ns >= ?",
                              (since_ns,)).fetchone()[0]
    sess = conn.execute(
        "SELECT COUNT(*) n, SUM(CASE WHEN broken THEN 1 ELSE 0 END) broken FROM ("
        " SELECT session_id, MIN(has_interaction)=0 AND MAX(has_interaction)=0 AS broken"
        " FROM ntraces WHERE last_ns >= ? AND last_ns < ? AND session_id IS NOT NULL"
        " GROUP BY session_id)", (since_ns, now_ns() - 120 * 10**9)).fetchone()
    by_agent = [dict(r) for r in conn.execute(
        "SELECT COALESCE(s.agent, 'unattributed') agent, COUNT(DISTINCT n.session_id) sessions,"
        " SUM(n.n_llm) llm, SUM(n.n_tools) tools, SUM(n.n_errors) errors, ROUND(SUM(n.usd),2) usd"
        " FROM ntraces n LEFT JOIN sessions s ON s.session_id = n.session_id"
        " WHERE n.last_ns >= ? GROUP BY 1 ORDER BY SUM(n.usd) DESC LIMIT 15", (since_ns,))]
    tools = [dict(r) for r in conn.execute(
        "SELECT COALESCE(json_extract(attrs,'$.tool_name'),'?') tool, COUNT(*) calls,"
        " CAST(SUM(COALESCE(end_ns-start_ns,0))/1e6 AS INTEGER) total_ms,"
        " CAST(MAX(COALESCE(end_ns-start_ns,0))/1e6 AS INTEGER) max_ms"
        " FROM spans WHERE name = ? AND start_ns >= ? GROUP BY 1 ORDER BY total_ms DESC LIMIT 12",
        (otlp.SPAN_TOOL, since_ns))]
    models = [dict(r) for r in conn.execute(
        "SELECT json_extract(attrs,'$.model') model, COUNT(*) calls, ROUND(SUM(usd),2) usd,"
        " CAST(AVG(json_extract(attrs,'$.ttft_ms')) AS INTEGER) avg_ttft_ms,"
        " SUM(err) errors FROM spans WHERE name = ? AND start_ns >= ?"
        " GROUP BY 1 ORDER BY SUM(usd) DESC LIMIT 10", (otlp.SPAN_LLM, since_ns))]
    for m in models:
        m["model"] = _short(m["model"])
    prompt = tot["prompt_tokens"] or 0
    return {
        "traces": tot["traces"], "sessions": n_sessions, "llm_calls": tot["llm"],
        "tool_calls": tot["tools"], "errors": tot["errors"], "tokens": tot["tokens"],
        "usd_equiv": round(tot["usd"] or 0, 2),
        "cache_hit_rate": round((tot["cr"] or 0) / prompt, 4) if prompt else None,
        "sessions_settled": sess["n"] or 0, "sessions_repaired": sess["broken"] or 0,
        "by_agent": by_agent, "top_tools": tools, "models": models,
    }


def list_traces(conn: sqlite3.Connection, since_ns: int, *, limit: int = 50,
                agent: Optional[str] = None, errors_only: bool = False) -> list[dict[str, Any]]:
    where, args = "n.last_ns >= ? AND n.logical_id IS NOT NULL", [since_ns]
    having = " HAVING SUM(n.n_errors) > 0" if errors_only else ""
    rows = conn.execute(
        "SELECT n.logical_id id, MIN(n.first_ns) first_ns, MAX(n.last_ns) last_ns,"
        " SUM(n.n_spans) spans, SUM(n.n_llm) llm,"
        " SUM(n.n_tools) tools, SUM(n.n_errors) errors, ROUND(SUM(n.usd),4) usd,"
        " SUM(n.input_tokens+n.output_tokens+n.cache_read_tokens+n.cache_creation_tokens) tokens,"
        " SUM(CASE WHEN n.has_interaction=0 THEN 1 ELSE 0 END) repaired_parts"
        f" FROM ntraces n WHERE {where} GROUP BY n.logical_id{having}"
        " ORDER BY MAX(n.last_ns) DESC LIMIT ?", (*args, max(1, min(limit, 500)) * (4 if agent else 1)))
    out: list[dict[str, Any]] = []
    t_now = now_ns()
    for r in rows:
        top = _top_session(conn, r["id"])
        item = dict(r)
        # Count sessions from the spans: a healthy child session shares its parent's
        # native trace, so ntraces.session_id alone undercounts (it keeps the first).
        item["sessions"] = conn.execute(
            "SELECT COUNT(DISTINCT session_id) FROM spans WHERE trace_id IN"
            " (SELECT trace_id FROM ntraces WHERE logical_id = ?)", (r["id"],)).fetchone()[0]
        item.update({"agent": (top or {}).get("agent") or "unattributed",
                     "cwd": (top or {}).get("cwd"), "session_id": (top or {}).get("session_id"),
                     "dur_ms": int(((r["last_ns"] or 0) - (r["first_ns"] or 0)) / 1e6),
                     "active": bool(r["last_ns"] and t_now - r["last_ns"] < 120 * 10**9),
                     "repaired": bool(r["repaired_parts"])})
        if agent and item["agent"] != agent:
            continue
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _top_session(conn: sqlite3.Connection, logical_id: str) -> Optional[dict[str, Any]]:
    job = next(iter(jobs_by(conn, trace_ids=[logical_id])), None)
    if job and job.get("agent"):   # a job trace whose job knows its agent: the job is the root
        return {"session_id": None, "agent": job.get("agent"), "cwd": None,
                "workflow": job.get("workflow")}
    # A job with no agent (a spawner that could not name itself) must not erase the
    # agent its sessions were attributed to: fall through to the sessions.
    r = conn.execute(
        "SELECT s.session_id, s.agent, s.cwd FROM ntraces n JOIN sessions s ON s.session_id = n.session_id"
        " WHERE n.logical_id = ? ORDER BY (s.link_span_id IS NOT NULL), n.first_ns LIMIT 1",
        (logical_id,)).fetchone()
    if r:
        return dict(r)
    r = conn.execute("SELECT session_id FROM ntraces WHERE logical_id = ? ORDER BY first_ns LIMIT 1",
                     (logical_id,)).fetchone()
    return {"session_id": r["session_id"]} if r else None


def _label(name: str, a: dict[str, Any]) -> tuple[str, str]:
    if name == otlp.SPAN_LLM:
        return "llm", f"{_short(a.get('model'))} · {int(a.get('output_tokens') or 0)} out"
    if name == otlp.SPAN_TOOL:
        tool = a.get("tool_name") or "tool"
        extra = a.get("bash_argv0") or ""
        return "tool", f"{tool} {extra}".strip()
    if name == otlp.SPAN_INTERACTION:
        return "interaction", f"turn {a.get('interaction.sequence') or ''}".strip()
    short = name.replace("claude_code.", "")
    kind = "tool-step" if short.startswith("tool.") else short.split(".")[0]
    return kind, short


def get_trace(conn: sqlite3.Connection, logical_id: str, *, max_spans: int = 4000,
              raw: bool = False) -> Optional[dict[str, Any]]:
    """One logical trace as a flat node list. Every span node's `detail` carries the
    OpenTelemetry GenAI attributes (semconv.py) beside the CLI's own keys.

    Synthetic nodes: `s:<session>` groups a session's spans and is labelled by its
    agent; a job node (kind 'job', id = the job's span id) is the root of a job
    trace (context.py) and never arrives as a span; `root` groups unlinked sessions.
    raw=True adds a `raw` dict per span node (ids, ns times, status, full attrs) for
    the OTLP exporter; the viewer never asks for it."""
    natives = [r[0] for r in conn.execute("SELECT trace_id FROM ntraces WHERE logical_id = ?",
                                          (logical_id,))]
    if not natives:
        return None
    marks = ",".join("?" * len(natives))
    spans = [dict(r) for r in conn.execute(
        f"SELECT trace_id, span_id, parent_span_id, session_id, name, start_ns, end_ns, err, usd,"
        f" status_code, received_ns, attrs FROM spans WHERE trace_id IN ({marks})"
        f" ORDER BY start_ns LIMIT ?", (*natives, max_spans + 1))]
    truncated = len(spans) > max_spans
    spans = spans[:max_spans]
    if not spans:
        return None
    ids = {s["span_id"] for s in spans}
    sids = sorted({s["session_id"] for s in spans if s["session_id"]})
    sess = {r["session_id"]: dict(r) for r in conn.execute(
        f"SELECT * FROM sessions WHERE session_id IN ({','.join('?' * len(sids))})", sids)} if sids else {}
    # Jobs: the trace's own job (logical id is the job's trace id) and any job a
    # session or span here names as its parent.
    jobs = jobs_by(conn, trace_ids=[logical_id, *natives],
                   span_ids=[m.get("link_span_id") for m in sess.values()]
                   + [s["parent_span_id"] for s in spans if s["parent_span_id"] not in ids])
    job_ids = {j["span_id"] for j in jobs}
    ids |= job_ids
    t0 = min([s["start_ns"] or 0 for s in spans] + [j["started_ns"] for j in jobs if j["started_ns"]])
    t1 = max((s["end_ns"] or s["start_ns"] or 0) for s in spans)

    nodes: list[dict[str, Any]] = []
    children: dict[str, int] = {}
    session_of = {s["span_id"]: s["session_id"] for s in spans}
    entered_from: dict[str, str] = {}   # child session -> the parent-session span it hangs under
    for s in spans:
        a = json.loads(s["attrs"] or "{}")
        kind, label = _label(s["name"], a)
        p = s["parent_span_id"]
        if p in ids and s["session_id"] and session_of.get(p) != s["session_id"]:
            # A healthy child session: its turn names the launching tool call (or the
            # job) as parent. Route it through a session node labelled by its agent.
            entered_from.setdefault(s["session_id"], p)
            parent = f"s:{s['session_id']}"
        elif p in ids:
            parent = p
        else:
            parent = f"s:{s['session_id']}" if s["session_id"] else None
        if parent:
            children[parent] = children.get(parent, 0) + 1
        agent = (sess.get(s["session_id"]) or {}).get("agent")
        detail = {k: a[k] for k in DETAIL_KEYS if k in a}
        detail.update(semconv.span_attrs(s["name"], a, agent=agent, status_code=s["status_code"]))
        node = {
            "id": s["span_id"], "parent": parent, "kind": kind, "name": s["name"], "label": label,
            "session_id": s["session_id"],
            "start_ms": round(((s["start_ns"] or t0) - t0) / 1e6, 1),
            "dur_ms": round(((s["end_ns"] or s["start_ns"] or 0) - (s["start_ns"] or 0)) / 1e6, 1),
            "error": bool(s["err"]), "usd": round(s["usd"] or 0, 5),
            "detail": detail,
        }
        if raw:
            node["raw"] = {k: s[k] for k in ("trace_id", "span_id", "parent_span_id", "start_ns",
                                             "end_ns", "status_code", "err", "received_ns")}
            node["raw"]["attrs"] = a
        nodes.append(node)
    tops: list[str] = []
    for sid in sids:
        nid = f"s:{sid}"
        if not children.get(nid):
            continue
        meta = sess.get(sid, {})
        parent = meta.get("link_span_id") if meta.get("link_span_id") in ids else entered_from.get(sid)
        if parent is None:
            tops.append(nid)
        else:
            children[parent] = children.get(parent, 0) + 1
        first = min((s["start_ns"] or t0) for s in spans if s["session_id"] == sid)
        last = max((s["end_ns"] or s["start_ns"] or t0) for s in spans if s["session_id"] == sid)
        nodes.append({
            "id": nid, "parent": parent, "kind": "session", "name": "session",
            "label": meta.get("agent") or _cwd_label(meta.get("cwd")) or f"session {sid[:8]}",
            "session_id": sid, "start_ms": round((first - t0) / 1e6, 1),
            "dur_ms": round((last - first) / 1e6, 1), "error": False, "usd": 0.0,
            "detail": {k: meta.get(k) for k in ("agent", "attribution", "cwd", "source") if meta.get(k)},
        })
    for j in jobs:
        if not children.get(j["span_id"]):
            continue
        tops.append(j["span_id"])
        start = j["started_ns"] or t0
        detail = {"agent": j.get("agent"), "workflow": j.get("workflow"), "pid": j.get("pid")}
        detail = {k: v for k, v in detail.items() if v is not None}
        detail.update(semconv.workflow_span_attrs(j.get("workflow"), j.get("agent")))
        nodes.append({
            "id": j["span_id"], "parent": None, "kind": "job", "name": "job",
            "label": j.get("agent") or j.get("workflow") or "job", "session_id": None,
            "start_ms": round((start - t0) / 1e6, 1), "dur_ms": round(max(0, t1 - start) / 1e6, 1),
            "error": False, "usd": 0.0, "detail": detail,
        })
    if len(tops) > 1:   # several unlinked roots in one group: give them one root
        for n in nodes:
            if n["id"] in tops:
                n["parent"] = "root"
        nodes.append({"id": "root", "parent": None, "kind": "group", "name": "group",
                      "label": "related sessions", "session_id": None, "start_ms": 0.0,
                      "dur_ms": round((t1 - t0) / 1e6, 1), "error": False, "usd": 0.0, "detail": {}})
    top = _top_session(conn, logical_id) or {}
    nodes.sort(key=lambda n: (n["start_ms"], n["kind"] not in ("job", "session")))
    return {
        "id": logical_id, "agent": top.get("agent") or "unattributed", "cwd": top.get("cwd"),
        "started_ns": t0, "dur_ms": round((t1 - t0) / 1e6, 1), "sessions": len(sids),
        "spans": len(spans), "truncated": truncated,
        "repaired": any(r[0] == 0 for r in conn.execute(
            f"SELECT has_interaction FROM ntraces WHERE trace_id IN ({marks})", natives)),
        "usd_equiv": round(sum(s["usd"] or 0 for s in spans), 4),
        "errors": sum(1 for s in spans if s["err"]),
        "nodes": nodes,
    }


def _cwd_label(cwd: Optional[str]) -> Optional[str]:
    if not cwd:
        return None
    return Path(cwd).name or cwd
