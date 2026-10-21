"""W3C trace context for Python processes that launch `claude -p`.

Without this, a cron job that calls claude five times produces five unrelated
traces: each child starts with no TRACEPARENT, so each is its own root.

    child_env(env)     -> {"TRACEPARENT": ..., "OTEL_RESOURCE_ATTRIBUTES": ...}
    shell_exports(env) -> "export TRACEPARENT=...; " for a spawner that builds a shell
                          command (claude run inside tmux does not inherit env)

Rules:

* An inherited, well-formed TRACEPARENT (this process was itself started by a
  Claude session's Bash tool, or by a parent job) is propagated UNTOUCHED: the child
  hangs under whatever launched us, and no job is recorded.
* Otherwise the claude call joins the innermost LOGICAL UNIT of work:
  - a spawner that launches unrelated runs makes each its own unit:
    `fresh_traceparent()` mints one per run, labelled with the run's agent, so
    unrelated tasks a long-lived spawner launches never share a root;
  - code that knows its own unit (one queue task, one eval case) wraps it in
    `with unit(agent=..., workflow=...):`, and every claude call inside it, on that
    thread or any thread started via contextvars.copy_context(), shares the unit's
    trace;
  - with neither, the process itself is the unit: ONE context for every claude call
    it makes (one cron job = one trace). A process that runs for days (a daemon)
    rotates to a fresh context after CCSTITCH_JOB_MAX_S (default 30 min), so it
    does not become one multi-day trace.
* Each minted context is recorded once to the collector as a `job` record (POST
  /v1/links, kind=job: trace id, span id, agent, workflow, pid, start). The job never
  sends a span of its own (it cannot know when it ends, and a span is immutable once
  sent); the store and the exporter synthesize the job's root span from the record,
  ending it at its last child. If the record is lost, the children still share the
  trace id, and the stitcher folds them together by trace id (stitch.LINK_GRACE_S).
* OTEL_RESOURCE_ATTRIBUTES gains `ccstitch.agent=<agent>` when the child env names an
  agent (CCSTITCH_AGENT). It reaches the collector on every span even when the
  session's hooks are off (a `claude -p` run with disableAllHooks), so
  such a session is still attributed. Existing resource attributes are kept.

Never raises; a failure means the child env is left as it was. Kill switch:
CCSTITCH_JOB_CONTEXT=0 (inherited TRACEPARENTs still pass through untouched,
because this module never removes anything).
"""
from __future__ import annotations

import contextlib
import contextvars
import os
import secrets
import sys
import threading
import time
from typing import Any, Iterator, Mapping, Optional
from urllib.parse import quote

from ccstitch import config, otlp

_LOCK = threading.Lock()
_STATE: dict[str, Any] = {}      # {"tp": str, "minted": float, "pid": int}
_UNIT: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("ccstitch_unit", default=None)


def _after_fork_in_child() -> None:
    """A fork can land while another thread holds _LOCK; the child would deadlock on
    its first claude call. Fresh lock, and the child mints its own context."""
    global _LOCK
    _LOCK = threading.Lock()
    _STATE.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


def _disabled(env: Mapping[str, str]) -> bool:
    return (env.get("CCSTITCH_JOB_CONTEXT") or "").strip().lower() in {"0", "off", "false", "no"}


def _workflow() -> Optional[str]:
    """What this job is: the `-m` module, else the script name. Never argv beyond
    that (arguments can carry prompts or secrets)."""
    try:
        spec = getattr(sys.modules.get("__main__"), "__spec__", None)
        if spec is not None and getattr(spec, "name", None):
            name = spec.name
            return name[:-len(".__main__")] if name.endswith(".__main__") else name
        if sys.argv and sys.argv[0]:
            return os.path.basename(sys.argv[0])[:120] or None
    except Exception:  # noqa: BLE001
        pass
    return None


def _record(rec: dict[str, Any]) -> None:
    if "PYTEST_CURRENT_TEST" in os.environ and not os.environ.get("CCSTITCH_JOB_RECORD_IN_TESTS"):
        return   # a test run must never write job rows into the live store
    try:
        from ccstitch import hook
        threading.Thread(target=hook.deliver, args=(rec,), daemon=True,
                         name="trace-job-record").start()
    except Exception:  # noqa: BLE001
        pass


def _mint(agent: Optional[str], workflow: Optional[str]) -> str:
    trace_id, span_id = secrets.token_hex(16), secrets.token_hex(8)
    _record({"kind": "job", "trace_id": trace_id, "span_id": span_id, "agent": agent,
             "workflow": workflow, "pid": os.getpid(), "started_ns": time.time_ns()})
    return f"00-{trace_id}-{span_id}-01"


def _inherited(env: Mapping[str, str]) -> Optional[str]:
    tp = env.get("TRACEPARENT")
    return tp if tp and otlp.parse_traceparent(tp) else None


def fresh_traceparent(env: Optional[Mapping[str, str]] = None, *, agent: Optional[str] = None,
                      workflow: Optional[str] = None) -> Optional[str]:
    """A TRACEPARENT for ONE logical unit (one spawned run): the inherited one when this
    process was launched by a traced parent (the unit hangs under that tool call),
    the enclosing unit() if any, else a NEW context recorded as its own job."""
    env = os.environ if env is None else env
    got = _inherited(env) or _UNIT.get()
    if got:
        return got
    if _disabled(env):
        return None
    return _mint(agent or (str(env.get("CCSTITCH_AGENT") or "").strip() or None), workflow)


@contextlib.contextmanager
def unit(*, agent: Optional[str] = None, workflow: Optional[str] = None,
         env: Optional[Mapping[str, str]] = None) -> Iterator[Optional[str]]:
    """Scope one logical unit of work: every claude call inside shares one trace.
    Nested units reuse the outer one; an inherited TRACEPARENT wins. Never raises."""
    try:
        tp = fresh_traceparent(env, agent=agent, workflow=workflow)
    except Exception:  # noqa: BLE001
        tp = None
    token = _UNIT.set(tp) if tp else None
    try:
        yield tp
    finally:
        if token is not None:
            _UNIT.reset(token)


def job_traceparent(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """The TRACEPARENT a child should carry: the inherited one if well-formed, else
    the enclosing unit(), else this process's job context (minted on first use,
    rotated after job_max_s). None when disabled and nothing was inherited."""
    env = os.environ if env is None else env
    got = _inherited(env) or _UNIT.get()
    if got:
        return got
    if _disabled(env):
        return None
    now = time.time()
    with _LOCK:
        tp = _STATE.get("tp")
        fresh = (tp and _STATE.get("pid") == os.getpid()
                 and now - _STATE.get("minted", 0) < config.job_max_s())
        if fresh:
            return tp
        tp = _mint(str(env.get("CCSTITCH_AGENT") or "").strip() or None, _workflow())
        _STATE.update(tp=tp, minted=now, pid=os.getpid())
    return tp


def _resource_attrs(existing: Optional[str], agent: str) -> str:
    """Append (or replace) ccstitch.agent in an OTEL_RESOURCE_ATTRIBUTES value
    (comma-separated key=value, values percent-encoded per the OTel spec)."""
    keep = [p for p in (existing or "").split(",")
            if p.strip() and p.split("=", 1)[0].strip() != otlp.RESOURCE_AGENT]
    keep.append(f"{otlp.RESOURCE_AGENT}={quote(agent, safe='')}")
    return ",".join(keep)


def child_env(env: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    """Env additions for one headless claude child, computed from the child env as
    built so far (so a CCSTITCH_AGENT the caller just set is seen).
    {} on any failure."""
    try:
        env = os.environ if env is None else env
        out: dict[str, str] = {}
        tp = job_traceparent(env)
        if tp and tp != env.get("TRACEPARENT"):
            out["TRACEPARENT"] = tp
        agent = str(env.get("CCSTITCH_AGENT") or "").strip()
        if agent and len(agent) <= 80:
            want = _resource_attrs(env.get("OTEL_RESOURCE_ATTRIBUTES"), agent)
            if want != env.get("OTEL_RESOURCE_ATTRIBUTES"):
                out["OTEL_RESOURCE_ATTRIBUTES"] = want
        return out
    except Exception:  # noqa: BLE001 - tracing never changes whether a spawn happens
        return {}


def shell_exports(env: Optional[Mapping[str, str]] = None, *, fresh: bool = False,
                  agent: Optional[str] = None, workflow: Optional[str] = None) -> str:
    """`export TRACEPARENT=...; ` for a spawner that runs claude through a shell
    string (tmux new-session does not inherit the spawner's env). fresh=True makes
    this spawn its own unit (fresh_traceparent). "" on failure."""
    try:
        import shlex
        tp = (fresh_traceparent(env, agent=agent, workflow=workflow) if fresh
              else job_traceparent(env))
        return f"export TRACEPARENT={shlex.quote(tp)}; " if tp else ""
    except Exception:  # noqa: BLE001
        return ""


def _reset_for_tests() -> None:
    with _LOCK:
        _STATE.clear()
    _UNIT.set(None)
