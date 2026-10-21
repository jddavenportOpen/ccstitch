"""SessionStart hook body: tell the collector who this session is and what launched it.

Wired by `ccstitch install` as `<python> -m ccstitch hook`. Records
{session_id, TRACEPARENT, cwd, source, agent}.
TRACEPARENT is the repair key for anthropics/claude-code#99607: when the CLI drops the
interaction span, the inbound trace context is otherwise lost, and a `claude -p`
launched from another session's Bash tool shows up as an unrelated trace.

Fail-open on every path: exit 0, print nothing, finish in well under a second. When
the collector is down the record goes to a spool file it replays on start.
Kill switch: CCSTITCH_HOOK_DISABLED=1.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from typing import Any, Callable, Optional

from ccstitch import config

POST_TIMEOUT_S = 0.5


def build_record(payload: dict[str, Any], env: dict[str, str],
                 attribute: Optional[Callable[[str], tuple[Optional[str], str]]] = None) -> dict[str, Any]:
    cwd = payload.get("cwd") or ""
    agent, method = (env.get("CCSTITCH_AGENT") or None), ("env" if env.get("CCSTITCH_AGENT") else None)
    if agent is None and attribute is not None:
        try:
            agent, method = attribute(cwd)
        except Exception:  # noqa: BLE001 - attribution is a nicety, never a failure
            agent, method = None, "error"
    return {"session_id": payload.get("session_id"), "traceparent": env.get("TRACEPARENT"),
            "cwd": cwd, "source": payload.get("source"), "agent": agent,
            "attribution": method, "started_ns": time.time_ns()}


def deliver(rec: dict[str, Any]) -> bool:
    body = json.dumps(rec).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{config.port()}/v1/links", data=body,
                                 headers={"content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=POST_TIMEOUT_S) as r:
            return 200 <= r.status < 300
    except Exception:  # noqa: BLE001
        try:
            p = config.spool_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            if p.exists() and p.stat().st_size > config.MAX_SPOOL_BYTES:
                return False   # a collector that never runs must not grow the disk
            with open(p, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except OSError:
            pass
        return False


def main(attribute: Optional[Callable[[str], tuple[Optional[str], str]]] = None) -> int:
    if (os.environ.get("CCSTITCH_HOOK_DISABLED") or "").strip() in {"1", "true", "yes"}:
        return 0
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
        if payload.get("session_id"):
            deliver(build_record(payload, dict(os.environ), attribute))
    except Exception:  # noqa: BLE001 - a telemetry hook must never fail a session
        pass
    return 0
