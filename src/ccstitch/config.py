"""Paths and knobs. Every one has an environment override, so tests and second
installs never touch the default store."""
from __future__ import annotations

import os
from pathlib import Path

DEFAULT_PORT = 4328          # 4317/4318 are the OTLP defaults; stay off them
MAX_BODY_BYTES = 16 * 1024 * 1024
STITCH_INTERVAL_S = 20
PRUNE_INTERVAL_S = 3600
# Sessions re-examined for a late-arriving parent span. A parent's tool span ends
# (and is exported) only after the child it launched exits, so links resolve late.
RESOLVE_HORIZON_S = 24 * 3600
# How long a child waits for its parent's span before the stitcher folds it by trace
# id alone (stitch.py). The forwarder's settle window must exceed it, or an orphan is
# exported alone before it folds.
LINK_GRACE_S = 600

# Identity attributes the CLI stamps on every span. Dropped at ingest: they are the
# same on every row, and an email or account id does not belong in a trace store.
DROP_ATTRS = frozenset({"user.email", "user.id", "organization.id", "user.account_id",
                        "user.account_uuid", "terminal.type"})


def _home_state() -> Path:
    xdg = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "state"
    return base / "ccstitch"


def default_state_dir() -> Path:
    return _home_state()


def state_dir() -> Path:
    raw = os.environ.get("CCSTITCH_DIR")
    return Path(raw).expanduser() if raw else _home_state()


def db_path() -> Path:
    raw = os.environ.get("CCSTITCH_DB")
    return Path(raw).expanduser() if raw else state_dir() / "traces.db"


def spool_path() -> Path:
    return state_dir() / "links.spool.jsonl"


def heartbeat_path() -> Path:
    return state_dir() / "collector-heartbeat.json"


def off_path() -> Path:
    return state_dir() / "OFF"


def port() -> int:
    try:
        return int(os.environ.get("CCSTITCH_PORT") or DEFAULT_PORT)
    except ValueError:
        return DEFAULT_PORT


def retention_days() -> float:
    try:
        return max(0.5, float(os.environ.get("CCSTITCH_RETENTION_DAYS") or 7))
    except ValueError:
        return 7.0


# Spool files (link + job records the collector could not take) stop growing here: a
# collector that never runs must not turn every session start into disk growth.
MAX_SPOOL_BYTES = 5 * 1024 * 1024

# Job trace context (context.py): a Python process with no inherited TRACEPARENT
# gets one trace for its claude calls, rotated after this long so a daemon that
# runs for days does not become one multi-day trace.
JOB_MAX_S_DEFAULT = 1800

# Forwarder (forward.py). Off unless CCSTITCH_FORWARD_ENDPOINT is set.
FORWARD_QUEUE_MAX = 256          # queued traces; beyond this they are dropped and counted
FORWARD_QUEUE_MAX_SPANS = 50_000 # queued spans (the memory bound); same drop rule
FORWARD_MAX_TRACE_SPANS = 20_000 # spans built per trace; the rest is counted as truncated
FORWARD_BATCH_SPANS = 2000       # spans per POST
FORWARD_MAX_ATTEMPTS = 4         # 1 try + 3 retries, backoff 1s, 2s, 4s
FORWARD_TIMEOUT_S = 10.0
FORWARD_TRACES_PER_PASS = 50


def _num(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return default


def job_max_s() -> float:
    return max(60.0, _num("CCSTITCH_JOB_MAX_S", JOB_MAX_S_DEFAULT))


def forward_endpoint() -> str | None:
    """The OTLP/HTTP traces URL repaired spans are forwarded to, or None (off).

    Accepts a base URL (http://host:6006) or a full traces URL (.../v1/traces), the
    same two shapes OTEL_EXPORTER_OTLP_ENDPOINT / _TRACES_ENDPOINT take."""
    raw = (os.environ.get("CCSTITCH_FORWARD_ENDPOINT") or "").strip()
    if not raw or not raw.startswith(("http://", "https://")):
        return None
    return raw if raw.rstrip("/").endswith("/v1/traces") else raw.rstrip("/") + "/v1/traces"


def forward_protocol() -> str:
    """CCSTITCH_FORWARD_PROTOCOL: http/protobuf (default, what every OTLP/HTTP receiver
    accepts; Arize Phoenix accepts nothing else) or http/json."""
    raw = (os.environ.get("CCSTITCH_FORWARD_PROTOCOL") or "").strip().lower()
    return "http/json" if raw == "http/json" else "http/protobuf"


def forward_headers() -> dict[str, str]:
    """CCSTITCH_FORWARD_HEADERS, in the OTEL_EXPORTER_OTLP_HEADERS format:
    comma-separated key=value, values percent-decoded (Langfuse needs Authorization)."""
    from urllib.parse import unquote
    out: dict[str, str] = {}
    for part in (os.environ.get("CCSTITCH_FORWARD_HEADERS") or "").split(","):
        k, sep, v = part.partition("=")
        if sep and k.strip():
            out[k.strip()] = unquote(v.strip())
    return out


def forward_settle_s() -> float:
    """A trace is forwarded once nothing new has arrived for this long, so the
    stitcher's late repairs are in: a parent's tool span lands after its child exits,
    and an orphan folds by trace id only after LINK_GRACE_S. Never below that + 60 s."""
    return max(LINK_GRACE_S + 60.0, _num("CCSTITCH_FORWARD_SETTLE_S", LINK_GRACE_S + 120))


def forward_backfill_s() -> float:
    """On start, how far back the forwarder reaches for traces not yet sent."""
    return max(0.0, _num("CCSTITCH_FORWARD_BACKFILL_S", 3600))


def max_db_bytes() -> int:
    try:
        return int(float(os.environ.get("CCSTITCH_MAX_DB_MB") or 1536) * 1024 * 1024)
    except ValueError:
        return 1536 * 1024 * 1024
