"""The local trace collector: an OTLP/HTTP JSON receiver backed by SQLite, plus the
built-in viewer.

    ccstitch serve            # or: python -m ccstitch serve
    ccstitch status

Endpoints (127.0.0.1 only):
    GET  /                 the viewer: trace list
    GET  /t/<id>           the viewer: one trace as a tree + waterfall
    POST /v1/traces        OTLP JSON spans (protobuf -> 415 naming the fix)
    POST /v1/logs|metrics  accepted and dropped, so a stray exporter never errors
    POST /v1/links         a SessionStart record from `ccstitch hook`
    GET  /healthz          liveness + counts
    GET  /api/overview?hours=24
    GET  /api/traces?hours=24&limit=50&agent=x&errors=1
    GET  /api/traces/<id>

Reads (the viewer and /api) answer only when the Host header names this machine
(127.0.0.1, localhost, [::1]), so a web page that rebinds its own DNS name to
127.0.0.1 cannot read your traces.

Optional: CCSTITCH_FORWARD_ENDPOINT forwards repaired traces to any OTLP/HTTP
JSON backend (forward.py). Off by default; never on the ingest path.

Never a reason for a Claude session to fail: the CLI drops spans silently when this
is down (measured on 2.1.283: exit 0, nothing on stderr). Kill switch: touch
<state dir>/OFF (spans are acknowledged and discarded).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from ccstitch import __version__, config, forward, otlp, stitch, store

log = logging.getLogger("ccstitch.collector")

LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "[::1]", "::1"})


class State:
    def __init__(self) -> None:
        self.conn = store.connect()
        self.lock = threading.Lock()
        self.started = time.time()
        self.received = 0
        self.rejected = 0
        self.last_span_at: float | None = None
        self.last_stitch: dict[str, Any] = {}
        self.last_error: str | None = None
        # Clients that hung up before reading our reply (BrokenPipe/ConnectionReset).
        # The spans were already stored by then, so this is noise, not data loss.
        self.client_gone = 0
        self.forwarder: forward.Forwarder | None = None


def viewer_html() -> bytes:
    return resources.files("ccstitch").joinpath("viewer.html").read_bytes()


def _hours(q: dict[str, list[str]], default: float = 24.0) -> int:
    try:
        h = float((q.get("hours") or [default])[0])
    except ValueError:
        h = default
    return store.now_ns() - int(max(0.05, min(h, 24 * 30)) * 3600 * 1e9)


def host_is_local(host: str | None) -> bool:
    if not host:
        return False
    h = host.strip().lower()
    if h.startswith("["):                      # [::1]:4328
        h = h.split("]", 1)[0] + "]"
    elif h.count(":") == 1:                    # 127.0.0.1:4328
        h = h.split(":", 1)[0]
    return h in LOCAL_HOSTS


def make_handler(state: State):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"ccstitch/{__version__}"

        def log_message(self, fmt: str, *a: Any) -> None:   # quiet: errors are counted instead
            pass

        def _raw(self, code: int, raw: bytes, ctype: str) -> None:
            try:
                self.send_response(code)
                self.send_header("content-type", ctype)
                self.send_header("content-length", str(len(raw)))
                self.send_header("cache-control", "no-store")
                self.send_header("x-content-type-options", "nosniff")
                if ctype.startswith("text/html"):
                    self.send_header("content-security-policy",
                                     "default-src 'self'; script-src 'unsafe-inline'; "
                                     "style-src 'unsafe-inline'; connect-src 'self'; "
                                     "img-src 'self' data:; frame-ancestors 'none'")
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                # The exporter gave up waiting (its own timeout) after we had already
                # stored what it sent. Count it; a traceback per hang-up is just noise.
                state.client_gone += 1
                self.close_connection = True

        def _send(self, code: int, body: Any) -> None:
            self._raw(code, json.dumps(body, default=str).encode(), "application/json")

        def _body(self) -> bytes | None:
            try:
                n = int(self.headers.get("content-length") or 0)
            except ValueError:
                n = 0
            if n > config.MAX_BODY_BYTES:
                self._send(413, {"error": f"body over {config.MAX_BODY_BYTES} bytes"})
                return None
            return self.rfile.read(n) if n > 0 else b""

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            body = self._body()
            if body is None:
                return
            if path in ("/v1/logs", "/v1/metrics"):
                return self._send(200, {})
            if path not in ("/v1/traces", "/v1/links"):
                return self._send(404, {"error": "unknown path"})
            ctype = (self.headers.get("content-type") or "").lower()
            if "json" not in ctype:
                state.rejected += 1
                return self._send(415, {"error": "only OTLP/HTTP JSON is accepted: set "
                                                 "OTEL_EXPORTER_OTLP_PROTOCOL=http/json"})
            try:
                payload = json.loads(body or b"{}")
            except ValueError:
                state.rejected += 1
                return self._send(400, {"error": "body is not JSON"})
            if not isinstance(payload, dict):
                state.rejected += 1
                return self._send(400, {"error": "body is not a JSON object"})
            if config.off_path().exists():
                return self._send(200, {"discarded": "tracing OFF"})
            try:
                with state.lock:
                    if path == "/v1/traces":
                        n = store.ingest(state.conn, otlp.parse_traces(payload))
                        state.received += n
                        if n:
                            state.last_span_at = time.time()
                    else:
                        store.record_link(state.conn, payload)
            except Exception as exc:  # noqa: BLE001 - never 500 an exporter into retries forever
                state.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("ingest failed: %s", state.last_error)
                return self._send(200, {"partialSuccess": {"errorMessage": state.last_error}})
            return self._send(200, {})

        def do_GET(self) -> None:  # noqa: N802
            u = urlparse(self.path)
            q = parse_qs(u.query)
            if u.path == "/healthz":
                return self._send(200, health(state))
            if not host_is_local(self.headers.get("host")):
                return self._send(403, {"error": "reads answer only on a local Host header"})
            try:
                if u.path == "/" or u.path.startswith("/t/"):
                    return self._raw(200, viewer_html(), "text/html; charset=utf-8")
                with state.lock:
                    if u.path == "/api/overview":
                        return self._send(200, store.overview(state.conn, _hours(q)))
                    if u.path == "/api/traces":
                        return self._send(200, {"traces": store.list_traces(
                            state.conn, _hours(q), limit=int((q.get("limit") or [50])[0]),
                            agent=(q.get("agent") or [None])[0],
                            errors_only=(q.get("errors") or ["0"])[0] == "1")})
                    if u.path.startswith("/api/traces/"):
                        t = store.get_trace(state.conn, unquote(u.path[len("/api/traces/"):])[:200])
                        return self._send(200 if t else 404, t or {"error": "no such trace"})
            except (ValueError, TypeError) as exc:
                return self._send(400, {"error": str(exc)})
            return self._send(404, {"error": "unknown path"})

    return Handler


def health(state: State) -> dict[str, Any]:
    return {"ok": True, "version": __version__, "uptime_s": int(time.time() - state.started),
            "spans_received": state.received,
            "last_span_age_s": (int(time.time() - state.last_span_at)
                                if state.last_span_at else None),
            "rejected": state.rejected, "client_gone": state.client_gone,
            "last_stitch": state.last_stitch,
            "forward": state.forwarder.health() if state.forwarder else {"enabled": False},
            "last_error": state.last_error, "off": config.off_path().exists(),
            "db": str(config.db_path()), "pid": os.getpid()}


def drain_spool(state: State) -> int:
    """Link records the hook could not POST (collector down) are replayed here."""
    p = config.spool_path()
    if not p.exists():
        return 0
    tmp = p.with_suffix(".draining")
    try:
        p.rename(tmp)
    except OSError:
        return 0
    n = 0
    for line in tmp.read_text(errors="replace").splitlines():
        try:
            rec = json.loads(line)
            if isinstance(rec, dict):
                with state.lock:
                    n += store.record_link(state.conn, rec)
        except ValueError:
            continue
    tmp.unlink(missing_ok=True)
    return n


def background(state: State, stop: threading.Event) -> None:
    last_prune = 0.0
    while not stop.wait(config.STITCH_INTERVAL_S):
        due = time.time() - last_prune > config.PRUNE_INTERVAL_S
        if background_pass(state, prune=due) and due:
            last_prune = time.time()


def background_pass(state: State, *, prune: bool = False) -> bool:
    """One stitch pass (+ spool replay, + retention when asked). False on failure."""
    try:
        spooled = drain_spool(state)
        with state.lock:
            res = stitch.run(state.conn)
            if prune:
                res["pruned"] = store.prune(state.conn)
        res["spooled_links"] = spooled
        res["at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        state.last_stitch = res
        write_heartbeat(state)
        return True
    except Exception as exc:  # noqa: BLE001 - the loop must outlive one bad pass
        state.last_error = f"background: {type(exc).__name__}: {exc}"
        log.warning(state.last_error)
        return False


def write_heartbeat(state: State) -> None:
    p = config.heartbeat_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps({**health(state), "port": config.port(), "ts": time.time()},
                              default=str))
    tmp.replace(p)


def serve(host: str = "127.0.0.1") -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config.state_dir().mkdir(parents=True, exist_ok=True)
    state = State()
    httpd = ThreadingHTTPServer((host, config.port()), make_handler(state))
    httpd.daemon_threads = True
    state.forwarder = forward.from_env()
    if state.forwarder is not None:
        state.forwarder.start(config.db_path())   # own threads, own connection
        log.info("forwarding repaired traces to %s", state.forwarder.health()["endpoint"])
    stop = threading.Event()
    threading.Thread(target=background, args=(state, stop), daemon=True).start()
    log.info("ccstitch %s on http://%s:%s  (store %s)", __version__, host, config.port(),
             config.db_path())
    write_heartbeat(state)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        if state.forwarder is not None:
            state.forwarder.close()
