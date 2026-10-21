"""Optional OTLP/HTTP forwarder: send REPAIRED traces to any standard backend.

Off unless CCSTITCH_FORWARD_ENDPOINT is set in the collector's environment:

    CCSTITCH_FORWARD_ENDPOINT=http://127.0.0.1:6006        # Arize Phoenix
    CCSTITCH_FORWARD_HEADERS=Authorization=Basic%20...      # Langfuse etc. (optional)
    CCSTITCH_FORWARD_PROTOCOL=http/protobuf                 # default; or http/json

Protobuf is the default because every OTLP/HTTP receiver accepts it and some accept
nothing else (Arize Phoenix answers 415 to JSON); it is encoded by otlp_pb.py with
the standard library.

Why forward from here instead of pointing Claude Code at the backend: the raw CLI
export is the broken one (#99607 orphans, children in unrelated traces). This sends
what the built-in viewer shows: stitched parentage, synthetic invoke_agent /
invoke_workflow roots, service.name = agent, gen_ai.* attributes (export.py, semconv.py).

Shape, chosen so it can never hurt ingest:

* collect()  runs in its OWN thread on its OWN SQLite connection (WAL: readers never
  block the ingest writer, and it never takes the collector's lock), every
  STITCH_INTERVAL_S, bounded to FORWARD_TRACES_PER_PASS traces. It picks logical
  traces that have SETTLED (nothing new for CCSTITCH_FORWARD_SETTLE_S, default
  720 s, so the stitcher's late repairs are in), builds their OTLP payload inside one
  read transaction (a consistent snapshot) and enqueues it. A trace that grows after
  it was sent is sent again with only the new spans.
* a bounded queue (FORWARD_QUEUE_MAX traces and FORWARD_QUEUE_MAX_SPANS spans, the
  real memory bound). Full -> the trace is dropped and counted, never waited on, and
  NOT marked sent, so a later pass retries it. A trace over FORWARD_MAX_TRACE_SPANS
  sends what fits and counts the rest as truncated; exports.sent_spans records what
  actually went out.
* one sender thread batches queued traces up to FORWARD_BATCH_SPANS spans per POST
  and retries a failed POST with backoff (1 s, 2 s, 4 s); a 4xx other than 408/429
  is not retried. A batch that still fails is dropped and counted.

The ingest path (POST /v1/traces) never touches any of this. Counters are in
/healthz under "forward".
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional
from urllib.parse import urlparse

from ccstitch import config, export, otlp_pb, store

log = logging.getLogger("tracing.forward")


class Forwarder:
    def __init__(self, endpoint: str, *, headers: Optional[dict[str, str]] = None,
                 queue_max: int = config.FORWARD_QUEUE_MAX,
                 queue_max_spans: int = config.FORWARD_QUEUE_MAX_SPANS,
                 max_trace_spans: int = config.FORWARD_MAX_TRACE_SPANS,
                 batch_spans: int = config.FORWARD_BATCH_SPANS,
                 max_attempts: int = config.FORWARD_MAX_ATTEMPTS,
                 timeout_s: float = config.FORWARD_TIMEOUT_S,
                 backoff_s: float = 1.0,
                 settle_s: Optional[float] = None,
                 backfill_s: Optional[float] = None,
                 protocol: str = "http/protobuf") -> None:
        self.endpoint = endpoint
        self.protocol = "http/json" if protocol == "http/json" else "http/protobuf"
        self.headers = dict(headers or {})
        self.q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=max(1, queue_max))
        # The queue's real memory bound is spans, not traces: one trace can be 20k spans.
        self.queue_max_spans = max(1, queue_max_spans)
        self.max_trace_spans = max(1, max_trace_spans)
        self._queued_spans = 0
        self._qlock = threading.Lock()
        self.batch_spans = max(1, batch_spans)
        self.max_attempts = max(1, max_attempts)
        self.timeout_s = timeout_s
        self.backoff_s = backoff_s
        self.settle_s = config.forward_settle_s() if settle_s is None else settle_s
        floor_s = config.forward_backfill_s() if backfill_s is None else backfill_s
        self.floor_ns = store.now_ns() - int(floor_s * 1e9)
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.stats = {"traces_enqueued": 0, "spans_sent": 0, "batches_sent": 0,
                      "dropped_traces": 0, "dropped_spans": 0, "failed_batches": 0,
                      "truncated_traces": 0, "truncated_spans": 0,
                      "retries": 0, "last_error": None, "last_sent_at": None}

    def _enqueue(self, payload: dict[str, Any], spans: int) -> bool:
        with self._qlock:
            if self._queued_spans + spans > self.queue_max_spans and self._queued_spans > 0:
                return False
            try:
                self.q.put_nowait({"payload": payload, "spans": spans})
            except queue.Full:
                return False
            self._queued_spans += spans
            return True

    def _dequeued(self, spans: int) -> None:
        with self._qlock:
            self._queued_spans = max(0, self._queued_spans - spans)

    # ── producer side (own thread, own connection) ──────────────────────────
    def collect(self, conn) -> int:
        """Enqueue settled traces not yet (fully) forwarded. Returns how many.

        Re-sends are by MEMBERSHIP, not only by arrival time: when the stitcher folds
        a native trace into a logical trace after it was sent (a late child), every
        span of that native trace goes out under the logical trace, not just the
        spans that arrived since."""
        cutoff = store.now_ns() - int(self.settle_s * 1e9)
        rows = conn.execute(
            "SELECT n.logical_id id, SUM(n.n_spans) spans, MAX(n.last_ns) last,"
            " MAX(n.updated_ns) upd, e.n_spans prev_spans, e.sent_spans prev_sent,"
            " e.exported_ns sent_at, e.synthetic syn, e.natives prev_natives,"
            " group_concat(n.trace_id) natives"
            " FROM ntraces n LEFT JOIN exports e ON e.logical_id = n.logical_id"
            " WHERE n.logical_id IS NOT NULL AND n.last_ns >= ?"
            " GROUP BY n.logical_id"
            " HAVING MAX(n.last_ns) < ? AND MAX(n.updated_ns) < ?"
            " AND (e.n_spans IS NULL OR SUM(n.n_spans) > e.n_spans)"
            " ORDER BY MAX(n.last_ns) LIMIT ?",
            (self.floor_ns, cutoff, cutoff, config.FORWARD_TRACES_PER_PASS)).fetchall()
        n = 0
        for r in rows:
            first = r["prev_spans"] is None
            prev_syn = [] if first else json.loads(r["syn"] or "[]")
            prev_natives = set() if first else set(json.loads(r["prev_natives"] or "[]"))
            natives = sorted(set((r["natives"] or "").split(",")) - {""})
            joined = [t for t in natives if t not in prev_natives]
            since = None if first else r["sent_at"]
            built_at = store.now_ns()      # before the read: a span landing mid-build is sent next time
            try:
                if conn.in_transaction:
                    conn.commit()
                conn.execute("BEGIN")      # one snapshot for every query the build makes
                try:
                    built = export.build(conn, r["id"], max_spans=self.max_trace_spans,
                                         skip_span_ids=prev_syn, since_received_ns=since,
                                         include_traces=joined)
                finally:
                    conn.execute("COMMIT")
            except Exception as exc:  # noqa: BLE001 - one bad trace must not stop the pass
                self.stats["last_error"] = f"build {r['id'][:40]}: {type(exc).__name__}: {exc}"
                built = None
            if built is None:
                continue
            spans = sum(len(ss["spans"]) for rs in built["payload"]["resourceSpans"]
                        for ss in rs["scopeSpans"])
            if spans and not self._enqueue(built["payload"], spans):
                self.stats["dropped_traces"] += 1
                self.stats["dropped_spans"] += spans
                continue          # not recorded: a later pass retries it
            if spans:
                self.stats["traces_enqueued"] += 1
                n += 1
            total = int(r["spans"] or 0)
            sent_total = int(r["prev_sent"] or 0) + int(built["real_spans"])
            if built["truncated"]:
                # Over the per-trace cap: what was built is sent; the rest is counted,
                # never silently recorded as sent.
                self.stats["truncated_traces"] += 1
                self.stats["truncated_spans"] += max(0, total - sent_total)
            synthetic = sorted(set(prev_syn) | set(built["synthetic_ids"]))
            with conn:
                conn.execute(
                    "INSERT INTO exports (logical_id, n_spans, sent_spans, exported_ns, synthetic,"
                    " natives) VALUES (?,?,?,?,?,?) ON CONFLICT(logical_id) DO UPDATE SET"
                    " n_spans=excluded.n_spans, sent_spans=excluded.sent_spans,"
                    " exported_ns=excluded.exported_ns, synthetic=excluded.synthetic,"
                    " natives=excluded.natives",
                    (r["id"], total, min(sent_total, total), built_at, json.dumps(synthetic),
                     json.dumps(natives)))
        return n

    def _collect_loop(self, db_path) -> None:
        conn = None
        while not self.stop.wait(config.STITCH_INTERVAL_S):
            try:
                if conn is None:
                    conn = store.connect(db_path)
                self.collect(conn)
            except Exception as exc:  # noqa: BLE001 - the loop must outlive one bad pass
                self.stats["last_error"] = f"collect: {type(exc).__name__}: {exc}"
                log.warning("forward: %s", self.stats["last_error"])

    # ── consumer side (own thread) ──────────────────────────────────────────
    def start(self, db_path=None) -> None:
        """The sender thread, plus (given the store's path) the collect thread."""
        self.thread = threading.Thread(target=self._run, name="trace-forwarder", daemon=True)
        self.thread.start()
        if db_path is not None:
            threading.Thread(target=self._collect_loop, args=(db_path,),
                             name="trace-forward-collect", daemon=True).start()

    def close(self) -> None:
        self.stop.set()

    def _next_batch(self) -> Optional[dict[str, Any]]:
        try:
            first = self.q.get(timeout=1.0)
        except queue.Empty:
            return None
        self._dequeued(first["spans"])
        resource_spans = list(first["payload"]["resourceSpans"])
        spans = first["spans"]
        while spans < self.batch_spans:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                break
            self._dequeued(item["spans"])
            resource_spans += item["payload"]["resourceSpans"]
            spans += item["spans"]
        return {"payload": {"resourceSpans": resource_spans}, "spans": spans}

    def _run(self) -> None:
        while not self.stop.is_set():
            batch = self._next_batch()
            if batch is None:
                continue
            self.send(batch)

    def send(self, batch: dict[str, Any]) -> bool:
        body = self.encode(batch["payload"])
        for attempt in range(1, self.max_attempts + 1):
            retry, err = self._post(body)
            if err is None:
                self.stats["spans_sent"] += batch["spans"]
                self.stats["batches_sent"] += 1
                self.stats["last_sent_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
                return True
            self.stats["last_error"] = err
            if not retry or attempt == self.max_attempts:
                break
            self.stats["retries"] += 1
            if self.stop.wait(self.backoff_s * (2 ** (attempt - 1))):
                break
        self.stats["failed_batches"] += 1
        self.stats["dropped_spans"] += batch["spans"]
        log.warning("forward: dropped a batch of %d spans: %s", batch["spans"], self.stats["last_error"])
        return False

    def encode(self, payload: dict[str, Any]) -> bytes:
        if self.protocol == "http/json":
            return json.dumps(payload, separators=(",", ":")).encode()
        return otlp_pb.export_request(payload)

    def _post(self, body: bytes) -> tuple[bool, Optional[str]]:
        """(retryable, error) — error None on success."""
        ctype = "application/json" if self.protocol == "http/json" else "application/x-protobuf"
        req = urllib.request.Request(self.endpoint, data=body, method="POST",
                                     headers={"content-type": ctype, **self.headers})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                r.read()
                return False, None
        except urllib.error.HTTPError as e:
            return e.code in (408, 429) or e.code >= 500, f"HTTP {e.code}"
        except Exception as e:  # noqa: BLE001 - refused, timeout, DNS: all transient
            return True, f"{type(e).__name__}: {e}"

    def health(self) -> dict[str, Any]:
        u = urlparse(self.endpoint)
        return {"enabled": True, "endpoint": f"{u.scheme}://{u.netloc}{u.path}",
                "protocol": self.protocol,
                "queued": self.q.qsize(), "queued_spans": self._queued_spans,
                "settle_s": self.settle_s, **self.stats}


def from_env() -> Optional[Forwarder]:
    ep = config.forward_endpoint()
    return (Forwarder(ep, headers=config.forward_headers(), protocol=config.forward_protocol())
            if ep else None)
