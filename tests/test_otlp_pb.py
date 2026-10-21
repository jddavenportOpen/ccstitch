"""otlp_pb: the stdlib protobuf encoder the forwarder uses by default. Checked with a
small wire-format decoder here, and against the official opentelemetry-proto schema
when that package happens to be installed (it is not a dependency)."""
from __future__ import annotations

import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ccstitch import export, forward, otlp_pb

TID, SID, PID = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331", "00f067aa0ba902b7"
PAYLOAD = {"resourceSpans": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "reviewer"}}]},
    "scopeSpans": [{"scope": {"name": "ccstitch", "version": "0.1.0"},
                    "schemaUrl": "https://opentelemetry.io/schemas/gen-ai-dev/1.42.0-dev",
                    "spans": [{
                        "traceId": TID, "spanId": SID, "parentSpanId": PID, "name": "chat claude-x",
                        "kind": 3, "startTimeUnixNano": "1700000000000000000",
                        "endTimeUnixNano": "1700000001500000000",
                        "attributes": export.kv_list({
                            "gen_ai.usage.input_tokens": 1060, "neg": -5, "ok": True,
                            "ttfc": 1.5, "reasons": ["end_turn"], "nested": {"a": "b"},
                            "unicode": "café"}),
                        "status": {"code": 2, "message": "rate_limit"}}]}]}]}


def _decode(buf: bytes) -> list[tuple[int, int, object]]:
    out, i = [], 0
    while i < len(buf):
        key, i = _uvarint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            v, i = _uvarint(buf, i)
        elif wire == 1:
            v, i = buf[i:i + 8], i + 8
        elif wire == 2:
            n, i = _uvarint(buf, i)
            v, i = buf[i:i + n], i + n
        else:
            raise AssertionError(f"unexpected wire type {wire}")
        out.append((field, wire, v))
    return out


def _uvarint(b: bytes, i: int) -> tuple[int, int]:
    shift = n = 0
    while True:
        x = b[i]
        i += 1
        n |= (x & 0x7F) << shift
        if not x & 0x80:
            return n, i
        shift += 7


def _one(fields, num):
    got = [v for f, _, v in fields if f == num]
    assert len(got) == 1, (num, got)
    return got[0]


def _attrs(span_fields):
    out = {}
    for f, _, kv in span_fields:
        if f != 9:
            continue
        kvf = _decode(kv)
        out[_one(kvf, 1).decode()] = _decode(_one(kvf, 2))
    return out


def test_wire_format_by_hand():
    req = _decode(otlp_pb.export_request(PAYLOAD))
    rs = _decode(_one(req, 1))
    res_kv = _decode(_one(_decode(_one(rs, 1)), 1))
    assert _one(res_kv, 1) == b"service.name"
    ss = _decode(_one(rs, 2))
    scope = _decode(_one(ss, 1))
    assert (_one(scope, 1), _one(scope, 2)) == (b"ccstitch", b"0.1.0")
    assert _one(ss, 3).startswith(b"https://opentelemetry.io/schemas/")
    sp = _decode(_one(ss, 2))
    assert _one(sp, 1) == bytes.fromhex(TID) and _one(sp, 2) == bytes.fromhex(SID)
    assert _one(sp, 4) == bytes.fromhex(PID) and _one(sp, 5) == b"chat claude-x"
    assert _one(sp, 6) == 3
    assert struct.unpack("<Q", _one(sp, 7))[0] == 1700000000000000000
    assert struct.unpack("<Q", _one(sp, 8))[0] == 1700000001500000000
    status = _decode(_one(sp, 15))
    assert (_one(status, 2), _one(status, 3)) == (b"rate_limit", 2)
    a = _attrs(sp)
    assert _one(a["gen_ai.usage.input_tokens"], 3) == 1060
    assert _one(a["neg"], 3) == (1 << 64) - 5                 # int64 two's complement
    assert _one(a["ok"], 2) == 1
    assert struct.unpack("<d", _one(a["ttfc"], 4))[0] == 1.5
    assert _one(a["unicode"], 1) == "café".encode()
    arr = _decode(_one(a["reasons"], 5))
    assert _decode(_one(arr, 1)) == [(1, 2, b"end_turn")]


def test_root_span_has_no_parent_field():
    p = {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"spans": [
        {"traceId": TID, "spanId": SID, "name": "invoke_agent", "startTimeUnixNano": "1",
         "endTimeUnixNano": "2", "attributes": [], "status": {"code": 0}}]}]}]}
    sp = _decode(_one(_decode(_one(_decode(_one(_decode(otlp_pb.export_request(p)), 1)), 2)), 2))
    assert 4 not in {f for f, _, _ in sp}


def test_matches_the_official_schema_when_available():
    pb = pytest.importorskip("opentelemetry.proto.collector.trace.v1.trace_service_pb2")
    msg = pb.ExportTraceServiceRequest()
    msg.ParseFromString(otlp_pb.export_request(PAYLOAD))
    s = msg.resource_spans[0].scope_spans[0].spans[0]
    assert s.trace_id.hex() == TID and s.parent_span_id.hex() == PID and s.kind == 3
    assert s.end_time_unix_nano == 1700000001500000000 and s.status.code == 2
    attrs = {kv.key: kv.value for kv in s.attributes}
    assert attrs["neg"].int_value == -5 and attrs["ttfc"].double_value == 1.5
    assert attrs["nested"].kvlist_value.values[0].value.string_value == "b"
    assert attrs["unicode"].string_value == "café"


def test_forwarder_posts_protobuf_by_default():
    got = []

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            got.append((self.headers.get("content-type"),
                        self.rfile.read(int(self.headers.get("content-length") or 0))))
            self.send_response(200)
            self.send_header("content-length", "0")
            self.end_headers()

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        fw = forward.Forwarder(f"http://127.0.0.1:{srv.server_address[1]}/v1/traces", backoff_s=0)
        assert fw.health()["protocol"] == "http/protobuf"
        assert fw.send({"payload": PAYLOAD, "spans": 1})
    finally:
        srv.shutdown()
    assert got[0][0] == "application/x-protobuf"
    assert got[0][1] == otlp_pb.export_request(PAYLOAD)


def test_forward_protocol_config(monkeypatch):
    from ccstitch import config
    assert config.forward_protocol() == "http/protobuf"
    monkeypatch.setenv("CCSTITCH_FORWARD_PROTOCOL", "http/json")
    assert config.forward_protocol() == "http/json"
    monkeypatch.setenv("CCSTITCH_FORWARD_PROTOCOL", "grpc")
    assert config.forward_protocol() == "http/protobuf"
