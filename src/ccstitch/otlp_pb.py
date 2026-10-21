"""OTLP JSON ExportTraceServiceRequest -> protobuf wire bytes, standard library only.

Some OTLP/HTTP receivers accept only `application/x-protobuf` (Arize Phoenix answers
415 to JSON), so the forwarder encodes its JSON payloads here instead of taking a
protobuf dependency. Only the fields export.py emits are encoded; field numbers are
from opentelemetry-proto (trace/v1/trace.proto, common/v1/common.proto,
resource/v1/resource.proto, collector/trace/v1/trace_service.proto).
"""
from __future__ import annotations

import struct
from typing import Any

_VARINT, _I64, _LEN, _I32 = 0, 1, 2, 5


def _varint(n: int) -> bytes:
    n &= (1 << 64) - 1                      # int64 negatives encode as 10-byte two's complement
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _key(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _len(field: int, payload: bytes) -> bytes:
    return _key(field, _LEN) + _varint(len(payload)) + payload


def _str(field: int, s: Any) -> bytes:
    return _len(field, str(s).encode("utf-8")) if s not in (None, "") else b""


def _hex(field: int, h: Any) -> bytes:
    return _len(field, bytes.fromhex(h)) if h else b""


def _fixed64(field: int, n: Any) -> bytes:
    return _key(field, _I64) + struct.pack("<Q", int(n or 0) & ((1 << 64) - 1))


def any_value(v: dict[str, Any]) -> bytes:
    if "stringValue" in v:
        return _len(1, str(v["stringValue"]).encode("utf-8"))
    if "boolValue" in v:
        return _key(2, _VARINT) + _varint(1 if v["boolValue"] else 0)
    if "intValue" in v:
        return _key(3, _VARINT) + _varint(int(v["intValue"]))
    if "doubleValue" in v:
        return _key(4, _I64) + struct.pack("<d", float(v["doubleValue"]))
    if "arrayValue" in v:
        vals = (v["arrayValue"] or {}).get("values") or []
        return _len(5, b"".join(_len(1, any_value(x)) for x in vals))
    if "kvlistValue" in v:
        vals = (v["kvlistValue"] or {}).get("values") or []
        return _len(6, b"".join(_len(1, key_value(x)) for x in vals))
    return b""


def key_value(kv: dict[str, Any]) -> bytes:
    return _str(1, kv.get("key")) + _len(2, any_value(kv.get("value") or {}))


def _attrs(field: int, items: Any) -> bytes:
    return b"".join(_len(field, key_value(kv)) for kv in items or [])


def span(s: dict[str, Any]) -> bytes:
    out = _hex(1, s.get("traceId")) + _hex(2, s.get("spanId"))
    out += _hex(4, s.get("parentSpanId"))
    out += _str(5, s.get("name"))
    if s.get("kind"):
        out += _key(6, _VARINT) + _varint(int(s["kind"]))
    out += _fixed64(7, s.get("startTimeUnixNano")) + _fixed64(8, s.get("endTimeUnixNano"))
    out += _attrs(9, s.get("attributes"))
    st = s.get("status") or {}
    status = _str(2, st.get("message"))
    if st.get("code"):
        status += _key(3, _VARINT) + _varint(int(st["code"]))
    out += _len(15, status)
    return out


def export_request(payload: dict[str, Any]) -> bytes:
    """ExportTraceServiceRequest (JSON mapping, as export.build makes it) -> bytes."""
    out = bytearray()
    for rs in payload.get("resourceSpans") or []:
        res = _attrs(1, (rs.get("resource") or {}).get("attributes"))
        body = _len(1, res)
        for ss in rs.get("scopeSpans") or []:
            sc = ss.get("scope") or {}
            scope = _str(1, sc.get("name")) + _str(2, sc.get("version"))
            sbody = _len(1, scope) + b"".join(_len(2, span(s)) for s in ss.get("spans") or [])
            sbody += _str(3, ss.get("schemaUrl"))
            body += _len(2, sbody)
        body += _str(3, rs.get("schemaUrl"))
        out += _len(1, body)
    return bytes(out)
