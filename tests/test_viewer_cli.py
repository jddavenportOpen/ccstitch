"""The built-in viewer, the read-side Host check, and the CLI entry points."""
from __future__ import annotations

import http.client
import io
import json
import subprocess
import sys
import threading
from contextlib import redirect_stdout
from http.server import ThreadingHTTPServer

import pytest

from ccstitch import cli, collector, config, hook, store


@pytest.fixture
def server():
    state = collector.State()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), collector.make_handler(state))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield httpd.server_address[1], state
    httpd.shutdown()


def _get(port, path, host=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    c.putrequest("GET", path, skip_host=True)
    c.putheader("Host", host or f"127.0.0.1:{port}")
    c.endheaders()
    r = c.getresponse()
    return r.status, dict(r.getheaders()), r.read()


def test_viewer_is_served_at_root_and_trace_paths(server):
    port, _ = server
    for path in ("/", "/t/s%3Aabc", f"/t/{'0' * 31}1"):
        code, headers, body = _get(port, path)
        assert code == 200 and headers["content-type"].startswith("text/html")
        assert b"<title>ccstitch traces</title>" in body
        assert "frame-ancestors 'none'" in headers["content-security-policy"]


def test_viewer_is_self_contained():
    html = collector.viewer_html().decode()
    for needle in ("http://", "https://", "<link ", "src="):
        assert needle not in html, needle          # no CDN, no external fetch: works offline
    assert "innerHTML" not in html                   # data is rendered as text, never as markup


def test_reads_refuse_a_foreign_host_header(server):
    port, _ = server
    assert _get(port, "/api/traces", host="evil.example:4328")[0] == 403
    assert _get(port, "/", host="evil.example")[0] == 403
    assert _get(port, "/api/traces", host="localhost:4328")[0] == 200
    assert _get(port, "/api/traces", host="[::1]:4328")[0] == 200
    assert _get(port, "/healthz", host="evil.example")[0] == 200     # liveness carries no data


def test_host_is_local():
    assert collector.host_is_local("127.0.0.1:1") and collector.host_is_local("LOCALHOST")
    assert not collector.host_is_local(None) and not collector.host_is_local("127.0.0.1.evil.example")


def test_unknown_trace_is_a_404_with_json(server):
    port, _ = server
    code, headers, body = _get(port, "/api/traces/nope")
    assert code == 404 and json.loads(body)["error"]


def test_non_object_json_is_rejected(server):
    port, state = server
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    c.request("POST", "/v1/links", body=b"[1,2]", headers={"content-type": "application/json"})
    assert c.getresponse().status == 400 and state.rejected == 1


def test_status_says_down_when_nothing_listens(monkeypatch, tmp_path):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = cli.main(["status"])
    assert rc == 1 and "DOWN" in buf.getvalue() and "ccstitch install" in buf.getvalue()


def test_status_says_up_against_a_live_collector(server, monkeypatch, tmp_path):
    port, _ = server
    monkeypatch.setenv("CCSTITCH_PORT", str(port))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = cli.main(["status", "--json"])
    out = json.loads(buf.getvalue())
    assert rc == 0 and out["collector"] == "up" and out["claude_code_setup"] == "off"


def test_hook_subcommand_spools_when_the_collector_is_down():
    rec = {"session_id": "sess-1", "cwd": "/tmp/project", "source": "startup"}
    env = {**__import__("os").environ, "CCSTITCH_PORT": "1", "TRACEPARENT": f"00-{'a' * 32}-{'b' * 16}-01"}
    r = subprocess.run([sys.executable, "-m", "ccstitch", "hook"], input=json.dumps(rec),
                       capture_output=True, text=True, env=env, timeout=30)
    assert r.returncode == 0 and r.stdout == "" and r.stderr == ""
    line = json.loads(config.spool_path().read_text().splitlines()[0])
    assert line["session_id"] == "sess-1" and line["traceparent"].startswith("00-aaaa")


def test_hook_survives_garbage_input_and_the_kill_switch(monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json"))
    assert hook.main() == 0
    monkeypatch.setenv("CCSTITCH_HOOK_DISABLED", "1")
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"session_id": "x"})))
    assert hook.main() == 0 and not config.spool_path().exists()


def test_export_subcommand_round_trips(tmp_path, capsys):
    conn = store.connect()
    conn.close()
    assert cli.main(["export", "missing", "--db", str(config.db_path())]) == 1


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["--version"])
    assert e.value.code == 0 and "ccstitch" in capsys.readouterr().out
