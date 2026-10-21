"""`ccstitch install`: merges, never clobbers, and --uninstall is an exact inverse."""
from __future__ import annotations

import io
import json

from ccstitch import install

CMD = "/usr/bin/python3 -m ccstitch hook"
BASE = {"model": "opus", "env": {"MY_VAR": "1"},
        "hooks": {"SessionEnd": [{"hooks": [{"type": "command", "command": "echo bye"}]}],
                  "SessionStart": [{"hooks": [{"type": "command", "command": "echo hi"}]}]}}


def test_install_then_uninstall_restores_the_original():
    p = install.plan_install(BASE, port=4328, command=CMD)
    on = p.new
    assert not p.conflicts and install.status(BASE) == "off" and install.status(on) == "on"
    assert on["env"]["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == "http://127.0.0.1:4328/v1/traces"
    assert on["env"]["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] == "http/json"
    assert on["model"] == "opus" and on["hooks"]["SessionEnd"] == BASE["hooks"]["SessionEnd"]
    assert len(on["hooks"]["SessionStart"]) == 2 and on["env"]["MY_VAR"] == "1"
    assert install.plan_install(on, port=4328, command=CMD).new == on      # idempotent
    assert install.plan_uninstall(on).new == BASE                          # exact inverse


def test_empty_file_round_trips_to_empty():
    on = install.plan_install({}, port=4328, command=CMD).new
    assert install.status(on) == "on"
    assert install.plan_uninstall(on).new == {}


def test_conflicting_value_is_refused_unless_forced():
    d = {"env": {"OTEL_TRACES_EXPORTER": "console"}}
    p = install.plan_install(d, port=4328, command=CMD)
    assert p.conflicts and "OTEL_TRACES_EXPORTER" in p.conflicts[0]
    forced = install.plan_install(d, port=4328, command=CMD, force=True)
    assert not forced.conflicts and forced.new["env"]["OTEL_TRACES_EXPORTER"] == "otlp"


def test_existing_metrics_pipeline_is_left_alone():
    d = {"env": {"OTEL_METRICS_EXPORTER": "otlp", "OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.example"}}
    on = install.plan_install(d, port=4328, command=CMD).new
    assert on["env"]["OTEL_METRICS_EXPORTER"] == "otlp"
    assert on["env"]["OTEL_EXPORTER_OTLP_ENDPOINT"] == "https://otel.example"
    assert "OTEL_METRICS_EXPORTER" not in on["env"][install.MANAGED].split(",")
    assert install.plan_uninstall(on).new == d


def test_uninstall_keeps_a_value_changed_by_hand():
    on = install.plan_install({}, port=4328, command=CMD).new
    on["env"]["OTEL_TRACES_EXPORTER"] = "console"
    p = install.plan_uninstall(on)
    assert p.new["env"] == {"OTEL_TRACES_EXPORTER": "console"} and p.conflicts


def test_custom_port_reaches_the_hook_and_the_endpoint():
    cmd = install.hook_command("/usr/bin/python3", 4400)
    assert cmd.endswith("-m ccstitch hook --port 4400")
    on = install.plan_install({}, port=4400, command=cmd).new
    assert install.status(on) == "on"
    assert install.plan_uninstall(on).new == {}


def test_run_writes_atomically_backs_up_once_and_dry_run_writes_nothing(tmp_path):
    p = tmp_path / "settings.json"
    p.write_text(json.dumps(BASE))
    out = io.StringIO()
    assert install.run(settings=p, dry_run=True, out=out) == 0
    assert json.loads(p.read_text()) == BASE and "+" in out.getvalue()
    assert install.run(settings=p, out=io.StringIO()) == 0
    assert install.status(json.loads(p.read_text())) == "on"
    assert json.loads((tmp_path / "settings.json.bak-ccstitch").read_text()) == BASE
    assert install.run(settings=p, out=io.StringIO()) == 0                  # no change
    assert install.run(settings=p, uninstall=True, out=io.StringIO()) == 0
    assert json.loads(p.read_text()) == BASE


def test_run_creates_a_missing_file_and_refuses_a_broken_one(tmp_path):
    p = tmp_path / "new" / "settings.json"
    assert install.run(settings=p, out=io.StringIO()) == 0
    assert install.status(json.loads(p.read_text())) == "on"
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    out = io.StringIO()
    assert install.run(settings=bad, out=out) == 2
    assert bad.read_text() == "{not json" and "refusing" in out.getvalue()
    arr = tmp_path / "arr.json"
    arr.write_text("[]")
    assert install.run(settings=arr, out=io.StringIO()) == 2


def test_run_conflict_writes_nothing(tmp_path):
    p = tmp_path / "settings.json"
    p.write_text(json.dumps({"env": {"OTEL_TRACES_EXPORTER": "console"}}))
    before = p.read_text()
    assert install.run(settings=p, out=io.StringIO()) == 3
    assert p.read_text() == before


def test_default_settings_path_honours_claude_config_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
    assert install.default_settings_path() == tmp_path / "settings.json"
