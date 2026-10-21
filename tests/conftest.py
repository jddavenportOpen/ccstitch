"""Hermetic by construction: before any ccstitch module is imported, the store is
pointed at a temp dir and the collector port at one nothing listens on, so a test
run can never write into a real store or post to a running collector."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

_REAL = (Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "ccstitch").resolve()
os.environ["CCSTITCH_DIR"] = tempfile.mkdtemp(prefix="ccstitch-test-")
os.environ.pop("CCSTITCH_DB", None)
os.environ["CCSTITCH_PORT"] = "1"            # nothing listens on port 1
for _k in ("TRACEPARENT", "CCSTITCH_AGENT", "CCSTITCH_JOB_CONTEXT", "CCSTITCH_FORWARD_ENDPOINT",
           "CCSTITCH_PRICES", "CCSTITCH_HOOK_DISABLED", "CLAUDE_CONFIG_DIR"):
    os.environ.pop(_k, None)

import pytest  # noqa: E402

from ccstitch import config, context  # noqa: E402


@pytest.fixture(autouse=True)
def _tmp_state(tmp_path, monkeypatch):
    monkeypatch.setenv("CCSTITCH_DIR", str(tmp_path))
    monkeypatch.delenv("CCSTITCH_DB", raising=False)
    monkeypatch.setenv("CCSTITCH_PORT", "1")
    monkeypatch.delenv("TRACEPARENT", raising=False)
    monkeypatch.delenv("CCSTITCH_JOB_CONTEXT", raising=False)
    monkeypatch.delenv("CCSTITCH_FORWARD_ENDPOINT", raising=False)
    assert _REAL != config.db_path().resolve().parent
    context._reset_for_tests()
    yield
    context._reset_for_tests()
