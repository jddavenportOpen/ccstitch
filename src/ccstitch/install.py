"""Write (or remove) the Claude Code settings that send traces to ccstitch.

    ccstitch install [--settings PATH] [--port N] [--dry-run] [--force]
    ccstitch install --uninstall [--settings PATH] [--dry-run]

Two things go into settings.json, and nothing else is touched:

* `env`: telemetry on, traces only, OTLP/HTTP JSON to the local collector. Only the
  TRACES signal is pointed here (OTEL_EXPORTER_OTLP_TRACES_*), so an existing
  metrics or logs pipeline on OTEL_EXPORTER_OTLP_ENDPOINT keeps working.
* a `SessionStart` hook running `<this python> -m ccstitch hook`.

Merging, never clobbering:

* a key that is absent is added; a key that already holds our value is left alone;
* a key that holds a DIFFERENT value is a conflict, and nothing is written unless you
  pass --force (each replaced value is printed);
* OTEL_METRICS_EXPORTER / OTEL_LOGS_EXPORTER are set to `none` only when absent;
* the keys we added are listed in `CCSTITCH_MANAGED`, so --uninstall removes exactly
  those (and only while they still hold our value) plus our hook entry. Other hooks,
  other env keys and every other setting stay as they were.

The file is backed up once (`settings.json.bak-ccstitch`) and replaced atomically.
An unreadable or non-object settings file is refused, never overwritten.
"""
from __future__ import annotations

import difflib
import json
import os
import re
import shlex
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ccstitch import config

MANAGED = "CCSTITCH_MANAGED"
HOOK_MARK = "-m ccstitch hook"
HOOK_TIMEOUT_S = 5
_ENDPOINT_RE = re.compile(r"^http://127\.0\.0\.1:(\d+)/v1/traces$")


def required_env(port: int) -> dict[str, str]:
    """Keys that must hold exactly these values for tracing to reach the collector."""
    return {
        "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
        "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1",
        "OTEL_TRACES_EXPORTER": "otlp",
        # http/json, never gRPC: anthropics/claude-code#97805 (headless sessions drop it)
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": f"http://127.0.0.1:{port}/v1/traces",
    }


# Set only when absent: telemetry on with no exporter named could otherwise default to
# something noisy, and a user who already exports metrics or logs keeps their choice.
SOFT_ENV = {"OTEL_METRICS_EXPORTER": "none", "OTEL_LOGS_EXPORTER": "none"}


def default_settings_path() -> Path:
    base = os.environ.get("CLAUDE_CONFIG_DIR")
    return (Path(base).expanduser() if base else Path.home() / ".claude") / "settings.json"


def hook_command(python: str | None = None, port: int | None = None) -> str:
    cmd = f"{shlex.quote(python or sys.executable)} {HOOK_MARK}"
    return cmd + (f" --port {int(port)}" if port and port != config.DEFAULT_PORT else "")


def _is_ours(h: Any) -> bool:
    return isinstance(h, dict) and HOOK_MARK in str(h.get("command") or "")


def has_hook(d: dict[str, Any]) -> bool:
    starts = ((d.get("hooks") or {}).get("SessionStart") or [])
    return any(_is_ours(h) for e in starts if isinstance(e, dict) for h in e.get("hooks") or [])


def _port_of(env: dict[str, Any], default: int) -> int:
    m = _ENDPOINT_RE.match(str(env.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or ""))
    return int(m.group(1)) if m else default


def status(d: dict[str, Any], port: int | None = None) -> str:
    """'on', 'off' or 'partial' for one parsed settings file."""
    env = d.get("env") if isinstance(d.get("env"), dict) else {}
    want = required_env(_port_of(env, port or config.port()))
    env_on = all(env.get(k) == v for k, v in want.items())
    env_off = MANAGED not in env and not any(env.get(k) == v for k, v in want.items()
                                             if k.startswith("OTEL_EXPORTER_OTLP_TRACES"))
    hook = has_hook(d)
    if env_on and hook:
        return "on"
    if env_off and not hook:
        return "off"
    return "partial"


@dataclass
class Plan:
    new: dict[str, Any]
    changes: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)


class SettingsError(Exception):
    pass


def _check_shape(d: Any) -> None:
    if not isinstance(d, dict):
        raise SettingsError("settings file is not a JSON object")
    if "env" in d and not isinstance(d["env"], dict):
        raise SettingsError("settings 'env' is not an object")
    if "hooks" in d and not isinstance(d["hooks"], dict):
        raise SettingsError("settings 'hooks' is not an object")
    starts = (d.get("hooks") or {}).get("SessionStart")
    if starts is not None and not isinstance(starts, list):
        raise SettingsError("settings 'hooks.SessionStart' is not a list")


def plan_install(d: dict[str, Any], *, port: int, command: str, force: bool = False) -> Plan:
    _check_shape(d)
    new = json.loads(json.dumps(d))
    plan = Plan(new)
    env = new.setdefault("env", {})
    managed = [k for k in str(env.get(MANAGED) or "").split(",") if k]
    for k, v in required_env(port).items():
        cur = env.get(k)
        if cur == v:
            continue
        if cur is None:
            env[k] = v
            managed.append(k)
            plan.changes.append(f"env.{k} = {v}")
        elif force:
            env[k] = v
            if k not in managed:
                managed.append(k)
            plan.changes.append(f"env.{k} = {v}  (replaced {cur!r})")
        else:
            plan.conflicts.append(f"env.{k} is {cur!r}, ccstitch needs {v!r}")
    for k, v in SOFT_ENV.items():
        if k not in env:
            env[k] = v
            managed.append(k)
            plan.changes.append(f"env.{k} = {v}")
    managed = list(dict.fromkeys(managed))
    if managed and env.get(MANAGED) != ",".join(managed):
        env[MANAGED] = ",".join(managed)

    hooks = new.setdefault("hooks", {})
    starts = list(hooks.get("SessionStart") or [])
    want = {"type": "command", "command": command, "timeout": HOOK_TIMEOUT_S, "async": True}
    found = False
    for e in starts:
        for i, h in enumerate((e.get("hooks") or []) if isinstance(e, dict) else []):
            if _is_ours(h):
                found = True
                if h != want:
                    e["hooks"][i] = want
                    plan.changes.append(f"hooks.SessionStart: updated to {command}")
    if not found:
        starts.append({"hooks": [want]})
        plan.changes.append(f"hooks.SessionStart += {command}")
    hooks["SessionStart"] = starts
    return plan


def plan_uninstall(d: dict[str, Any], *, port: int | None = None) -> Plan:
    _check_shape(d)
    new = json.loads(json.dumps(d))
    plan = Plan(new)
    env = new.get("env")
    if isinstance(env, dict):
        p = _port_of(env, port or config.port())
        ours = {**required_env(p), **SOFT_ENV}
        managed = [k for k in str(env.get(MANAGED) or "").split(",") if k]
        for k in managed:
            if k not in env:
                continue
            if env[k] == ours.get(k):
                del env[k]
                plan.changes.append(f"env.{k} removed")
            else:
                plan.conflicts.append(f"env.{k} was changed by hand ({env[k]!r}); left in place")
        if MANAGED in env:
            del env[MANAGED]
            plan.changes.append(f"env.{MANAGED} removed")
        if not env and d.get("env") != {}:     # emptied by us: drop it (an original {} stays)
            new.pop("env", None)
    hooks = new.get("hooks")
    if isinstance(hooks, dict) and isinstance(hooks.get("SessionStart"), list):
        kept = []
        for e in hooks["SessionStart"]:
            if not isinstance(e, dict):
                kept.append(e)
                continue
            inner = [h for h in e.get("hooks") or [] if not _is_ours(h)]
            if len(inner) != len(e.get("hooks") or []):
                plan.changes.append("hooks.SessionStart: ccstitch hook removed")
            if inner:
                kept.append({**e, "hooks": inner})
        if kept:
            hooks["SessionStart"] = kept
        else:
            hooks.pop("SessionStart", None)
        if not hooks and d.get("hooks") != {}:
            new.pop("hooks", None)
    return plan


def read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        d = json.loads(path.read_text() or "{}")
    except ValueError as exc:
        raise SettingsError(f"{path} is not valid JSON ({exc}); fix it by hand first") from exc
    _check_shape(d)
    return d


def write(path: Path, d: dict[str, Any]) -> Path | None:
    """Atomic replace; the first write backs the original up. Returns the backup path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    bak = path.with_name(path.name + ".bak-ccstitch")
    made = None
    if path.exists() and not bak.exists():
        bak.write_bytes(path.read_bytes())
        made = bak
    mode = (path.stat().st_mode & 0o777) if path.exists() else 0o600
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".settings.", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(d, f, indent=2)
            f.write("\n")
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return made


def _diff(path: Path, old: dict[str, Any], new: dict[str, Any]) -> str:
    a = (json.dumps(old, indent=2) + "\n").splitlines(keepends=True) if old else []
    b = (json.dumps(new, indent=2) + "\n").splitlines(keepends=True)
    return "".join(difflib.unified_diff(a, b, f"{path} (now)", f"{path} (after)"))


def run(*, settings: Path | None = None, uninstall: bool = False, dry_run: bool = False,
        force: bool = False, port: int | None = None, python: str | None = None,
        out=sys.stdout) -> int:
    path = Path(settings).expanduser() if settings else default_settings_path()
    port = port or config.port()
    try:
        old = read(path)
        plan = (plan_uninstall(old, port=port) if uninstall
                else plan_install(old, port=port, command=hook_command(python, port), force=force))
    except SettingsError as exc:
        print(f"refusing: {exc}", file=out)
        return 2
    for c in plan.conflicts:
        print(f"conflict: {c}", file=out)
    if plan.conflicts and not uninstall:
        print("nothing written. Re-run with --force to replace those values.", file=out)
        return 3
    if plan.new == old or (not old and not path.exists() and uninstall):
        print(f"no change: {path} is already {'clean' if uninstall else 'set up'}", file=out)
        return 0
    if dry_run:
        print(_diff(path, old, plan.new), end="", file=out)
        print(f"(dry run: {path} not written)", file=out)
        return 0
    bak = write(path, plan.new)
    for c in plan.changes:
        print(f"  {c}", file=out)
    print(f"{'removed from' if uninstall else 'wrote'} {path}"
          + (f" (backup: {bak})" if bak else ""), file=out)
    if not uninstall:
        print("New Claude Code sessions will send traces; running ones keep the env they "
              "started with. Start the collector: ccstitch serve", file=out)
    return 0
