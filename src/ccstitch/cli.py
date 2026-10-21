"""ccstitch command line.

    ccstitch serve   [--port N] [--db PATH]     run the collector + viewer
    ccstitch status  [--json]                   is the collector up, and is Claude Code set up?
    ccstitch hook                               the SessionStart hook body (reads stdin)
    ccstitch export  <trace_id> [--out F]       one repaired trace as OTLP/HTTP JSON
    ccstitch install [--dry-run] [--uninstall]  write/remove the settings.json block
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from typing import Any

from ccstitch import __version__, config


def probe(port: int, timeout: float = 1.0) -> dict[str, Any] | None:
    """GET /healthz on the collector: proof by use, not by a heartbeat file."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:  # noqa: BLE001
        return None


def cmd_status(args: argparse.Namespace) -> int:
    from ccstitch import install

    port = config.port()
    live = probe(port)
    settings = install.default_settings_path()
    try:
        setup = install.status(install.read(settings), port)
    except install.SettingsError as exc:
        setup = f"unreadable ({exc})"
    hb_age = None
    try:
        hb = json.loads(config.heartbeat_path().read_text())
        hb_age = int(time.time() - float(hb.get("ts") or 0))
    except (OSError, ValueError):
        pass
    out = {"collector": "up" if live else "down", "url": f"http://127.0.0.1:{port}/",
           "settings": str(settings), "claude_code_setup": setup, "db": str(config.db_path()),
           "heartbeat_age_s": hb_age, "health": live}
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        if live:
            age = live.get("last_span_age_s")
            print(f"collector  up on {out['url']}  (v{live.get('version')}, "
                  f"{live.get('spans_received', 0)} spans since start, "
                  f"last span {'never' if age is None else f'{age}s ago'})")
            if live.get("off"):
                print("           OFF switch present: spans are being discarded")
            if live.get("last_error"):
                print(f"           last error: {live['last_error']}")
        else:
            print(f"collector  DOWN (nothing answers on 127.0.0.1:{port}). Start it: ccstitch serve")
        print(f"settings   {setup}  ({settings})"
              + ("" if setup == "on" else "  -> run: ccstitch install"))
        print(f"store      {config.db_path()}")
    return 0 if live else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ccstitch", description=(
        "Local trace store for Claude Code's OpenTelemetry spans, with the "
        "anthropics/claude-code#99607 repair."))
    ap.add_argument("--version", action="version", version=f"ccstitch {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the collector and the viewer on 127.0.0.1")
    s.add_argument("--port", type=int, help=f"port (default {config.DEFAULT_PORT}, or CCSTITCH_PORT)")
    s.add_argument("--db", help="SQLite file (default: <state dir>/traces.db, or CCSTITCH_DB)")

    st = sub.add_parser("status", help="is the collector up, and are settings in place?")
    st.add_argument("--json", action="store_true")

    hk = sub.add_parser("hook", help="SessionStart hook body (Claude Code runs this)")
    hk.add_argument("--port", type=int, help="collector port the record is posted to")

    ex = sub.add_parser("export", help="one repaired trace as OTLP/HTTP JSON", add_help=False)
    ex.add_argument("rest", nargs=argparse.REMAINDER)

    ins = sub.add_parser("install", help="write the settings.json env block + SessionStart hook")
    ins.add_argument("--settings", help="settings file (default: ~/.claude/settings.json, "
                                        "or $CLAUDE_CONFIG_DIR/settings.json)")
    ins.add_argument("--port", type=int, help=f"collector port (default {config.DEFAULT_PORT})")
    ins.add_argument("--dry-run", action="store_true", help="print the diff, write nothing")
    ins.add_argument("--uninstall", action="store_true", help="remove exactly what install added")
    ins.add_argument("--force", action="store_true", help="replace conflicting values")

    args = ap.parse_args(argv)
    if args.cmd == "serve":
        if args.port:
            os.environ["CCSTITCH_PORT"] = str(args.port)
        if args.db:
            os.environ["CCSTITCH_DB"] = args.db
        from ccstitch import collector
        collector.serve()
        return 0
    if args.cmd == "status":
        return cmd_status(args)
    if args.cmd == "hook":
        if args.port:
            os.environ["CCSTITCH_PORT"] = str(args.port)
        from ccstitch import hook
        return hook.main()
    if args.cmd == "export":
        from ccstitch import export
        return export.main(args.rest)
    if args.cmd == "install":
        from ccstitch import install
        return install.run(settings=args.settings, uninstall=args.uninstall,
                           dry_run=args.dry_run, force=args.force, port=args.port)
    return 2


if __name__ == "__main__":
    sys.exit(main())
