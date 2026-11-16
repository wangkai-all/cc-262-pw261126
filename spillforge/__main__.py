"""Command-line entry point.

    python -m spillforge serve            # start the UI/API
    python -m spillforge selftest         # end-to-end checks in a temp dir
    python -m spillforge crash-test NAME  # crash at a fault point, then recover

All commands accept ``--data DIR`` and budget overrides so the tests and the
demo can run with a deliberately small disk budget.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from dataclasses import asdict, replace
from pathlib import Path

from .config import Config, load_config_from_env


def _config(args) -> Config:
    base = load_config_from_env(Path(args.data)) if args.data else load_config_from_env()
    overrides = {}
    if args.disk_budget is not None:
        overrides["disk_budget_bytes"] = args.disk_budget
    if args.memory_budget is not None:
        overrides["memory_budget_bytes"] = args.memory_budget
    if args.policy:
        overrides["scheduler_policy"] = args.policy
    if overrides:
        base = replace(base, **overrides)
    base.ensure_dirs()
    base.validate()
    return base


def cmd_serve(args) -> int:
    from .server import run_forever

    cfg = _config(args)
    if args.port is not None:
        cfg = replace(cfg, port=args.port)
    run_forever(cfg)
    return 0


def cmd_selftest(args) -> int:
    from .selftest import run_selftest

    return 0 if run_selftest(verbose=True) else 1


def cmd_crash_test(args) -> int:
    from .selftest import crash_case

    ok = crash_case(args.point, verbose=True)
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="spillforge")
    parser.add_argument("command", choices=["serve", "selftest", "crash-test"])
    parser.add_argument("point", nargs="?", default="mid_merge")
    parser.add_argument("--data", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--disk-budget", type=int, default=None)
    parser.add_argument("--memory-budget", type=int, default=None)
    parser.add_argument("--policy", default=None)
    args = parser.parse_args(argv)
    if args.command == "serve":
        return cmd_serve(args)
    if args.command == "selftest":
        return cmd_selftest(args)
    return cmd_crash_test(args)


if __name__ == "__main__":
    sys.exit(main())
