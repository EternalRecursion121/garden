"""Garden CLI.

Subcommands:
  garden init                  — initialize the carry repo at data/.carry
  garden list                  — list registered agents and functions
  garden run <agent>.<fn>      — invoke a function (with --params JSON)
  garden schedule              — start the cron loop
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
from pathlib import Path

from utils.carry import Carry

from .dispatcher import Dispatcher
from .registry import Registry
from .scheduler import Scheduler


GARDEN_ROOT = Path(__file__).resolve().parent.parent


def _load_config() -> dict:
    cfg_path = GARDEN_ROOT / "garden.toml"
    if not cfg_path.exists():
        return {}
    with open(cfg_path, "rb") as f:
        return tomllib.load(f)


def _make_dispatcher() -> Dispatcher:
    cfg = _load_config()
    repo = GARDEN_ROOT / cfg.get("carry", {}).get("repo", "data")
    registry = Registry(GARDEN_ROOT / "agents")
    carry = Carry(repo)
    return Dispatcher(registry, carry)


def cmd_init(args: argparse.Namespace) -> int:
    cfg = _load_config()
    repo = GARDEN_ROOT / cfg.get("carry", {}).get("repo", "data")
    carry = Carry(repo)
    if not carry.available():
        print(
            "carry CLI not found. Install from https://github.com/tonk-labs/carry",
            file=sys.stderr,
        )
        return 1
    if carry.initialized():
        print(f"carry repo already initialized at {repo}/.carry")
        return 0
    carry.init("garden")
    print(f"initialized carry repo at {repo}/.carry")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    registry = Registry(GARDEN_ROOT / "agents")
    if not registry.agents:
        print("(no agents registered)")
        return 0
    for name, m in sorted(registry.agents.items()):
        desc = f" — {m.description}" if m.description else ""
        print(f"{name}{desc}")
        for fn in m.functions.values():
            sched = f"  [cron: {fn.schedule}]" if fn.schedule else ""
            params = ", ".join(f"{k}: {v}" for k, v in fn.params.items())
            params_str = f"({params})" if params else "()"
            fdesc = f" — {fn.description}" if fn.description else ""
            print(f"    .{fn.name}{params_str}{sched}{fdesc}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    dispatcher = _make_dispatcher()
    params = json.loads(args.params) if args.params else {}
    result = dispatcher.call(args.qualified, params=params)
    if result is not None:
        print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_schedule(args: argparse.Namespace) -> int:
    cfg = _load_config()
    poll = args.poll if args.poll is not None else cfg.get("scheduler", {}).get(
        "poll_interval", 30
    )
    dispatcher = _make_dispatcher()
    Scheduler(dispatcher, poll_interval=float(poll)).run()
    return 0


def cmd_gateway(args: argparse.Namespace) -> int:
    cfg = _load_config().get("gateway", {}).get(args.kind, {})
    dispatcher = _make_dispatcher()
    if args.kind == "discord":
        from .gateways.discord import from_config

        from_config(dispatcher, cfg).run()
        return 0
    print(f"unknown gateway: {args.kind!r}", file=sys.stderr)
    return 2


def main() -> None:
    p = argparse.ArgumentParser(prog="garden")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="initialize the shared carry repo")
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("list", help="list agents and their functions")
    s.set_defaults(func=cmd_list)

    s = sub.add_parser("run", help="invoke a function: <agent>.<function>")
    s.add_argument("qualified")
    s.add_argument("--params", "-p", default="", help="JSON params object")
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("schedule", help="run the cron loop")
    s.add_argument("--poll", type=float, default=None, help="seconds between polls")
    s.set_defaults(func=cmd_schedule)

    s = sub.add_parser("gateway", help="run an event gateway: <kind>")
    s.add_argument("kind", choices=["discord"])
    s.set_defaults(func=cmd_gateway)

    args = p.parse_args()
    sys.exit(args.func(args) or 0)


if __name__ == "__main__":
    main()
