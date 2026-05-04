"""Per-call bwrap sandboxing for agents.

An agent opts in via `[agent.sandbox]` in its manifest:

    [agent.sandbox]
    enabled = true
    network = true                       # default; set false for offline agents
    extra_ro_binds = ["/some/host/path"] # additional paths visible read-only
    extra_rw_binds = []                  # additional writable paths

Each call spawns a fresh `bwrap` invocation that:
  * binds garden's `core/` and `utils/` read-only,
  * binds the agent's own folder read-only,
  * gives the agent a private writable scratch dir under `data/sandbox/<name>/`,
  * binds `data/.carry/` writable so carry still works,
  * unshares pid/uts/ipc, optionally network,
  * runs Python (`sys.executable`) inside, executing `core.sandbox`'s harness.

The harness imports the impl, calls it with params, and writes the JSON
result back over stdout. Inside the sandbox, `ctx.call` is disabled (we'd
need a stdin RPC channel back to the parent) and `ctx.service(...)` returns
None (services live in the parent process).

Constraints implied by this:
  * sandboxed functions are leaves — they don't fan out via ctx.call/map.
  * sandboxed functions get no live discord/etc service handles.
  * carry is the one shared write surface (CRDT-merge-safe by design).

Usage from the dispatcher:

    if manifest.sandbox and manifest.sandbox.enabled:
        impl = make_sandboxed_python_impl(garden_root, manifest, fn, manifest.sandbox)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable


# --- config --------------------------------------------------------------


@dataclass
class SandboxConfig:
    enabled: bool = False
    network: bool = True
    extra_ro_binds: list[str] = field(default_factory=list)
    extra_rw_binds: list[str] = field(default_factory=list)

    @classmethod
    def parse(cls, table: Any) -> "SandboxConfig | None":
        """Parse the `[agent.sandbox]` table from a manifest. None if absent
        or disabled."""
        if not table:
            return None
        if not isinstance(table, dict):
            raise ValueError("[agent.sandbox] must be a table")
        cfg = cls(
            enabled=bool(table.get("enabled", True)),
            network=bool(table.get("network", True)),
            extra_ro_binds=[str(p) for p in table.get("extra_ro_binds", [])],
            extra_rw_binds=[str(p) for p in table.get("extra_rw_binds", [])],
        )
        return cfg if cfg.enabled else None


# --- argv builder --------------------------------------------------------


def _venv_root() -> Path | None:
    """If we're running inside a venv, return its root so we can bind it.

    We deliberately do NOT resolve symlinks: a venv's `bin/python` is
    typically a symlink to the real interpreter (e.g. /usr/bin/python3.12),
    and `.resolve()` would chase that and lose the venv path. We want the
    user-facing path so we can bind the venv into the sandbox.
    """
    exe = Path(sys.executable)
    candidate = exe.parent.parent  # .../venv/bin/python -> .../venv
    if (candidate / "pyvenv.cfg").exists():
        return candidate
    return None


def build_bwrap_argv(
    garden_root: Path,
    agent_folder: Path,
    scratch_dir: Path,
    cfg: SandboxConfig,
) -> list[str]:
    """Construct the `bwrap ...` prefix for a sandboxed call.

    Does not include the actual command being run — append it after.
    """
    if not shutil.which("bwrap"):
        raise RuntimeError(
            "bwrap not installed (apt install bubblewrap). "
            "Disable sandboxing or install it."
        )

    scratch_dir.mkdir(parents=True, exist_ok=True)
    carry_dir = garden_root / "data" / ".carry"

    argv: list[str] = [
        "bwrap",
        # Kernel mounts first — order matters: a later --tmpfs /tmp would
        # shadow earlier binds under /tmp, so we set these up before any
        # binds that might happen to live under them.
        "--proc", "/proc",
        "--dev", "/dev",
        "--tmpfs", "/tmp",
        # System libraries / interpreters
        "--ro-bind", "/usr", "/usr",
        "--ro-bind", "/etc", "/etc",
    ]
    for lib in ("/lib", "/lib64", "/bin", "/sbin"):
        if Path(lib).exists():
            argv += ["--ro-bind", lib, lib]

    # Garden runtime + this agent only
    argv += [
        "--ro-bind", str(garden_root / "core"), str(garden_root / "core"),
        "--ro-bind", str(garden_root / "utils"), str(garden_root / "utils"),
        "--ro-bind", str(agent_folder), str(agent_folder),
    ]

    # Writable: scratch + carry repo (if it exists)
    argv += ["--bind", str(scratch_dir), str(scratch_dir)]
    if carry_dir.exists():
        argv += ["--bind", str(carry_dir), str(carry_dir)]

    # Venv (if we're in one)
    venv = _venv_root()
    if venv is not None and venv != garden_root:
        argv += ["--ro-bind", str(venv), str(venv)]

    # User-specified extras
    for p in cfg.extra_ro_binds:
        argv += ["--ro-bind", p, p]
    for p in cfg.extra_rw_binds:
        argv += ["--bind", p, p]

    # Namespaces
    argv += [
        "--unshare-pid",
        "--unshare-uts",
        "--unshare-ipc",
        "--die-with-parent",
        "--new-session",
    ]
    if not cfg.network:
        argv.append("--unshare-net")

    # Make sure PYTHONPATH lets the harness find core.* and utils.*
    argv += [
        "--setenv", "PYTHONPATH", str(garden_root),
        "--setenv", "GARDEN_ROOT", str(garden_root),
        "--setenv", "GARDEN_AGENT_SCRATCH", str(scratch_dir),
    ]

    return argv


# --- dispatcher integration ---------------------------------------------


def make_sandboxed_python_impl(
    garden_root: Path,
    manifest,  # AgentManifest, but importing would be circular
    fn,        # FunctionDef
    cfg: SandboxConfig,
) -> Callable[..., Any]:
    """Return an impl that runs a Python function inside bwrap by spawning
    `python -m core.sandbox <impl_path>:<func>` per call."""
    assert fn.impl is not None
    impl_path_part, _, func_name = fn.impl.partition(":")
    func_name = func_name or "run"
    impl_path = (manifest.folder / impl_path_part).resolve()

    scratch_dir = garden_root / "data" / "sandbox" / manifest.name

    def impl(params: dict, ctx) -> Any:
        bwrap_argv = build_bwrap_argv(garden_root, manifest.folder, scratch_dir, cfg)
        cmd = bwrap_argv + [
            sys.executable,
            "-m", "core.sandbox",
            "--impl", f"{impl_path}:{func_name}",
        ]
        payload = json.dumps({
            "params": params,
            "ctx": {
                "run_id": ctx.run_id,
                "parent_run_id": ctx.parent_run_id,
                "scope": ctx.scope,
                "depth": ctx.depth,
            },
        })
        proc = subprocess.run(
            cmd, input=payload, capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"sandbox exited {proc.returncode}: {proc.stderr.strip()}"
            )
        try:
            envelope = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"sandbox produced non-JSON output: {e}\n{proc.stdout}") from e
        if envelope.get("error"):
            raise RuntimeError(
                f"{envelope['error']['type']}: {envelope['error']['message']}\n"
                f"{envelope['error'].get('traceback', '')}"
            )
        return envelope.get("result")

    return impl


def make_sandboxed_command_impl(
    garden_root: Path,
    manifest,
    fn,
    cfg: SandboxConfig,
) -> Callable[..., Any]:
    """Return an impl that runs an arbitrary `command = [...]` inside bwrap."""
    assert fn.command is not None
    user_cmd = list(fn.command)
    scratch_dir = garden_root / "data" / "sandbox" / manifest.name

    def impl(params: dict, ctx) -> Any:
        bwrap_argv = build_bwrap_argv(garden_root, manifest.folder, scratch_dir, cfg)
        payload = json.dumps({
            "params": params,
            "ctx": {
                "run_id": ctx.run_id,
                "parent_run_id": ctx.parent_run_id,
                "scope": ctx.scope,
            },
        })
        proc = subprocess.run(
            bwrap_argv + user_cmd,
            input=payload,
            capture_output=True, text=True, cwd=str(manifest.folder),
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"sandbox command exited {proc.returncode}: {proc.stderr}")
        out = proc.stdout.strip()
        if not out:
            return None
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            return out

    return impl


# --- in-sandbox harness --------------------------------------------------


class _SandboxedContext:
    """Lightweight ctx exposed to functions inside the sandbox.

    Cross-agent dispatch and live services are unavailable here — those
    require IPC back to the parent process which we deliberately don't
    plumb in v1. Functions that need them shouldn't be sandboxed.
    """

    def __init__(self, run_id: str, parent_run_id: str | None, scope: str | None, depth: int):
        self.run_id = run_id
        self.parent_run_id = parent_run_id
        self.scope = scope
        self.depth = depth

    def call(self, *args, **kwargs):
        raise RuntimeError(
            "ctx.call() is unavailable inside a sandboxed function. "
            "Disable sandboxing for this agent or split the function."
        )

    def map(self, *args, **kwargs):
        raise RuntimeError(
            "ctx.map() is unavailable inside a sandboxed function."
        )

    def service(self, name: str):
        return None


def _runner_main() -> int:
    """Entry point for `python -m core.sandbox`. Reads JSON from stdin,
    invokes the requested impl, writes a JSON envelope to stdout."""
    import argparse, importlib.util

    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", required=True, help="absolute/path.py:function")
    args = parser.parse_args()

    raw = sys.stdin.read()
    try:
        msg = json.loads(raw or "{}")
    except json.JSONDecodeError as e:
        _emit_error("BadInput", f"could not parse stdin: {e}", "")
        return 2

    params = msg.get("params", {})
    ctx_d = msg.get("ctx", {})
    ctx = _SandboxedContext(
        run_id=ctx_d.get("run_id", ""),
        parent_run_id=ctx_d.get("parent_run_id"),
        scope=ctx_d.get("scope"),
        depth=int(ctx_d.get("depth", 0)),
    )

    impl_arg = args.impl
    path_part, _, func_name = impl_arg.partition(":")
    func_name = func_name or "run"
    impl_path = Path(path_part)
    if not impl_path.exists():
        _emit_error("FileNotFoundError", f"impl not found: {impl_path}", "")
        return 2

    # Make the agent folder importable so impls can import siblings.
    sys.path.insert(0, str(impl_path.parent))

    spec_name = f"_garden_sandbox_{impl_path.stem}"
    spec = importlib.util.spec_from_file_location(spec_name, impl_path)
    if spec is None or spec.loader is None:
        _emit_error("ImportError", f"cannot load {impl_path}", "")
        return 2
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        _emit_error(type(e).__name__, str(e), traceback.format_exc())
        return 1
    if not hasattr(mod, func_name):
        _emit_error("AttributeError", f"{impl_path} has no function {func_name!r}", "")
        return 2

    try:
        result = getattr(mod, func_name)(params, ctx)
    except Exception as e:
        _emit_error(type(e).__name__, str(e), traceback.format_exc())
        return 1

    sys.stdout.write(json.dumps({"result": result}, default=str))
    sys.stdout.flush()
    return 0


def _emit_error(typ: str, msg: str, tb: str) -> None:
    sys.stdout.write(json.dumps({
        "error": {"type": typ, "message": msg, "traceback": tb}
    }))
    sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(_runner_main())
