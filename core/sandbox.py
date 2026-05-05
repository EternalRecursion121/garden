"""Per-call bwrap sandboxing for agents.

Sandboxing is **on by default** for every agent. To opt out, declare:

    [agent.sandbox]
    enabled = false

To configure (defaults shown):

    [agent.sandbox]
    enabled         = true
    network         = false                       # off by default
    env_passthrough = ["ANTHROPIC_API_KEY"]       # which env vars to forward
    extra_ro_binds  = ["/some/host/path"]
    extra_rw_binds  = []
    timeout         = 300                         # seconds; 0 disables

Each call spawns a fresh `bwrap` invocation that:
  * binds garden's `core/` and `utils/` read-only,
  * binds the agent's own folder read-only,
  * gives the agent a private writable scratch dir under `data/sandbox/<name>/`,
  * binds `data/.carry/` writable so carry still works,
  * binds *only* the specific files in `/etc` it needs (resolv.conf, hosts,
    ssl/ca-certificates, passwd, group, nsswitch.conf), not all of `/etc`,
  * does **not** bind `/run` (which would expose user runtime sockets like
    ssh-agent, dbus, gpg-agent) — instead binds only
    `/run/systemd/resolve` if it exists, for systemd-resolved DNS,
  * unshares pid/uts/ipc, optionally network,
  * **clears env** and re-injects only PATH/HOME/LANG plus any vars listed in
    `env_passthrough`,
  * runs Python (`sys.executable`) inside, executing `core.sandbox`'s harness.

The harness imports the impl, calls it with params, and writes the JSON
result back over a dedicated fd. We dup fd 1 before user code runs and
redirect stdout to stderr, so user `print()`s can't corrupt the result
envelope. Inside the sandbox, `ctx.call` is disabled (we'd need a stdin
RPC channel back to the parent) and `ctx.service(...)` returns None.

Constraints implied by this:
  * sandboxed functions are leaves — they don't fan out via ctx.call/map.
  * sandboxed functions get no live discord/etc service handles.
  * carry is the one shared write surface (CRDT-merge-safe by design).
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


# Bind targets that are obviously sensitive. We refuse to honour an agent's
# request to bind these — the manifest is self-declared, so without a check
# the sandbox is whatever the agent says.
_FORBIDDEN_BINDS: frozenset[str] = frozenset({
    "/", "/boot", "/etc", "/root", "/home", "/var", "/run",
    "/usr", "/lib", "/lib64", "/bin", "/sbin", "/proc", "/sys", "/dev",
})


def _validate_extra_bind(p: str, kind: str) -> str:
    if not p:
        raise ValueError(f"{kind} entry is empty")
    if not os.path.isabs(p):
        raise ValueError(f"{kind} {p!r} must be absolute")
    if ".." in Path(p).parts:
        raise ValueError(f"{kind} {p!r} must not contain '..'")
    if p.startswith("--"):
        raise ValueError(f"{kind} {p!r} cannot start with '--'")
    norm = os.path.normpath(p)
    if norm in _FORBIDDEN_BINDS:
        raise ValueError(
            f"{kind} {p!r} targets a system path; refuse to bind it. "
            f"Pick a more specific subdir."
        )
    return norm


@dataclass
class SandboxConfig:
    enabled: bool = True
    # Network is OFF by default: the sandbox should not phone home unless the
    # agent explicitly asks for it. Combined with --clearenv, this is what
    # actually contains a prompt-injected agent.
    network: bool = False
    # Env vars to forward into the sandbox after --clearenv. Default is empty
    # — agents must declare what they need (e.g. ANTHROPIC_API_KEY).
    env_passthrough: list[str] = field(default_factory=list)
    extra_ro_binds: list[str] = field(default_factory=list)
    extra_rw_binds: list[str] = field(default_factory=list)
    # Hard cap on a single sandboxed call. 0 disables.
    timeout: float = 300.0

    @classmethod
    def parse(cls, table: Any) -> "SandboxConfig":
        """Parse the `[agent.sandbox]` table from a manifest. Missing or empty
        ⇒ default config (which is *enabled*). Explicit `enabled = false`
        returns a disabled config; the caller checks `.enabled`."""
        if table is None:
            return cls()
        if not isinstance(table, dict):
            raise ValueError("[agent.sandbox] must be a table")
        return cls(
            enabled=bool(table.get("enabled", True)),
            network=bool(table.get("network", False)),
            env_passthrough=[str(s) for s in table.get("env_passthrough", [])],
            extra_ro_binds=[
                _validate_extra_bind(str(p), "extra_ro_binds")
                for p in table.get("extra_ro_binds", [])
            ],
            extra_rw_binds=[
                _validate_extra_bind(str(p), "extra_rw_binds")
                for p in table.get("extra_rw_binds", [])
            ],
            timeout=float(table.get("timeout", 300.0)),
        )


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


# Files in /etc the sandbox actually needs. Anything not in this list stays
# invisible. Add new entries here explicitly when you need them.
_ETC_FILES = (
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/nsswitch.conf",
    "/etc/passwd",
    "/etc/group",
    "/etc/ssl",
    "/etc/ca-certificates",
    "/etc/ca-certificates.conf",
    "/etc/pki",
)


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
        # Scrub env first; we re-inject a minimal set further down.
        "--clearenv",
        # Kernel mounts first — order matters: a later --tmpfs /tmp would
        # shadow earlier binds under /tmp, so we set these up before any
        # binds that might happen to live under them.
        "--proc", "/proc",
        "--dev", "/dev",
        "--tmpfs", "/tmp",
    ]
    # System libraries / interpreters. Note: we do NOT bind /etc wholesale.
    argv += ["--ro-bind", "/usr", "/usr"]
    for lib in ("/lib", "/lib64", "/bin", "/sbin"):
        if Path(lib).exists():
            argv += ["--ro-bind", lib, lib]

    # Just the /etc files we actually need — passwd/group for name lookups,
    # resolv.conf/hosts/nsswitch for DNS, ssl/ca-certificates for TLS.
    for path in _ETC_FILES:
        if Path(path).exists():
            argv += ["--ro-bind", path, path]

    # systemd-resolved hosts symlink resolv.conf into /run/systemd/resolve;
    # bind ONLY that subdir, never /run as a whole (which would expose
    # /run/user/$UID/{ssh-agent,gpg-agent,dbus,keyring,...}).
    sd_resolve = Path("/run/systemd/resolve")
    if sd_resolve.exists():
        argv += ["--ro-bind", str(sd_resolve), str(sd_resolve)]

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

    # User-specified extras (already validated in SandboxConfig.parse).
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

    # Re-inject a minimal, predictable env after --clearenv. Sandboxed code
    # gets only the basics plus whatever the manifest declares.
    argv += [
        "--setenv", "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "--setenv", "HOME", str(scratch_dir),
        "--setenv", "LANG", "C.UTF-8",
        "--setenv", "PYTHONPATH", str(garden_root),
        "--setenv", "GARDEN_ROOT", str(garden_root),
        "--setenv", "GARDEN_AGENT_SCRATCH", str(scratch_dir),
        # Pin git's global config to the scratch dir even when the manifest
        # passes HOME through (which agents do so claude code finds ~/.claude).
        # Without this, git would look at /root/.gitconfig — which isn't bound
        # — and commits would fail with "please tell me who you are".
        "--setenv", "GIT_CONFIG_GLOBAL", str(scratch_dir / ".gitconfig"),
    ]
    for name in cfg.env_passthrough:
        val = os.environ.get(name)
        if val is not None:
            argv += ["--setenv", name, val]

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
    agent_root = manifest.folder.resolve()
    impl_path = (agent_root / impl_path_part).resolve()
    # Refuse path-traversal: the impl must live inside the agent's folder.
    try:
        impl_path.relative_to(agent_root)
    except ValueError:
        raise ValueError(
            f"impl {fn.impl!r} resolves to {impl_path}, outside agent folder {agent_root}"
        ) from None

    scratch_dir = garden_root / "data" / "sandbox" / manifest.name
    # Per-function `timeout` overrides agent-level `[agent.sandbox].timeout`.
    timeout = fn.timeout if fn.timeout is not None else (cfg.timeout if cfg.timeout > 0 else None)

    def impl(params: dict, ctx) -> Any:
        bwrap_argv = build_bwrap_argv(garden_root, agent_root, scratch_dir, cfg)
        cmd = bwrap_argv + [
            sys.executable,
            "-m", "core.sandbox",
            "--impl", f"{impl_path}:{func_name}",
        ]
        payload = json.dumps({
            "params": params,
            "ctx": {
                "agent": manifest.name,
                "run_id": ctx.run_id,
                "parent_run_id": ctx.parent_run_id,
                "scope": ctx.scope,
                "depth": ctx.depth,
            },
        })
        try:
            proc = subprocess.run(
                cmd, input=payload, capture_output=True, text=True,
                check=False, timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"sandbox timed out after {timeout}s") from e
        if proc.returncode != 0:
            raise RuntimeError(
                f"sandbox exited {proc.returncode}: {proc.stderr.strip()}"
            )
        try:
            envelope = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"sandbox produced non-JSON output: {e}\n"
                f"stdout: {proc.stdout!r}\nstderr: {proc.stderr!r}"
            ) from e
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
    agent_folder_abs = manifest.folder.resolve()
    # Per-function `timeout` overrides agent-level `[agent.sandbox].timeout`.
    timeout = fn.timeout if fn.timeout is not None else (cfg.timeout if cfg.timeout > 0 else None)

    def impl(params: dict, ctx) -> Any:
        bwrap_argv = build_bwrap_argv(garden_root, agent_folder_abs, scratch_dir, cfg)
        payload = json.dumps({
            "params": params,
            "ctx": {
                "agent": manifest.name,
                "run_id": ctx.run_id,
                "parent_run_id": ctx.parent_run_id,
                "scope": ctx.scope,
            },
        })
        try:
            proc = subprocess.run(
                bwrap_argv + user_cmd,
                input=payload,
                capture_output=True, text=True, cwd=str(manifest.folder),
                check=False, timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"sandbox command timed out after {timeout}s") from e
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

    def __init__(
        self,
        agent: str,
        run_id: str,
        parent_run_id: str | None,
        scope: str | None,
        depth: int,
    ):
        self.agent = agent
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

    def list_functions(self, agent: str | None = None) -> list[dict]:
        # No registry IPC inside the sandbox.
        return []


def _runner_main() -> int:
    """Entry point for `python -m core.sandbox`. Reads JSON from stdin,
    invokes the requested impl, writes a JSON envelope to a dedicated fd.

    To prevent user `print()`s in the impl from corrupting the JSON output,
    we save fd 1 into `result_fd` first, then redirect stdout to stderr.
    User prints flow to the parent's captured stderr (visible for debugging);
    only the harness writes the envelope, and only via `result_fd`.
    """
    import argparse, importlib.util

    # Hijack stdout BEFORE anything user-controlled runs.
    result_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = os.fdopen(1, "w", buffering=1)

    def emit(envelope: dict[str, Any]) -> None:
        os.write(result_fd, json.dumps(envelope, default=str).encode())

    def emit_error(typ: str, msg: str, tb: str) -> None:
        emit({"error": {"type": typ, "message": msg, "traceback": tb}})

    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", required=True, help="absolute/path.py:function")
    args = parser.parse_args()

    raw = sys.stdin.read()
    try:
        msg = json.loads(raw or "{}")
    except json.JSONDecodeError as e:
        emit_error("BadInput", f"could not parse stdin: {e}", "")
        return 2

    params = msg.get("params", {})
    ctx_d = msg.get("ctx", {})
    ctx = _SandboxedContext(
        agent=ctx_d.get("agent", ""),
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
        emit_error("FileNotFoundError", f"impl not found: {impl_path}", "")
        return 2

    sys.path.insert(0, str(impl_path.parent))

    spec_name = f"_garden_sandbox_{impl_path.stem}"
    spec = importlib.util.spec_from_file_location(spec_name, impl_path)
    if spec is None or spec.loader is None:
        emit_error("ImportError", f"cannot load {impl_path}", "")
        return 2
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        emit_error(type(e).__name__, str(e), traceback.format_exc())
        return 1
    if not hasattr(mod, func_name):
        emit_error("AttributeError", f"{impl_path} has no function {func_name!r}", "")
        return 2

    try:
        result = getattr(mod, func_name)(params, ctx)
    except Exception as e:
        emit_error(type(e).__name__, str(e), traceback.format_exc())
        return 1

    emit({"result": result})
    return 0


if __name__ == "__main__":
    sys.exit(_runner_main())
