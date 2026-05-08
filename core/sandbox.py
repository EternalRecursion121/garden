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
    can_call        = ["kira.consult", "tilth.*"] # see "Cross-agent calls"

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

The harness imports the impl, calls it with params, and exchanges JSON-RPC
messages with the parent over stdin/stdout. We dup fd 1 before user code
runs and redirect stdout to stderr, so user `print()`s can't corrupt the
RPC stream — only the harness writes to the saved fd, and only as
newline-delimited JSON. `ctx.service(...)` still returns None inside the
sandbox (live service handles aren't proxyable).

Cross-agent calls
-----------------
`ctx.call` and `ctx.map` work inside the sandbox via RPC back to the
parent dispatcher; the dispatched call runs unsandboxed in the parent's
process (or sandboxed if the *target* agent declares it). `ctx.list_functions`
also RPCs back to read the live registry. Parallel `ctx.map` requests
serialise on the RPC channel — the call still happens, just one at a time.

Sandboxed agents can restrict which siblings they're allowed to call:

    [agent.sandbox]
    can_call = ["kira.consult", "tilth.*"]   # only these targets
    can_call = []                            # deny all cross-agent calls
    # (field absent)                         # allow all (default)

Patterns are exact `agent.fn` or `agent.*` (any function in that agent).

Constraints that still apply:
  * sandboxed functions get no live discord/etc service handles
    (`ctx.service(...)` returns None).
  * carry is still the shared write surface; cross-agent calls add
    request/response RPC on top.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


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


def _validate_can_call_pattern(p: str) -> str:
    if not isinstance(p, str) or not p:
        raise ValueError("can_call entry must be a non-empty string")
    # Either "agent.fn" or "agent.*". Reject anything else.
    parts = p.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"can_call {p!r} must be 'agent.fn' or 'agent.*'"
        )
    return p


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
    # Cross-agent call allowlist. None = unrestricted (default); [] = deny
    # all; ["kira.consult", "tilth.*"] = only those targets. Patterns are
    # exact qualified names or "agent.*" globs. Enforced by the parent
    # dispatcher when a sandboxed function issues an RPC `call` request.
    can_call: Optional[list[str]] = None

    @classmethod
    def parse(cls, table: Any) -> "SandboxConfig":
        """Parse the `[agent.sandbox]` table from a manifest. Missing or empty
        ⇒ default config (which is *enabled*). Explicit `enabled = false`
        returns a disabled config; the caller checks `.enabled`."""
        if table is None:
            return cls()
        if not isinstance(table, dict):
            raise ValueError("[agent.sandbox] must be a table")
        # `can_call` is None when absent, list (possibly empty) when set.
        # This three-state distinction matters: missing = allow all,
        # `can_call = []` = deny all.
        raw_can_call = table.get("can_call", None)
        if raw_can_call is None:
            can_call: Optional[list[str]] = None
        else:
            if not isinstance(raw_can_call, list):
                raise ValueError("can_call must be a list of patterns")
            can_call = [_validate_can_call_pattern(str(s)) for s in raw_can_call]
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
            can_call=can_call,
        )


def can_call_matches(can_call: Optional[list[str]], qualified: str) -> bool:
    """Check whether `qualified` is permitted under a `can_call` allowlist.

    None  → unrestricted (allow). [] → deny everything. Otherwise the
    qualified name must match an exact entry or an "agent.*" glob.
    """
    if can_call is None:
        return True
    for pattern in can_call:
        if pattern == qualified:
            return True
        if pattern.endswith(".*") and qualified.startswith(pattern[:-1]):
            return True
    return False


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

    # Runtime docs, if present. Optional — gardens without `docs/` skip it.
    # Bound RO so any agent (not just `garden.help`) can read reference
    # material at runtime without copying it into its own folder.
    docs_dir = garden_root / "docs"
    if docs_dir.is_dir():
        argv += ["--ro-bind", str(docs_dir), str(docs_dir)]

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

    can_call = cfg.can_call

    def impl(params: dict, ctx) -> Any:
        bwrap_argv = build_bwrap_argv(garden_root, agent_root, scratch_dir, cfg)
        cmd = bwrap_argv + [
            sys.executable,
            "-m", "core.sandbox",
            "--impl", f"{impl_path}:{func_name}",
            "--agent-root", str(agent_root),
        ]
        envelope = json.dumps({
            "params": params,
            "ctx": {
                "agent": manifest.name,
                "run_id": ctx.run_id,
                "parent_run_id": ctx.parent_run_id,
                "scope": ctx.scope,
                "depth": ctx.depth,
            },
        })

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered
        )

        # Drain stderr in a thread so the child can never block on a full
        # pipe. We accumulate it for inclusion in error messages.
        stderr_chunks: list[str] = []

        def drain_stderr() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                stderr_chunks.append(line)

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()

        deadline = time.monotonic() + timeout if timeout else None

        def _kill_and_raise(reason: str) -> "RuntimeError":
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                pass
            stderr_thread.join(timeout=1)
            return RuntimeError(reason)

        # Send the initial envelope.
        assert proc.stdin is not None and proc.stdout is not None
        try:
            proc.stdin.write(envelope + "\n")
            proc.stdin.flush()
        except BrokenPipeError as e:
            raise _kill_and_raise(
                f"sandbox closed stdin before reading envelope: stderr={''.join(stderr_chunks)!r}"
            ) from e

        final: dict[str, Any] | None = None
        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise _kill_and_raise(f"sandbox timed out after {timeout}s")

            line = proc.stdout.readline()
            if not line:
                # Child closed the channel without a final result.
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                # Stray non-JSON line on the result fd shouldn't happen — the
                # harness redirects user prints to stderr — but be defensive.
                continue

            op = msg.get("op")
            if op == "result":
                final = {"result": msg.get("result")}
                break
            if op == "error":
                final = {"error": msg.get("error", {})}
                break

            # RPC requests from the child.
            req_id = msg.get("id")
            if op == "call":
                qualified = msg.get("qualified", "")
                call_params = msg.get("params") or {}
                call_scope = msg.get("scope")
                if not can_call_matches(can_call, qualified):
                    resp: dict[str, Any] = {
                        "id": req_id,
                        "ok": False,
                        "error": {
                            "type": "PermissionError",
                            "message": (
                                f"sandboxed agent {manifest.name!r} not permitted "
                                f"to call {qualified!r}; add it to "
                                f"[agent.sandbox] can_call to allow."
                            ),
                        },
                    }
                else:
                    try:
                        value = ctx.call(qualified, call_params, scope=call_scope)
                        resp = {"id": req_id, "ok": True, "value": value}
                    except Exception as e:
                        resp = {
                            "id": req_id,
                            "ok": False,
                            "error": {"type": type(e).__name__, "message": str(e)},
                        }
            elif op == "list_functions":
                try:
                    value = ctx.list_functions(msg.get("agent"))
                    resp = {"id": req_id, "ok": True, "value": value}
                except Exception as e:
                    resp = {
                        "id": req_id,
                        "ok": False,
                        "error": {"type": type(e).__name__, "message": str(e)},
                    }
            elif op == "list_agents":
                try:
                    value = ctx.list_agents()
                    resp = {"id": req_id, "ok": True, "value": value}
                except Exception as e:
                    resp = {
                        "id": req_id,
                        "ok": False,
                        "error": {"type": type(e).__name__, "message": str(e)},
                    }
            else:
                resp = {
                    "id": req_id,
                    "ok": False,
                    "error": {"type": "ProtocolError", "message": f"unknown op {op!r}"},
                }

            try:
                proc.stdin.write(json.dumps(resp, default=str) + "\n")
                proc.stdin.flush()
            except BrokenPipeError:
                # Child died between request and response; loop will exit
                # on next readline returning empty.
                break

        # Wait for child to exit and stderr drain to finish.
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            raise _kill_and_raise(
                f"sandbox didn't exit after final result: "
                f"stderr={''.join(stderr_chunks)!r}"
            )
        stderr_thread.join(timeout=1)

        stderr_text = "".join(stderr_chunks).strip()

        if final and "error" in final:
            err = final["error"] or {}
            raise RuntimeError(
                f"{err.get('type', 'SandboxError')}: {err.get('message', '')}\n"
                f"{err.get('traceback', '')}"
            )
        if proc.returncode != 0:
            raise RuntimeError(
                f"sandbox exited {proc.returncode}: stderr={stderr_text!r}"
            )
        if final is None:
            raise RuntimeError(
                f"sandbox produced no result envelope: stderr={stderr_text!r}"
            )
        return final.get("result")

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


class _SandboxRPCError(RuntimeError):
    """Raised when an RPC the child issued fails on the parent side."""


class _SandboxRPCClient:
    """Issues request/response RPCs from the sandboxed child to the parent.

    The protocol is newline-delimited JSON:
      child → parent (via the saved fd 1):  {"op": "...", "id": "...", ...}
      parent → child (via stdin):           {"id": "...", "ok": bool, ...}

    A lock serialises concurrent callers (e.g. ctx.map fan-out): the
    channel is single-stream, so requests run one at a time. That's
    fine for correctness — the parent still parallelises if it wants to.
    """

    def __init__(self, write_line: Callable[[dict], None], stdin) -> None:
        self._write_line = write_line
        self._stdin = stdin
        self._next_id = 0
        self._lock = threading.Lock()

    def request(self, op: str, **payload: Any) -> Any:
        with self._lock:
            self._next_id += 1
            req_id = str(self._next_id)
            self._write_line({"op": op, "id": req_id, **payload})
            line = self._stdin.readline()
            if not line:
                raise _SandboxRPCError(
                    "sandbox parent closed RPC channel before responding"
                )
            try:
                resp = json.loads(line)
            except json.JSONDecodeError as e:
                raise _SandboxRPCError(f"bad RPC response from parent: {e}")
            if resp.get("id") != req_id:
                raise _SandboxRPCError(
                    f"RPC id mismatch: got {resp.get('id')!r}, expected {req_id!r}"
                )
            if not resp.get("ok"):
                err = resp.get("error", {})
                raise _SandboxRPCError(
                    f"{err.get('type', 'CallError')}: {err.get('message', 'unknown')}"
                )
            return resp.get("value")


class _SandboxedContext:
    """Lightweight ctx exposed to functions inside the sandbox.

    `call`, `map`, and `list_functions` round-trip to the parent over
    stdin/stdout RPC. `service(...)` still returns None because live
    service handles (Discord client, etc.) aren't proxyable across the
    sandbox boundary.
    """

    def __init__(
        self,
        agent: str,
        run_id: str,
        parent_run_id: str | None,
        scope: str | None,
        depth: int,
        rpc: _SandboxRPCClient,
    ):
        self.agent = agent
        self.run_id = run_id
        self.parent_run_id = parent_run_id
        self.scope = scope
        self.depth = depth
        self._rpc = rpc

    def call(
        self,
        qualified: str,
        params: dict | None = None,
        scope: str | None = None,
    ) -> Any:
        return self._rpc.request(
            "call",
            qualified=qualified,
            params=params or {},
            scope=scope,
        )

    def map(
        self,
        qualified: str,
        params_list: list[dict],
        scope_fn: Callable[[dict], str] | None = None,
        max_workers: int = 8,  # noqa: ARG002 — kept for API parity
    ) -> list[Any]:
        # Sandbox RPC channel is single-stream; serialise here. The parent
        # could parallelise if it wanted, but unless we add request
        # multiplexing the wire is one-at-a-time.
        return [
            self.call(qualified, p, scope=(scope_fn(p) if scope_fn else None))
            for p in params_list
        ]

    def service(self, name: str):
        return None

    def list_functions(self, agent: str | None = None) -> list[dict]:
        return self._rpc.request("list_functions", agent=agent)

    def list_agents(self) -> list[dict]:
        return self._rpc.request("list_agents")


def _runner_main() -> int:
    """Entry point for `python -m core.sandbox`. Speaks JSON-RPC over
    stdin/stdout with the parent process.

    Wire protocol (newline-delimited JSON, one message per line):

        parent → child (stdin):
            first line:  {"params": {...}, "ctx": {...}}
            then:        {"id": "...", "ok": bool, "value": ...}   # RPC response

        child → parent (saved fd 1):
            during run:  {"op": "call"|"list_functions"|"list_agents", "id": "...", ...}
            at end:      {"op": "result", "result": ...}
                     or  {"op": "error", "error": {"type", "message", "traceback"}}

    Stdout fd 1 is dup'd into `result_fd` first, then fd 1 is repointed
    at fd 2 so user `print()` calls flow to stderr. Only this harness
    writes to `result_fd`, so the RPC stream stays clean.
    """
    import argparse, importlib.util

    # Hijack stdout BEFORE anything user-controlled runs.
    result_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = os.fdopen(1, "w", buffering=1)
    result_writer = os.fdopen(result_fd, "w", buffering=1)

    write_lock = threading.Lock()

    def write_line(msg: dict[str, Any]) -> None:
        # Single writer in practice (the impl thread), but guard anyway —
        # the RPC client's own lock already serialises requests, and the
        # final result/error emit happens after the impl returns.
        with write_lock:
            result_writer.write(json.dumps(msg, default=str) + "\n")
            result_writer.flush()

    def emit_error(typ: str, msg: str, tb: str) -> None:
        write_line({"op": "error", "error": {"type": typ, "message": msg, "traceback": tb}})

    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", required=True, help="absolute/path.py:function")
    parser.add_argument("--agent-root", default="", help="absolute agent folder")
    args = parser.parse_args()

    raw = sys.stdin.readline()
    try:
        msg = json.loads(raw or "{}")
    except json.JSONDecodeError as e:
        emit_error("BadInput", f"could not parse stdin envelope: {e}", "")
        return 2

    params = msg.get("params", {})
    ctx_d = msg.get("ctx", {})
    rpc = _SandboxRPCClient(write_line, sys.stdin)
    ctx = _SandboxedContext(
        agent=ctx_d.get("agent", ""),
        run_id=ctx_d.get("run_id", ""),
        parent_run_id=ctx_d.get("parent_run_id"),
        scope=ctx_d.get("scope"),
        depth=int(ctx_d.get("depth", 0)),
        rpc=rpc,
    )

    impl_arg = args.impl
    path_part, _, func_name = impl_arg.partition(":")
    func_name = func_name or "run"
    impl_path = Path(path_part)
    if not impl_path.exists():
        emit_error("FileNotFoundError", f"impl not found: {impl_path}", "")
        return 2

    if args.agent_root:
        sys.path.insert(0, args.agent_root)
    sys.path.insert(0, str(impl_path.parent))

    spec_name = f"_garden_sandbox_{impl_path.stem}"
    spec = importlib.util.spec_from_file_location(spec_name, impl_path)
    if spec is None:
        emit_error("ImportError", f"cannot load {impl_path}", "")
        return 2
    mod = importlib.util.module_from_spec(spec)
    try:
        source = impl_path.read_text(encoding="utf-8")
        exec(compile(source, str(impl_path), "exec"), mod.__dict__)
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

    write_line({"op": "result", "result": result})
    return 0


if __name__ == "__main__":
    sys.exit(_runner_main())
