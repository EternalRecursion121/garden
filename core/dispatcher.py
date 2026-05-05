"""Resolves and invokes functions; tracks runs in carry.

The dispatcher's job is small on purpose:
  1. resolve <agent>.<function>
  2. validate required params are present
  3. open a `garden.run` claim
  4. load the impl (Python module or subprocess) and call it
  5. close the run with status + result, or status + error

It does not materialise prompts or pick models — those belong inside the
function.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from utils.carry import Carry, CarryError
from utils.context import Context

from .manifest import AgentManifest, FunctionDef
from .registry import Registry


class Dispatcher:
    def __init__(self, registry: Registry, carry: Carry, *, log: bool = True):
        self.registry = registry
        self.carry = carry
        self.log = log
        self._impl_cache: dict[str, tuple[tuple[Any, ...], Callable[..., Any]]] = {}
        self._impl_cache_lock = threading.Lock()
        # Outbound services attached by long-running processes (gateways).
        # Functions reach them via ctx.service(name); inbound and outbound
        # are independent — a function subscribed to a Discord channel does
        # not auto-emit through Discord.
        self.services: dict[str, Any] = {}
        # Optional hook invoked after every call() completes (success or
        # failure). Signature: (event: dict) -> None. Long-running processes
        # (e.g. the Discord gateway) wire this to mirror the carry run-log
        # to a side channel. Failures inside the hook are caught so a broken
        # audit pipeline can't break dispatch.
        self.audit_hook: Optional[Callable[[dict], None]] = None

    # public entrypoint --------------------------------------------------

    def call(
        self,
        qualified: str,
        params: dict | None = None,
        *,
        parent_ctx: Optional[Context] = None,
        scope: Optional[str] = None,
    ) -> Any:
        manifest, fn = self.registry.lookup(qualified)
        params = params or {}
        self._validate_params(qualified, fn, params)

        run_id = uuid.uuid4().hex
        parent_run_id = parent_ctx.run_id if parent_ctx else None
        depth = (parent_ctx.depth + 1) if parent_ctx else 0
        indent = "  " * depth
        started = time.time()

        if self.log:
            print(f"{indent}→ {qualified} [{run_id[:8]}]", flush=True)
        self._record_run_start(run_id, manifest, fn, params, parent_run_id, scope, started)

        ctx = Context(
            run_id=run_id,
            parent_run_id=parent_run_id,
            scope=scope,
            agent=manifest.name,
            depth=depth,
            _dispatch=self._dispatch_from_ctx,
            _services=self.services,
            _list_functions=self._list_functions_from_ctx,
        )

        try:
            impl = self._load_impl(manifest, fn)
            result = impl(params, ctx)
            duration = time.time() - started
            self._record_run_end(run_id, "ok", duration, result=result)
            if self.log:
                print(f"{indent}✓ {qualified} [{run_id[:8]}] in {duration:.2f}s", flush=True)
            self._fire_audit({
                "qualified": qualified, "run_id": run_id, "parent_run_id": parent_run_id,
                "depth": depth, "scope": scope, "status": "ok", "duration": duration,
                "params": params, "result": result, "error": None,
            })
            return result
        except Exception as e:
            duration = time.time() - started
            self._record_run_end(
                run_id, "error", duration,
                error=f"{type(e).__name__}: {e}",
                traceback=traceback.format_exc(),
            )
            if self.log:
                print(f"{indent}✗ {qualified} [{run_id[:8]}]: {e}", flush=True)
            self._fire_audit({
                "qualified": qualified, "run_id": run_id, "parent_run_id": parent_run_id,
                "depth": depth, "scope": scope, "status": "error", "duration": duration,
                "params": params, "result": None, "error": f"{type(e).__name__}: {e}",
            })
            raise

    # internals ----------------------------------------------------------

    def _fire_audit(self, event: dict) -> None:
        hook = self.audit_hook
        if hook is None:
            return
        try:
            hook(event)
        except Exception as e:
            if self.log:
                print(f"[audit] hook raised: {e}", file=sys.stderr, flush=True)

    def _dispatch_from_ctx(
        self,
        qualified: str,
        params: dict,
        parent_ctx: Context,
        scope: Optional[str],
    ) -> Any:
        return self.call(qualified, params, parent_ctx=parent_ctx, scope=scope)

    def _list_functions_from_ctx(self, agent: Optional[str]) -> list[dict]:
        out: list[dict] = []
        if agent is None:
            items = sorted(self.registry.agents.items())
        elif agent in self.registry.agents:
            items = [(agent, self.registry.agents[agent])]
        else:
            return []
        for agent_name, manifest in items:
            for fn in manifest.functions.values():
                out.append({
                    "qualified": f"{agent_name}.{fn.name}",
                    "agent": agent_name,
                    "function": fn.name,
                    "description": fn.description,
                    "params": dict(fn.params),
                    "schedule": fn.schedule,
                })
        return out

    @staticmethod
    def _validate_params(qualified: str, fn: FunctionDef, params: dict) -> None:
        for key, type_str in fn.params.items():
            optional = type_str.endswith("?")
            if not optional and key not in params:
                raise ValueError(f"{qualified}: missing required param {key!r}")

    def _load_impl(self, manifest: AgentManifest, fn: FunctionDef) -> Callable[..., Any]:
        cache_key = f"{manifest.name}.{fn.name}"
        fingerprint = self._impl_fingerprint(manifest, fn)
        # Lock so two threads loading the same impl for the first time don't
        # both exec_module and double-fire side effects.
        with self._impl_cache_lock:
            cached = self._impl_cache.get(cache_key)
            if cached is not None:
                cached_fingerprint, cached_impl = cached
                if cached_fingerprint == fingerprint:
                    return cached_impl

            # Per-function override wins; otherwise fall back to agent-level setting.
            agent_sandboxed = bool(manifest.sandbox.enabled)
            sandboxed = (
                fn.sandbox_override if fn.sandbox_override is not None else agent_sandboxed
            )
            if sandboxed:
                from .sandbox import make_sandboxed_python_impl, make_sandboxed_command_impl
                cfg = manifest.sandbox
                garden_root = manifest.folder.resolve().parent.parent
                if fn.command:
                    impl = make_sandboxed_command_impl(garden_root, manifest, fn, cfg)
                else:
                    impl = make_sandboxed_python_impl(garden_root, manifest, fn, cfg)
            elif fn.command:
                impl = self._make_command_impl(manifest, fn)
            else:
                impl = self._load_python_impl(manifest, fn)

            self._impl_cache[cache_key] = (fingerprint, impl)
            return impl

    @staticmethod
    def _impl_fingerprint(manifest: AgentManifest, fn: FunctionDef) -> tuple[Any, ...]:
        impl_mtime_ns = None
        if fn.impl:
            path_part, _, _ = fn.impl.partition(":")
            impl_path = (manifest.folder.resolve() / path_part).resolve()
            try:
                impl_mtime_ns = impl_path.stat().st_mtime_ns
            except FileNotFoundError:
                impl_mtime_ns = None
        sandbox = manifest.sandbox
        return (
            fn.impl,
            tuple(fn.command or ()),
            tuple(fn.params.items()),
            fn.sandbox_override,
            fn.timeout,
            bool(sandbox.enabled),
            bool(sandbox.network),
            tuple(sandbox.env_passthrough),
            tuple(sandbox.extra_ro_binds),
            tuple(sandbox.extra_rw_binds),
            sandbox.timeout,
            impl_mtime_ns,
        )

    @staticmethod
    def _load_python_impl(manifest: AgentManifest, fn: FunctionDef) -> Callable[..., Any]:
        assert fn.impl is not None
        path_part, _, func_name = fn.impl.partition(":")
        func_name = func_name or "run"
        agent_root = manifest.folder.resolve()
        impl_path = (agent_root / path_part).resolve()
        # Refuse path-traversal: the impl must live inside the agent's folder.
        try:
            impl_path.relative_to(agent_root)
        except ValueError:
            raise ValueError(
                f"impl {fn.impl!r} resolves to {impl_path}, outside agent folder {agent_root}"
            ) from None
        if not impl_path.exists():
            raise FileNotFoundError(f"impl not found: {impl_path}")

        # Make the agent folder importable so the impl can `import functions.foo`
        # or its own siblings. Also add the impl's directory so it can
        # `from _lib import ...` like the sandbox harness allows — keeps
        # sandboxed/unsandboxed import behaviour consistent.
        agent_str = str(manifest.folder.resolve())
        if agent_str not in sys.path:
            sys.path.insert(0, agent_str)
        impl_dir = str(impl_path.parent)
        if impl_dir not in sys.path:
            sys.path.insert(0, impl_dir)

        spec_name = f"_garden_{manifest.name}_{fn.name}_{impl_path.stem}"
        spec = importlib.util.spec_from_file_location(spec_name, impl_path)
        if spec is None:
            raise ImportError(f"cannot load {impl_path}")
        module = importlib.util.module_from_spec(spec)
        source = impl_path.read_text(encoding="utf-8")
        exec(compile(source, str(impl_path), "exec"), module.__dict__)

        if not hasattr(module, func_name):
            raise AttributeError(f"{impl_path} has no function {func_name!r}")
        return getattr(module, func_name)

    @staticmethod
    def _make_command_impl(manifest: AgentManifest, fn: FunctionDef) -> Callable[..., Any]:
        cmd = fn.command
        cwd = str(manifest.folder)
        timeout = fn.timeout

        def impl(params: dict, ctx: Context) -> Any:
            payload = json.dumps({
                "params": params,
                "ctx": {
                    "run_id": ctx.run_id,
                    "parent_run_id": ctx.parent_run_id,
                    "scope": ctx.scope,
                },
            })
            try:
                proc = subprocess.run(
                    cmd, input=payload, capture_output=True, text=True, cwd=cwd,
                    check=False, timeout=timeout,
                )
            except subprocess.TimeoutExpired as e:
                raise RuntimeError(f"command timed out after {timeout}s") from e
            if proc.returncode != 0:
                raise RuntimeError(f"command exited {proc.returncode}: {proc.stderr}")
            stdout = proc.stdout.strip()
            if not stdout:
                return None
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                return stdout

        return impl

    # bookkeeping --------------------------------------------------------

    def _record_run_start(
        self,
        run_id: str,
        manifest: AgentManifest,
        fn: FunctionDef,
        params: dict,
        parent_run_id: Optional[str],
        scope: Optional[str],
        started_at: float,
    ) -> None:
        try:
            self.carry.assert_(
                "garden.run",
                **{
                    "id": run_id,
                    "agent": manifest.name,
                    "function": fn.name,
                    "params": params,
                    "parent-run": parent_run_id or "",
                    "scope": scope or "",
                    "status": "running",
                    "started-at": started_at,
                },
            )
        except CarryError as e:
            if self.log:
                print(f"[carry] could not record run start: {e}", file=sys.stderr)

    def _record_run_end(
        self,
        run_id: str,
        status: str,
        duration: float,
        result: Any = None,
        error: str | None = None,
        traceback: str | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "id": run_id,
            "status": status,
            "ended-at": time.time(),
            "duration": duration,
        }
        if result is not None:
            try:
                fields["result"] = json.dumps(result, default=str)
            except (TypeError, ValueError):
                fields["result"] = str(result)
        if error:
            fields["error"] = error
        if traceback:
            fields["traceback"] = traceback
        try:
            self.carry.assert_("garden.run", **fields)
        except CarryError as e:
            if self.log:
                print(f"[carry] could not record run end: {e}", file=sys.stderr)
