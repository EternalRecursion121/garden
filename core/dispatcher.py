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
        self._impl_cache: dict[str, Callable[..., Any]] = {}
        # Outbound services attached by long-running processes (gateways).
        # Functions reach them via ctx.service(name); inbound and outbound
        # are independent — a function subscribed to a Discord channel does
        # not auto-emit through Discord.
        self.services: dict[str, Any] = {}

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
            depth=depth,
            _dispatch=self._dispatch_from_ctx,
            _services=self.services,
            _list_functions=self._list_functions_from_ctx,
        )

        try:
            impl = self._load_impl(manifest, fn)
            result = impl(params, ctx)
            self._record_run_end(run_id, "ok", time.time() - started, result=result)
            if self.log:
                print(
                    f"{indent}✓ {qualified} [{run_id[:8]}] in {time.time() - started:.2f}s",
                    flush=True,
                )
            return result
        except Exception as e:
            self._record_run_end(
                run_id,
                "error",
                time.time() - started,
                error=f"{type(e).__name__}: {e}",
                traceback=traceback.format_exc(),
            )
            if self.log:
                print(f"{indent}✗ {qualified} [{run_id[:8]}]: {e}", flush=True)
            raise

    # internals ----------------------------------------------------------

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
        if cache_key in self._impl_cache:
            return self._impl_cache[cache_key]

        # Per-function override wins; otherwise fall back to agent-level setting.
        agent_sandboxed = bool(manifest.sandbox and manifest.sandbox.enabled)
        sandboxed = (
            fn.sandbox_override if fn.sandbox_override is not None else agent_sandboxed
        )
        if sandboxed:
            from .sandbox import make_sandboxed_python_impl, make_sandboxed_command_impl
            cfg = manifest.sandbox or __import__("core.sandbox", fromlist=["SandboxConfig"]).SandboxConfig(enabled=True)
            garden_root = manifest.folder.resolve().parent.parent
            if fn.command:
                impl = make_sandboxed_command_impl(garden_root, manifest, fn, cfg)
            else:
                impl = make_sandboxed_python_impl(garden_root, manifest, fn, cfg)
        elif fn.command:
            impl = self._make_command_impl(manifest, fn)
        else:
            impl = self._load_python_impl(manifest, fn)

        self._impl_cache[cache_key] = impl
        return impl

    @staticmethod
    def _load_python_impl(manifest: AgentManifest, fn: FunctionDef) -> Callable[..., Any]:
        assert fn.impl is not None
        path_part, _, func_name = fn.impl.partition(":")
        func_name = func_name or "run"
        impl_path = (manifest.folder / path_part).resolve()
        if not impl_path.exists():
            raise FileNotFoundError(f"impl not found: {impl_path}")

        # Make the agent folder importable so the impl can `import functions.foo`
        # or its own siblings.
        agent_str = str(manifest.folder.resolve())
        if agent_str not in sys.path:
            sys.path.insert(0, agent_str)

        spec_name = f"_garden_{manifest.name}_{fn.name}_{impl_path.stem}"
        spec = importlib.util.spec_from_file_location(spec_name, impl_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {impl_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, func_name):
            raise AttributeError(f"{impl_path} has no function {func_name!r}")
        return getattr(module, func_name)

    @staticmethod
    def _make_command_impl(manifest: AgentManifest, fn: FunctionDef) -> Callable[..., Any]:
        cmd = fn.command
        cwd = str(manifest.folder)

        def impl(params: dict, ctx: Context) -> Any:
            payload = json.dumps({
                "params": params,
                "ctx": {
                    "run_id": ctx.run_id,
                    "parent_run_id": ctx.parent_run_id,
                    "scope": ctx.scope,
                },
            })
            proc = subprocess.run(
                cmd, input=payload, capture_output=True, text=True, cwd=cwd, check=False
            )
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
