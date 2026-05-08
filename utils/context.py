"""Per-run context handed to every function impl.

A function receives `(params, ctx)`. ctx carries:
  - run identity (run_id, parent_run_id, scope)
  - call(): invoke another function in-process (sync)
  - map():  fan out the same function across many params (threadpool)
  - service(name): look up a runtime output service ("discord", ...) if one
    is registered. Inbound subscription does not imply outbound — functions
    must explicitly call services to emit anything.

Children inherit only what params and scope explicitly carry — there is no
implicit conversation history.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class Context:
    run_id: str
    parent_run_id: Optional[str]
    scope: Optional[str]
    agent: str = ""                       # name of the agent currently running
    depth: int = 0
    _dispatch: Callable[..., Any] = None  # (qualified, params, parent_ctx, scope) -> Any
    _services: dict[str, Any] = field(default_factory=dict)
    _list_functions: Callable[[Optional[str]], list[dict]] = None
    _list_agents: Callable[[], list[dict]] = None

    def list_functions(self, agent: str | None = None) -> list[dict]:
        """Discover what other functions exist in the garden.

        Returns one dict per registered function with keys:
        `qualified`, `agent`, `function`, `description`, `params`, `schedule`.
        Pass `agent` to filter to a single namespace. Descriptions and param
        types are free-form prose from each agent's manifest, not a schema.
        """
        if self._list_functions is None:
            return []
        return self._list_functions(agent)

    def list_agents(self) -> list[dict]:
        """Discover which agents exist in the garden.

        Returns one dict per registered agent with keys: `name`,
        `description`, `function_count`. Use `list_functions(agent=...)`
        to drill into a specific namespace.
        """
        if self._list_agents is None:
            return []
        return self._list_agents()

    def service(self, name: str) -> Any | None:
        """Return a registered runtime service or None if not running.

        Services represent outbound capabilities a long-running process has
        attached to the dispatcher (e.g. a Discord client). Functions opt
        into output explicitly:

            d = ctx.service("discord")
            if d:
                d.send(channel_id="...", text="hello")
        """
        return self._services.get(name) if self._services else None

    def call(
        self,
        qualified: str,
        params: dict | None = None,
        scope: str | None = None,
    ) -> Any:
        return self._dispatch(qualified, params or {}, self, scope)

    def map(
        self,
        qualified: str,
        params_list: list[dict],
        scope_fn: Callable[[dict], str] | None = None,
        max_workers: int = 8,
    ) -> list[Any]:
        """Fan out the same function across many param sets, in parallel."""
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [
                ex.submit(
                    self._dispatch,
                    qualified,
                    p,
                    self,
                    (scope_fn(p) if scope_fn else None),
                )
                for p in params_list
            ]
            return [f.result() for f in futs]
