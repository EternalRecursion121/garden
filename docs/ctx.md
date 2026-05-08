# `ctx` — the per-call context

Every function impl is invoked as `run(params, ctx)`. `ctx` is the
runtime handle: identity, dispatch, services, discovery.

## Identity fields

```python
ctx.agent          # str: name of the agent currently running
ctx.run_id         # str: unique hex id for this call
ctx.parent_run_id  # str|None: caller's run_id, or None at top of tree
ctx.scope          # str|None: caller-supplied tag, propagates to children
ctx.depth          # int: 0 at top of tree, N+1 below depth N
```

`run_id` is what carry uses to chain `garden.run` claims via `parent-run`.
The whole call tree of a request is queryable by walking that field.

## Dispatching to other functions

```python
result = ctx.call("kira.consult", {"question": "..."}, scope="optional-tag")
```

The dispatched call runs through the same dispatcher, so it picks up the
target's sandbox config, validates params, records its own `garden.run`,
and inherits this run as its parent.

```python
results = ctx.map("kira.consult", [
    {"question": "a"},
    {"question": "b"},
])
```

`ctx.map` fans out the same target across many param sets. From an
unsandboxed function it parallelises (ThreadPool, default 8 workers).
From inside the sandbox it serialises on the RPC channel — calls still
happen, just one at a time.

## Discovery

```python
ctx.list_agents()         # [{name, description, function_count}, ...]
ctx.list_functions()      # all functions, every agent
ctx.list_functions(agent="kira")   # just one namespace
```

`list_functions` rows have keys: `qualified` (e.g. `"kira.consult"`),
`agent`, `function`, `description`, `params` (dict of name → type-token),
`schedule` (cron string or None).

The `params` value is the manifest's type-token dict, e.g.
`{"question": "string", "context": "string?"}`. Trailing `?` = optional.
It's a contract, not a schema — types are validated at dispatch.

## Services (live runtime handles)

```python
d = ctx.service("discord")
if d is not None:
    d.send(channel_id="...", text="hello")
```

Services are *outbound* capabilities a long-running process attaches at
startup (the Discord gateway is the canonical one). Subscribing to a
channel via `channels = [...]` does **not** auto-allow emit — you have
to opt in by calling the service.

`ctx.service(...)` returns `None` inside the sandbox: live handles
aren't proxyable across the bwrap boundary. Sandboxed functions emit by
returning `{reply: "..."}` / `{replies: [...]}` / `{silent: true}` from
gateway-dispatched handlers, or by routing through an unsandboxed
sibling that has the service.

## Source of truth

- `utils/context.py` — Context dataclass.
- `core/dispatcher.py` — `Dispatcher.call()`, `_list_agents_from_ctx`,
  `_list_functions_from_ctx`.
- `core/sandbox.py` — `_SandboxedContext` (the sandboxed mirror; same
  surface, except `service` returns None).
