# Sandbox

Every agent runs sandboxed by default. A fresh `bwrap` jail is built per
call: cleared env, unshared pid/uts/ipc namespaces, optionally network,
read-only `core/` + `utils/` + agent folder, writable scratch + carry.

## What's writable

- `data/.carry/` — the shared carry repo.
- `data/sandbox/<agent>/` — private scratch dir, reset per call (HOME).
- Anything declared in `[agent.sandbox] extra_rw_binds`.

## What's read-only or invisible

- The agent's own folder (RO).
- `core/`, `utils/`, the venv (RO).
- A whitelist of `/etc` files (resolv.conf, hosts, passwd, group, ssl).
- Other agents' folders: **not bound at all**. Cross-agent collaboration
  goes through `ctx.call` (RPC) or carry, never the filesystem.

## ctx inside the sandbox

`ctx.call`, `ctx.map`, `ctx.list_functions`, `ctx.list_agents` all work
— they round-trip to the parent dispatcher over newline-delimited
JSON-RPC on stdin/stdout. The parent runs the dispatched call with your
run as its parent, so the run tree stays correct.

`ctx.service(...)` still returns `None`. Live handles (Discord client,
etc.) aren't proxyable. Emit through gateway return values, or route via
an unsandboxed sibling.

`ctx.map` from inside the sandbox is **sequential** — the RPC channel
is single-stream. Parent-side `ctx.map` is parallel.

## Restricting cross-agent calls

```toml
[agent.sandbox]
can_call = ["kira.consult", "tilth.*"]   # only these targets
```

- Field absent (default): allow all calls.
- `can_call = []`: deny every cross-agent call.
- Patterns are exact `agent.fn` or `agent.*` (any function in that agent).

The parent enforces the allowlist before dispatching; denied calls raise
a `PermissionError` back into the sandbox.

## Configuring the sandbox

```toml
[agent.sandbox]
enabled         = true                       # default
network         = false                      # default; off unless you need it
env_passthrough = ["ANTHROPIC_API_KEY"]      # forward specific env vars
extra_ro_binds  = ["/some/host/path"]        # extra read-only mounts
extra_rw_binds  = []                         # extra writable mounts
timeout         = 300                        # seconds; 0 disables
can_call        = ["kira.consult"]           # see above
```

`extra_*_binds` paths must be absolute and outside system roots
(`/`, `/etc`, `/usr`, `/run`, etc. are refused). Put the agent's
own folder in `extra_rw_binds` if it needs to self-modify.

## Per-function override

```toml
[[function]]
name    = "tend"
sandbox = false        # this one runs unsandboxed
```

The per-function `sandbox` field overrides the agent default. Use this
sparingly — typically for a single "stewardship" function that needs
`ctx.service("discord")` directly.

## Source of truth

- `core/sandbox.py` — bwrap argv builder, RPC harness, `can_call_matches`.
- `core/dispatcher.py:_load_impl` — picks sandboxed vs unsandboxed.
