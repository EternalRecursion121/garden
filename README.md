# garden

**A space for agents to grow and flourish alongside each other.**

Garden is a small runtime for running heterogeneous LLM-driven agents in one place. Each agent is a *namespace of functions*. Functions get scheduled (cron), called (CLI / inter-agent / inbound events), and share a single local-first knowledge store.

Garden is opinionated about plumbing. It is **not** opinionated about what agents do, what models they use, or what knowledge they store. Those decisions belong to the agents.

> See [`SPEC.md`](./SPEC.md) for the full conceptual reference.

---

## What's in the box

- **Function dispatcher** — `(params, ctx) → JSON-serialisable value`. Every call records a `garden.run` claim with parent linkage so call trees are queryable.
- **Cron scheduler** — runs any function with a `schedule = "..."` field in its manifest.
- **Inbound gateways** — long-running event sources that turn outside events into dispatcher calls. Currently: **Discord** (channel-subscription routing). Add more under `core/gateways/`.
- **Outbound services** — gateways register themselves on the dispatcher; functions reach them via `ctx.service("discord")`.
- **Per-call sandboxing** — `bwrap` isolation **on by default** for every agent, with file-level overrides for self-modification (e.g. an agent's identity doc bound writable while the rest of its folder stays read-only). Sandbox env is scrubbed; agents declare which env vars they need.
- **Carry-backed knowledge store** — one shared CRDT-merge-safe repo at `data/.carry/`. Helpers in `utils/notes.py` for `note` (durable knowledge with personal/shared scope and `[[wikilink]]` navigation) and `message` (inter-agent comms).

---

## Quick start

### 1. Dependencies

| Tool | Why | Install |
|------|-----|---------|
| Python ≥ 3.11 | Runtime | system |
| [`carry`](https://github.com/tonk-labs/carry) | Knowledge store | `cargo build --release` from source (prebuilds may have glibc mismatches) |
| `bubblewrap` | Per-call sandbox | `apt install bubblewrap` |
| `discord.py` | Optional, only if running the Discord gateway | `pip install discord.py` |
| Backends | Whatever your functions call: `claude` CLI, OpenRouter API key, … | per-agent |

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

### 2. Initialise the carry repo

```bash
.venv/bin/python -m core.cli init
```

Creates `data/.carry/`. First run only.

### 3. Drop in an agent

`agents/` ships empty. Either copy from `examples/` or start from scratch. The minimum is `agents/<name>/agent.toml`:

```toml
[agent]
name        = "scribe"
description = "Summarises URLs."

[[function]]
name        = "summarise"
description = "Summarise a URL."
impl        = "functions/summarise.py:run"
params      = { url = "string", depth = "int?" }
```

`agents/<name>/` belongs to that agent. It can be its own git repo, install its own deps, restructure freely. Garden only requires `agent.toml` at the folder root.

### 4. Run things

```bash
.venv/bin/python -m core.cli list
.venv/bin/python -m core.cli run scribe.summarise --params '{"url":"…"}'
.venv/bin/python -m core.cli schedule
.venv/bin/python -m core.cli gateway discord   # if configured
```

---

## Manifest reference

```toml
[agent]
name        = "<agent-name>"
description = "free-form"

# Per-call bwrap sandboxing. Sandbox is ON by default; declare this block
# to override defaults, or set `enabled = false` to opt out.
[agent.sandbox]
enabled         = true
network         = false                      # default off; true to allow internet
env_passthrough = ["ANTHROPIC_API_KEY"]      # which env vars to forward (default: none)
extra_ro_binds  = ["/path/visible/inside"]   # additional read-only mounts (validated)
extra_rw_binds  = ["/path/writable/inside"]  # e.g. agent's own identity doc
timeout         = 300                        # seconds; 0 disables the cap

# One [[function]] block per registered function.
[[function]]
name        = "<fn-name>"
description = "free-form"
impl        = "relative/path.py:func"   # OR `command = [...]` for an executable
params      = { foo = "string", bar = "int?" }   # ? = optional
schedule    = "0 9 * * *"               # optional cron
channels    = ["1078123456789012345"]   # optional Discord channel IDs to subscribe
sandbox     = false                     # optional override of agent-level setting
```

---

## Sandboxing

`bwrap`-based, per-call. **On by default for every agent** — declare
`[agent.sandbox] enabled = false` to opt out. Defaults:

- **Read-only:** `/usr`, `/lib`, `/lib64`, `/bin`, `/sbin`, garden's `core/` and `utils/`, the agent's own folder, the active venv. Specific files only from `/etc` (resolv.conf, hosts, nsswitch.conf, passwd, group, ssl/ca-certificates) — **not** all of `/etc`. **Not** `/run` (which would expose `/run/user/$UID/{ssh-agent,gpg-agent,dbus,…}`); only `/run/systemd/resolve` if it exists, for systemd-resolved DNS.
- **Read-write:** a private scratch dir under `data/sandbox/<agent>/`, plus `data/.carry/` (shared CRDT store).
- Other agents' folders are **not** mounted — agents can't see each other's files.
- Tmpfs `/tmp`, namespaces unshared (pid/uts/ipc), `--unshare-net` by default (network is opt-in).
- **Env is scrubbed (`--clearenv`).** Only `PATH`, `HOME` (= scratch dir), `LANG`, `PYTHONPATH`, `GARDEN_ROOT`, `GARDEN_AGENT_SCRATCH` are set. Anything else (API keys, `DISCORD_TOKEN`, etc.) only reaches the sandbox if the manifest lists it in `env_passthrough`.
- `extra_ro_binds` / `extra_rw_binds` are validated: absolute, no `..`, and not equal to `/`, `/etc`, `/proc`, `/sys`, `/dev`, `/root`, `/home`, `/var`, `/run`, `/usr`, `/lib`, `/lib64`, `/bin`, `/sbin`, `/boot`. Pick a more specific subdir.
- Each call has a `timeout` (default 300s; per-function `timeout = N` overrides).
- Inside the sandbox `ctx.call()` and `ctx.map()` raise; `ctx.service(...)` returns None. Sandboxed functions are leaves and reach Discord only via return-value protocol (a `{reply: "..."}` dict picked up by the gateway).

Functions that need cross-agent dispatch or live service handles should set `sandbox = false` in their manifest entry.

### Discord gateway config

`[gateway.discord]` in `garden.toml`:

```toml
[gateway.discord]
token_env        = "DISCORD_TOKEN"
allowed_guilds   = [123456789012345678]      # optional; restrict to specific guilds
allowed_dm_users = [987654321012345678]      # user IDs allowed to DM the bot
dedup_ttl        = 300
```

`allowed_dm_users` is a hard allow-list. **If unset (or empty), all DMs are dropped.** This keeps random users from triggering paid LLM calls just by DMing the bot. Add your own user ID (and any collaborators) before running the gateway.

### Trust model

The runtime treats `agent.toml` as trusted code — the runtime owner is expected to read an agent's manifest before installing it. The forbidden-bind list and env scrubbing make accidental escalation harder, but a malicious manifest can still set `sandbox = false`, and a non-sandboxed `command = […]` runs as the gateway user with full access. Don't drop in agents you haven't reviewed.

Carry-level scoping (`personal` notes, message recipients) is enforced through the helpers in `utils/notes.py` (which derive identity from `ctx.agent`), but the carry CLI itself is not ACL'd. An agent that bypasses the helpers and writes claims directly can forge `agent`/`from`/`to` fields. Treat per-agent scoping in carry as a discipline, not a security boundary; agents you don't fully trust shouldn't share a carry repo.

---

## Knowledge store

Two domains, one repo:

- **`garden.note`** — durable knowledge. Each note has `scope` (`personal` filtered by agent, or `shared`), an optional `title` (for `[[wikilink]]` references), `body`, `tags`, `source`, `created-at`. Use `utils.notes.write_note`, `list_notes`, `follow_link`, `expand_links`. Identity (`agent`/`reader`) is derived from `ctx.agent` — you don't pass it.
- **`garden.message`** — async inter-agent communication. Each message has `from`, `to` (an agent name or `"broadcast"`), `subject`, `body`, optional `in-reply-to`, and a `delivered` flag the recipient flips after processing. Use `send_message`, `fetch_inbox`, `mark_delivered`. `from`/`recipient` come from `ctx.agent`; `mark_delivered` refuses to flip a message not addressed to the running agent.

Because everything sits in carry, two sandboxed agents can write the same store concurrently and CRDT-merge cleanly.

---

## Repository layout

```
garden/
├── SPEC.md
├── README.md
├── garden.toml            # global config (defaults, secrets refs)
├── core/                  # the runtime — registry, dispatcher, scheduler, gateways, sandbox, adapters
├── utils/                 # carry wrapper, context, notes/messages helpers
├── examples/              # reference agents — copy into agents/ to use
├── agents/                # populated per installation; gitignored
└── data/                  # populated per installation; carry repo lives here; gitignored
```

`agents/` and `data/` ship empty so anyone can clone and make their own design decisions.

---

## Adding a backend

A backend is a class in `core/adapters/` exposing `invoke(prompt: str, **kwargs) -> Result`. The shipped ones: `OpenRouter` (HTTP), `ClaudeCode` (CLI shell-out), and stubs for `Hermes` and `Codex`. Functions import and instantiate per call:

```python
from core.adapters.openrouter import OpenRouter

def run(params, ctx):
    out = OpenRouter(model="anthropic/claude-haiku-4-5").invoke(prompt="…")
    return {"text": out.text}
```

---

## License

(unspecified — pick one for your fork)
