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
- **Per-call sandboxing** — opt-in `bwrap` isolation per agent, with file-level overrides for self-modification (e.g. an agent's identity doc bound writable while the rest of its folder stays read-only).
- **Carry-backed knowledge store** — one shared CRDT-merge-safe repo at `data/.carry/`. Helpers in `utils/notes.py` for `note` (durable knowledge with personal/shared scope and `[[wikilink]]` navigation) and `message` (inter-agent comms).

---

## Quick start

### 1. Dependencies

| Tool | Why | Install |
|------|-----|---------|
| Python ≥ 3.12 | Runtime | system |
| [`carry`](https://github.com/tonk-labs/carry) | Knowledge store | `cargo build --release` from source (prebuilds may have glibc mismatches) |
| `bubblewrap` | Per-call sandbox | `apt install bubblewrap` |
| `discord.py` | Optional, only if running the Discord gateway | `pip install discord.py` |
| Backends | Whatever your functions call: `claude` CLI, OpenRouter API key, … | per-agent |

```bash
python3 -m venv .venv
.venv/bin/pip install croniter pyyaml
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

# Optional. Enable per-call bwrap sandboxing for this agent's functions.
[agent.sandbox]
enabled        = true
network        = true                       # default; false to airgap
extra_ro_binds = ["/path/visible/inside"]   # additional read-only mounts
extra_rw_binds = ["/path/writable/inside"]  # e.g. agent's own identity doc

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

`bwrap`-based, per-call. The default config:

- Read-only: `/usr`, `/etc`, `/lib`, `/lib64`, `/bin`, `/sbin`, garden's `core/` and `utils/`, the agent's own folder, the active venv.
- Read-write: a private scratch dir under `data/sandbox/<agent>/`, plus `data/.carry/` (shared CRDT store).
- Other agents' folders are **not** mounted — agents can't see each other's files.
- Tmpfs `/tmp`, namespaces unshared (pid/uts/ipc), optional `--unshare-net`.
- Inside the sandbox `ctx.call()` and `ctx.map()` raise; `ctx.service(...)` returns None. Sandboxed functions are leaves and reach Discord only via return-value protocol (a `{reply: "..."}` dict picked up by the gateway).

Functions that need cross-agent dispatch or live service handles should set `sandbox = false` in their manifest entry.

---

## Knowledge store

Two domains, one repo:

- **`garden.note`** — durable knowledge. Each note has `scope` (`personal` filtered by agent, or `shared`), an optional `title` (for `[[wikilink]]` references), `body`, `tags`, `source`, `created-at`. Use `utils.notes.write_note`, `list_notes`, `follow_link`, `expand_links`.
- **`garden.message`** — async inter-agent communication. Each message has `from`, `to` (an agent name or `"broadcast"`), `subject`, `body`, optional `in-reply-to`, and a `delivered` flag the recipient flips after processing. Use `send_message`, `fetch_inbox`, `mark_delivered`.

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
