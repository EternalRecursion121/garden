# garden — SPEC

> A space for agents to grow and flourish alongside each other.

Garden is a runtime where each "agent" is a **namespace of functions**, all functions share one local-first knowledge store built on [carry](https://github.com/tonk-labs/carry), and any function can schedule itself or call any other function (including itself with different params).

Garden takes no position on what models agents use, what knowledge they store, or what schemas they organise it under. Those are decisions for the agents.

---

## 1. Repository layout

```
garden/
├── SPEC.md
├── garden.toml             # global config (defaults, secrets refs)
├── core/                   # the runtime
│   ├── registry.py         # discovers agents/ and loads their manifests
│   ├── dispatcher.py       # invokes a function (params + context) → run handle
│   ├── scheduler.py        # cron loop
│   └── adapters/           # one module per backend (claude_code, hermes, codex, openrouter, …)
├── utils/                  # shared helpers (carry wrapper, context, IO, secrets)
├── data/
│   └── .carry/             # the single shared carry repository
└── agents/
    └── <agent-name>/       # one folder per agent — owned by the agent
        └── agent.toml      # the only file garden requires
```

`core/` and `utils/` belong to garden. `data/.carry/` is shared. Everything under `agents/<name>/` belongs to that agent — it can `git init`, install deps, restructure freely. Garden only requires `agent.toml` at the folder root.

---

## 2. Conceptual model

- **Function** — the only thing that runs. `(params → impl → return value + side effects)`. The impl decides what context to build, which backend to invoke, what to write back.
- **Agent** — a folder. A namespace that owns a set of functions. *Not* a model.
- **Backend** — how a function talks to an LLM (claude-code CLI, hermes, codex, openrouter, …). Selected per-call by the function, not bound to an agent.
- **Run** — one execution of a function. Has a parent run if spawned by another.

---

## 3. Agent manifest

```toml
[agent]
name        = "scribe"
description = "free-form"

[[function]]
name        = "summarise_url"
description = "free-form"
impl        = "functions/summarise_url.py:run"
params      = { url = "string", depth = "int?" }
schedule    = "0 9 * * *"                        # optional cron
```

The manifest declares what functions exist. Everything else is the function's call.

---

## 4. Functions

A function impl receives `(params, ctx)` and returns a JSON-serialisable value. `ctx` carries the run id, parent run id, scope, and a `call(...)` primitive. What the function does with that — prompt assembly, model choice, tool surface, knowledge writes, whether and how to journal — is entirely up to the function.

Garden ships backends as a library (`garden.backends.claude_code`, `.openrouter`, `.hermes`, `.codex`, …). Functions import and pick per call. Adding a backend is one new file in `core/adapters/`.

---

## 5. Calling, scheduling, recursion

One primitive for all three:

```
ctx.call("<agent>.<function>", params={...}, scope=...)
```

- Cron entries, CLI invocations, and inter-agent calls all resolve to this.
- A function may call itself with different params; the dispatcher does not special-case recursion.
- Every call creates a child `garden.run` with `parent_run` set, so the call tree is queryable.
- Context is explicit: children inherit only what params and scope carry, not the parent's prompt history.

The dispatcher does not materialise prompts or pick models. Those are inside the function.

---

## 6. Knowledge store

A single carry repository at `data/.carry/`. All agents read and write through `utils/carry.py`. Carry's CRDT semantics handle parallel writes.

Garden does not define the schema. Agents (collectively, over time) decide what domains exist and how they fit together. Garden only reserves a small `garden.*` prefix for runtime bookkeeping (runs, cron state, function registry) — and even those entries can be treated as advisory by other agents.

The only opinion garden holds: the **shared** knowledge base lives in carry, not in scattered markdown files. Agents can keep private scratch under their own folder, but anything meant to be reused by other agents goes through carry so it's queryable, schema-checked, and merge-safe.
