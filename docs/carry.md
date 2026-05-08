# Carry — the shared write surface

Carry is a CRDT-merge-safe knowledge graph stored at `data/.carry/`.
Every agent writes here; reads are queries, writes are claim assertions.
The Python wrapper at `utils/carry.py` shells out to the `carry` CLI for
every operation.

## Two conventions you must follow

### 1. Field names use kebab-case

Carry's attribute namespace forbids underscores. So you write:

```python
carry.assert_("garden.note", **{
    "created-at": time.time(),
    "parent-run": run_id,
    "in-reply-to": prior_id,
})
```

Pass kebab-keyed fields via `**{"kebab": value}` because Python
identifiers can't contain hyphens.

### 2. List/dict values are JSON-encoded

`Carry._serialize` JSON-encodes anything that isn't a primitive on the
way in, and `query()` best-effort-decodes string values that start with
`[` or `{` on the way out. So you can do:

```python
carry.assert_("garden.run", id=rid, params={"a": 1, "b": [2, 3]})
```

…and `params` round-trips as a dict. Strings that don't look like JSON
(no leading `[`/`{`) come back as plain strings — don't accidentally
write `"3"` and expect it to parse as an int.

## Reserved domain prefix

Garden owns `garden.*` for runtime bookkeeping:

| Domain          | Purpose                                          |
|-----------------|--------------------------------------------------|
| `garden.run`    | One per dispatched call — call tree + audit log  |
| `garden.note`   | Long-form agent knowledge (see `messaging.md`)   |
| `garden.message`| Inter-agent inbox messages (see `messaging.md`)  |
| `garden.cron`   | Scheduler bookkeeping                            |

Everything else is free-form. Pick a domain prefix for your own state
(e.g. `kira.session`, `tilth.archive`) — don't squat on `garden.*`.

## Updating an entity

Pass `this=<DID>` to update an existing entity rather than creating a
new one:

```python
carry.assert_("garden.message", this=msg_did, delivered=True)
```

The DID is the `id` field returned by `carry.query(..., format="json")`.
**Don't** assert your own `id` field — it shadows the entity DID and
breaks lookups. Use a domain-specific name like `msg-id` or `run-id`.

## Source of truth

- `utils/carry.py` — wrapper, serialisation, query format.
- `core/dispatcher.py:_record_run_*` — how `garden.run` is shaped.
- `utils/notes.py` — note + message helpers built on top of carry.
