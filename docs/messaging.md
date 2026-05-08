# Notes, messages, inbox

Two communication primitives, both built on carry. Notes are durable
knowledge; messages are addressed payloads with delivery semantics.

## Notes — `garden.note`

```python
from utils.notes import write_note, list_notes, follow_link, expand_links

write_note(
    carry, ctx,
    body="The user prefers terse responses with no preamble.",
    scope="personal",        # "personal" (default) or "shared"
    tags=["preference"],
    title="User comms style",
)
```

- `scope="personal"` → readable only by the asserting agent (queryable
  via `list_notes(carry, ctx)` filtered by `agent`).
- `scope="shared"` → all agents can read; this is the curatable pool.
- `title` enables `[[wikilink]]` resolution. Resolve a link with
  `follow_link(carry, ctx, title="...")`. Expand links inline within a
  body via `expand_links(carry, ctx, body=...)` (one level deep).

```python
mine_only        = list_notes(carry, ctx)                       # personal + shared
patterns_shared  = list_notes(carry, ctx, tag="pattern", scope="shared")
```

## Messages — `garden.message`

```python
from utils.notes import send_message, fetch_inbox, mark_delivered

send_message(
    carry, ctx,
    recipient="kira",       # or "broadcast"
    subject="quick consult request",
    body="...",
    in_reply_to=prior_msg_id,   # optional thread chain
)
```

Sender is always `ctx.agent` — the helper won't let you spoof. Message
ids are uuid4 hex strings.

## Inbox — pull vs push

### Pull (at-least-once with manual ack)

```python
def my_inbox_handler(params, ctx):
    msgs = fetch_inbox(carry, ctx, include_broadcast=True)
    for msg in msgs:
        try:
            handle(msg)
            mark_delivered(carry, ctx, msg["msg-id"])
        except Exception:
            pass  # leave undelivered → retry on next run
```

Use `fetch_inbox` + `mark_delivered` when you need agent-controlled
batching, retries, or scheduling. Typical pattern: a cron-scheduled
`inbox` function that runs every N minutes.

### Push (once-and-done, runtime-driven)

```toml
[[function]]
name  = "inbox"
impl  = "functions/inbox.py:run"
inbox = true
```

The runtime's `InboxWatcher` (rides along with `core.cli schedule`)
polls carry for messages addressed to your agent (or `broadcast`) and
dispatches the first matching `inbox = true` function with
`{message: {...}}` as params. Marks `delivered = true` after dispatch
**regardless of handler outcome** — same semantics as a Discord
channel: one delivery attempt, period.

Choose pull if you need retries; choose push if delivery once is
enough. Multiple push-mode handlers on one agent all fire in parallel
for each message.

To slow how often the runtime drains your agent's inbox, declare a
per-agent throttle in `agent.toml`:

```toml
[agent.inbox]
poll_interval = 300   # seconds; drain queue at most every 5 minutes
```

The watcher still polls carry on its global cadence, but messages
addressed to your agent only get dispatched once that interval has
elapsed since the last drain. Broadcasts ignore the throttle.

## Source of truth

- `utils/notes.py` — `write_note`, `list_notes`, `follow_link`,
  `expand_links`, `send_message`, `fetch_inbox`, `mark_delivered`.
- `core/inbox.py` — InboxWatcher (push delivery).
