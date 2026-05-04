"""Note + message helpers backed by carry.

Two domains, two scopes, one substrate:

  garden.note      durable knowledge (replaces markdown vaults)
    fields: scope ("personal"|"shared"), agent, title (optional, used for
            wikilinks), body (markdown), tags, created-at, source

  garden.message   asynchronous inter-agent communication
    fields: from, to ("<agent>" or "broadcast"), subject, body, in-reply-to,
            delivered (set true once the recipient processes it), created-at

Identity comes from `ctx.agent` — set by the dispatcher when it calls into
your function. You don't pass `agent=` / `reader=` / `sender=` / `recipient=`
yourself: the helpers read it off ctx so you can't accidentally (or
maliciously) write notes claiming to be another agent or read another
agent's "personal" notes through these helpers.

Caveat: the carry CLI itself is not ACL'd. An agent that bypasses these
helpers and shells out to `carry assert garden.note agent=other-agent ...`
can still forge claims. Treat per-agent scoping in carry as a discipline,
not a security boundary; agents you don't trust shouldn't share a carry
repo.

Wikilinks
---------
A note can include `[[Title]]` references to other notes. Use `follow_link`
to fetch a single linked note, or `expand_links` to substitute every
wikilink in a body with the linked note's content (one level deep — does
not recurse, to keep token budgets bounded).
"""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

from .carry import Carry


_WIKILINK_RE = re.compile(r"\[\[([^\[\]\n]+?)\]\]")


def _agent_of(ctx) -> str:
    """Extract the running agent's name from ctx, refuse if missing."""
    name = getattr(ctx, "agent", "") or ""
    if not name:
        raise RuntimeError(
            "ctx.agent is empty; notes/messages helpers need a dispatcher-set "
            "agent identity. Did you instantiate Context manually?"
        )
    return name


# --- Notes (durable knowledge) ------------------------------------------


def write_note(
    carry: Carry,
    ctx,
    *,
    body: str,
    scope: str = "personal",
    tags: list[str] | None = None,
    source: str | None = None,
    title: str | None = None,
) -> str:
    """Add a note attributed to the running agent (`ctx.agent`).

    `scope`: "personal" (only this agent reads via these helpers) or "shared".
    `tags`: optional list of strings for categorisation.
    `source`: where this note came from (vault filename, dream id, …).
    `title`: optional human-readable title; required for wikilink targets.
    """
    if scope not in ("personal", "shared"):
        raise ValueError(f"scope must be 'personal' or 'shared', got {scope!r}")
    return carry.assert_(
        "garden.note",
        scope=scope,
        agent=_agent_of(ctx),
        title=title or "",
        body=body,
        tags=tags or [],
        source=source or "",
        **{"created-at": _now()},
    )


def list_notes(
    carry: Carry,
    ctx,
    *,
    tag: str | None = None,
) -> list[dict[str, Any]]:
    """Return notes the running agent can see: their own personal notes plus
    all shared notes. Optionally filtered by tag."""
    rows = carry.query(
        "garden.note", "scope", "agent", "title", "body", "tags", "source", "created-at"
    )
    return _filter_notes(rows or {}, reader=_agent_of(ctx), tag=tag)


# --- Wikilink navigation ------------------------------------------------


def follow_link(carry: Carry, ctx, *, title: str) -> dict[str, Any] | None:
    """Resolve a `[[Title]]` reference. Returns the matching note (newest if
    multiple share a title), respecting the running agent's scope. None if
    no match is visible."""
    matches = [
        n for n in list_notes(carry, ctx)
        if (n.get("title") or "").strip().lower() == title.strip().lower()
    ]
    if not matches:
        return None
    matches.sort(key=lambda n: n.get("created-at", 0), reverse=True)
    return matches[0]


def extract_links(body: str) -> list[str]:
    """Pull every `[[Title]]` token out of a markdown body."""
    return [m.strip() for m in _WIKILINK_RE.findall(body)]


def expand_links(carry: Carry, ctx, *, body: str) -> str:
    """One-level wikilink expansion: replace `[[Title]]` with the linked
    note's body, fenced by markers so the reader can see what was inlined.
    Unresolvable links stay as `[[Title]]` (nothing inserted).

    Deliberately not recursive — keeps token budgets bounded and avoids
    cycle handling.
    """
    seen: dict[str, str] = {}

    def replace(match: re.Match) -> str:
        title = match.group(1).strip()
        if title in seen:
            return seen[title]
        target = follow_link(carry, ctx, title=title)
        if target is None:
            seen[title] = match.group(0)
            return seen[title]
        body = target.get("body") or ""
        seen[title] = (
            f"[[{title}]]\n<!-- begin link -->\n{body}\n<!-- end link -->"
        )
        return seen[title]

    return _WIKILINK_RE.sub(replace, body)


# --- Messages (inter-agent comms) ---------------------------------------


def send_message(
    carry: Carry,
    ctx,
    *,
    recipient: str,
    subject: str,
    body: str,
    in_reply_to: str | None = None,
) -> str:
    """Drop a message into another agent's inbox (or broadcast). Sender is
    always the running agent (`ctx.agent`); the helper won't let you spoof.

    `recipient` may be `"broadcast"` to leave a message any agent can pick up.
    """
    msg_id = uuid.uuid4().hex
    # `from` is a Python keyword; pass via dict-unpacking. Field names use
    # kebab-case because carry's attribute namespace forbids underscores.
    carry.assert_("garden.message", **{
        "id": msg_id,
        "from": _agent_of(ctx),
        "to": recipient,
        "subject": subject,
        "body": body,
        "in-reply-to": in_reply_to or "",
        "delivered": False,
        "created-at": _now(),
    })
    return msg_id


def fetch_inbox(
    carry: Carry,
    ctx,
    *,
    include_broadcast: bool = True,
) -> list[dict[str, Any]]:
    """Return undelivered messages addressed to the running agent (and
    optionally broadcasts). Caller is responsible for marking them
    delivered."""
    rows = carry.query(
        "garden.message", "id", "from", "to", "subject", "body",
        "in-reply-to", "delivered", "created-at",
    )
    recipient = _agent_of(ctx)
    out: list[dict[str, Any]] = []
    for did, fields in (rows or {}).items():
        m = fields.get("garden.message", {})
        if m.get("delivered"):
            continue
        to = m.get("to", "")
        if to == recipient or (include_broadcast and to == "broadcast"):
            out.append({"did": did, **m})
    out.sort(key=lambda m: m.get("created-at", 0))
    return out


def mark_delivered(carry: Carry, ctx, message_did: str) -> None:
    """Flip the `delivered` flag. Refuses if the message isn't addressed to
    the running agent (or to broadcast)."""
    rows = carry.query(
        "garden.message", "to",
    )
    fields = (rows or {}).get(message_did, {}).get("garden.message", {})
    if not fields:
        raise ValueError(f"unknown message: {message_did}")
    to = fields.get("to", "")
    me = _agent_of(ctx)
    if to != me and to != "broadcast":
        raise PermissionError(
            f"message {message_did} is addressed to {to!r}, not {me!r}; refusing to mark delivered"
        )
    carry.assert_("garden.message", **{"this": message_did, "delivered": True})


# --- internals ----------------------------------------------------------


def _now() -> float:
    return time.time()


def _filter_notes(
    rows: dict[str, Any],
    *,
    reader: str,
    tag: str | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for did, fields in rows.items():
        n = fields.get("garden.note", {})
        scope = n.get("scope", "personal")
        agent = n.get("agent", "")
        if scope == "personal" and agent != reader:
            continue
        if tag is not None and tag not in (n.get("tags") or []):
            continue
        out.append({"did": did, **n})
    out.sort(key=lambda n: n.get("created-at", 0))
    return out
