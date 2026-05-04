"""Note + message helpers backed by carry.

Two domains, two scopes, one substrate:

  garden.note      durable knowledge (replaces markdown vaults)
    fields: scope ("personal"|"shared"), agent, title (optional, used for
            wikilinks), body (markdown), tags, created-at, source

  garden.message   asynchronous inter-agent communication
    fields: from, to ("<agent>" or "broadcast"), subject, body, in-reply-to,
            delivered (set true once the recipient processes it), created-at

Why carry instead of files / Discord:
  * CRDT-merge-safe — concurrent writes from sandboxed agents Just Work.
  * Auditable — every cross-agent exchange is queryable.
  * Asynchronous by default — Kira can answer Iris hours later without
    losing thread, since `in-reply-to` chains the conversation.

Wikilinks
---------
A note can include `[[Title]]` references to other notes. Use `follow_link`
to fetch a single linked note, or `expand_links` to substitute every
wikilink in a body with the linked note's content (one level deep — does
not recurse, to keep token budgets bounded).

Both agents (and any future agent) can call these helpers because the carry
binary lives in /usr/local/bin (mounted read-only by every sandbox) and the
repo at <garden_root>/data/.carry/ is bound writable.
"""

from __future__ import annotations

import re
import time
import uuid
from pathlib import Path
from typing import Any

from .carry import Carry


_WIKILINK_RE = re.compile(r"\[\[([^\[\]\n]+?)\]\]")


# --- Notes (durable knowledge) ------------------------------------------


def write_note(
    carry: Carry,
    *,
    agent: str,
    body: str,
    scope: str = "personal",
    tags: list[str] | None = None,
    source: str | None = None,
    title: str | None = None,
) -> str:
    """Add a note. Returns the new claim's entity DID.

    `scope`: "personal" (only this agent reads) or "shared" (any agent).
    `tags`: optional list of strings for categorisation.
    `source`: where this note came from (vault filename, dream id, …).
    `title`: optional human-readable title; required for wikilink targets.
    """
    if scope not in ("personal", "shared"):
        raise ValueError(f"scope must be 'personal' or 'shared', got {scope!r}")
    return carry.assert_(
        "garden.note",
        scope=scope,
        agent=agent,
        title=title or "",
        body=body,
        tags=tags or [],
        source=source or "",
        **{"created-at": _now()},
    )


def list_notes(
    carry: Carry,
    *,
    reader: str,
    tag: str | None = None,
) -> list[dict[str, Any]]:
    """Return notes the `reader` agent can see: their own personal notes
    plus all shared notes. Optionally filtered by tag."""
    rows = carry.query(
        "garden.note", "scope", "agent", "title", "body", "tags", "source", "created-at"
    )
    return _filter_notes(rows or {}, reader=reader, tag=tag)


# --- Wikilink navigation ------------------------------------------------


def follow_link(carry: Carry, *, reader: str, title: str) -> dict[str, Any] | None:
    """Resolve a `[[Title]]` reference. Returns the matching note (newest
    if multiple share a title), respecting the reader's scope. None if no
    match is visible to this reader."""
    matches = [
        n for n in list_notes(carry, reader=reader)
        if (n.get("title") or "").strip().lower() == title.strip().lower()
    ]
    if not matches:
        return None
    matches.sort(key=lambda n: n.get("created-at", 0), reverse=True)
    return matches[0]


def extract_links(body: str) -> list[str]:
    """Pull every `[[Title]]` token out of a markdown body."""
    return [m.strip() for m in _WIKILINK_RE.findall(body)]


def expand_links(carry: Carry, *, reader: str, body: str) -> str:
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
        target = follow_link(carry, reader=reader, title=title)
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
    *,
    sender: str,
    recipient: str,
    subject: str,
    body: str,
    in_reply_to: str | None = None,
) -> str:
    """Drop a message into another agent's inbox (or broadcast).

    `recipient` may be `"broadcast"` to leave a message any agent can pick up.
    """
    msg_id = uuid.uuid4().hex
    # `from` is a Python keyword; pass via dict-unpacking. Field names use
    # kebab-case because carry's attribute namespace forbids underscores.
    carry.assert_("garden.message", **{
        "id": msg_id,
        "from": sender,
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
    *,
    recipient: str,
    include_broadcast: bool = True,
) -> list[dict[str, Any]]:
    """Return undelivered messages addressed to this agent (and optionally
    broadcasts). Caller is responsible for marking them delivered."""
    rows = carry.query(
        "garden.message", "id", "from", "to", "subject", "body",
        "in-reply-to", "delivered", "created-at",
    )
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


def mark_delivered(carry: Carry, message_did: str) -> None:
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
