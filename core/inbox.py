"""Inbox watcher. Polls `garden.message` and dispatches undelivered messages
to subscribed handlers — mirrors how the Discord gateway dispatches channel
messages, but the source is carry instead of Discord.

A function opts into its agent's inbox by setting `inbox = true` in the
manifest, alongside `schedule` / `channels` / `commands`:

    [[function]]
    name  = "on_mail"
    impl  = "functions/on_mail.py:run"
    inbox = true

Each tick:
  1. Refresh the registry.
  2. Query `garden.message` for rows with `delivered != true`.
  3. For each message: find subscribers (the recipient agent's inbox handlers,
     or all inbox handlers if `to == "broadcast"`).
  4. Mark the message delivered *before* submitting workers — so a re-entry
     of `tick()` mid-flight won't fan it out twice.
  5. Submit each subscriber to the executor with `{message: {...}}` params.

Delivery semantics
------------------
We mark `delivered = true` after dispatch, regardless of handler outcome.
This matches channel semantics (Discord doesn't redeliver to a crashing
subscriber). Handlers that need at-least-once retry should write their own
state to carry on failure; the per-run audit trail still captures the error
via `garden.run`.

A message with no subscribers stays undelivered and will be re-evaluated on
the next tick — useful so that inbox handlers added later still pick up
recent traffic. To stop that loop for a stale message, retract it.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Optional

from utils.carry import Carry

from .dispatcher import Dispatcher


class InboxWatcher:
    def __init__(
        self,
        dispatcher: Dispatcher,
        *,
        poll_interval: float = 30.0,
        max_workers: int = 4,
    ):
        self.dispatcher = dispatcher
        self.carry: Carry = dispatcher.carry
        self.poll_interval = poll_interval
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="garden-inbox"
        )

    # public -------------------------------------------------------------

    def tick(self) -> int:
        """Process one batch of undelivered messages. Returns the number of
        message dispatches submitted (a broadcast hitting N agents counts as N).
        """
        self.dispatcher.registry.refresh()
        if not self.dispatcher.registry.has_inbox_subscribers():
            return 0

        undelivered = self._undelivered()
        if not undelivered:
            return 0

        submitted = 0
        for msg in undelivered:
            recipient = msg.get("to") or ""
            subs = self.dispatcher.registry.inbox_subscribers_for(recipient)
            if not subs:
                # No handler for this recipient (yet). Leave delivered=false;
                # a future tick after a manifest change will pick it up.
                continue

            did = msg["did"]
            try:
                self.carry.assert_("garden.message", **{"this": did, "delivered": True})
            except Exception as e:
                print(f"[inbox] could not mark {did} delivered: {e}")
                continue

            params = {"message": _msg_payload(msg)}
            for qualified, _fn in subs:
                fut = self._executor.submit(self._dispatch, qualified, params)
                self._track(qualified, fut)
                submitted += 1

        return submitted

    def run(self) -> None:
        print(f"[inbox] starting; poll={self.poll_interval}s")
        try:
            while True:
                try:
                    self.tick()
                except Exception as e:
                    print(f"[inbox] tick failed: {e}")
                time.sleep(self.poll_interval)
        finally:
            self._executor.shutdown(wait=False, cancel_futures=True)

    # internals ----------------------------------------------------------

    def _dispatch(self, qualified: str, params: dict) -> None:
        try:
            self.dispatcher.call(qualified, params=params)
        except Exception:
            # The dispatcher already records the error to garden.run and
            # prints it. Swallow here so the executor's done-callback
            # doesn't double-log.
            pass

    @staticmethod
    def _track(qualified: str, fut: Future) -> None:
        def _done(f: Future) -> None:
            exc = f.exception()
            if exc is not None:
                print(f"[inbox] {qualified} failed: {exc}")
        fut.add_done_callback(_done)

    def _undelivered(self) -> list[dict]:
        rows = self.carry.query(
            "garden.message", "msg-id", "from", "to", "subject", "body",
            "in-reply-to", "delivered", "created-at",
        )
        out: list[dict] = []
        for did, fields in (rows or {}).items():
            m = fields.get("garden.message", {})
            if _is_delivered(m.get("delivered")):
                continue
            out.append({"did": did, **m})
        out.sort(key=lambda m: m.get("created-at", 0))
        return out


def _is_delivered(value) -> bool:
    """Carry returns booleans as JSON booleans through `--format json`, but
    older rows / hand-written rows may carry the string "true". Be permissive."""
    if value is True:
        return True
    if isinstance(value, str) and value.lower() == "true":
        return True
    return False


def _msg_payload(msg: dict) -> dict:
    """Strip carry-internal fields, hand the handler a clean message dict."""
    return {
        "did": msg.get("did", ""),
        "msg-id": msg.get("msg-id", ""),
        "from": msg.get("from", ""),
        "to": msg.get("to", ""),
        "subject": msg.get("subject", ""),
        "body": msg.get("body", ""),
        "in-reply-to": msg.get("in-reply-to", ""),
        "created-at": msg.get("created-at", 0),
    }
