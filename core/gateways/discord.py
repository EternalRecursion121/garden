"""Discord gateway. Long-running process that translates Discord events
into dispatcher calls, and exposes Discord output as a service that any
function can opt into.

Same shape as `Scheduler`: holds a `Dispatcher`, runs a loop, calls
`dispatcher.call(...)` on each external trigger. Where Scheduler is driven by
cron, this is driven by Discord events.

Inbound: channel subscription
-----------------------------
Functions opt into channels in their manifest, alongside `schedule`:

    [[function]]
    name        = "receive"
    impl        = "functions/receive.py:run"
    channels    = ["1078123456789012345"]   # guild channel or DM channel ID

On each message the gateway looks up `registry.subscribers_for(channel_id)`
and dispatches to every subscriber in parallel. If nobody subscribed, the
message is silently dropped — there is no default catch-all.

DMs aren't a special case: a Discord DM is a channel with its own ID.

Inbound dedup
-------------
Discord replays events on RESUME / reconnect. The gateway keeps a TTL
cache of seen message_ids and drops repeats — without this we'd dispatch
each replayed message a second time.

Outbound: two paths
-------------------
Subscription is purely an *input* contract. A function that subscribes to a
channel is not obligated to output to it. Two ways to emit:

1. **Return a `reply`/`replies`/`silent` dict** from a gateway-dispatched
   function. The gateway prefixes with `**[<agent>]**` and posts to the
   originating channel. Returning `{"silent": true}` (or returning anything
   without a reply field) means "I saw it but I'm not responding." This is
   the only outbound path available to sandboxed functions, since
   `ctx.service(...)` returns None across process boundaries.

2. **Call `ctx.service("discord")`** for proactive sends from any function
   (cron, in-process, etc.). Returns None if the gateway isn't running.
   Sandboxed functions can't use this — `ctx.service(...)` returns None
   inside the sandbox.

Inbound params
--------------
    {
      "message":     str,
      "user_id":     str,
      "user_name":   str,
      "channel_id":  str,
      "guild_id":    str,        # "" for DMs
      "is_dm":       bool,
      "message_id":  str,
      "reply_to":    str | None  # message_id this is replying to, if any
    }
"""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ..dispatcher import Dispatcher

try:
    import discord  # type: ignore
except ImportError:
    discord = None  # soft-fail; raised in __init__


# --- dedup ---------------------------------------------------------------


class MessageDeduplicator:
    """TTL cache of recently-seen message_ids. Prevents double-dispatch when
    Discord replays events on reconnect / RESUME."""

    def __init__(self, ttl_seconds: float = 300.0):
        self.ttl = ttl_seconds
        self._seen: dict[str, float] = {}

    def seen(self, message_id: str) -> bool:
        now = time.time()
        self._purge(now)
        if message_id in self._seen:
            return True
        self._seen[message_id] = now
        return False

    def _purge(self, now: float) -> None:
        if len(self._seen) < 1024:
            return
        cutoff = now - self.ttl
        for mid in [m for m, t in self._seen.items() if t < cutoff]:
            self._seen.pop(mid, None)


# --- outbound service ----------------------------------------------------


class DiscordService:
    """Outbound facade registered on `dispatcher.services["discord"]`.

    Functions are sync (run on a thread pool); the discord client lives on
    an asyncio loop. We bridge with `run_coroutine_threadsafe`.
    """

    def __init__(self, client, loop: asyncio.AbstractEventLoop, log=None):
        self._client = client
        self._loop = loop
        self._log = log or (lambda _msg: None)

    def _attach_logger(self, fut, label: str) -> None:
        """Log exceptions on fire-and-forget futures so silent failures don't
        vanish into the void."""
        def _on_done(f):
            exc = f.exception()
            if exc is not None:
                self._log(f"{label} failed: {exc!r}")
        fut.add_done_callback(_on_done)

    def send(self, *, channel_id: str | int, text: str, wait: bool = False) -> None:
        """Post `text` to a Discord channel (guild channel or DM channel).

        Fire-and-forget by default. Pass `wait=True` to block until Discord
        acks (raises on failure)."""
        coro = self._send(int(channel_id), text)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result(timeout=30)
        else:
            self._attach_logger(fut, f"send to {channel_id}")

    def dm(self, *, user_id: str | int, text: str, wait: bool = False) -> None:
        """Send a DM to a user, opening the DM channel if needed."""
        coro = self._dm(int(user_id), text)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result(timeout=30)
        else:
            self._attach_logger(fut, f"dm to {user_id}")

    async def _send(self, channel_id: int, text: str) -> None:
        channel = self._client.get_channel(channel_id)
        if channel is None:
            channel = await self._client.fetch_channel(channel_id)
        await channel.send(text)

    async def _dm(self, user_id: int, text: str) -> None:
        user = await self._client.fetch_user(user_id)
        await user.send(text)


# --- gateway -------------------------------------------------------------


class DiscordGateway:
    def __init__(
        self,
        dispatcher: Dispatcher,
        *,
        token: str,
        allowed_guilds: Optional[set[int]] = None,
        allowed_dm_users: Optional[set[int]] = None,
        ignore_bots: bool = True,
        max_parallel: int = 8,
        dedup_ttl: float = 300.0,
        refresh_interval: float = 5.0,
        audit_channel_id: Optional[int] = None,
        log: bool = True,
    ):
        if discord is None:
            raise RuntimeError(
                "discord.py not installed. Run `pip install discord.py`."
            )
        if not token:
            raise ValueError("DiscordGateway requires a token")

        self.dispatcher = dispatcher
        self.token = token
        self.allowed_guilds = allowed_guilds
        # If set, only DMs from these user IDs are dispatched. DMs from anyone
        # else are dropped silently — keeps random users from triggering
        # paid LLM calls just by DMing the bot. None = allow no DMs at all
        # (safer default than "anyone can DM"). Pass an empty set explicitly
        # to mean "block all DMs"; pass None to mean the same.
        self.allowed_dm_users = allowed_dm_users
        self.ignore_bots = ignore_bots
        self.max_parallel = max_parallel
        self.audit_channel_id = audit_channel_id
        self.log = log
        self._dedup = MessageDeduplicator(ttl_seconds=dedup_ttl)
        # Shared across all messages so we don't pay thread-pool startup per
        # event and so `max_parallel` is a gateway-wide cap rather than a
        # per-message cap. Sized for fan-out across concurrent inbound events.
        self._executor = ThreadPoolExecutor(
            max_workers=max_parallel, thread_name_prefix="garden-gw-discord"
        )
        # Debounce registry refresh: file-system scan on every Discord message
        # blocks the event loop and wastes work. `refresh_interval` is the
        # minimum gap between scans.
        self._refresh_interval = refresh_interval
        self._last_refresh = 0.0

        intents = discord.Intents.default()
        intents.message_content = True
        intents.dm_messages = True
        self.client = discord.Client(intents=intents)
        self._wire()

    # event wiring -------------------------------------------------------

    def _wire(self) -> None:
        @self.client.event
        async def on_ready():
            self._register_service()
            self._log_subscribers()
            self._log(f"connected as {self.client.user}")

        @self.client.event
        async def on_message(message):
            if message.author == self.client.user:
                return
            if self.ignore_bots and message.author.bot:
                return
            if (
                self.allowed_guilds is not None
                and message.guild is not None
                and message.guild.id not in self.allowed_guilds
            ):
                self._log(
                    f"dropping message from guild {message.guild.id} "
                    f"({message.guild.name!r}) — not in DISCORD_GUILD_IDS"
                )
                return
            # DM allow-list: drop DMs from anyone not on the list. With no
            # list configured, all DMs are dropped (no "anyone with the bot's
            # name can spam it" default).
            if message.guild is None:
                allowed = self.allowed_dm_users or set()
                if message.author.id not in allowed:
                    self._log(
                        f"dropping DM from {message.author.id} ({message.author.display_name!r}) "
                        f"— not in allowed_dm_users"
                    )
                    return
            if self._dedup.seen(str(message.id)):
                self._log(f"dedup: dropping replay of {message.id}")
                return
            await self._handle(message)

    def _register_service(self) -> None:
        service = DiscordService(self.client, self.client.loop, log=self._log)
        self.dispatcher.services["discord"] = service
        if self.audit_channel_id is not None:
            self.dispatcher.audit_hook = self._make_audit_hook(service)
            self._log(f"audit channel: {self.audit_channel_id}")

    def _make_audit_hook(self, service: "DiscordService"):
        """Build a fire-and-forget audit hook bound to the discord service.

        Hook is called from worker threads (the dispatcher runs impls under
        the gateway's executor). DiscordService.send already bridges back
        to the asyncio loop, so there's no extra threading work here. We
        keep the formatting minimal — one line per call.
        """
        channel_id = self.audit_channel_id

        def fmt_preview(value, limit: int = 120) -> str:
            try:
                s = value if isinstance(value, str) else __import__("json").dumps(value, default=str)
            except Exception:
                s = str(value)
            s = s.replace("\n", " ").replace("\r", " ")
            return s if len(s) <= limit else s[: limit - 1] + "…"

        def hook(event: dict) -> None:
            icon = "✓" if event.get("status") == "ok" else "✗"
            qualified = event.get("qualified", "?")
            run_id = (event.get("run_id") or "")[:8]
            duration = event.get("duration", 0.0)
            depth = int(event.get("depth", 0))
            indent = "·" * depth
            params = event.get("params") or {}
            tail = (
                f"err: {fmt_preview(event.get('error'), 200)}"
                if event.get("status") != "ok"
                else fmt_preview(event.get("result"))
            )
            params_preview = fmt_preview(params, 80) if params else ""
            line = (
                f"`{icon} {indent}{qualified} [{run_id}] {duration:.2f}s`"
                + (f"  in: {params_preview}" if params_preview else "")
                + (f"  → {tail}" if tail else "")
            )
            # Discord hard-caps at 2000 chars; trim defensively.
            if len(line) > 1900:
                line = line[:1899] + "…"
            try:
                service.send(channel_id=channel_id, text=line)
            except Exception as e:
                self._log(f"audit send failed: {e}")

        return hook

    async def _maybe_refresh_registry(self) -> None:
        """Refresh the registry at most once per `_refresh_interval`. The scan
        walks the agents directory, so we don't want to do it on the asyncio
        loop for every inbound message."""
        if time.time() - self._last_refresh < self._refresh_interval:
            return
        self._last_refresh = time.time()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.dispatcher.registry.refresh)

    # dispatch -----------------------------------------------------------

    async def _handle(self, message) -> None:
        channel_id = str(message.channel.id)
        await self._maybe_refresh_registry()
        subs = self.dispatcher.registry.subscribers_for(channel_id)
        if not subs:
            return

        reply_to = (
            str(message.reference.message_id)
            if message.reference and message.reference.message_id
            else None
        )
        params = {
            "message": message.content,
            "user_id": str(message.author.id),
            "user_name": message.author.display_name,
            "channel_id": channel_id,
            "guild_id": str(message.guild.id) if message.guild else "",
            "is_dm": message.guild is None,
            "message_id": str(message.id),
            "reply_to": reply_to,
        }

        self._log(
            f"channel {channel_id} -> "
            + ", ".join(q for q, _ in subs)
        )
        await self._dispatch_all(subs, params, message.channel)

    async def _dispatch_all(self, subs, params, channel):
        """Dispatch every subscriber in parallel. Each may return a dict; if
        it has a `reply` field, post it to the channel prefixed with the
        agent's name. Sandboxed functions can't reach `ctx.service('discord')`
        from another process, so this return-protocol is the only outbound
        path available to them. Unsandboxed functions can use either.

        Uses the gateway-wide executor — concurrent messages compete for the
        same `max_parallel` worker budget rather than each spawning their own
        pool. Shows a typing indicator in the channel for the duration of
        the dispatch (discord.py refreshes every 5s automatically)."""
        loop = asyncio.get_running_loop()

        def call_one(qualified: str):
            try:
                return qualified, self.dispatcher.call(qualified, params=params)
            except Exception as e:
                self._log(f"{qualified} raised: {e}")
                return qualified, None

        async with channel.typing():
            results = await asyncio.gather(
                *(loop.run_in_executor(self._executor, call_one, qualified) for qualified, _ in subs)
            )

        for qualified, result in results:
            if not isinstance(result, dict) or result.get("silent"):
                continue
            agent_name = qualified.partition(".")[0]
            replies: list[str] = []
            if isinstance(result.get("reply"), str):
                replies.append(result["reply"])
            replies.extend(
                r for r in (result.get("replies") or []) if isinstance(r, str)
            )
            for text in replies:
                if not text.strip():
                    continue
                try:
                    await channel.send(f"**[{agent_name}]** {text}")
                except Exception as e:
                    self._log(f"channel.send failed: {e}")

    # entrypoint ---------------------------------------------------------

    def run(self) -> None:
        self._log("starting; channel-subscription routing, output via ctx.service('discord')")
        try:
            self.client.run(self.token)
        finally:
            self._executor.shutdown(wait=False, cancel_futures=True)

    # logging ------------------------------------------------------------

    def _log(self, msg: str) -> None:
        if self.log:
            print(f"[gateway:discord] {msg}", flush=True)

    def _log_subscribers(self) -> None:
        if not self.log:
            return
        seen: dict[str, list[str]] = {}
        for m in self.dispatcher.registry.agents.values():
            for fn in m.functions.values():
                for ch in fn.channels:
                    seen.setdefault(ch, []).append(f"{m.name}.{fn.name}")
        if not seen:
            self._log("no channel subscribers registered")
            return
        for ch, qs in sorted(seen.items()):
            self._log(f"  {ch} -> {', '.join(qs)}")


def from_config(dispatcher: Dispatcher, cfg: dict) -> DiscordGateway:
    """Build a gateway from a `[gateway.discord]` table in garden.toml.

    Token from `DISCORD_TOKEN` env var. Guild allowlist from
    `DISCORD_GUILD_IDS` env var (comma-separated ints). Both live in
    `.env`, never in committed config.

    Recognized toml keys (all optional):
      allowed_dm_users  list[int] — user IDs allowed to DM the bot. Omit/empty
                        ⇒ all DMs dropped (default; safer than "anyone").
      dedup_ttl         seconds to remember message ids (default 300)
    """
    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        raise RuntimeError(
            "DISCORD_TOKEN is unset; export it before starting the gateway"
        )
    raw_guilds = os.environ.get("DISCORD_GUILD_IDS", "").strip()
    allowed_guilds: Optional[set[int]] = None
    if raw_guilds:
        allowed_guilds = {int(g) for g in raw_guilds.split(",") if g.strip()}
    raw_audit = os.environ.get("DISCORD_AUDIT_CHANNEL_ID", "").strip()
    audit_channel_id = int(raw_audit) if raw_audit else None
    return DiscordGateway(
        dispatcher,
        token=token,
        allowed_guilds=allowed_guilds,
        allowed_dm_users=(
            set(int(u) for u in cfg["allowed_dm_users"])
            if cfg.get("allowed_dm_users") else None
        ),
        dedup_ttl=float(cfg.get("dedup_ttl", 300)),
        audit_channel_id=audit_channel_id,
    )
