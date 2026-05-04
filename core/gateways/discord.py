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

    def __init__(self, client, loop: asyncio.AbstractEventLoop):
        self._client = client
        self._loop = loop

    def send(self, *, channel_id: str | int, text: str, wait: bool = False) -> None:
        """Post `text` to a Discord channel (guild channel or DM channel).

        Fire-and-forget by default. Pass `wait=True` to block until Discord
        acks (raises on failure)."""
        coro = self._send(int(channel_id), text)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result(timeout=30)

    def dm(self, *, user_id: str | int, text: str, wait: bool = False) -> None:
        """Send a DM to a user, opening the DM channel if needed."""
        coro = self._dm(int(user_id), text)
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result(timeout=30)

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
        ignore_bots: bool = True,
        max_parallel: int = 8,
        dedup_ttl: float = 300.0,
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
        self.ignore_bots = ignore_bots
        self.max_parallel = max_parallel
        self.log = log
        self._dedup = MessageDeduplicator(ttl_seconds=dedup_ttl)

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
                return
            if self._dedup.seen(str(message.id)):
                self._log(f"dedup: dropping replay of {message.id}")
                return
            await self._handle(message)

    def _register_service(self) -> None:
        self.dispatcher.services["discord"] = DiscordService(
            self.client, self.client.loop
        )

    # dispatch -----------------------------------------------------------

    async def _handle(self, message) -> None:
        channel_id = str(message.channel.id)
        self.dispatcher.registry.refresh()
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
        path available to them. Unsandboxed functions can use either."""
        loop = asyncio.get_running_loop()

        def call_one(qualified: str):
            try:
                return qualified, self.dispatcher.call(qualified, params=params)
            except Exception as e:
                self._log(f"{qualified} raised: {e}")
                return qualified, None

        with ThreadPoolExecutor(max_workers=min(self.max_parallel, len(subs))) as ex:
            results = await asyncio.gather(
                *(loop.run_in_executor(ex, call_one, qualified) for qualified, _ in subs)
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
        self.client.run(self.token)

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

    Recognized keys:
      allowed_guilds   list[int] — restrict to specific guilds (optional)
      token_env        env var holding the bot token (default DISCORD_TOKEN)
      dedup_ttl        seconds to remember message ids (default 300)
    """
    token_env = cfg.get("token_env", "DISCORD_TOKEN")
    token = os.environ.get(token_env, "")
    if not token:
        raise RuntimeError(
            f"{token_env} is unset; export it before starting the gateway"
        )
    return DiscordGateway(
        dispatcher,
        token=token,
        allowed_guilds=set(cfg["allowed_guilds"]) if cfg.get("allowed_guilds") else None,
        dedup_ttl=float(cfg.get("dedup_ttl", 300)),
    )
