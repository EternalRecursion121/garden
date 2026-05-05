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
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from ..dispatcher import Dispatcher

try:
    import discord  # type: ignore
    from discord import app_commands  # type: ignore
except ImportError:
    discord = None  # soft-fail; raised in __init__
    app_commands = None  # type: ignore


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


# --- audit batching ------------------------------------------------------


def _fmt_preview(value, limit: int = 120) -> str:
    try:
        s = value if isinstance(value, str) else json.dumps(value, default=str)
    except Exception:
        s = str(value)
    s = s.replace("\n", " ").replace("\r", " ")
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _format_audit_line(event: dict) -> str:
    icon = "✓" if event.get("status") == "ok" else "✗"
    qualified = event.get("qualified", "?")
    run_id = (event.get("run_id") or "")[:8]
    duration = event.get("duration", 0.0)
    depth = int(event.get("depth", 0))
    indent = "·" * depth
    params = event.get("params") or {}
    tail = (
        f"err: {_fmt_preview(event.get('error'), 200)}"
        if event.get("status") != "ok"
        else _fmt_preview(event.get("result"))
    )
    params_preview = _fmt_preview(params, 80) if params else ""
    line = (
        f"`{icon} {indent}{qualified} [{run_id}] {duration:.2f}s`"
        + (f"  in: {params_preview}" if params_preview else "")
        + (f"  → {tail}" if tail else "")
    )
    if len(line) > 1900:
        line = line[:1899] + "…"
    return line


class _AuditBuffer:
    """Coalesces audit lines into batched Discord posts.

    Discord caps text channels at ~5 messages/sec; per-call audit posts
    blow that out under any fan-out (cron tick + user message + ctx.map).
    We append lines from worker threads, then a single async loop flushes
    every `interval` seconds, packing as many lines as fit in 1900 chars
    per Discord message.
    """

    def __init__(self, channel_id: int, send_async, log, interval: float = 0.7):
        self._channel_id = channel_id
        self._send_async = send_async  # async (channel_id, text) -> None
        self._log = log
        self._interval = interval
        self._lines: list[str] = []
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            with self._lock:
                if not self._lines:
                    continue
                pending = self._lines
                self._lines = []
            # Pack into chunks of ≤ 1900 chars (Discord limit is 2000).
            chunks: list[str] = []
            current: list[str] = []
            current_len = 0
            for line in pending:
                add = len(line) + 1
                if current and current_len + add > 1900:
                    chunks.append("\n".join(current))
                    current, current_len = [], 0
                current.append(line)
                current_len += add
            if current:
                chunks.append("\n".join(current))
            for chunk in chunks:
                try:
                    await self._send_async(self._channel_id, chunk)
                except Exception as e:
                    self._log(f"audit flush failed: {e}")


def _thread_name(content: str, fallback: str = "conversation") -> str:
    """Build a Discord thread name from a message body.

    Discord caps thread names at 100 chars and disallows newlines. We trim
    to 80 to leave headroom and replace whitespace runs with single spaces.
    """
    s = (content or "").strip().replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())
    if not s:
        s = fallback
    return s[:80]


def _reply_payloads(agent_name: str, replies: list[str], limit: int = 1990) -> list[str]:
    """Build Discord-safe reply payloads with the agent prefix on each chunk."""
    prefix = f"**[{agent_name}]** "
    if len(prefix) >= limit:
        prefix = prefix[: max(0, limit - 1)]
    room = max(1, limit - len(prefix))
    payloads: list[str] = []
    for body in replies:
        text = body.strip()
        if not text:
            continue
        while text:
            chunk = text[:room]
            payloads.append(prefix + chunk)
            text = text[room:]
    return payloads


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
        # CommandTree owns Discord application (slash) commands. We register
        # one app command per `commands = [...]` entry in the manifest and
        # sync per-guild on connect for instant propagation. The legacy
        # text-prefix path in `_handle` still works in parallel — slash UI
        # fires `on_interaction`, not `on_message`, so they don't double-fire.
        self.tree = app_commands.CommandTree(self.client)
        self._wire()

    # event wiring -------------------------------------------------------

    def _wire(self) -> None:
        @self.client.event
        async def on_ready():
            self._register_service()
            self._log_subscribers()
            await self._register_app_commands()
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
            self._audit_buffer = _AuditBuffer(
                channel_id=self.audit_channel_id,
                send_async=self._audit_send,
                log=self._log,
            )
            self.dispatcher.audit_hook = self._make_audit_hook(self._audit_buffer)
            asyncio.create_task(self._audit_buffer.run_forever())
            self._log(f"audit channel: {self.audit_channel_id}")

    # app commands ------------------------------------------------------

    async def _register_app_commands(self) -> None:
        """Walk the registry and register one Discord application command per
        manifest `commands = [...]` entry, then sync.

        Sync strategy: per-guild for any guild in `allowed_guilds` (instant
        propagation), or global as a fallback (Discord caches up to 1h).
        Re-running this clears prior registrations on the tree first so a
        reconnect doesn't accumulate stale commands.

        Each app command takes a single optional `text` parameter — that's
        what gets passed to the function as `args`, matching the legacy
        text-prefix path. Per-command rich parameter schemas would require
        a manifest extension; out of scope for now.
        """
        # Make sure the registry is fresh — on_ready fires before any
        # message would have triggered _maybe_refresh_registry.
        self.dispatcher.registry.refresh()

        # Wipe any commands previously bound to this tree (in-memory).
        self.tree.clear_commands(guild=None)
        for gid in (self.allowed_guilds or []):
            self.tree.clear_commands(guild=discord.Object(id=gid))

        seen: dict[str, str] = {}
        for agent_name in sorted(self.dispatcher.registry.agents):
            m = self.dispatcher.registry.agents[agent_name]
            for fn in m.functions.values():
                for token in fn.commands:
                    name = token.lstrip("/").strip()
                    if not name:
                        continue
                    if name in seen:
                        self._log(
                            f"app command /{name}: collision between "
                            f"{seen[name]} and {agent_name}.{fn.name}; keeping first"
                        )
                        continue
                    qualified = f"{agent_name}.{fn.name}"
                    seen[name] = qualified
                    description = (fn.description or qualified)[:100]
                    self._add_app_command(name, description, qualified)

        if not seen:
            self._log("app commands: no /commands declared in manifests")
            return

        if self.allowed_guilds:
            for gid in self.allowed_guilds:
                guild = discord.Object(id=gid)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                self._log(
                    f"app commands: synced {len(synced)} to guild {gid} "
                    f"({', '.join('/' + c.name for c in synced)})"
                )
        else:
            synced = await self.tree.sync()
            self._log(
                f"app commands: synced {len(synced)} globally — propagation "
                f"may take up to 1h ({', '.join('/' + c.name for c in synced)})"
            )

    def _add_app_command(self, name: str, description: str, qualified: str) -> None:
        """Build and add one app command. Bound `qualified` via default-arg
        capture so the closure doesn't drift across loop iterations."""
        async def callback(
            interaction: "discord.Interaction",
            text: str = "",
            _qualified: str = qualified,
        ) -> None:
            await self._handle_app_command(interaction, _qualified, text)

        cmd = app_commands.Command(
            name=name,
            description=description,
            callback=callback,
        )
        self.tree.add_command(cmd)

    async def _handle_app_command(
        self,
        interaction: "discord.Interaction",
        qualified: str,
        text: str,
    ) -> None:
        """Dispatch an app command to a garden function and post the reply.

        Mirrors `_dispatch_all`: defers the interaction so we have up to 15
        minutes to reply, runs the impl on the gateway's executor (so app
        commands compete for the same `max_parallel` budget as channel
        messages), then posts the reply via followup.
        """
        # Same allow-listing as on_message — slash commands shouldn't bypass
        # the guild/DM filters.
        if interaction.guild is None:
            allowed = self.allowed_dm_users or set()
            if interaction.user.id not in allowed:
                await interaction.response.send_message(
                    "DMs aren't enabled for this user.", ephemeral=True
                )
                return
        elif (
            self.allowed_guilds is not None
            and interaction.guild_id not in self.allowed_guilds
        ):
            await interaction.response.send_message(
                "this guild isn't in the allow-list.", ephemeral=True
            )
            return

        await interaction.response.defer(thinking=True)

        params = {
            "message": text,
            "user_id": str(interaction.user.id),
            "user_name": interaction.user.display_name,
            "channel_id": str(interaction.channel_id) if interaction.channel_id else "",
            "guild_id": str(interaction.guild_id) if interaction.guild_id else "",
            "is_dm": interaction.guild is None,
            "message_id": "",
            "reply_to": None,
            "command": "/" + (interaction.command.name if interaction.command else ""),
            "args": text,
        }

        loop = asyncio.get_running_loop()

        def call_one() -> Optional[dict]:
            try:
                return self.dispatcher.call(qualified, params=params)
            except Exception as e:
                self._log(f"{qualified} raised: {e}")
                return {"reply": f"✗ {qualified} raised: {e}"}

        result = await loop.run_in_executor(self._executor, call_one)

        if not isinstance(result, dict) or result.get("silent"):
            await interaction.followup.send("(no reply)", ephemeral=True)
            return

        agent_name = qualified.partition(".")[0]
        replies: list[str] = []
        if isinstance(result.get("reply"), str):
            replies.append(result["reply"])
        replies.extend(r for r in (result.get("replies") or []) if isinstance(r, str))
        replies = [r for r in replies if r.strip()]
        if not replies:
            await interaction.followup.send("(no reply)", ephemeral=True)
            return

        for i, payload in enumerate(_reply_payloads(agent_name, replies)):
            try:
                if i == 0:
                    await interaction.followup.send(payload)
                else:
                    await interaction.channel.send(payload)
            except Exception as e:
                self._log(f"app command followup failed: {e}")

    async def _audit_send(self, channel_id: int, text: str) -> None:
        """Direct send used by the audit batcher — bypasses DiscordService so
        we don't double-bridge through run_coroutine_threadsafe (we're already
        on the asyncio loop here)."""
        channel = self.client.get_channel(channel_id)
        if channel is None:
            channel = await self.client.fetch_channel(channel_id)
        await channel.send(text)

    def _make_audit_hook(self, buffer: "_AuditBuffer"):
        """Build an audit hook that appends to the batch buffer.

        Called from dispatcher worker threads. `buffer.append` is thread-safe.
        Formatting is done synchronously (cheap); actual Discord send is
        coalesced by the buffer's flush loop.
        """
        def hook(event: dict) -> None:
            buffer.append(_format_audit_line(event))
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
        channel = message.channel
        # If the message arrives in a thread, route subscribers by parent
        # channel id (manifests subscribe to the parent, not per-thread).
        # Replies stay in the thread the user wrote in.
        parent_id = getattr(channel, "parent_id", None)
        is_thread = parent_id is not None
        lookup_id = str(parent_id) if is_thread else str(channel.id)

        await self._maybe_refresh_registry()

        reply_to = (
            str(message.reference.message_id)
            if message.reference and message.reference.message_id
            else None
        )
        params = {
            "message": message.content,
            "user_id": str(message.author.id),
            "user_name": message.author.display_name,
            "channel_id": str(channel.id),
            "guild_id": str(message.guild.id) if message.guild else "",
            "is_dm": message.guild is None,
            "message_id": str(message.id),
            "reply_to": reply_to,
        }

        # Slash commands short-circuit channel routing: if the first word
        # of the message matches a registered command, dispatch only to the
        # command subscribers — channel subscribers don't also see it.
        # Keeps `/push` in #developer from also waking up loam.respond.
        first_word = (message.content or "").split(maxsplit=1)
        command_subs = []
        if first_word and first_word[0].startswith("/"):
            command_subs = self.dispatcher.registry.command_subscribers_for(first_word[0])
        if command_subs:
            params = dict(params, command=first_word[0],
                          args=first_word[1] if len(first_word) > 1 else "")
            self._log(f"command {first_word[0]} -> " + ", ".join(q for q, _ in command_subs))
            target = await self._reply_target(message, is_thread)
            await self._dispatch_all(command_subs, params, target)
            return

        subs = self.dispatcher.registry.subscribers_for(lookup_id)
        if not subs:
            return
        self._log(f"channel {lookup_id} -> " + ", ".join(q for q, _ in subs))
        target = await self._reply_target(message, is_thread)
        await self._dispatch_all(subs, params, target)

    async def _reply_target(self, message, is_thread: bool):
        """Resolve where replies should be posted.

        - DM: post in the DM channel (Discord doesn't support DM threads).
        - Already-in-a-thread: keep replies in that thread (multi-turn).
        - Parent channel message: spawn a public thread from the user's
          message and post replies there. Concurrent conversations get
          their own threads instead of interleaving in the parent channel.

        Falls back to the original channel if thread creation fails (e.g.
        missing 'Create Public Threads' permission).
        """
        if message.guild is None or is_thread:
            return message.channel
        try:
            return await message.create_thread(
                name=_thread_name(message.content),
                auto_archive_duration=1440,
            )
        except Exception as e:
            self._log(f"thread create failed, replying in channel: {e}")
            return message.channel

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
            for payload in _reply_payloads(agent_name, replies):
                try:
                    await channel.send(payload)
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
