"""Discovers agents by scanning agents/<name>/agent.toml."""

from __future__ import annotations

from pathlib import Path

from .manifest import AgentManifest, FunctionDef


class Registry:
    def __init__(self, agents_dir: Path):
        self.agents_dir = Path(agents_dir)
        self.agents: dict[str, AgentManifest] = {}
        self.refresh()

    def refresh(self) -> None:
        self.agents.clear()
        if not self.agents_dir.exists():
            return
        for entry in sorted(self.agents_dir.iterdir()):
            if not entry.is_dir():
                continue
            if not (entry / "agent.toml").exists():
                continue
            manifest = AgentManifest.load(entry)
            if manifest.name in self.agents:
                prev = self.agents[manifest.name].folder
                raise ValueError(
                    f"duplicate agent name {manifest.name!r}: {prev} and {entry}"
                )
            self.agents[manifest.name] = manifest

    def lookup(self, qualified: str) -> tuple[AgentManifest, FunctionDef]:
        agent_name, _, fn_name = qualified.partition(".")
        if not fn_name:
            raise ValueError(
                f"function reference must be '<agent>.<function>', got {qualified!r}"
            )
        manifest = self.agents.get(agent_name)
        if manifest is None:
            raise KeyError(f"unknown agent: {agent_name!r}")
        fn = manifest.functions.get(fn_name)
        if fn is None:
            raise KeyError(f"agent {agent_name!r} has no function {fn_name!r}")
        return manifest, fn

    def all_scheduled(self) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for agent_name, m in self.agents.items():
            for fn in m.functions.values():
                if fn.schedule:
                    out.append((f"{agent_name}.{fn.name}", fn.schedule))
        return out

    def subscribers_for(self, channel_id: str) -> list[tuple[str, FunctionDef]]:
        """Functions that subscribed to a Discord channel ID via `channels = [...]`.

        Order is deterministic: agent name, then function declaration order.
        """
        out: list[tuple[str, FunctionDef]] = []
        for agent_name in sorted(self.agents):
            m = self.agents[agent_name]
            for fn in m.functions.values():
                if channel_id in fn.channels:
                    out.append((f"{agent_name}.{fn.name}", fn))
        return out

    def inbox_subscribers_for(self, recipient: str) -> list[tuple[str, FunctionDef]]:
        """Functions that subscribed to their agent's inbox via `inbox = true`.

        For `recipient == "broadcast"` returns every inbox-subscribing function
        across all agents. For a specific agent name, returns only that agent's
        inbox handlers. Order is deterministic: agent name, then declaration.
        """
        out: list[tuple[str, FunctionDef]] = []
        for agent_name in sorted(self.agents):
            if recipient != "broadcast" and recipient != agent_name:
                continue
            m = self.agents[agent_name]
            for fn in m.functions.values():
                if fn.inbox:
                    out.append((f"{agent_name}.{fn.name}", fn))
        return out

    def has_inbox_subscribers(self) -> bool:
        return any(fn.inbox for m in self.agents.values() for fn in m.functions.values())

    def command_subscribers_for(self, token: str) -> list[tuple[str, FunctionDef]]:
        """Functions that registered a slash-command token via `commands = [...]`.

        Tokens are matched verbatim (case-sensitive) against the first word of
        a Discord message. A command is global — it fires regardless of which
        channel the message came from, on top of the gateway's existing
        guild/DM allow-listing.
        """
        out: list[tuple[str, FunctionDef]] = []
        for agent_name in sorted(self.agents):
            m = self.agents[agent_name]
            for fn in m.functions.values():
                if token in fn.commands:
                    out.append((f"{agent_name}.{fn.name}", fn))
        return out
