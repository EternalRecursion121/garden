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
