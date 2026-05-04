"""Parses an `agent.toml` into typed structs.

The manifest is the only contract between an agent and the runtime. Anything
not declared here is the agent's private business.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .sandbox import SandboxConfig


@dataclass
class FunctionDef:
    name: str
    description: str = ""
    impl: str | None = None              # "relative/path.py:func"
    command: list[str] | None = None     # alternative: any executable
    params: dict[str, str] = field(default_factory=dict)
    schedule: str | None = None          # cron string
    channels: list[str] = field(default_factory=list)  # discord channel IDs (incl. DM channels)
    sandbox_override: bool | None = None  # None = use agent default; bool = override
    timeout: float | None = None         # seconds; only enforced for subprocess impls
    overlap: str = "skip"                # "skip" | "parallel"; scheduler-only


@dataclass
class AgentManifest:
    name: str
    folder: Path
    description: str = ""
    functions: dict[str, FunctionDef] = field(default_factory=dict)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)

    @classmethod
    def load(cls, agent_dir: Path) -> "AgentManifest":
        manifest_path = agent_dir / "agent.toml"
        with open(manifest_path, "rb") as f:
            data = tomllib.load(f)

        agent = data.get("agent", {})
        name = agent.get("name") or agent_dir.name
        # Sandbox is on by default. Missing `[agent.sandbox]` → default config
        # (enabled, network off, no env passthrough). Explicit `enabled = false`
        # returns a config whose `.enabled` is False.
        sandbox = SandboxConfig.parse(agent.get("sandbox"))

        functions: dict[str, FunctionDef] = {}
        for entry in data.get("function", []):
            overlap = entry.get("overlap", "skip")
            if overlap not in ("skip", "parallel"):
                raise ValueError(
                    f"{manifest_path}: function {entry.get('name')!r} has invalid "
                    f"overlap={overlap!r}; want 'skip' or 'parallel'"
                )
            fn = FunctionDef(
                name=entry["name"],
                description=entry.get("description", ""),
                impl=entry.get("impl"),
                command=entry.get("command"),
                params=entry.get("params", {}),
                schedule=entry.get("schedule"),
                channels=[str(c) for c in entry.get("channels", [])],
                sandbox_override=entry.get("sandbox"),
                timeout=float(entry["timeout"]) if entry.get("timeout") is not None else None,
                overlap=overlap,
            )
            if not fn.impl and not fn.command:
                raise ValueError(
                    f"{manifest_path}: function {fn.name!r} needs `impl` or `command`"
                )
            functions[fn.name] = fn

        return cls(
            name=name,
            folder=agent_dir,
            description=agent.get("description", ""),
            functions=functions,
            sandbox=sandbox,
        )
