"""Parses an `agent.toml` into typed structs.

The manifest is the only contract between an agent and the runtime. Anything
not declared here is the agent's private business.
"""

from __future__ import annotations

import tomllib
import re
from dataclasses import dataclass, field
from pathlib import Path

from .sandbox import SandboxConfig


_COMMAND_NAME_RE = re.compile(r"^[a-z0-9_-]{1,32}$")

# Allowed param-type tokens (with optional trailing '?' for "may be missing
# or None"). Keep this set small and predictable: validated at manifest-load
# time so typos in agent.toml fail there, and re-used by the dispatcher to
# type-check inbound params before invoking the impl.
_PARAM_TYPES: set[str] = {"string", "int", "float", "bool", "list", "dict", "any"}


def _split_param_spec(spec: str) -> tuple[str, bool]:
    """Return (base_type, optional). Doesn't validate — see _validate_param_spec."""
    optional = spec.endswith("?")
    return (spec[:-1] if optional else spec), optional


def _validate_param_spec(spec, name, fn_name, manifest_path) -> None:
    if not isinstance(spec, str) or not spec:
        raise ValueError(
            f"{manifest_path}: function {fn_name!r} param {name!r} type must be a "
            f"non-empty string, got {spec!r}"
        )
    base, _ = _split_param_spec(spec)
    if base not in _PARAM_TYPES:
        raise ValueError(
            f"{manifest_path}: function {fn_name!r} param {name!r} has unknown "
            f"type {spec!r}; want one of {sorted(_PARAM_TYPES)} "
            f"(append '?' to mark optional)"
        )


def param_value_matches(value, spec: str) -> bool:
    """True if `value` satisfies the manifest type token. `?`-optional is
    handled by the caller; this checks the base type only.

    Strict on bool/int boundary (bool isn't accepted as int, even though Python
    says `isinstance(True, int)`) — that's a subtle footgun and `int` should
    mean a real integer."""
    base, _ = _split_param_spec(spec)
    if base == "any":
        return True
    if base == "string":
        return isinstance(value, str)
    if base == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if base == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if base == "bool":
        return isinstance(value, bool)
    if base == "list":
        return isinstance(value, list)
    if base == "dict":
        return isinstance(value, dict)
    # Unreachable: _validate_param_spec rejects unknown bases at load time.
    return True


def _normalize_command(token, manifest_path, fn_name):
    """Validate a slash-command token from the manifest. Tokens must start with
    `/` and contain a single word — the gateway matches against the first
    whitespace-separated chunk of a Discord message."""
    if not isinstance(token, str) or not token.startswith("/") or len(token) < 2:
        raise ValueError(
            f"{manifest_path}: function {fn_name!r} has invalid command "
            f"{token!r}; want a string like '/push'"
        )
    if any(c.isspace() for c in token):
        raise ValueError(
            f"{manifest_path}: function {fn_name!r} command {token!r} "
            f"contains whitespace; commands are single tokens"
        )
    name = token[1:]
    if not _COMMAND_NAME_RE.fullmatch(name):
        raise ValueError(
            f"{manifest_path}: function {fn_name!r} command {token!r} "
            "must be 1-32 chars after '/', using only lowercase letters, "
            "digits, '_' or '-'"
        )
    return f"/{name}"


@dataclass
class FunctionDef:
    name: str
    description: str = ""
    impl: str | None = None              # "relative/path.py:func"
    command: list[str] | None = None     # alternative: any executable
    params: dict[str, str] = field(default_factory=dict)
    schedule: str | None = None          # cron string
    channels: list[str] = field(default_factory=list)  # discord channel IDs (incl. DM channels)
    commands: list[str] = field(default_factory=list)  # discord slash-command tokens, e.g. ["/push"]
    inbox: bool = False                  # subscribe to garden.message addressed to this agent (+ broadcast)
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
            params = entry.get("params", {}) or {}
            for pname, pspec in params.items():
                _validate_param_spec(pspec, pname, entry.get("name"), manifest_path)
            fn = FunctionDef(
                name=entry["name"],
                description=entry.get("description", ""),
                impl=entry.get("impl"),
                command=entry.get("command"),
                params=params,
                schedule=entry.get("schedule"),
                channels=[str(c) for c in entry.get("channels", [])],
                commands=[_normalize_command(c, manifest_path, entry.get("name"))
                          for c in entry.get("commands", [])],
                inbox=bool(entry.get("inbox", False)),
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
