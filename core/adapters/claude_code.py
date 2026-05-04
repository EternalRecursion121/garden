"""Claude Code CLI backend.

Shells out to the official `claude` binary. Brings its own agent loop, tool
use, permission handling, session memory, and auto-compaction — the function
only supplies the prompt and (importantly) a stable session id.

Why session_id matters
----------------------
Anthropic's official harness depends on prompt caching for both speed and
TOS compliance. If we spawn `claude` fresh every call with no session
continuity, the system prompt (e.g. an agent's identity doc) is re-uploaded
each time and never cache-hits. Concretely, this:

  * costs the user 2–10× more compute per turn on a Pro/Max subscription
  * is what Anthropic specifically called out when restricting third-party
    harnesses ("outsized strain on infrastructure")
  * makes responses noticeably slower

The protocol: first call uses `--session-id <uuid>` (creates the session
with that deterministic ID). Subsequent calls use `--resume <uuid>` (which
hits cache and inherits prior context). We detect existence via the
session JSONL at `~/.claude/projects/<cwd-encoded>/<uuid>.jsonl`.

Auto-compaction
---------------
Claude Code auto-compacts internally — Session Memory writes summaries
continuously, and `/compact` loads the pre-written summary. The trigger
threshold is hardcoded server-side at ~150k tokens regardless of the
model's actual context window (a known issue for opus 4.7 with 1M
context). Pass `autocompact_pct=95` to set the undocumented
`CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` env var which pushes compaction later.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid as _uuid
from pathlib import Path
from typing import Any

from .base import Result


def stable_session_id(scope: str) -> str:
    """Derive a deterministic UUID for a given scope (e.g. an agent name).

    Same scope → same UUID across runs, so we never lose continuity even if
    the on-disk pointer gets blown away."""
    return str(_uuid.uuid5(_uuid.NAMESPACE_DNS, f"garden.session/{scope}"))


def _session_exists(session_id: str, cwd: str | None = None) -> bool:
    """Check whether claude has an on-disk record of this session.

    Claude stores session JSONLs at ~/.claude/projects/<cwd-encoded>/<uuid>.jsonl
    where cwd-encoded replaces '/' with '-' (leading dash preserved).
    """
    home = Path(os.environ.get("HOME", "/root"))
    target = (cwd or os.getcwd()).rstrip("/")
    encoded = target.replace("/", "-")
    return (home / ".claude" / "projects" / encoded / f"{session_id}.jsonl").exists()


class ClaudeCode:
    def __init__(
        self,
        model: str | None = None,
        allowed_tools: list[str] | None = None,
        cwd: str | None = None,
        bin: str = "claude",
        session_id: str | None = None,
        exclude_dynamic_sections: bool = True,
        autocompact_pct: int | None = None,
    ):
        self.model = model
        self.allowed_tools = allowed_tools or []
        self.cwd = cwd
        self.bin = bin
        self.session_id = session_id
        self.exclude_dynamic_sections = exclude_dynamic_sections
        self.autocompact_pct = autocompact_pct
        if shutil.which(self.bin) is None:
            raise RuntimeError(f"`{self.bin}` CLI not found in PATH")

    def invoke(
        self,
        *,
        prompt: str,
        system: str | None = None,
        timeout: float | None = 300.0,
        fork_on_overflow: bool = True,
        **kwargs: Any,
    ) -> Result:
        cmd = [self.bin, "-p", prompt]
        if self.session_id:
            # First call → create with this UUID; subsequent calls → resume.
            if _session_exists(self.session_id, self.cwd):
                cmd += ["--resume", self.session_id]
            else:
                cmd += ["--session-id", self.session_id]
        if self.exclude_dynamic_sections:
            cmd += ["--exclude-dynamic-system-prompt-sections"]
        if system:
            cmd += ["--append-system-prompt", system]
        if self.model:
            cmd += ["--model", self.model]
        if self.allowed_tools:
            cmd += ["--allowedTools", ",".join(self.allowed_tools)]

        env = os.environ.copy()
        if self.autocompact_pct is not None:
            env["CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"] = str(self.autocompact_pct)

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=self.cwd,
            check=False,
            timeout=timeout,
            env=env,
        )
        if proc.returncode != 0:
            stderr = proc.stderr or ""
            if fork_on_overflow and self.session_id and (
                "context" in stderr.lower() or "too long" in stderr.lower()
            ):
                # Resumed context is over the model limit — fork into a fresh
                # session-id and retry once. The new session is empty so the
                # prompt cache primes and continuity carries forward via the
                # auto-compact summary that claude wrote before failure.
                forked = stable_session_id(
                    f"{self.session_id}/fork-{int(__import__('time').time())}"
                )
                fork_cmd = [self.bin, "-p", prompt, "--session-id", forked]
                if self.exclude_dynamic_sections:
                    fork_cmd += ["--exclude-dynamic-system-prompt-sections"]
                if system:
                    fork_cmd += ["--append-system-prompt", system]
                if self.model:
                    fork_cmd += ["--model", self.model]
                proc = subprocess.run(
                    fork_cmd, capture_output=True, text=True,
                    cwd=self.cwd, check=False, timeout=timeout, env=env,
                )
                if proc.returncode == 0:
                    return Result(text=proc.stdout, raw={"forked_session": forked})
            raise RuntimeError(f"claude exited {proc.returncode}: {stderr}")
        return Result(text=proc.stdout)
