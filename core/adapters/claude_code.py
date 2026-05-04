"""Claude Code CLI backend.

Shells out to `claude -p <prompt>`. Brings its own agent loop, tool use, and
permission handling — the function only supplies the prompt.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Any

from .base import Result


class ClaudeCode:
    def __init__(
        self,
        model: str | None = None,
        allowed_tools: list[str] | None = None,
        cwd: str | None = None,
        bin: str = "claude",
    ):
        self.model = model
        self.allowed_tools = allowed_tools or []
        self.cwd = cwd
        self.bin = bin
        if shutil.which(self.bin) is None:
            raise RuntimeError(f"`{self.bin}` CLI not found in PATH")

    def invoke(self, *, prompt: str, **kwargs: Any) -> Result:
        cmd = [self.bin, "-p", prompt]
        if self.model:
            cmd += ["--model", self.model]
        if self.allowed_tools:
            cmd += ["--allowedTools", ",".join(self.allowed_tools)]
        proc = subprocess.run(
            cmd, capture_output=True, text=True, cwd=self.cwd, check=False
        )
        if proc.returncode != 0:
            raise RuntimeError(f"claude exited {proc.returncode}: {proc.stderr}")
        return Result(text=proc.stdout)
