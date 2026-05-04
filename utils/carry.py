"""Thin wrapper around the `carry` CLI.

Carry is a Rust binary that owns the on-disk repo at <repo>/.carry/. We shell
out for everything; that keeps the data model honest (whatever carry assert
accepts is what we write) and avoids reimplementing the protocol.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


class CarryError(RuntimeError):
    pass


class Carry:
    def __init__(self, repo_path: Path | str):
        self.repo_path = Path(repo_path)

    def available(self) -> bool:
        return shutil.which("carry") is not None

    def initialized(self) -> bool:
        return (self.repo_path / ".carry").exists()

    def init(self, label: str = "garden") -> None:
        if self.initialized():
            return
        self.repo_path.mkdir(parents=True, exist_ok=True)
        self._run(["carry", "init", label])

    def assert_(self, domain: str, **fields: Any) -> str:
        """Assert a claim. Pass `this=<DID>` in fields to update an existing entity.
        Returns the carry CLI's stdout (typically the entity DID)."""
        args = ["carry", "assert", domain]
        for k, v in fields.items():
            if v is None:
                continue
            args.append(f"{k}={self._serialize(v)}")
        return self._run(args).strip()

    def query(self, domain: str, *fields: str, **filters: Any) -> Any:
        args = ["carry", "query", domain]
        for k, v in filters.items():
            args.append(f"{k}={self._serialize(v)}")
        for f in fields:
            args.append(f)
        out = self._run(args)
        try:
            import yaml
            return yaml.safe_load(out)
        except Exception:
            return out

    def retract(self, domain: str, did: str, *fields: str) -> None:
        args = ["carry", "retract", domain, f"this={did}", *fields]
        self._run(args)

    def _run(self, args: list[str]) -> str:
        if not self.available():
            raise CarryError(
                "`carry` CLI not found in PATH. Install from "
                "https://github.com/tonk-labs/carry"
            )
        result = subprocess.run(
            args, cwd=self.repo_path, capture_output=True, text=True
        )
        if result.returncode != 0:
            raise CarryError(
                f"carry {' '.join(args[1:])} failed: {result.stderr.strip()}"
            )
        return result.stdout

    @staticmethod
    def _serialize(v: Any) -> str:
        if isinstance(v, (dict, list)):
            return json.dumps(v, default=str)
        if isinstance(v, bool):
            return "true" if v else "false"
        return str(v)
