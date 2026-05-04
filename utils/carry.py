"""Thin wrapper around the `carry` CLI.

Carry is a Rust binary that owns the on-disk repo at <repo>/.carry/. We shell
out for everything; that keeps the data model honest (whatever carry assert
accepts is what we write) and avoids reimplementing the protocol.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Any


class CarryError(RuntimeError):
    pass


class Carry:
    # Hard cap on a single carry CLI invocation. A wedged carry shouldn't
    # block the gateway/scheduler indefinitely.
    _DEFAULT_TIMEOUT: float = 60.0

    def __init__(self, repo_path: Path | str, *, timeout: float | None = None):
        self.repo_path = Path(repo_path)
        self.timeout = self._DEFAULT_TIMEOUT if timeout is None else timeout
        # Carry's on-disk store isn't safe under concurrent CLI invocations
        # against the same .carry/ dir. Now that scheduler + gateway dispatch
        # in parallel, serialize CLI calls on this Carry instance.
        self._cli_lock = threading.Lock()

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

    def query(self, domain: str, *fields: str, **filters: Any) -> dict[str, dict[str, Any]]:
        """Query a domain. Returns `{did: {domain: {field: value}}}`.

        Uses `--format json` to avoid YAML parsing landmines when bodies
        contain markdown / JSON / quotes. List-valued fields stored via
        `_serialize` come back as JSON strings — best-effort decoded here.
        """
        args = ["carry", "query", domain]
        for k, v in filters.items():
            args.append(f"{k}={self._serialize(v)}")
        for f in fields:
            args.append(f)
        args += ["--format", "json"]
        out = self._run(args)
        try:
            rows = json.loads(out) if out.strip() else []
        except json.JSONDecodeError:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            did = row.pop("id", None) or row.pop("entity", None)
            if did is None:
                continue
            # Best-effort decode of fields that look like JSON containers.
            for k, v in list(row.items()):
                if isinstance(v, str) and v and v[0] in "[{":
                    try:
                        row[k] = json.loads(v)
                    except json.JSONDecodeError:
                        pass
            result[did] = {domain: row}
        return result

    def retract(self, domain: str, did: str, *fields: str) -> None:
        args = ["carry", "retract", domain, f"this={did}", *fields]
        self._run(args)

    def _run(self, args: list[str]) -> str:
        if not self.available():
            raise CarryError(
                "`carry` CLI not found in PATH. Install from "
                "https://github.com/tonk-labs/carry"
            )
        with self._cli_lock:
            try:
                result = subprocess.run(
                    args, cwd=self.repo_path, capture_output=True, text=True,
                    timeout=self.timeout if self.timeout > 0 else None,
                )
            except subprocess.TimeoutExpired as e:
                raise CarryError(
                    f"carry {' '.join(args[1:])} timed out after {self.timeout}s"
                ) from e
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
