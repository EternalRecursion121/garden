"""Serve runtime documentation from the repo's `docs/` directory.

The sandbox binds `<garden_root>/docs/` RO into every agent's view, so
this works whether called from sandboxed or unsandboxed code. Resolution
order for the docs root:
  1. $GARDEN_DOCS  — explicit override (handy for tests).
  2. $GARDEN_ROOT/docs  — set by the sandbox harness; also set by the CLI.
  3. /root/garden/docs  — final fallback for ad-hoc invocation.
"""
from __future__ import annotations

import os
from pathlib import Path


def _docs_root() -> Path:
    explicit = os.environ.get("GARDEN_DOCS")
    if explicit:
        return Path(explicit)
    root = os.environ.get("GARDEN_ROOT")
    if root:
        return Path(root) / "docs"
    return Path("/root/garden/docs")


def _list_topics(docs: Path) -> list[str]:
    if not docs.is_dir():
        return []
    return sorted(p.stem for p in docs.glob("*.md"))


def topics(params, ctx):
    docs = _docs_root()
    return {"topics": _list_topics(docs), "docs_root": str(docs)}


def run(params, ctx):
    topic = params.get("topic")
    docs = _docs_root()
    available = _list_topics(docs)

    if not topic:
        return {
            "topics": available,
            "usage": "garden.help(topic='ctx') — pick one of the topics above",
        }

    path = docs / f"{topic}.md"
    if not path.exists():
        return {
            "error": f"no doc named {topic!r}",
            "available": available,
        }
    return {"topic": topic, "body": path.read_text(encoding="utf-8")}
