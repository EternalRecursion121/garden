from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class Result:
    text: str
    raw: dict[str, Any] = field(default_factory=dict)


class Backend(Protocol):
    def invoke(self, *, prompt: str, **kwargs: Any) -> Result: ...
