"""Hermes adapter — v1 stub.

Drop in an implementation when the first function needs it. The Backend
protocol is one method (invoke), so the surface area is small.
"""

from __future__ import annotations

from typing import Any

from .base import Result


class Hermes:
    def __init__(self, *args: Any, **kwargs: Any):
        raise NotImplementedError("hermes adapter is a v1 stub; implement when needed")

    def invoke(self, *, prompt: str, **kwargs: Any) -> Result:  # pragma: no cover
        raise NotImplementedError
