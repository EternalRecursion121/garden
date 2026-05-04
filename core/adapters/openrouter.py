"""OpenRouter chat-completions backend.

Stdlib-only HTTP. One-shot completion — no agent loop. If a function wants a
loop it should write one itself, or pick a backend (claude_code, hermes) that
brings its own.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from .base import Result


class OpenRouter:
    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        base_url: str = "https://openrouter.ai/api/v1",
    ):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        self.base_url = base_url.rstrip("/")
        if not self.api_key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is not set; pass api_key= or export it."
            )

    def invoke(
        self,
        *,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 2048,
        **kwargs: Any,
    ) -> Result:
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        body = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            **kwargs,
        }
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/garden",
                "X-Title": "garden",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"openrouter HTTP {e.code}: {e.read().decode()}") from e

        text = data["choices"][0]["message"]["content"]
        return Result(text=text, raw=data)
