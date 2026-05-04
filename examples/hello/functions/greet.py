"""Example function impls.

Shows the two things every garden function does:
  1. Build context from params (here: just the name + tier).
  2. Pick a backend (here: openrouter, model chosen by `tier`).
  3. Invoke and return.

`greet_many` shows fan-out via `ctx.map`.
"""

from __future__ import annotations

from core.adapters.openrouter import OpenRouter


def run(params, ctx):
    tier = params.get("tier", "lite")
    model = (
        "anthropic/claude-haiku-4-5"
        if tier == "lite"
        else "anthropic/claude-opus-4.7"
    )
    backend = OpenRouter(model=model)
    result = backend.invoke(
        prompt=f"Greet {params['name']} warmly in one short sentence.",
        max_tokens=128,
    )
    return {"name": params["name"], "tier": tier, "greeting": result.text.strip()}


def greet_many(params, ctx):
    """Fan out greet() across many names. Each child run is its own garden.run
    with parent_run set to this run."""
    names = params["names"]
    results = ctx.map(
        "hello.greet",
        [{"name": n, "tier": "lite"} for n in names],
        max_workers=min(8, len(names)),
    )
    return {"count": len(results), "results": results}
