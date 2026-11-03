"""
Estimated token cost for pipeline stages that do not report a provider cost.

The pi runner returns its own ``usage.cost_usd``; ``opencode_client`` stages
(extract, red-flags) only return token counts, so their cost is derived from
``pricing.yaml``. Every rate there is explicitly an estimate.

Usage:
    from pricing import token_cost
    cost = token_cost("deepseek-v4.1-flash", prompt_tokens=1000, completion_tokens=200)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_PRICING_PATH = Path(__file__).resolve().parent / "pricing.yaml"


def load_pricing(path: str | Path | None = None) -> dict[str, Any]:
    """Load the pricing table (explicitly marked ``estimated: true``)."""
    source = Path(path) if path is not None else _PRICING_PATH
    with open(source, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def token_cost(
    model: str | None,
    prompt_tokens: int | float | None,
    completion_tokens: int | float | None,
    *,
    pricing: dict[str, Any] | None = None,
) -> float:
    """USD cost for ``prompt_tokens`` + ``completion_tokens`` at ``model``'s rates.

    Returns ``0.0`` when the model is unknown (the caller still gets a total,
    and the omission is visible as an un-bucketed difference) and when both
    token counts are zero.
    """
    prompt = float(prompt_tokens or 0)
    completion = float(completion_tokens or 0)
    if not model or (prompt == 0 and completion == 0):
        return 0.0
    table = pricing if pricing is not None else load_pricing()
    rates = (table.get("models") or {}).get(model)
    if not isinstance(rates, dict):
        return 0.0
    input_rate = float(rates.get("input_per_mtok", 0) or 0)
    output_rate = float(rates.get("output_per_mtok", 0) or 0)
    return (prompt * input_rate + completion * output_rate) / 1_000_000


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Estimate token cost from pricing.yaml")
    p.add_argument("model")
    p.add_argument("prompt_tokens", type=int)
    p.add_argument("completion_tokens", type=int)
    a = p.parse_args()
    print(f"{token_cost(a.model, a.prompt_tokens, a.completion_tokens):.6f}")
