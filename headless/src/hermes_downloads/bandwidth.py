"""Deterministic whole-queue payload bandwidth allocation."""
from __future__ import annotations

__all__ = ["allocate"]


def allocate(
    *, global_limit_bps: int | None, item_caps_bps: list[int | None]
) -> list[int | None]:
    """Return capped equal shares; zero means wait and ``None`` means unlimited."""

    _require_limit(global_limit_bps, "global_limit_bps")
    if type(item_caps_bps) is not list:
        raise TypeError("item_caps_bps must be a list")
    for cap in item_caps_bps:
        _require_limit(cap, "item cap")
    if global_limit_bps is None:
        return item_caps_bps.copy()

    shares: list[int | None] = [0] * len(item_caps_bps)
    remaining = global_limit_bps
    active = list(range(len(item_caps_bps)))
    while active:
        quotient, remainder = divmod(remaining, len(active))
        capped = [
            index
            for position, index in enumerate(active)
            if item_caps_bps[index] is not None
            and item_caps_bps[index] <= quotient + (position < remainder)
        ]
        if not capped:
            for position, index in enumerate(active):
                shares[index] = quotient + (position < remainder)
            return shares
        for index in capped:
            cap = item_caps_bps[index]
            assert cap is not None
            shares[index] = cap
            remaining -= cap
            active.remove(index)
    return shares


def _require_limit(value: object, name: str) -> None:
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError(f"{name} must be a nonnegative integer or None")
