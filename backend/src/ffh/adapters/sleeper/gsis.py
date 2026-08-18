"""The one rule for Sleeper gsis_id hygiene, shared by `player_ref` and the lake catalog."""

from __future__ import annotations


def normalize_gsis_id(value: str | None) -> str | None:
    """Sleeper gsis_ids sometimes carry a leading space (" 00-0033873"): strip, and never
    hand the crosswalk an empty string."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
