#!/usr/bin/env python3
"""Single source of truth for "which TFT set are we on?" and the per-set file
paths, so onboarding a new set is config/data — never scattered edits.

Lightweight on purpose (stdlib + requests only) so the standalone patch scripts
and new_set.py can import it without pulling in refresh.py's heavy deps
(duckdb etc.). refresh.py keeps its own equivalent resolver for the same reason
(it must run even if this import graph changes), but both honor the identical
contract:

    active set  =  TFT_ACTIVE_SET env var
                >  newest set number published by CommunityDragon
                >  DEFAULT_ACTIVE_SET
"""
from __future__ import annotations

import os
from typing import Optional

try:
    import requests
except Exception:  # requests always present in this project, but stay defensive
    requests = None  # type: ignore

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA_DIR = os.path.join(ROOT, "public", "data")

CDRAGON_TFT = "https://raw.communitydragon.org/latest/cdragon/tft/en_us.json"
CDRAGON_PBE_TFT = "https://raw.communitydragon.org/pbe/cdragon/tft/en_us.json"
DEFAULT_ACTIVE_SET = 18


def detect_latest_cdragon_set(include_pbe: bool = False) -> Optional[int]:
    """Highest TFT set number CDragon publishes (live, optionally also PBE)."""
    if requests is None:
        return None
    urls = [CDRAGON_TFT] + ([CDRAGON_PBE_TFT] if include_pbe else [])
    best: Optional[int] = None
    for url in urls:
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            nums = [s.get("number") for s in resp.json().get("setData", [])
                    if isinstance(s.get("number"), int)]
            if nums:
                best = max(best or 0, max(nums))
        except Exception:
            continue
    return best


def resolve_active_set(force: Optional[int] = None) -> int:
    """TFT set the tooling should target (see module docstring for priority)."""
    if force:
        return int(force)
    env = os.environ.get("TFT_ACTIVE_SET", "").strip()
    if env.isdigit() and int(env) > 0:
        return int(env)
    return detect_latest_cdragon_set() or DEFAULT_ACTIVE_SET


# ── Per-set file paths ───────────────────────────────────────────────────────

def tftacademy_path(set_num: int) -> str:
    return os.path.join(HERE, f"tftacademy_set{set_num}.json")


def tftactics_path(set_num: int) -> str:
    return os.path.join(HERE, f"tftactics_set{set_num}.json")


def live_snapshot_path(region: str = "na1", tier: str = "challenger") -> str:
    """The homepage ("active set") snapshot alias — no set number in the name."""
    return os.path.join(DATA_DIR, f"snapshot_{region}_{tier}.json")


def set_snapshot_path(set_num: int, region: str = "na1", tier: str = "challenger") -> str:
    """The set-numbered snapshot copy (historical + current-set mirror)."""
    return os.path.join(DATA_DIR, f"snapshot_{region}_{tier}_{set_num}.json")


def snapshot_paths(set_num: int, region: str = "na1", tier: str = "challenger") -> list:
    """Both snapshots the meta page reads for the active set: the live alias and
    the set-numbered copy. Only the ones that exist on disk."""
    return [p for p in (live_snapshot_path(region, tier),
                        set_snapshot_path(set_num, region, tier)) if os.path.exists(p)]
