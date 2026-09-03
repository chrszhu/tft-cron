#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# [7.0] DATA REFRESH SCRIPT — scripts/refresh.py
# ═══════════════════════════════════════════════════════════════════════════════
#
# This is the "backend worker" of the serverless architecture.
# It replaces Django's background scheduler (tft2/meta/scheduler.py).
#
# WHAT it does:
#   1. Fetches the challenger/GM/master ladder from Riot API
#   2. For each player, fetches their recent match history
#   3. Computes insights (top items, units, traits, placements)
#   4. Stores everything in PostgreSQL
#   5. Computes and caches comp archetypes (winning board clusters)
#
# WHY a standalone script (not part of Next.js):
#   Serverless functions have timeouts (10-60 seconds on Vercel).
#   This script takes 5-15 minutes to run due to Riot API rate limits.
#   It CAN'T run in a serverless function — it must run outside Vercel.
#
# HOW to run:
#   Development: python scripts/refresh.py --region na1
#   Production: Via cron job (GitHub Actions, etc.)
#
# MIGRATION STORY:
#   In Django, this was a background thread (threading.Thread) that ran
#   continuously inside the Django process (see tft2/meta/scheduler.py).
#   When we migrated to serverless, we extracted this logic into a
#   standalone script that runs on a schedule instead of continuously.
#
# 💡 Pro tip: Run with --ladder-only for a quick test (skips match insights)
# ⚠️ Watch out: Riot API has rate limits. The script handles 429 responses
#    with exponential backoff, but it still takes 5-15 minutes.
#
# 📚 Learn more: https://developer.riotgames.com/docs/tft
# ═══════════════════════════════════════════════════════════════════════════════
"""
Standalone TFT challenger refresh script.
No Django. No Render. Just psycopg2 + requests.

Usage:
    python scripts/refresh.py [--region na1] [--tier challenger]

Reads DATABASE_URL and RIOT_API_KEY from .env.local (project root) or environment.

Requirements:
    pip install psycopg2-binary requests python-dotenv
"""

import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from statistics import median as _median
from typing import Optional

import requests

# ── Load .env.local ───────────────────────────────────────────────────────────
_root = Path(__file__).resolve().parent.parent
for _env_file in [".env.local", ".env"]:
    _path = _root / _env_file
    if _path.exists():
        from dotenv import load_dotenv
        load_dotenv(_path, override=False)
        print(f"[refresh] Loaded env from {_path}")
        break

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.WARNING)

# ── Constants ─────────────────────────────────────────────────────────────────
CDRAGON_BASE = "https://raw.communitydragon.org/latest/cdragon/tft/en_us.json"
ALLOWED_TIERS = {"challenger", "grandmaster", "master"}
MAX_RETRIES = 5
BASE_DELAY = 1.0
REQUEST_DELAY = 0.15
FETCH_TIMEOUT = 30
PATCH_WINDOW_DAYS = 7   # rolling window for current-set refresh

# Historical backfills search the FULL set window rather than a trailing slice.
# Measured hit rates for current high-elo PUUIDs against old sets:
#   last 7 days of set  → 1-2% of players have any match
#   full set window     → 17-67% of players have matches
# Most of today's challengers weren't high-elo 1-2 years ago, so restricting to
# the final week of a set throws away ~95% of the recoverable data. Instead we
# scan the whole set and cap matches per player to bound API cost.
BACKFILL_MAX_MATCHES_PER_PLAYER = 25

# Backfills record every participant in each fetched match, not just the seed
# player. Anyone appearing in a match provably played that set, so this finds
# players regardless of their rank today — essential for old sets where few
# current challengers were active. Cap bounds memory and DB growth.
BACKFILL_MAX_HARVESTED_PLAYERS = 4000
BACKFILL_TARGET_BOARDS = 1000   # stop fetching more matches once this many boards are accumulated
# When seeding a freshly-launched set, re-resolve up to this many previous-set
# high-elo players (by Riot ID) to use as a confirmed high-elo crawl baseline.
SEED_PREV_SET_LIMIT = 200
PLACEHOLDER_ITEMS = {"TFT_Item_EmptyBag", "TFT_Item_Empty", ""}
NON_PLAYABLE_UNIT_MARKERS = {
    "PVE_", "FakeUnit", "TimebreakerCore", "TFT17_Summon",
    "TFT_BlueGolem", "TFT_TrainingDummy",
}
PLATFORM_ROUTING = {
    "na1": "americas", "br1": "americas", "la1": "americas", "la2": "americas",
    "oc1": "sea", "euw1": "europe", "eun1": "europe", "tr1": "europe",
    "ru": "europe", "kr": "asia", "jp1": "asia",
    "ph2": "sea", "sg2": "sea", "th2": "sea", "tw2": "sea", "vn2": "sea",
}

# ── DB connection ─────────────────────────────────────────────────────────────
_conn = None

def _get_conn():
    global _conn
    raw_url = os.environ.get("DATABASE_URL", "")
    if not raw_url:
        raise RuntimeError("DATABASE_URL is not set")
    # Strip channel_binding param — not supported by all pg versions
    db_url = re.sub(r"[&?]channel_binding=[^&]*", "", raw_url)
    try:
        if _conn is None or _conn.closed:
            raise Exception("reconnect")
        _conn.cursor().execute("SELECT 1")
    except Exception:
        try:
            if _conn and not _conn.closed:
                _conn.close()
        except Exception:
            pass
        _conn = psycopg2.connect(db_url)
        _conn.autocommit = False
    return _conn


def _execute(sql, params=None, fetch=None):
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            conn.commit()
            if fetch == "all":
                return cur.fetchall()
            if fetch == "one":
                return cur.fetchone()
    except Exception as exc:
        conn.rollback()
        print(f"[db] SQL error: {exc}")
    return None


def _ensure_schema():
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ladder_meta (
                platform TEXT NOT NULL, tier TEXT NOT NULL,
                fetched_at BIGINT, total_entries INT,
                PRIMARY KEY (platform, tier)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS challenger_players (
                platform TEXT NOT NULL, tier TEXT NOT NULL, puuid TEXT NOT NULL,
                league_points INT NOT NULL DEFAULT 0, summoner_id TEXT, summoner_name TEXT,
                wins INT DEFAULT 0, losses INT DEFAULT 0, rank_val TEXT,
                inactive BOOLEAN DEFAULT FALSE, fresh_blood BOOLEAN DEFAULT FALSE,
                hot_streak BOOLEAN DEFAULT FALSE, ladder_position INT DEFAULT 0,
                ladder_fetched_at BIGINT, insights JSONB, insights_error TEXT,
                insights_fetched_at BIGINT, profile_icon_id INT,
                insights_cursor BIGINT, set_number INT NOT NULL DEFAULT 0,
                PRIMARY KEY (platform, tier, puuid)
            )
        """)
        for ddl in [
            "ALTER TABLE challenger_players ADD COLUMN IF NOT EXISTS profile_icon_id INT",
            "ALTER TABLE challenger_players ADD COLUMN IF NOT EXISTS insights_cursor BIGINT",
            "ALTER TABLE challenger_players ADD COLUMN IF NOT EXISTS set_number INT NOT NULL DEFAULT 0",
            "ALTER TABLE ladder_meta ADD COLUMN IF NOT EXISTS set_number INT NOT NULL DEFAULT 0",
            "CREATE INDEX IF NOT EXISTS idx_challengers_lp ON challenger_players (platform, tier, league_points DESC)",
            """CREATE TABLE IF NOT EXISTS meta_cache (
                cache_key TEXT PRIMARY KEY, payload JSONB NOT NULL, computed_at BIGINT NOT NULL
            )""",
            # Stores backfilled insights for historical sets (PK includes set_number so
            # one player can have rows for multiple sets without conflicting with
            # challenger_players, whose PK is (platform, tier, puuid)).
            """CREATE TABLE IF NOT EXISTS historical_insights (
                platform     TEXT    NOT NULL,
                tier         TEXT    NOT NULL,
                puuid        TEXT    NOT NULL,
                set_number   INT     NOT NULL,
                summoner_name TEXT,
                insights     JSONB,
                computed_at  BIGINT  NOT NULL,
                PRIMARY KEY (platform, tier, puuid, set_number)
            )""",
            # Per-archetype full board list for on-demand "load more" paging.
            # One row per board; PK prefix (…, arch_id) makes offset/limit an
            # indexed range scan (cheap RU) instead of a JSONB scan.
            """CREATE TABLE IF NOT EXISTS archetype_boards (
                platform    TEXT NOT NULL,
                tier        TEXT NOT NULL,
                set_number  INT  NOT NULL,
                arch_id     TEXT NOT NULL,
                board_idx   INT  NOT NULL,
                placement   INT,
                board       JSONB NOT NULL,
                PRIMARY KEY (platform, tier, set_number, arch_id, board_idx)
            )""",
        ]:
            try:
                cur.execute(ddl)
            except Exception:
                conn.rollback()
    conn.commit()
    print("[db] Schema ready")


# ── CDragon catalog ───────────────────────────────────────────────────────────
def _fetch_catalog(active_set: int) -> dict:
    """Fetch trait/unit/item/augment name+icon maps from CDragon."""
    print("[catalog] Fetching from CDragon...")
    try:
        resp = requests.get(CDRAGON_BASE, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[catalog] WARNING: CDragon fetch failed ({e}), names may be humanized")
        return {"items": {}, "traits": {}, "units": {}, "augments": {}}

    sets = data.get("setData", [])
    set_data = next((s for s in sets if s.get("number") == active_set), None)
    if not set_data:
        set_data = max(sets, key=lambda s: s.get("number", 0), default={})

    items: dict = {}
    item_roles: dict = {}  # norm(displayName) → net offensive(+)/defensive(−) score
    for item in data.get("items", []):
        api = item.get("apiName") or ""
        name = item.get("name") or ""
        # CDragon uses "icon" (not "iconPath" or "squareIconPath")
        icon = item.get("icon") or item.get("iconPath") or item.get("squareIconPath") or ""
        if api and name:
            entry = {"name": name, "iconUrl": _normalize_icon(icon)}
            items[api] = entry
            if item.get("id") is not None:
                items[str(item["id"])] = entry
        comp = item.get("composition") or []
        if name and comp:
            score = 0
            for c in comp:
                n = _norm_key(c)
                if any(o in n for o in OFFENSIVE_COMPONENTS):
                    score += 1
                if any(dd in n for dd in DEFENSIVE_COMPONENTS):
                    score -= 1
            key = _norm_key(name)
            # Prefer a definitive (non-zero) classification when a display name
            # is shared by multiple item variants (some with empty recipes).
            if key not in item_roles or (item_roles[key] == 0 and score != 0):
                item_roles[key] = score

    # Item component recipes: norm(displayName) → [{name, iconUrl}]. Components
    # are themselves items in CDragon, so resolve each composition apiName back
    # to the items map. Used for carousel priority (aggregate components a comp
    # needs). Prefer the first variant that has a non-empty recipe.
    item_components: dict = {}
    for item in data.get("items", []):
        name = item.get("name") or ""
        comp = item.get("composition") or []
        if not (name and comp):
            continue
        key = _norm_key(name)
        if key in item_components:
            continue
        resolved = []
        for c in comp:
            ci = items.get(c) or items.get(str(c))
            resolved.append({
                "name": (ci or {}).get("name") or humanize_api_name(c),
                "iconUrl": (ci or {}).get("iconUrl"),
            })
        item_components[key] = resolved

    traits: dict = {}
    for trait in set_data.get("traits", []):
        api = trait.get("apiName") or ""
        name = trait.get("name") or ""
        icon = trait.get("icon") or trait.get("iconPath") or ""
        if api and name:
            traits[api] = {"name": name, "iconUrl": _normalize_icon(icon)}
            traits[api.lower()] = {"name": name, "iconUrl": _normalize_icon(icon)}

    units: dict = {}
    unit_ranges: dict = {}  # norm(displayName) → attack range (front vs back)
    for unit in set_data.get("champions", []):
        api = unit.get("apiName") or ""
        name = unit.get("name") or ""
        # CDragon uses "tileIcon" / "squareIcon" (not the *Path variants)
        icon = unit.get("tileIcon") or unit.get("squareIcon") or unit.get("tileIconPath") or ""
        cost = unit.get("cost", 0)
        rng = (unit.get("stats") or {}).get("range")
        if api and name:
            entry = {"name": name, "iconUrl": _normalize_icon(icon), "cost": cost, "range": rng}
            units[api] = entry
            units[api.lower()] = entry
            if isinstance(rng, (int, float)):
                unit_ranges[_norm_key(name)] = rng

    augments: dict = {}
    for aug in data.get("augments", []) or []:
        api = aug.get("apiName") or ""
        name = aug.get("name") or ""
        icon = aug.get("iconPath") or ""
        tier = aug.get("tier")
        if api and name:
            augments[api] = {"name": name, "iconUrl": _normalize_icon(icon), "tier": tier}
            augments[api.lower()] = {"name": name, "iconUrl": _normalize_icon(icon), "tier": tier}

    print(f"[catalog] Loaded {len(traits)} traits, {len(units)//2} units, {len(items)} items")
    return {
        "items": items, "traits": traits, "units": units, "augments": augments,
        "itemRoles": item_roles, "unitRanges": unit_ranges,
        "itemComponents": item_components,
    }


def _normalize_icon(path: str) -> Optional[str]:
    if not path:
        return None
    # CDragon paths use ASSETS/... (capital) and .tex extension;
    # lowercase everything and swap .tex → .png to get a valid URL.
    path = path.lower().replace("\\", "/")
    if path.endswith(".tex"):
        path = path[:-4] + ".png"
    # Strip leading /lol-game-data/ if present (some paths have it, some don't)
    if path.startswith("/lol-game-data/"):
        path = path[len("/lol-game-data/"):]
    return f"https://raw.communitydragon.org/latest/game/{path}"


# ── Utility ───────────────────────────────────────────────────────────────────
def humanize_api_name(api_name: str) -> str:
    name = re.sub(r"^TFT\d+_", "", api_name, flags=re.IGNORECASE)
    name = re.sub(r"^TFT_", "", name)
    return name.replace("_", " ").strip().title()


def _map_name(catalog: dict, api_name: str) -> str:
    entry = catalog.get(api_name) or catalog.get(api_name.lower())
    return entry["name"] if entry else (humanize_api_name(api_name) or api_name)


def _map_icon(catalog: dict, api_name: str) -> Optional[str]:
    entry = catalog.get(api_name) or catalog.get(api_name.lower())
    return entry["iconUrl"] if entry else None


def _is_non_playable(character_id: str) -> bool:
    return any(m in character_id for m in NON_PLAYABLE_UNIT_MARKERS)


# ── Suggested-board inference ──────────────────────────────────────────────────
# Component base names used to guess whether an item holder is a damage carry
# (offensive components) or a tank (defensive components). Kept in sync with the
# frontend so the precomputed layout matches the item-recipe classifier.
OFFENSIVE_COMPONENTS = ("bfsword", "recurvebow", "needlesslylargerod")
DEFENSIVE_COMPONENTS = ("chainvest", "negatroncloak", "giantsbelt")
BOARD_HOLDER_THRESHOLD = 50  # itemHolderPct to treat a unit as an item holder


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# Non-playable / summoned units that shouldn't appear on the suggested board
# (kept in sync with HIDDEN_UNIT_NAMES on the frontend).
HIDDEN_BOARD_UNITS = {
    "bia & bayin", "summon", "cosmic elder dragon", "cosmic gromp", "cosmic bruiser",
    "cosmic squid", "cosmic flutterbye", "cosmic scrapper", "mini black hole",
    "timebreakercore", "training dummy",
}


def _is_hidden_board_unit(name: str) -> bool:
    lower = (name or "").lower()
    return (
        lower in HIDDEN_BOARD_UNITS
        or lower.startswith("pve_")
        or (lower.startswith("cosmic ") and "dragon" in lower)
    )


def _classify_board_role(unit: dict, item_roles: dict) -> str:
    """Return 'carry', 'tank', or 'filler' for a unit from its item recipes."""
    if (unit.get("itemHolderPct") or 0) < BOARD_HOLDER_THRESHOLD:
        return "filler"
    score = 0
    for it in (unit.get("topItems") or [])[:3]:
        score += item_roles.get(_norm_key(it.get("name", "")), 0) * (it.get("count") or 1)
    return "carry" if score > 0 else "tank"


def _compute_board_layout(units: list, catalog: dict) -> list:
    """
    Infer a 4×7 board layout for a comp. The Riot API does not expose unit
    coordinates, so placement is heuristic:
      • front (melee, range ≤ 1) vs back (ranged) from CDragon stats.range
      • front-row tanks cluster center (branching out); a damage carry stuck in
        the front row goes to the top-left corner
      • back-row carries fill from the back-left (highest cost / main holder) right
    Returns a list of 4 rows × 7 cells, each cell a unit name or None.
    """
    item_roles = catalog.get("itemRoles", {})
    unit_ranges = catalog.get("unitRanges", {})

    def cost_of(u: dict) -> float:
        c = u.get("cost")
        return c if isinstance(c, (int, float)) and c > 0 else 99

    front, back = [], []
    for u in units:
        rng = unit_ranges.get(_norm_key(u.get("name", "")))
        (back if isinstance(rng, (int, float)) and rng > 1 else front).append(u)

    grid = [[None] * 7 for _ in range(4)]

    # ── Front row (row 0) ──────────────────────────────────────────────────────
    fc = sorted([u for u in front if _classify_board_role(u, item_roles) == "carry"], key=lambda u: -cost_of(u))
    ft = sorted([u for u in front if _classify_board_role(u, item_roles) == "tank"], key=lambda u: -cost_of(u))
    ff = sorted([u for u in front if _classify_board_role(u, item_roles) == "filler"], key=lambda u: -cost_of(u))
    row0 = grid[0]
    li = 0
    for u in fc:  # damage carry → top-left
        if li < 7:
            row0[li] = u.get("name")
            li += 1
    center_order = [3, 2, 4, 1, 5, 0, 6]

    def place_center(lst):
        for u in lst:
            p = next((s for s in center_order if row0[s] is None), None)
            if p is not None:
                row0[p] = u.get("name")
            else:
                o = next((i for i, v in enumerate(grid[1]) if v is None), None)
                if o is not None:
                    grid[1][o] = u.get("name")

    place_center(ft)   # main tank dead center, branching out
    place_center(ff)   # secondary melee fill around the center

    # ── Back row (row 3) ───────────────────────────────────────────────────────
    bh = sorted([u for u in back if _classify_board_role(u, item_roles) != "filler"], key=lambda u: -cost_of(u))
    bf = sorted([u for u in back if _classify_board_role(u, item_roles) == "filler"], key=lambda u: -cost_of(u))
    row3 = grid[3]
    bi = 0

    def place_back(lst):
        nonlocal bi
        for u in lst:
            if bi < 7:
                row3[bi] = u.get("name")  # back-left → right, main carry first
                bi += 1
            else:
                o = next((i for i, v in enumerate(grid[2]) if v is None), None)
                if o is not None:
                    grid[2][o] = u.get("name")

    place_back(bh)
    place_back(bf)
    return grid


# Standard TFT leveling curves + roll guidance per category, following the
# patterns used by high-level guides (e.g. bunnymuffins.lol): a level-by-round
# curve ("Lx @stage") plus SEPARATE roll/stop guidance (a condition, not a final
# chronological step). Curves reflect a win/mixed-streak baseline; on a hard loss
# streak each level typically comes ~1 round later.
LEVELING_GUIDE = {
    "1-Cost Reroll": {
        "curve": ["L4 @2-1", "L5 @2-5", "L6 @4-1", "L7 @5-1"],
        "roll": "Slow-roll at Lvl 4–5 through Stage 3. Priority is your MAIN carry to 3★ (the itemized unit) — the other 1-costs get 3★ for board strength and don't need items. All-in on 4-1 if the carry isn't hit. Stop rolling and start leveling once your carry is 3★ and the board is stable.",
    },
    "2-Cost Reroll": {
        "curve": ["L4 @2-1", "L5 @2-5", "L6 @3-2", "L7 @4-5", "L8 @5-2"],
        "roll": "Roll to stabilize at Lvl 6 on 3-2, then slow-roll at Lvl 6 for 3★s — your MAIN carry first (it takes the items), supports after for stats. Stop once your carry is 3★, then resume leveling. Go 9 late for a 5-cost.",
    },
    "3-Cost Reroll": {
        "curve": ["L4 @2-1", "L5 @2-5", "L6 @3-2", "L7 @4-1", "L8 @5+", "L9 @6+"],
        "roll": "Level 7 on 4-1 and slow-roll (down to ~30–50g) for 3★s — prioritize your MAIN carry (items go here); 3★ the others for board strength. Stop once your carry is 3★, then push levels.",
    },
    "Standard (Fast 8)": {
        "curve": ["L4 @2-1", "L5 @2-5", "L6 @3-1", "L7 @3-5", "L8 @4-2", "L9 @5-2"],
        "roll": "Hold econ (50g) until 8. Roll down on 8 (4-2) for your 4-cost carries. Stop at 2★ carries + full board; level 9 late if healthy.",
    },
    "Fast 9 / Legendaries": {
        "curve": ["L4 @2-1", "L5 @2-5", "L6 @3-1", "L7 @3-5", "L8 @4-2", "L9 @5-2"],
        "roll": "Hard econ — sacrifice Stage 4 rather than rolling on 8. Push level 9 by 5-1/5-2, then roll for your board. Only roll on 8 to stabilize if dying.",
    },
}


def _classify_comp_leveling(core_units: list, flex_units: list, catalog: dict) -> dict:
    """
    Categorize a comp into a leveling archetype (reroll vs carry vs fast 9) and
    attach the level-by-round curve + roll guidance plus the main carry/tank.

    Carry/tank are chosen from units that actually hold items in the harvested
    games (itemHolderPct), classified by whether their items are built from
    offensive components (carry) or defensive components (tank). "Main" = the
    most consistently itemized holder of that type (tie-break by cost).
    """
    item_roles = catalog.get("itemRoles", {})
    units = core_units + flex_units

    def cost(u: dict) -> int:
        c = u.get("cost")
        return int(c) if isinstance(c, (int, float)) and c > 0 else 0

    # Rank by how consistently the unit is itemized, then by cost.
    def holder_score(u: dict):
        return (u.get("itemHolderPct") or 0, cost(u))

    carries = [u for u in units if _classify_board_role(u, item_roles) == "carry"]
    tanks = [u for u in units if _classify_board_role(u, item_roles) == "tank"]
    carry = max(carries, key=holder_score) if carries else None
    tank = max(tanks, key=holder_score) if tanks else None

    # Reroll comps are defined by 3-starring a low-cost unit.
    reroll = [u for u in units if (u.get("threeStarPct") or 0) >= 35 and 1 <= cost(u) <= 3]
    if reroll:
        rc = max(reroll, key=lambda u: (u.get("threeStarPct") or 0, cost(u)))
        category = f"{cost(rc)}-Cost Reroll"
        carry = rc  # the reroll unit is the carry you invest in
    else:
        cc = cost(carry) if carry else 0
        category = "Fast 9 / Legendaries" if cc >= 5 else "Standard (Fast 8)"

    guide = LEVELING_GUIDE.get(category, {"curve": [], "roll": ""})
    return {
        "category": category,
        "levelCurve": guide["curve"],
        "rollGuidance": guide["roll"],
        "levelingPath": " · ".join(guide["curve"]),  # kept for backward compat
        "carryName": (carry or {}).get("name"),
        "tankName": (tank or {}).get("name"),
    }


# ── Riot API ──────────────────────────────────────────────────────────────────
class ApiKeyExpiredError(SystemExit):
    """Raised (exit code 2) when Riot returns 401/403 — key is invalid or expired."""
    def __init__(self, status: int):
        super().__init__(2)
        self.status = status


def _fetch(url: str, api_key: str) -> Optional[requests.Response]:
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers={"X-Riot-Token": api_key}, timeout=FETCH_TIMEOUT)
        except requests.RequestException:
            time.sleep(BASE_DELAY * (attempt + 1))
            continue
        if resp.ok:
            return resp
        if resp.status_code in (401, 403):
            print(f"\n[ERROR] Riot API returned {resp.status_code} — API key is invalid or expired.")
            print("[ERROR] Update RIOT_API_KEY in GitHub Secrets and re-run the workflow.")
            raise ApiKeyExpiredError(resp.status_code)
        if resp.status_code == 429:
            delay = float(resp.headers.get("Retry-After", BASE_DELAY * (attempt + 1)))
            time.sleep(delay)
            continue
        if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
            time.sleep(BASE_DELAY * (attempt + 1))
            continue
        return resp
    return None


def _fetch_ladder(platform: str, tier: str, api_key: str) -> list:
    url = f"https://{platform}.api.riotgames.com/tft/league/v1/{tier}?queue=RANKED_TFT"
    resp = _fetch(url, api_key)
    if not resp or not resp.ok:
        status = resp.status_code if resp else "timeout"
        body = ""
        try:
            body = resp.json() if resp else {}
        except Exception:
            pass
        raise RuntimeError(f"Ladder fetch failed (HTTP {status}): {body}")
    payload = resp.json()
    entries = payload.get("entries", [])
    if len(entries) < 5:
        print(f"[warn] Only {len(entries)} entries returned. Response keys: {list(payload.keys())}")
        print(f"[warn] This usually means the API key is expired. Get a new one at developer.riotgames.com")
    entries.sort(key=lambda e: -e.get("leaguePoints", 0))
    for i, e in enumerate(entries):
        e["ladderPosition"] = i + 1
    return entries


def _resolve_riot_id(routing: str, riot_id: str, api_key: str) -> Optional[str]:
    """Resolve a 'gameName#tagLine' Riot ID to a *current* PUUID.

    Stored PUUIDs go stale (Riot periodically rotates them), so match lookups on
    old PUUIDs silently return an empty list. Re-resolving from the Riot ID gives
    a fresh, valid PUUID. Returns None if the ID can't be resolved.
    """
    import urllib.parse
    if not riot_id or "#" not in riot_id:
        return None
    game_name, tag = riot_id.rsplit("#", 1)
    url = (f"https://{routing}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"
           f"{urllib.parse.quote(game_name)}/{urllib.parse.quote(tag)}")
    resp = _fetch(url, api_key)
    if resp and resp.ok:
        try:
            return resp.json().get("puuid")
        except Exception:
            return None
    return None


# Known TFT set time windows (UTC epoch seconds).
# Used by --backfill-set to scope match fetches to a specific set's live period.
# Update when new sets ship (approximate dates are fine — we filter by
# tft_set_number inside each match response too).
SET_TIME_WINDOWS: dict[int, tuple[int, int]] = {
    12: (1722384000, 1732060800),   # Set 12 Magic N' Mayhem:  Jul 31 2024 – Nov 20 2024
    13: (1732060800, 1743552000),   # Set 13 Into the Arcane:  Nov 20 2024 – Apr 02 2025
    14: (1743552000, 1753833600),   # Set 14 Cyber City:       Apr 02 2025 – Jul 30 2025
    15: (1753833600, 1764979200),   # Set 15 K.O. Coliseum:    Jul 30 2025 – Dec 03 2025
    16: (1764979200, 1776211200),   # Set 16 Lore & Legends:   Dec 03 2025 – Apr 15 2026
    17: (1776211200, 1787616000),   # Set 17 Space Gods:       Apr 15 2026 – Aug 25 2026
    18: (1787616000, 9999999999),   # Set 18 Enchanted Wilds:  Aug 25 2026 – present
}


def _fetch_match_ids(
    routing: str,
    puuid: str,
    api_key: str,
    since_ts_s: Optional[int],
    active_set: int,
    end_ts_s: Optional[int] = None,   # hard ceiling (used for backfill)
    ignore_patch_floor: bool = False,  # skip the 14-day rolling floor
    max_ids: int = 500,               # cap total IDs pulled (backfill uses a small cap)
) -> list:
    if ignore_patch_floor:
        # Backfill: use the set time window directly, no rolling floor
        floor_s = since_ts_s or 0
    else:
        # Current set: fetch the last PATCH_WINDOW_DAYS of matches.
        # If the player already has a cursor (last seen match), use that
        # instead so we only pull genuinely new matches.
        rolling_floor = int(time.time()) - PATCH_WINDOW_DAYS * 86400
        floor_s = max(since_ts_s or 0, rolling_floor)

    all_ids: list = []
    offset = 0
    while len(all_ids) < max_ids:
        n = min(200, max_ids - len(all_ids))
        url = (f"https://{routing}.api.riotgames.com/tft/match/v1/matches/"
               f"by-puuid/{puuid}/ids?count={n}&queue=1100&start={offset}&startTime={floor_s}")
        if end_ts_s:
            url += f"&endTime={end_ts_s}"
        resp = _fetch(url, api_key)
        if not resp or not resp.ok:
            break
        batch = resp.json()
        if not batch:
            break
        all_ids.extend(batch)
        if len(batch) < n:
            break
        offset += len(batch)
        time.sleep(REQUEST_DELAY)
    return all_ids


# ── Accumulator ───────────────────────────────────────────────────────────────
def _empty_acc() -> dict:
    return {
        "matchCount": 0, "winCount": 0, "top4Count": 0,
        "totalPlacement": 0, "totalLevel": 0, "totalGold": 0,
        "totalDamage": 0, "totalEliminated": 0, "totalLastRound": 0,
        "lowGoldMatches": 0, "placementCounts": {}, "traitCounts": {},
        "traitTotalPl": {}, "itemCounts": {}, "itemTotalPl": {},
        "unitCounts": {}, "unitTotalPl": {}, "unitTotal": 0, "itemTotal": 0,
        "oneStar": 0, "twoStar": 0, "threeStar": 0,
        "unitItemHolders": {}, "augmentCounts": {}, "topBoards": [],
        "placementSeq": [], "cursorTs": None,
    }


def _accumulate(acc: dict, participant: dict, catalog: dict, match_ts_s: int) -> None:
    pl = participant["placement"]
    if pl > 3:
        if match_ts_s and (acc["cursorTs"] is None or match_ts_s > acc["cursorTs"]):
            acc["cursorTs"] = match_ts_s
        return

    acc["matchCount"] += 1
    acc["totalPlacement"] += pl
    acc["totalLevel"] += participant.get("level", 0)
    acc["totalGold"] += participant.get("gold_left", 0)
    acc["totalDamage"] += participant.get("total_damage_to_players", 0)
    acc["totalEliminated"] += participant.get("players_eliminated", 0)
    acc["totalLastRound"] += participant.get("last_round", 0)
    acc["placementCounts"][str(pl)] = acc["placementCounts"].get(str(pl), 0) + 1
    if pl == 1:
        acc["winCount"] += 1
    if pl <= 4:
        acc["top4Count"] += 1
    if participant.get("gold_left", 0) <= 5:
        acc["lowGoldMatches"] += 1

    for trait in participant.get("traits", []):
        if trait.get("tier_current", 0) > 0:
            n = trait["name"]
            acc["traitCounts"][n] = acc["traitCounts"].get(n, 0) + 1
            acc["traitTotalPl"][n] = acc["traitTotalPl"].get(n, 0) + pl

    units_list = participant.get("units", [])
    for unit in units_list:
        cid = unit.get("character_id")
        if not cid or _is_non_playable(cid):
            continue
        acc["unitCounts"][cid] = acc["unitCounts"].get(cid, 0) + 1
        acc["unitTotalPl"][cid] = acc["unitTotalPl"].get(cid, 0) + pl
        acc["unitTotal"] += 1
        t = unit.get("tier")
        if t == 1: acc["oneStar"] += 1
        elif t == 2: acc["twoStar"] += 1
        elif t == 3: acc["threeStar"] += 1
        raw_items = unit.get("itemNames") or unit.get("items") or []
        unit_items = [str(i) for i in raw_items if str(i) not in PLACEHOLDER_ITEMS]
        acc["itemTotal"] += len(unit_items)
        for iname in unit_items:
            acc["itemCounts"][iname] = acc["itemCounts"].get(iname, 0) + 1
            acc["itemTotalPl"][iname] = acc["itemTotalPl"].get(iname, 0) + pl
            holder = acc["unitItemHolders"].setdefault(cid, {})
            holder[iname] = holder.get(iname, 0) + 1

    if len(acc["topBoards"]) < 50:
        board_units = []
        for u in units_list:
            cid2 = u.get("character_id", "")
            if not cid2:
                continue
            raw2 = u.get("itemNames") or u.get("items") or []
            items_clean = [
                {"name": _map_name(catalog["items"], str(i)), "iconUrl": _map_icon(catalog["items"], str(i))}
                for i in raw2 if str(i) not in PLACEHOLDER_ITEMS
            ]
            board_units.append({
                "name": _map_name(catalog["units"], cid2),
                "iconUrl": _map_icon(catalog["units"], cid2),
                "cost": (catalog["units"].get(cid2) or {}).get("cost"),
                "star": u.get("tier", 1),
                "items": items_clean,
            })
        active_traits = [
            {"name": _map_name(catalog["traits"], t["name"]), "tier": t.get("tier_current", 0)}
            for t in participant.get("traits", []) if t.get("tier_current", 0) > 0
        ]
        acc["topBoards"].append({
            "placement": pl, "units": board_units, "traits": active_traits,
            "augments": [str(a) for a in participant.get("augments", []) if a],
        })

    for aug in participant.get("augments", []):
        ap = acc["augmentCounts"].setdefault(aug, {"games": 0, "total": 0})
        ap["games"] += 1
        ap["total"] += pl

    if match_ts_s and (acc["cursorTs"] is None or match_ts_s > acc["cursorTs"]):
        acc["cursorTs"] = match_ts_s
    acc["placementSeq"].append(pl)


def _derive_insights(acc: dict, catalog: dict) -> dict:
    n = acc["matchCount"]
    if n == 0:
        return {}

    top_traits = sorted([
        {"name": _map_name(catalog["traits"], k), "games": v, "iconUrl": _map_icon(catalog["traits"], k)}
        for k, v in acc["traitCounts"].items()
    ], key=lambda x: -x["games"])[:15]

    top_items = sorted([
        {"name": _map_name(catalog["items"], k), "games": v, "iconUrl": _map_icon(catalog["items"], k)}
        for k, v in acc["itemCounts"].items() if k not in PLACEHOLDER_ITEMS
    ], key=lambda x: -x["games"])[:10]

    top_units = sorted([
        {"name": _map_name(catalog["units"], k), "games": v,
         "avgPlacement": acc["unitTotalPl"][k] / v,
         "iconUrl": _map_icon(catalog["units"], k),
         "cost": (catalog["units"].get(k) or {}).get("cost")}
        for k, v in acc["unitCounts"].items()
    ], key=lambda x: -x["games"])[:10]

    item_holders = []
    for unit_api, imap in acc["unitItemHolders"].items():
        if not imap:
            continue
        top3 = sorted(imap.items(), key=lambda x: -x[1])[:3]
        items_list = [{"name": _map_name(catalog["items"], iname), "iconUrl": _map_icon(catalog["items"], iname)}
                      for iname, _ in top3]
        if items_list:
            item_holders.append({
                "unitName": _map_name(catalog["units"], unit_api),
                "unitIconUrl": _map_icon(catalog["units"], unit_api),
                "items": items_list,
                "games": round(sum(imap.values()) / max(len(imap), 1)),
            })
    item_holders.sort(key=lambda x: -x["games"])

    seq = acc["placementSeq"]
    longest_top4 = longest_win = current_top4 = current_win = 0
    run_t4 = run_w = 0
    for i, pl in enumerate(seq):
        if pl <= 4:
            run_t4 += 1
            if i == 0: current_top4 = run_t4
        else:
            if i == 0: current_top4 = 0
            run_t4 = 0
        longest_top4 = max(longest_top4, run_t4)
        if pl == 1:
            run_w += 1
            if i == 0: current_win = run_w
        else:
            if i == 0: current_win = 0
            run_w = 0
        longest_win = max(longest_win, run_w)

    placements = []
    for pl_str, cnt in acc["placementCounts"].items():
        placements.extend([int(pl_str)] * cnt)

    avg_last_round = acc["totalLastRound"] / n
    avg_level = acc["totalLevel"] / n
    if avg_last_round >= 5.5 or avg_level >= 9:
        stage = "late (stage 5+)"
    elif avg_last_round <= 3.9 or avg_level <= 7:
        stage = "early (stage 3-4)"
    else:
        stage = "mid (stage 4)"

    derived = {
        "matchCount": n,
        "avgPlacement": acc["totalPlacement"] / n,
        "medianPlacement": float(_median(placements)) if placements else 0.0,
        "winRate": acc["winCount"] / n,
        "top4Rate": acc["top4Count"] / n,
        "placementCounts": acc["placementCounts"],
        "avgLevel": avg_level,
        "avgGoldLeft": acc["totalGold"] / n,
        "avgDamageToPlayers": acc["totalDamage"] / n,
        "avgPlayersEliminated": acc["totalEliminated"] / n,
        "avgLastRound": avg_last_round,
        "topTraits": top_traits,
        "topItems": top_items,
        "topUnits": top_units,
        "topAugments": sorted([
            {"name": (catalog["augments"].get(k, {}).get("name") or humanize_api_name(k)),
             "games": v["games"], "avgPlacement": v["total"] / v["games"],
             "tier": catalog["augments"].get(k, {}).get("tier"),
             "iconUrl": catalog["augments"].get(k, {}).get("iconUrl")}
            for k, v in acc["augmentCounts"].items()
        ], key=lambda x: -x["games"])[:5],
        "itemHolders": item_holders[:5],
        "traitsByPlacement": sorted([
            {"name": _map_name(catalog["traits"], k),
             "games": acc["traitCounts"][k],
             "avgPlacement": acc["traitTotalPl"][k] / acc["traitCounts"][k]}
            for k in acc["traitCounts"]
        ], key=lambda x: x["avgPlacement"])[:5],
        "itemsByPlacement": sorted([
            {"name": _map_name(catalog["items"], k), "games": acc["itemCounts"][k],
             "avgPlacement": acc["itemTotalPl"][k] / acc["itemCounts"][k],
             "iconUrl": _map_icon(catalog["items"], k)}
            for k in acc["itemCounts"] if k not in PLACEHOLDER_ITEMS
        ], key=lambda x: x["avgPlacement"])[:5],
        "unitStarDistribution": {"oneStar": acc["oneStar"], "twoStar": acc["twoStar"], "threeStar": acc["threeStar"]},
        "streaks": {"longestTop4": longest_top4, "longestWin": longest_win,
                    "currentTop4": current_top4, "currentWin": current_win},
        "rollDownSignal": {
            "lowGoldRate": acc["lowGoldMatches"] / n,
            "avgItemsPerMatch": acc["itemTotal"] / n,
            "avgItemsPerUnit": acc["itemTotal"] / acc["unitTotal"] if acc["unitTotal"] else 0,
            "stageEstimate": stage,
        },
        "carouselNote": "Carousel picks are not exposed in public match data.",
        "topBoards": acc["topBoards"],
    }
    raw_snapshot = {k: v for k, v in acc.items() if k != "placementSeq"}
    derived["_raw"] = True
    derived["patchStartTs"] = int(time.time()) - PATCH_WINDOW_DAYS * 86400
    derived.update(raw_snapshot)
    return derived


# ── Archetype clustering ──────────────────────────────────────────────────────
def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a and not b:
        return 1.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _item_components(item_name: str, catalog: dict) -> list:
    """Components (base items) that build the given full item."""
    return (catalog.get("itemComponents") or {}).get(_norm_key(item_name)) or []


def _compute_carousel_priority(core_units: list, catalog: dict, limit: int = 10) -> list:
    """
    Rank the components a comp wants across its item holders' BIS items, so a
    player can prioritize carousel picks. Counts each holder's top ~3 items.
    """
    tally: dict = {}
    icons: dict = {}
    for u in core_units:
        if (u.get("itemHolderPct") or 0) < BOARD_HOLDER_THRESHOLD:
            continue
        for it in (u.get("topItems") or [])[:3]:
            for c in _item_components(it.get("name", ""), catalog):
                cn = c.get("name")
                if not cn:
                    continue
                tally[cn] = tally.get(cn, 0) + 1
                if cn not in icons:
                    icons[cn] = c.get("iconUrl")
    ranked = sorted(
        [{"name": k, "iconUrl": icons.get(k), "count": v} for k, v in tally.items()],
        key=lambda x: -x["count"],
    )
    return ranked[:limit]


def _trim_board(b: dict) -> dict | None:
    """Trim a raw board to names only (icons resolved client-side)."""
    units = []
    for u in b.get("units", []):
        nm = u.get("name")
        if not nm:
            continue
        item_names = []
        for it in (u.get("items") or []):
            iname = it.get("name") if isinstance(it, dict) else str(it)
            if iname:
                item_names.append(iname)
        units.append({
            "name": nm,
            "cost": u.get("cost"),
            "star": u.get("star", 1),
            "items": item_names,
        })
    if not units:
        return None
    return {"placement": b.get("placement"), "units": units}


def _trim_boards_sorted(cluster_boards: list) -> list:
    """All boards in a cluster, best placements first, trimmed to names only."""
    ordered = sorted(cluster_boards, key=lambda b: (b.get("placement") or 9))
    out = []
    for b in ordered:
        tb = _trim_board(b)
        if tb:
            out.append(tb)
    return out


def _pick_example_boards(cluster_boards: list, limit: int = 12) -> list:
    """Representative real boards from a cluster (best placements first)."""
    return _trim_boards_sorted(cluster_boards)[:limit]


def _archetype_id(core_units: list) -> str:
    """Stable id for an archetype from its core unit set (order-independent)."""
    import hashlib
    key = "|".join(sorted(_norm_key(u.get("name", "")) for u in core_units if u.get("name")))
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


def _cluster_boards(boards: list, min_jaccard: float = 0.45, min_size: int = 2, catalog: dict | None = None) -> list:
    unit_sets = [frozenset(u["name"] for u in b.get("units", []) if u.get("name")) for b in boards]
    cluster_counts: list = []
    cluster_sizes: list = []
    cluster_members: list = []
    unit_to_clusters: dict = {}

    for i, units in enumerate(unit_sets):
        candidate_ids: set = set()
        for u in units:
            candidate_ids.update(unit_to_clusters.get(u, set()))
        best_j, best_sim = None, min_jaccard - 0.001
        for j in candidate_ids:
            n = cluster_sizes[j]
            centre = frozenset(u for u, c in cluster_counts[j].items() if c / n >= 0.4)
            sim = _jaccard(units, centre)
            if sim > best_sim:
                best_sim, best_j = sim, j
        if best_j is not None:
            cluster_members[best_j].append(i)
            cluster_sizes[best_j] += 1
            for u in units:
                cluster_counts[best_j][u] = cluster_counts[best_j].get(u, 0) + 1
                unit_to_clusters.setdefault(u, set()).add(best_j)
        else:
            j = len(cluster_members)
            cluster_members.append([i])
            cluster_sizes.append(1)
            cluster_counts.append({u: 1 for u in units})
            for u in units:
                unit_to_clusters.setdefault(u, set()).add(j)

    results = []
    for indices in cluster_members:
        if len(indices) < min_size:
            continue
        total = len(indices)
        cluster_boards = [boards[i] for i in indices]
        unit_data: dict = {}
        for board in cluster_boards:
            for unit in board.get("units", []):
                name = unit.get("name")
                if not name:
                    continue
                ud = unit_data.setdefault(name, {
                    "iconUrl": unit.get("iconUrl"), "cost": unit.get("cost"),
                    "count": 0, "itemBoardCount": 0, "items": {}, "threeStar": 0,
                })
                ud["count"] += 1
                if (unit.get("star") or 0) >= 3:
                    ud["threeStar"] += 1
                unit_items = unit.get("items", [])
                if len(unit_items) >= 2:
                    ud["itemBoardCount"] += 1
                for it in unit_items:
                    iname = it.get("name")
                    if iname:
                        ie = ud["items"].setdefault(iname, {"iconUrl": it.get("iconUrl"), "count": 0})
                        ie["count"] += 1
        trait_counts: dict = {}
        aug_counts: dict = {}  # api name -> {"count", "totalPl", "boards"}
        boards_with_aug = 0
        for board in cluster_boards:
            for t in board.get("traits", []):
                name = t.get("name")
                if name:
                    trait_counts[name] = trait_counts.get(name, 0) + 1
            board_augs = board.get("augments") or []
            if board_augs:
                boards_with_aug += 1
            bpl = board.get("placement") or 0
            for a in board_augs:
                ae = aug_counts.setdefault(a, {"count": 0, "totalPl": 0})
                ae["count"] += 1
                ae["totalPl"] += bpl
        core_units, flex_units = [], []
        for name, d in sorted(unit_data.items(), key=lambda x: -x[1]["count"]):
            pct = d["count"] / total
            top_items = sorted(
                [{"name": k, "iconUrl": v["iconUrl"], "count": v["count"]} for k, v in d["items"].items()],
                key=lambda x: -x["count"],
            )[:10]
            entry = {
                "name": name, "iconUrl": d["iconUrl"], "cost": d["cost"],
                "pct": min(100, round(pct * 100)), "count": d["count"],
                "itemHolderPct": round(d["itemBoardCount"] / d["count"] * 100) if d["count"] > 0 else 0,
                "threeStarPct": round(d["threeStar"] / d["count"] * 100) if d["count"] > 0 else 0,
                "topItems": top_items,
            }
            if pct >= 0.7:
                core_units.append(entry)
            elif pct >= 0.2:
                flex_units.append(entry)
        traits = sorted(
            [{"name": k, "count": v} for k, v in trait_counts.items() if v / total >= 0.3],
            key=lambda x: -x["count"],
        )
        arch = {"boardCount": total, "coreUnits": core_units, "flexUnits": flex_units, "traits": traits}
        # Per-archetype augment patterns. % is relative to boards that actually
        # recorded augments (older harvested boards may predate augment capture).
        if aug_counts and boards_with_aug > 0:
            aug_map = (catalog or {}).get("augments", {})
            top_augments = sorted(
                [{
                    "name": (aug_map.get(a, {}).get("name") or humanize_api_name(a)),
                    "iconUrl": aug_map.get(a, {}).get("iconUrl"),
                    "tier": aug_map.get(a, {}).get("tier"),
                    "count": e["count"],
                    "pct": round(e["count"] / boards_with_aug * 100),
                    "avgPlacement": round(e["totalPl"] / e["count"], 2) if e["count"] else None,
                } for a, e in aug_counts.items()],
                key=lambda x: (-x["count"], x["avgPlacement"] if x["avgPlacement"] is not None else 9),
            )
            # Keep meaningful patterns: appear in ≥20% of boards, min 2 boards.
            top_augments = [x for x in top_augments if x["pct"] >= 20 and x["count"] >= 2][:6]
            if top_augments:
                arch["topAugments"] = top_augments
                arch["augmentSampleBoards"] = boards_with_aug
        # Precompute the suggested-board layout so the frontend renders static
        # positions (no client-side inference / catalog dependency at render).
        if catalog:
            # Suggested board = core units only (the units that define the comp);
            # flex units are situational and left off to keep the board clean.
            board_units, seen = [], set()
            for u in core_units:
                if _is_hidden_board_unit(u["name"]) or u["name"] in seen:
                    continue
                seen.add(u["name"])
                board_units.append(u)
            arch["board"] = _compute_board_layout(board_units[:12], catalog)
            arch.update(_classify_comp_leveling(core_units, flex_units, catalog))
            # Carousel priority: aggregate the components a comp's item holders
            # need across their BIS items, ranked by how many are required.
            arch["carouselPriority"] = _compute_carousel_priority(core_units, catalog)
        # Stable id (from core units) so the frontend can page more boards from
        # the DB on demand without a JSONB scan.
        arch["id"] = _archetype_id(core_units)
        # Full trimmed board list for on-demand paging (written to the
        # archetype_boards table, then stripped from the cached payload). The
        # snapshot only keeps the first 20 as static example boards.
        all_boards = _trim_boards_sorted(cluster_boards)
        arch["_allBoards"] = all_boards
        arch["exampleBoards"] = all_boards[:20]
        results.append(arch)

    results.sort(key=lambda x: -x["boardCount"])
    return results[:30]


def _store_archetype_boards(platform: str, tier: str, set_num: int, archetypes: list):
    """
    Replace the full per-archetype board list for a set in archetype_boards.
    Wipes the set first so archetypes removed between runs don't leave stragglers.
    """
    from psycopg2.extras import execute_values

    all_rows = []
    for arch in archetypes:
        arch_id = arch.get("id")
        boards = arch.get("_allBoards") or []
        if not arch_id or not boards:
            continue
        for idx, b in enumerate(boards):
            all_rows.append((platform, tier, set_num, arch_id, idx,
                             b.get("placement"), json.dumps(b)))

    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM archetype_boards WHERE platform=%s AND tier=%s AND set_number=%s",
            [platform, tier, set_num],
        )
        # Batched multi-row inserts — one round-trip per chunk (vs. per row).
        for i in range(0, len(all_rows), 500):
            execute_values(
                cur,
                "INSERT INTO archetype_boards "
                "(platform, tier, set_number, arch_id, board_idx, placement, board) VALUES %s",
                all_rows[i:i + 500],
            )
    conn.commit()
    print(f"[archetypes] Stored {len(all_rows)} boards for on-demand paging (set {set_num})")


def _cache_archetypes(platform: str, tier: str, active_set: int, target_set: int | None = None,
                      catalog: dict | None = None):
    """
    Compute and cache comp archetypes for a given set.

    For the active set, reads from challenger_players.
    For historical sets, reads from historical_insights (which may contain
    data pooled from multiple tiers — all stored as 'challenger').
    """
    set_num = target_set if target_set is not None else active_set
    is_historical = (set_num != active_set)

    now = int(time.time() * 1000)
    patch_start = int(time.time()) - PATCH_WINDOW_DAYS * 86400

    if is_historical:
        rows = _execute(
            "SELECT COALESCE(insights->'topBoards', insights->'winBoards') AS boards "
            "FROM historical_insights "
            "WHERE platform=%s AND tier=%s AND insights IS NOT NULL AND set_number=%s",
            [platform, tier, set_num], fetch="all",
        )
    else:
        rows = _execute(
            "SELECT COALESCE(insights->'topBoards', insights->'winBoards') AS boards "
            "FROM challenger_players "
            "WHERE platform=%s AND tier=%s AND insights IS NOT NULL AND set_number=%s",
            [platform, tier, active_set], fetch="all",
        )
    all_boards: list = []
    player_count = 0
    for row in (rows or []):
        b = row["boards"] or []
        if b:
            player_count += 1
            all_boards.extend(b)

    if not all_boards:
        print(f"[archetypes] No boards found for set {active_set}")
        return

    if catalog is None:
        catalog = _fetch_catalog(set_num)
    archetypes = _cluster_boards(all_boards, catalog=catalog)
    # Persist each archetype's full board list for on-demand "load more" paging,
    # then strip the heavy field so meta_cache / snapshots stay lean.
    _store_archetype_boards(platform, tier, set_num, archetypes)
    for arch in archetypes:
        arch.pop("_allBoards", None)
    result = {
        "archetypes": archetypes,
        "totalBoards": len(all_boards),
        "playerCount": player_count,
        "cachedAt": now,
        "patchStartTs": patch_start,
        "patchWindowDays": PATCH_WINDOW_DAYS,
    }
    db_key = f"archetypes:{platform}:{tier}:{set_num}"
    _execute(
        "INSERT INTO meta_cache (cache_key, payload, computed_at) VALUES (%s, %s, %s) "
        "ON CONFLICT (cache_key) DO UPDATE SET payload=EXCLUDED.payload, computed_at=EXCLUDED.computed_at",
        [db_key, json.dumps(result), now],
    )
    print(f"[archetypes] Set {set_num}: {len(archetypes)} archetypes from {len(all_boards)} boards → cached")


# ── Static snapshot export ────────────────────────────────────────────────────
def _export_static_snapshot(platform: str, tier: str, active_set: int):
    """
    Write a static JSON snapshot to public/data/snapshot_{platform}_{tier}.json.

    This file is committed to git and deployed to Vercel's CDN so the frontend
    can fetch it directly (no serverless cold-start, no DB round-trip) on the
    initial page load.

    Snapshot contents (everything needed to render the full first-page view):
      • ladder    — page 1 of the challenger ladder (with insights)
      • globalSummary  — aggregated top items/units/traits
      • winningBoards  — comp archetypes from meta_cache
      • championExplorer — per-unit item frequency data
      • availableSets  — which TFT sets exist in the DB
    """
    import math

    PAGE_SIZE = 10

    print(f"\n[snapshot] Building static snapshot for {tier}@{platform} set {active_set}…")

    # ── 1. Ladder page 1 (from DB, with insights already written) ─────────────
    page1_rows = _execute(
        "SELECT * FROM challenger_players "
        "WHERE platform=%s AND tier=%s AND set_number=%s "
        "ORDER BY league_points DESC LIMIT %s",
        [platform, tier, active_set, PAGE_SIZE], fetch="all",
    ) or []

    total_row = _execute(
        "SELECT COUNT(*) AS cnt FROM challenger_players "
        "WHERE platform=%s AND tier=%s AND set_number=%s",
        [platform, tier, active_set], fetch="one",
    )
    total = int((total_row or {}).get("cnt", 0))

    def _row_to_entry(r: dict) -> dict:
        """Convert a DB row (snake_case) to the API response format (camelCase)."""
        ins = r.get("insights")
        return {
            "platform": r.get("platform"),
            "tier": r.get("tier"),
            "leaguePoints": r.get("league_points"),
            "puuid": r.get("puuid"),
            "summonerId": r.get("summoner_id"),
            "summonerName": r.get("summoner_name"),
            "wins": r.get("wins"),
            "losses": r.get("losses"),
            "rank": r.get("rank_val"),
            "inactive": r.get("inactive"),
            "freshBlood": r.get("fresh_blood"),
            "hotStreak": r.get("hot_streak"),
            "ladderPosition": r.get("ladder_position"),
            "insights": ins,
            "insightsError": r.get("insights_error"),
            "insightsFetchedAt": r.get("insights_fetched_at"),
            "profileIconId": r.get("profile_icon_id"),
            "insightsCursor": r.get("insights_cursor"),
            "setNumber": r.get("set_number"),
        }

    ladder = {
        "meta": {
            "region": platform,
            "tier": tier,
            "totalEntries": total,
            "page": 1,
            "pageSize": PAGE_SIZE,
            "totalPages": max(1, math.ceil(total / PAGE_SIZE)),
            "ladderSource": "cache",
            "activeSet": active_set,
        },
        "entries": [_row_to_entry(r) for r in page1_rows],
    }

    # ── 2. Global summary (aggregate topItems/topUnits/topTraits) ─────────────
    all_insights_rows = _execute(
        "SELECT insights FROM challenger_players "
        "WHERE platform=%s AND tier=%s AND insights IS NOT NULL AND set_number=%s",
        [platform, tier, active_set], fetch="all",
    ) or []

    item_map: dict = {}
    unit_map: dict = {}
    trait_map: dict = {}
    gs_player_count = 0

    for row in all_insights_rows:
        ins = row.get("insights") or {}
        top_items = ins.get("topItems") or []
        top_units = ins.get("topUnits") or []
        top_traits = ins.get("topTraits") or []
        if not top_items and not top_units and not top_traits:
            continue
        gs_player_count += 1
        for item in top_items:
            n = item.get("name")
            if not n:
                continue
            e = item_map.setdefault(n, {"games": 0, "iconUrl": item.get("iconUrl")})
            e["games"] += item.get("games", 0)
        for unit in top_units:
            n = unit.get("name")
            if not n:
                continue
            e = unit_map.setdefault(n, {"games": 0, "iconUrl": unit.get("iconUrl"), "cost": unit.get("cost")})
            e["games"] += unit.get("games", 0)
        for trait in top_traits:
            n = trait.get("name")
            if not n:
                continue
            e = trait_map.setdefault(n, {"games": 0, "iconUrl": trait.get("iconUrl")})
            e["games"] += trait.get("games", 0)

    def _top_n(d: dict, n: int = 20) -> list:
        return sorted(
            [{"name": k, **v} for k, v in d.items()],
            key=lambda x: -x.get("games", 0),
        )[:n]

    global_summary = {
        "topItems": _top_n(item_map),
        "topUnits": _top_n(unit_map),
        "topTraits": _top_n(trait_map),
        "playerCount": gs_player_count,
    }

    # ── 3. Winning boards (pre-computed archetypes from meta_cache) ────────────
    db_key = f"archetypes:{platform}:{tier}:{active_set}"
    archetype_row = _execute(
        "SELECT payload FROM meta_cache WHERE cache_key=%s",
        [db_key], fetch="one",
    )
    winning_boards = (archetype_row or {}).get("payload") or {
        "archetypes": [], "totalBoards": 0, "playerCount": 0
    }

    # ── 4. Champion explorer (per-unit item frequency) ─────────────────────────
    unit_data: dict = {}
    for row in all_insights_rows:
        ins = row.get("insights") or {}
        for holder in (ins.get("itemHolders") or []):
            uname = holder.get("unitName")
            if not uname:
                continue
            ud = unit_data.setdefault(uname, {
                "iconUrl": holder.get("unitIconUrl"),
                "games": 0,
                "items": {},
            })
            ud["games"] += holder.get("games") or 1
            for item in (holder.get("items") or []):
                iname = item.get("name")
                if iname:
                    ie = ud["items"].setdefault(iname, {"count": 0, "iconUrl": item.get("iconUrl")})
                    ie["count"] += 1

    champion_explorer = sorted(
        [
            {
                "unitName": uname,
                "unitIconUrl": data["iconUrl"],
                "games": data["games"],
                "cost": None,  # enriched client-side from catalog
                "topItems": sorted(
                    [{"name": k, "iconUrl": v["iconUrl"], "count": v["count"]}
                     for k, v in data["items"].items()],
                    key=lambda x: -x["count"],
                )[:10],
            }
            for uname, data in unit_data.items()
        ],
        key=lambda x: -x["games"],
    )

    # ── 5. Available sets ──────────────────────────────────────────────────────
    # Must include historical_insights, otherwise the UI can't know which
    # backfilled sets exist and leaves their tabs disabled.
    sets_rows = _execute(
        "SELECT DISTINCT set_number FROM challenger_players WHERE platform=%s AND tier=%s "
        "UNION "
        "SELECT DISTINCT set_number FROM historical_insights WHERE platform=%s AND tier=%s "
        "ORDER BY set_number",
        [platform, tier, platform, tier], fetch="all",
    ) or []
    available_sets = {
        "sets": [int(r.get("set_number", 0)) for r in sets_rows],
        "activeSet": active_set,
    }

    # Also write it as a standalone tiny file. The main snapshot is ~1 MB, so
    # gating tab rendering on it causes a visible 1-2 s delay where every
    # historical tab looks disabled. This file is a few hundred bytes.
    sets_dir = Path(__file__).resolve().parent.parent / "public" / "data"
    sets_dir.mkdir(parents=True, exist_ok=True)
    with open(sets_dir / f"sets_{platform}_{tier}.json", "w", encoding="utf-8") as f:
        json.dump(available_sets, f, separators=(",", ":"))
    print(f"[snapshot] Wrote sets_{platform}_{tier}.json → {available_sets['sets']}")

    # ── 6. Assemble and write ──────────────────────────────────────────────────
    snapshot = {
        "generatedAt": int(time.time() * 1000),
        "region": platform,
        "tier": tier,
        "setNum": active_set,
        "ladder": ladder,
        "globalSummary": global_summary,
        "winningBoards": winning_boards,
        "championExplorer": champion_explorer,
        "availableSets": available_sets,
    }

    # Resolve output path relative to this script: scripts/ → project root → public/data/
    out_dir = Path(__file__).resolve().parent.parent / "public" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"snapshot_{platform}_{tier}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, separators=(",", ":"), default=str)

    size_kb = out_path.stat().st_size // 1024
    print(f"[snapshot] Wrote {out_path.name} ({size_kb} KB)")
    print(f"[snapshot] Commit public/data/ and push to trigger a Vercel redeploy.")


def _export_historical_snapshot(platform: str, tier: str, active_set: int, target_set: int):
    """
    Write a static JSON snapshot for a historical TFT set.

    Reads from historical_insights (not challenger_players) so the shape is the
    same as the current-set snapshot but sourced from the backfill table.
    File: public/data/snapshot_{platform}_{tier}_{target_set}.json
    """
    import math

    PAGE_SIZE = 20

    print(f"\n[snapshot] Building historical snapshot for set {target_set}…")

    # ── 1. Ladder page 1 from historical_insights ─────────────────────────────
    page1_rows = _execute(
        "SELECT * FROM historical_insights "
        "WHERE platform=%s AND tier=%s AND set_number=%s "
        "ORDER BY summoner_name ASC LIMIT %s",
        [platform, tier, target_set, PAGE_SIZE], fetch="all",
    ) or []

    total_row = _execute(
        "SELECT COUNT(*) AS cnt FROM historical_insights "
        "WHERE platform=%s AND tier=%s AND set_number=%s",
        [platform, tier, target_set], fetch="one",
    )
    total = int((total_row or {}).get("cnt", 0))

    def _hist_row_to_entry(r: dict) -> dict:
        ins = r.get("insights")
        return {
            "platform": r.get("platform"),
            "tier": r.get("tier"),
            "leaguePoints": None,
            "puuid": r.get("puuid"),
            "summonerId": None,
            "summonerName": r.get("summoner_name"),
            "wins": None,
            "losses": None,
            "rank": None,
            "inactive": False,
            "freshBlood": False,
            "hotStreak": False,
            "ladderPosition": None,
            "insights": ins,
            "insightsError": None,
            "insightsFetchedAt": r.get("computed_at"),
            "profileIconId": None,
            "setNumber": r.get("set_number"),
        }

    ladder = {
        "meta": {
            "region": platform,
            "tier": tier,
            "totalEntries": total,
            "page": 1,
            "pageSize": PAGE_SIZE,
            "totalPages": max(1, math.ceil(total / PAGE_SIZE)),
            "ladderSource": "cache",
            "activeSet": active_set,
        },
        "entries": [_hist_row_to_entry(r) for r in page1_rows],
    }

    # ── 2. Global summary from historical_insights ────────────────────────────
    all_rows = _execute(
        "SELECT insights FROM historical_insights "
        "WHERE platform=%s AND tier=%s AND insights IS NOT NULL AND set_number=%s",
        [platform, tier, target_set], fetch="all",
    ) or []

    item_map: dict = {}
    unit_map: dict = {}
    trait_map: dict = {}
    gs_player_count = 0

    for row in all_rows:
        ins = row.get("insights") or {}
        top_items = ins.get("topItems") or []
        top_units = ins.get("topUnits") or []
        top_traits = ins.get("topTraits") or []
        if not top_items and not top_units and not top_traits:
            continue
        gs_player_count += 1
        for item in top_items:
            n = item.get("name")
            if n:
                e = item_map.setdefault(n, {"games": 0, "iconUrl": item.get("iconUrl")})
                e["games"] += item.get("games", 0)
        for unit in top_units:
            n = unit.get("name")
            if n:
                e = unit_map.setdefault(n, {"games": 0, "iconUrl": unit.get("iconUrl"), "cost": unit.get("cost")})
                e["games"] += unit.get("games", 0)
        for trait in top_traits:
            n = trait.get("name")
            if n:
                e = trait_map.setdefault(n, {"games": 0, "iconUrl": trait.get("iconUrl")})
                e["games"] += trait.get("games", 0)

    def _top_n(d: dict, n: int = 20) -> list:
        return sorted([{"name": k, **v} for k, v in d.items()], key=lambda x: -x.get("games", 0))[:n]

    global_summary = {
        "topItems": _top_n(item_map),
        "topUnits": _top_n(unit_map),
        "topTraits": _top_n(trait_map),
        "playerCount": gs_player_count,
    }

    # ── 3. Winning boards from meta_cache ─────────────────────────────────────
    db_key = f"archetypes:{platform}:{tier}:{target_set}"
    archetype_row = _execute("SELECT payload FROM meta_cache WHERE cache_key=%s", [db_key], fetch="one")
    winning_boards = (archetype_row or {}).get("payload") or {"archetypes": [], "totalBoards": 0, "playerCount": 0}

    # ── 4. Champion explorer from historical_insights ─────────────────────────
    unit_data: dict = {}
    for row in all_rows:
        ins = row.get("insights") or {}
        for holder in (ins.get("itemHolders") or []):
            uname = holder.get("unitName")
            if not uname:
                continue
            ud = unit_data.setdefault(uname, {"iconUrl": holder.get("unitIconUrl"), "games": 0, "items": {}})
            ud["games"] += holder.get("games") or 1
            for item in (holder.get("items") or []):
                iname = item.get("name")
                if iname:
                    ie = ud["items"].setdefault(iname, {"count": 0, "iconUrl": item.get("iconUrl")})
                    ie["count"] += 1

    champion_explorer = sorted(
        [
            {
                "unitName": uname,
                "unitIconUrl": data["iconUrl"],
                "games": data["games"],
                "cost": None,
                "topItems": sorted(
                    [{"name": k, "iconUrl": v["iconUrl"], "count": v["count"]} for k, v in data["items"].items()],
                    key=lambda x: -x["count"],
                )[:10],
            }
            for uname, data in unit_data.items()
        ],
        key=lambda x: -x["games"],
    )

    # ── 5. Assemble and write ─────────────────────────────────────────────────
    snapshot = {
        "generatedAt": int(time.time() * 1000),
        "region": platform,
        "tier": tier,
        "setNum": target_set,
        "ladder": ladder,
        "globalSummary": global_summary,
        "winningBoards": winning_boards,
        "championExplorer": champion_explorer,
    }

    out_dir = Path(__file__).resolve().parent.parent / "public" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"snapshot_{platform}_{tier}_{target_set}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, separators=(",", ":"), default=str)

    size_kb = out_path.stat().st_size // 1024
    print(f"[snapshot] Wrote {out_path.name} ({size_kb} KB, {total} players)")


def _promote_current(platform: str, tier: str, set_num: int):
    """
    Promote a set's per-set snapshot to be the *main* current-set snapshot.

    Used for a freshly-launched set whose ranked ladder is still empty: we
    harvest its comp data via the backfill path (into historical_insights) but
    still want it to be the default homepage view. This copies the per-set
    snapshot to snapshot_{platform}_{tier}.json (the file the frontend loads for
    the active set), stamps availableSets with activeSet=set_num, and rewrites
    the tiny sets_{platform}_{tier}.json so tabs mark it "Current".
    """
    out_dir = Path(__file__).resolve().parent.parent / "public" / "data"
    per_set = out_dir / f"snapshot_{platform}_{tier}_{set_num}.json"
    if not per_set.exists():
        print(f"[promote] {per_set.name} missing — nothing to promote.")
        return

    with open(per_set, "r", encoding="utf-8") as f:
        snap = json.load(f)

    # Build the available-sets list (union of both tables) so the tab bar is complete.
    sets_rows = _execute(
        "SELECT DISTINCT set_number FROM challenger_players WHERE platform=%s AND tier=%s "
        "UNION "
        "SELECT DISTINCT set_number FROM historical_insights WHERE platform=%s AND tier=%s "
        "ORDER BY set_number",
        [platform, tier, platform, tier], fetch="all",
    ) or []
    available_sets = {
        "sets": [int(r.get("set_number", 0)) for r in sets_rows],
        "activeSet": set_num,
    }

    snap["setNum"] = set_num
    snap["availableSets"] = available_sets
    if isinstance(snap.get("ladder"), dict) and isinstance(snap["ladder"].get("meta"), dict):
        snap["ladder"]["meta"]["activeSet"] = set_num

    with open(out_dir / f"snapshot_{platform}_{tier}.json", "w", encoding="utf-8") as f:
        json.dump(snap, f, separators=(",", ":"), default=str)
    with open(out_dir / f"sets_{platform}_{tier}.json", "w", encoding="utf-8") as f:
        json.dump(available_sets, f, separators=(",", ":"))

    print(f"[promote] Set {set_num} is now the current snapshot "
          f"(sets={available_sets['sets']}, active={set_num}).")


# ── Retag historical data ──────────────────────────────────────────────────────
def _retag_historical(platform: str, active_set: int):
    """Re-tag all set_number=0 (pre-tracking) rows as active_set, then recompute
    archetypes so historical and current data are merged into a single Set view.
    No Riot API calls needed — purely DB + CDragon."""

    print(f"\n[retag] Re-tagging pre-tracking data (set_number=0 → {active_set}) for {platform}")

    # Count rows to retag
    count_rows = _execute(
        "SELECT COUNT(*) AS n FROM challenger_players WHERE platform=%s AND set_number=0",
        [platform], fetch="one",
    )
    count = (count_rows or {}).get("n", 0)
    print(f"[retag] Found {count} pre-tracking players")

    if count == 0:
        print("[retag] Nothing to retag.")
        return

    # Re-tag: update set_number from 0 to active_set.
    # Use ON CONFLICT logic: if a player already exists for active_set, keep the
    # active_set row and just drop the pre-tracking duplicate.
    _execute(
        "UPDATE challenger_players SET set_number=%s "
        "WHERE platform=%s AND set_number=0",
        [active_set, platform],
    )
    print(f"[retag] Re-tagged {count} rows to set_number={active_set}")

    # Also retag ladder_meta if it exists for set_number=0
    _execute(
        "UPDATE ladder_meta SET set_number=%s WHERE platform=%s AND set_number=0",
        [active_set, platform],
    )

    # Recompute archetypes for all tiers now that we have more data
    for tier in ["challenger", "grandmaster", "master"]:
        rows_check = _execute(
            "SELECT COUNT(*) AS n FROM challenger_players "
            "WHERE platform=%s AND tier=%s AND set_number=%s AND insights IS NOT NULL",
            [platform, tier, active_set], fetch="one",
        )
        n = (rows_check or {}).get("n", 0)
        if n > 0:
            print(f"[retag] Recomputing archetypes for {tier} ({n} players)...")
            _cache_archetypes(platform, tier, active_set)

    print(f"\n[retag] Done! Pre-tracking data is now part of Set {active_set}.")
    print("[retag] Refresh your browser — no redeploy needed.")


# ── Backfill historical set data ──────────────────────────────────────────────
def _backfill_set(platform: str, target_set: int, tier: str = "all"):
    """
    Retroactively fetch match data for a historical TFT set using the PUUIDs
    already stored in the DB from current/past refresh runs.

    How it works:
      1. Read all PUUIDs from challenger_players for this platform.
      2. For each PUUID, fetch match IDs within the known time window for
         target_set (using startTime + endTime query params).
      3. Filter each match by tft_set_number to be safe.
      4. Compute insights and store with set_number = target_set.

    Limitation: you only get data for players whose PUUIDs are in the DB.
    The actual historical challenger ladder (who was rank #1 that season) is
    not recoverable — the Riot ladder API only returns current standings.
    But you do get "how did these high-elo players perform in Set X", which is
    what most historical set views show.
    """
    api_key = os.environ.get("RIOT_API_KEY", "").strip()
    if not api_key:
        print("ERROR: RIOT_API_KEY not set"); raise SystemExit(1)

    if target_set not in SET_TIME_WINDOWS:
        print(f"ERROR: No time window known for Set {target_set}.")
        print(f"Known sets: {sorted(SET_TIME_WINDOWS.keys())}")
        raise SystemExit(1)

    start_ts, end_ts = SET_TIME_WINDOWS[target_set]
    now = int(time.time())
    effective_end = min(end_ts, now)  # don't query into the future

    # Scan the full set window (start_ts stays as the set's start date) and cap
    # matches per player instead — see BACKFILL_MAX_MATCHES_PER_PLAYER.
    print(f"[backfill] Full set window, max {BACKFILL_MAX_MATCHES_PER_PLAYER} matches/player")

    print(f"\n[backfill] ── Set {target_set} | {platform} ──")
    print(f"[backfill] Time window: {time.strftime('%Y-%m-%d', time.gmtime(start_ts))} "
          f"→ {time.strftime('%Y-%m-%d', time.gmtime(effective_end))}")

    # Load catalog for the target set
    catalog = _fetch_catalog(target_set)

    # For historical backfills we pool ALL high-elo PUUIDs (challenger + grandmaster)
    # to maximise sample size — current challengers alone have a very low hit-rate
    # against sets from 1-2 years ago, since the player base turns over.
    # All results are stored under tier='challenger' in historical_insights since
    # we can't know a player's actual tier at the time of that historical set.
    run_tier = "challenger"  # storage tier label for historical_insights

    all_puuid_rows = _execute(
        """SELECT DISTINCT ON (puuid) puuid, summoner_name
           FROM challenger_players
           WHERE platform=%s AND tier IN ('challenger', 'grandmaster') AND puuid != ''
           ORDER BY puuid, insights_fetched_at DESC NULLS LAST""",
        [platform], fetch="all",
    ) or []

    if not all_puuid_rows:
        print(f"[backfill] No PUUIDs found for {platform} — skipping.")
    else:
        rows = all_puuid_rows
        print(f"[backfill] Pooled {len(rows)} unique PUUIDs (challenger + grandmaster)")

        # Resume support. We track *seeds* separately from harvested players:
        # harvesting writes thousands of participant rows into historical_insights,
        # so presence in that table no longer means a PUUID was used as a seed.
        seed_key = f"backfill_seeds:{platform}:{target_set}"
        seed_row = _execute(
            "SELECT payload FROM meta_cache WHERE cache_key=%s", [seed_key], fetch="one",
        )
        done_puuids = set((seed_row or {}).get("payload") or [])
        remaining = [r for r in rows if r["puuid"] not in done_puuids]

        if not remaining:
            print(f"[backfill] Set {target_set}: already complete ({len(rows)} players). Skipping.")
        else:
            routing = PLATFORM_ROUTING.get(platform)
            if not routing:
                print(f"[backfill] Unknown routing for {platform}")
            else:
                print(f"\n[backfill] Set {target_set}: {len(done_puuids)} already done, "
                      f"{len(remaining)} remaining…")
                rows = remaining

                # Every match contains 8 participants, all of whom demonstrably
                # played this set. Accumulating all of them instead of only the
                # seed player multiplies data ~8x for zero extra API calls, and
                # captures players who are no longer high-elo today (or never
                # were) — which is the only way to get depth on old sets.
                accs: dict[str, dict] = {}
                names: dict[str, str] = {}
                seen_matches: set[str] = set()

                processed_seeds: set[str] = set(done_puuids)

                def _flush(acc_map: dict[str, dict]) -> int:
                    written = 0
                    for pid, a in acc_map.items():
                        if a["matchCount"] == 0:
                            continue
                        ins = _derive_insights(a, catalog)
                        ins["_raw"] = True
                        ins["patchStartTs"] = start_ts
                        _execute(
                            """INSERT INTO historical_insights
                                (platform, tier, puuid, set_number, summoner_name, insights, computed_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT (platform, tier, puuid, set_number) DO UPDATE SET
                                 insights = EXCLUDED.insights,
                                 computed_at = EXCLUDED.computed_at""",
                            [platform, run_tier, pid, target_set,
                             names.get(pid), json.dumps(ins), int(time.time() * 1000)],
                        )
                        written += 1
                    # Persist seed progress so a timeout resumes instead of restarting.
                    _execute(
                        "INSERT INTO meta_cache (cache_key, payload, computed_at) VALUES (%s,%s,%s) "
                        "ON CONFLICT (cache_key) DO UPDATE SET payload=EXCLUDED.payload, "
                        "computed_at=EXCLUDED.computed_at",
                        [seed_key, json.dumps(sorted(processed_seeds)), int(time.time() * 1000)],
                    )
                    print(f"\n[backfill]   flushed {written} players "
                          f"({len(processed_seeds)} seeds done)")
                    return written

                for i, row in enumerate(rows):
                    # Flush at the TOP of the iteration. Putting it at the bottom
                    # meant the `continue` for seeds with no matches skipped it —
                    # and on old sets most seeds have no matches, so results
                    # accumulated in memory and were lost on timeout.
                    if i > 0 and i % 25 == 0:
                        _flush(accs)

                    # Stop early once we have enough boards — no point burning
                    # more API calls after the target is reached.
                    total_boards = sum(
                        a.get("matchCount", 0) for a in accs.values()
                    )
                    if total_boards >= BACKFILL_TARGET_BOARDS:
                        print(f"\n[backfill] Reached {total_boards} boards — stopping early.")
                        break

                    puuid = row["puuid"]
                    names.setdefault(puuid, row.get("summoner_name"))
                    processed_seeds.add(puuid)
                    time.sleep(REQUEST_DELAY)

                    match_ids = _fetch_match_ids(
                        routing, puuid, api_key,
                        since_ts_s=start_ts,
                        active_set=target_set,
                        end_ts_s=effective_end,
                        ignore_patch_floor=True,  # backfill ignores the rolling patch window
                        max_ids=BACKFILL_MAX_MATCHES_PER_PLAYER,
                    )

                    if not match_ids:
                        print(f"\r[backfill]   {i+1}/{len(rows)} — {len(accs)} players harvested",
                              end="", flush=True)
                        continue

                    for mid in match_ids:
                        if mid in seen_matches:
                            continue          # another seed already pulled this game
                        seen_matches.add(mid)
                        time.sleep(REQUEST_DELAY)
                        resp = _fetch(f"https://{routing}.api.riotgames.com/tft/match/v1/matches/{mid}", api_key)
                        if not resp or not resp.ok:
                            continue
                        match = resp.json()
                        info = match.get("info", {})
                        match_set = info.get("tft_set_number")
                        if match_set is not None and match_set != target_set:
                            continue
                        match_ts_s = info.get("game_datetime", 0) // 1000
                        for p in info.get("participants", []):
                            pid = p.get("puuid")
                            if not pid:
                                continue
                            if pid not in accs and len(accs) >= BACKFILL_MAX_HARVESTED_PLAYERS:
                                continue      # cap reached; keep updating known players only
                            _accumulate(accs.setdefault(pid, _empty_acc()), p, catalog, match_ts_s)

                    print(f"\r[backfill]   {i+1}/{len(rows)} — {len(accs)} players harvested "
                          f"from {len(seen_matches)} matches", end="", flush=True)

                found = _flush(accs)
                print()
                print(f"[backfill] Harvested {found} players from {len(seen_matches)} "
                      f"Set {target_set} matches (seeded by {len(rows)} known players).")

                if found > 0:
                    print(f"[backfill] Computing archetypes for Set {target_set}…")
                    active_set_num = int(os.environ.get("TFT_ACTIVE_SET", "17"))
                    _cache_archetypes(platform, run_tier, active_set=active_set_num, target_set=target_set, catalog=catalog)
                    _export_historical_snapshot(platform, run_tier, active_set_num, target_set)
                    # A freshly-launched set has no ranked ladder yet, so it's
                    # harvested via this backfill path. Promote it to be the
                    # default homepage snapshot when it's the active set.
                    if target_set == active_set_num:
                        _promote_current(platform, run_tier, target_set)

    print(f"\n[backfill] Done! Set {target_set} data is now in the DB.")
    print("[backfill] The UI set-selector will show it automatically on next page load.")


# ── Seed a freshly-launched set via BFS from the live ladder ───────────────────
def _seed_current_set(platform: str, active_set: int, tier: str = "all"):
    """
    Populate comp data for a just-launched set whose ranked ladder is still
    empty/tiny and whose stored historical PUUIDs are inactive.

    Strategy: start from whatever live ranked players exist (challenger →
    grandmaster → master), then breadth-first expand through the participants of
    their Set-N matches. Everyone in a Set-N match is provably an active player
    this set, so the crawl snowballs from a handful of seeds into a broad sample.

    Results are written to historical_insights (set_number = active_set) and then
    promoted to be the default homepage snapshot via _promote_current(), because
    the normal current-set pipeline (_run_tier → challenger_players) has no ladder
    to read yet.
    """
    from collections import deque

    api_key = os.environ.get("RIOT_API_KEY", "").strip()
    if not api_key:
        print("ERROR: RIOT_API_KEY not set"); raise SystemExit(1)

    routing = PLATFORM_ROUTING.get(platform)
    if not routing:
        print(f"ERROR: Unknown region '{platform}'"); raise SystemExit(1)

    start_ts, end_ts = SET_TIME_WINDOWS.get(active_set, (0, 9999999999))
    effective_end = min(end_ts, int(time.time()))
    catalog = _fetch_catalog(active_set)

    # Board target and high-elo seed count are env-overridable so a light 6-hour
    # refresh (defaults) and a heavier weekly deep-crawl can share this code.
    target_boards = int(os.environ.get("TFT_TARGET_BOARDS", BACKFILL_TARGET_BOARDS))
    prev_limit = int(os.environ.get("TFT_SEED_PREV_LIMIT", SEED_PREV_SET_LIMIT))
    print(f"[seed] target {target_boards} boards, up to {prev_limit} high-elo baseline seeds")

    # ── Seed from the live ranked ladder(s) ───────────────────────────────────
    tiers = ["challenger", "grandmaster", "master"] if tier == "all" else [tier]
    seeds: list[str] = []
    names: dict[str, str] = {}
    for t in tiers:
        try:
            entries = _fetch_ladder(platform, t, api_key)
        except Exception as e:
            print(f"[seed] {t} ladder fetch failed: {e}")
            entries = []
        for e in entries:
            pid = e.get("puuid")
            if pid:
                seeds.append(pid)
                if e.get("summonerName"):
                    names[pid] = e["summonerName"]
        print(f"[seed] {t}: {len(entries)} ranked players")

    # ── High-elo baseline: re-resolve previous-set challengers by Riot ID ──────
    # The live ladder for a brand-new set is nearly empty, so we anchor the crawl
    # on last set's top players — most of them are already grinding the new set.
    # Their stored PUUIDs are stale (Riot rotates them), so resolve fresh ones
    # from their Riot ID. These are PREPENDED so they're crawled first, keeping
    # the harvested sample biased toward high elo.
    prev_names = _execute(
        "SELECT summoner_name FROM challenger_players "
        "WHERE platform=%s AND tier IN ('challenger','grandmaster') "
        "AND summoner_name LIKE '%%#%%' "
        "ORDER BY league_points DESC LIMIT %s",
        [platform, prev_limit], fetch="all",
    ) or []
    if prev_names:
        print(f"[seed] Re-resolving {len(prev_names)} previous-set high-elo players "
              f"by Riot ID (for a high-elo baseline)…")
        baseline: list[str] = []
        seen_seed = set(seeds)
        for idx, r in enumerate(prev_names):
            nm = r.get("summoner_name")
            time.sleep(REQUEST_DELAY)
            fresh = _resolve_riot_id(routing, nm, api_key)
            if fresh and fresh not in seen_seed:
                baseline.append(fresh)
                seen_seed.add(fresh)
                names[fresh] = nm
            if (idx + 1) % 25 == 0:
                print(f"\r[seed]   resolved {idx+1}/{len(prev_names)} "
                      f"({len(baseline)} valid)", end="", flush=True)
        print(f"\n[seed] Got {len(baseline)} high-elo baseline seeds.")
        seeds = baseline + seeds   # high-elo players crawled first

    if not seeds:
        print("[seed] No live ranked players found yet — the set may be too fresh. "
              "Nothing to seed.")
        return

    print(f"\n[seed] Set {active_set}: BFS crawl from {len(seeds)} seed players "
          f"(target {target_boards} boards)…")

    queue: deque[str] = deque(seeds)
    queued: set[str] = set(seeds)
    accs: dict[str, dict] = {}
    seen_matches: set[str] = set()
    processed = 0
    DISCOVER_CAP = BACKFILL_MAX_HARVESTED_PLAYERS

    def _boards() -> int:
        return sum(a["matchCount"] for a in accs.values())

    def _flush() -> int:
        """Persist accumulated players to historical_insights so a long crawl is
        checkpointed and survives interruption."""
        written = 0
        for ppid, a in accs.items():
            if a["matchCount"] == 0:
                continue
            ins = _derive_insights(a, catalog)
            ins["_raw"] = True
            ins["patchStartTs"] = start_ts
            _execute(
                """INSERT INTO historical_insights
                    (platform, tier, puuid, set_number, summoner_name, insights, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (platform, tier, puuid, set_number) DO UPDATE SET
                     insights = EXCLUDED.insights, computed_at = EXCLUDED.computed_at""",
                [platform, "challenger", ppid, active_set,
                 names.get(ppid), json.dumps(ins), int(time.time() * 1000)],
            )
            written += 1
        return written

    while queue and _boards() < target_boards:
        if processed > 0 and processed % 10 == 0:
            n = _flush()
            print(f"\n[seed]   checkpoint: flushed {n} players ({_boards()} boards)")
        pid = queue.popleft()
        processed += 1
        time.sleep(REQUEST_DELAY)
        match_ids = _fetch_match_ids(
            routing, pid, api_key,
            since_ts_s=start_ts, active_set=active_set, end_ts_s=effective_end,
            ignore_patch_floor=True, max_ids=BACKFILL_MAX_MATCHES_PER_PLAYER,
        )
        for mid in match_ids:
            if mid in seen_matches:
                continue
            seen_matches.add(mid)
            time.sleep(REQUEST_DELAY)
            resp = _fetch(f"https://{routing}.api.riotgames.com/tft/match/v1/matches/{mid}", api_key)
            if not resp or not resp.ok:
                continue
            info = resp.json().get("info", {})
            if info.get("tft_set_number") not in (None, active_set):
                continue
            match_ts_s = info.get("game_datetime", 0) // 1000
            for p in info.get("participants", []):
                ppid = p.get("puuid")
                if not ppid:
                    continue
                # Discover: enqueue newly-seen active players for further crawling.
                if ppid not in queued and len(queued) < DISCOVER_CAP:
                    queue.append(ppid); queued.add(ppid)
                # Capture a display name from the match payload if we lack one.
                if ppid not in names and p.get("riotIdGameName"):
                    tag = p.get("riotIdTagline", "")
                    names[ppid] = f"{p['riotIdGameName']}#{tag}" if tag else p["riotIdGameName"]
                if ppid not in accs and len(accs) >= DISCOVER_CAP:
                    continue
                _accumulate(accs.setdefault(ppid, _empty_acc()), p, catalog, match_ts_s)
        print(f"\r[seed]   processed {processed} players · discovered {len(queued)} · "
              f"{len(seen_matches)} matches · {_boards()} boards", end="", flush=True)
    print()

    # ── Write harvested players to historical_insights (set = active_set) ──────
    written = _flush()
    print(f"[seed] Wrote {written} players ({_boards()} boards) from {len(seen_matches)} "
          f"Set {active_set} matches.")

    if written > 0:
        # Force the historical read path (active_set=0) so archetypes are computed
        # from historical_insights, where the fresh-set data actually lives.
        _cache_archetypes(platform, "challenger", active_set=0, target_set=active_set, catalog=catalog)
        _export_historical_snapshot(platform, "challenger", active_set, active_set)
        _promote_current(platform, "challenger", active_set)

    print(f"\n[seed] Done! Set {active_set} is now the current snapshot.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Refresh TFT challenger data (standalone, no Django)")
    parser.add_argument("--region", default="na1", help="Platform region (default: na1)")
    parser.add_argument("--tier", default="all",
                        help="Tier to fetch: challenger, grandmaster, master, or all (default: all)")
    parser.add_argument("--ladder-only", action="store_true", help="Only fetch ladder, skip insights")
    parser.add_argument("--retag-historical", action="store_true",
                        help="Re-tag pre-tracking (set_number=0) rows as the active set and recompute "
                             "archetypes. No Riot API calls needed.")
    parser.add_argument("--backfill-set", type=int, metavar="SET_NUM",
                        help="Retroactively fetch match data for a historical TFT set using "
                             "all PUUIDs already in the DB. Example: --backfill-set 16")
    parser.add_argument("--seed-current", action="store_true",
                        help="Seed a freshly-launched active set by BFS-crawling from the live "
                             "ladder (use when the ranked ladder is still empty/tiny).")
    args = parser.parse_args()

    platform = args.region.lower()
    tier = args.tier.lower()

    # Ensure DB tables exist once before running any tiers
    _ensure_schema()

    if args.retag_historical:
        active_set = int(os.environ.get("TFT_ACTIVE_SET", "17"))
        _retag_historical(platform, active_set)
        return

    if args.backfill_set:
        _backfill_set(platform, args.backfill_set, tier)
        return

    if args.seed_current:
        active_set = int(os.environ.get("TFT_ACTIVE_SET", "18"))
        _seed_current_set(platform, active_set, tier)
        return

    # "all" expands to all three top tiers
    if tier == "all":
        for t in ["challenger", "grandmaster", "master"]:
            _run_tier(platform, t, args.ladder_only)
        return

    _run_tier(platform, tier, args.ladder_only)


def _run_tier(platform: str, tier: str, ladder_only: bool):
    api_key = os.environ.get("RIOT_API_KEY", "").strip()
    if not api_key:
        print("ERROR: RIOT_API_KEY is not set in .env.local or environment")
        raise SystemExit(1)

    active_set = int(os.environ.get("TFT_ACTIVE_SET", "17"))
    routing = PLATFORM_ROUTING.get(platform)
    if not routing:
        print(f"ERROR: Unknown region '{platform}'")
        raise SystemExit(1)

    print(f"\n[refresh] ── {tier.upper()} | {platform} | Set {active_set} ──")

    # ── Step 1: Fetch ladder ──────────────────────────────────────────────────
    print("[refresh] Step 1/3: Fetching ladder...")
    entries = _fetch_ladder(platform, tier, api_key)
    print(f"[refresh] Got {len(entries)} challengers")

    now_ms = int(time.time() * 1000)
    conn = _get_conn()
    with conn.cursor() as cur:
        for entry in entries:
            cur.execute("""
                INSERT INTO challenger_players
                    (platform, tier, puuid, league_points, summoner_id, summoner_name,
                     wins, losses, rank_val, inactive, fresh_blood, hot_streak,
                     ladder_position, ladder_fetched_at, set_number)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (platform, tier, puuid) DO UPDATE SET
                    league_points=EXCLUDED.league_points,
                    summoner_id=EXCLUDED.summoner_id,
                    summoner_name=EXCLUDED.summoner_name,
                    wins=EXCLUDED.wins, losses=EXCLUDED.losses,
                    rank_val=EXCLUDED.rank_val, inactive=EXCLUDED.inactive,
                    fresh_blood=EXCLUDED.fresh_blood, hot_streak=EXCLUDED.hot_streak,
                    ladder_position=EXCLUDED.ladder_position,
                    ladder_fetched_at=EXCLUDED.ladder_fetched_at,
                    set_number=EXCLUDED.set_number
            """, (
                platform, tier,
                entry.get("puuid") or "",
                int(entry.get("leaguePoints", 0)),
                entry.get("summonerId") or "",
                entry.get("summonerName") or "",
                int(entry.get("wins", 0)),
                int(entry.get("losses", 0)),
                entry.get("rank", "I") or "I",
                bool(entry.get("inactive", False)),
                bool(entry.get("freshBlood", False)),
                bool(entry.get("hotStreak", False)),
                int(entry.get("ladderPosition", 0)),
                now_ms,
                active_set,
            ))
        cur.execute("""
            INSERT INTO ladder_meta (platform, tier, fetched_at, total_entries, set_number)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (platform, tier) DO UPDATE SET
                fetched_at=EXCLUDED.fetched_at, total_entries=EXCLUDED.total_entries
        """, [platform, tier, now_ms, len(entries), active_set])
    conn.commit()
    print(f"[refresh] Stored {len(entries)} challengers in DB")

    if ladder_only:
        print("[refresh] --ladder-only: done.")
        return

    # ── Step 2: Fetch catalog from CDragon ────────────────────────────────────
    catalog = _fetch_catalog(active_set)

    # ── Step 3: Fetch insights for every player ───────────────────────────────
    print(f"\n[refresh] Step 2/3: Fetching match insights ({len(entries)} players)...")
    skipped = 0
    RECENT_MS = 24 * 60 * 60 * 1000

    # Load existing rows so we can do incremental updates
    existing_rows = _execute(
        "SELECT puuid, summoner_name, profile_icon_id, insights, insights_fetched_at, insights_cursor "
        "FROM challenger_players WHERE platform=%s AND tier=%s AND set_number=%s",
        [platform, tier, active_set], fetch="all",
    ) or []
    existing_map = {r["puuid"]: r for r in existing_rows}

    for i, entry in enumerate(entries):
        puuid = entry.get("puuid", "")
        if not puuid:
            continue

        existing = existing_map.get(puuid, {})
        fetched_at = existing.get("insights_fetched_at") or 0
        existing_insights = existing.get("insights")
        has_name = bool(existing.get("summoner_name"))
        has_icon = existing.get("profile_icon_id") is not None

        # Skip recently refreshed players
        if (now_ms - fetched_at) < RECENT_MS and has_name and has_icon:
            if existing_insights and existing_insights.get("matchCount", 0) > 0:
                skipped += 1
                print(f"\r[refresh]   {i+1}/{len(entries)} players done ({skipped} skipped)", end="", flush=True)
                continue

        time.sleep(REQUEST_DELAY)

        # Fetch account name
        account_resp = _fetch(
            f"https://{routing}.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}", api_key)
        account_name = None
        if account_resp and account_resp.ok:
            acc_data = account_resp.json()
            if acc_data.get("gameName"):
                tag = acc_data.get("tagLine", "")
                account_name = f"{acc_data['gameName']}#{tag}" if tag else acc_data["gameName"]

        # Fetch profile icon
        profile_icon_id = None
        summoner_resp = _fetch(
            f"https://{platform}.api.riotgames.com/tft/summoner/v1/summoners/by-puuid/{puuid}", api_key)
        if summoner_resp and summoner_resp.ok:
            s = summoner_resp.json()
            if s.get("profileIconId") is not None:
                profile_icon_id = int(s["profileIconId"])

        # Fetch match IDs (incremental: only since last cursor)
        cursor_ts = existing.get("insights_cursor")
        match_ids = _fetch_match_ids(routing, puuid, api_key, cursor_ts, active_set)

        # Build/restore accumulator
        acc = _empty_acc()
        patch_start = int(time.time()) - PATCH_WINDOW_DAYS * 86400
        stored_patch = (existing_insights or {}).get("patchStartTs") if existing_insights else None
        patch_changed = stored_patch != patch_start

        if existing_insights and existing_insights.get("_raw") and not patch_changed:
            for key in acc:
                if key in existing_insights:
                    ev = existing_insights[key]
                    if isinstance(acc[key], dict) and isinstance(ev, dict):
                        acc[key].update(ev)
                    elif isinstance(acc[key], list) and isinstance(ev, list):
                        acc[key] = ev[:]
                    else:
                        acc[key] = ev
            acc["topBoards"] = acc["topBoards"][:50]

        # Process new matches
        for mid in match_ids:
            time.sleep(REQUEST_DELAY)
            resp = _fetch(f"https://{routing}.api.riotgames.com/tft/match/v1/matches/{mid}", api_key)
            if not resp or not resp.ok:
                continue
            match = resp.json()
            match_set = match.get("info", {}).get("tft_set_number")
            if match_set is not None and match_set != active_set:
                continue
            participant = next(
                (p for p in match.get("info", {}).get("participants", []) if p.get("puuid") == puuid),
                None
            )
            if not participant:
                continue
            match_ts_s = match.get("info", {}).get("game_datetime", 0) // 1000
            _accumulate(acc, participant, catalog, match_ts_s)

        insights = _derive_insights(acc, catalog) if acc["matchCount"] > 0 else None
        error = None if insights else "no_matches"

        # Write back to DB
        extra_parts, extra_params = [], []
        if account_name:
            extra_parts.append("summoner_name=%s"); extra_params.append(account_name)
        if profile_icon_id is not None:
            extra_parts.append("profile_icon_id=%s"); extra_params.append(profile_icon_id)
        if acc["cursorTs"] is not None:
            extra_parts.append("insights_cursor=%s"); extra_params.append(acc["cursorTs"])
        extra_sql = (", " + ", ".join(extra_parts)) if extra_parts else ""
        _execute(
            f"UPDATE challenger_players SET insights=%s, insights_error=%s, insights_fetched_at=%s{extra_sql} "
            f"WHERE platform=%s AND tier=%s AND puuid=%s",
            [json.dumps(insights) if insights else None, error, int(time.time() * 1000),
             *extra_params, platform, tier, puuid],
        )

        print(f"\r[refresh]   {i+1}/{len(entries)} players done ({skipped} skipped)", end="", flush=True)

    print()  # newline after \r

    # ── Step 4: Cache archetypes ──────────────────────────────────────────────
    print("\n[refresh] Step 3/3: Computing comp archetypes...")
    _cache_archetypes(platform, tier, active_set, catalog=catalog)

    # ── Step 5: Export static snapshot ───────────────────────────────────────
    # Writes public/data/snapshot_{platform}_{tier}.json so Vercel serves it
    # from the CDN edge — no serverless cold-start, no DB query on page load.
    if not ladder_only:
        _export_static_snapshot(platform, tier, active_set)

    print("\n[refresh] Done! Data is live.")
    print("[refresh] Push public/data/ to git so Vercel picks up the new snapshot.")


if __name__ == "__main__":
    main()
