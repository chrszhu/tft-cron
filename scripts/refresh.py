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
            # Reads filter by (platform, tier, set_number) WITHOUT puuid, so the
            # PK can't serve them and every read full-scans the table (tens of
            # MiB, growing with the data → the "exponential" RU curve). This
            # secondary index turns those into cheap range scans.
            "CREATE INDEX IF NOT EXISTS idx_hist_set ON historical_insights (platform, tier, set_number)",
            # Same rationale for the current-set boards read on challenger_players.
            "CREATE INDEX IF NOT EXISTS idx_challengers_set ON challenger_players (platform, tier, set_number)",
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
            # Augments carry a description + effect values in CDragon's items
            # list; keep them so augment tooltips can show what each one does.
            if "augment" in api.lower():
                entry["desc"] = item.get("desc") or ""
                entry["effects"] = item.get("effects") or {}
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

    team_planner = _fetch_team_planner(active_set)

    print(f"[catalog] Loaded {len(traits)} traits, {len(units)//2} units, {len(items)} items")
    return {
        "items": items, "traits": traits, "units": units, "augments": augments,
        "itemRoles": item_roles, "unitRanges": unit_ranges,
        "itemComponents": item_components,
        "teamPlanner": team_planner,
        "activeSet": active_set,
    }


# CommunityDragon team-planner data: authoritative per-set champion codes used
# by the in-game Team Planner import ("Copy team code" feature).
TEAM_PLANNER_URL = (
    "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/"
    "global/default/v1/tftchampions-teamplanner.json"
)


def _fetch_team_planner(active_set: int) -> dict:
    """Build {norm(displayName) → team_planner_code} for the active set.

    The in-game Team Planner code is built from these per-champion codes; see
    _team_code(). Returns {} if the set isn't published yet on CDragon.
    """
    try:
        resp = requests.get(TEAM_PLANNER_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[teamplanner] fetch failed ({e}); team codes disabled")
        return {}
    entries = data.get(f"TFTSet{active_set}") or []
    code_by_name: dict = {}
    for it in entries:
        name = it.get("display_name") or ""
        code = it.get("team_planner_code")
        if name and isinstance(code, int):
            # First-wins so the canonical variant keeps the slot (e.g. Akali).
            code_by_name.setdefault(_norm_key(name), code)
    print(f"[teamplanner] {len(code_by_name)} champion codes for set {active_set}")
    return code_by_name


def _team_code(unit_names: list, team_planner: dict, active_set: int) -> Optional[str]:
    """Encode a comp into an in-game Team Planner code.

    Format (sets >15): "02" + <3-hex-digit code per champ> + "000" padding to
    10 slots + "TFTSetN", exactly 40 chars. Champions with no known code are
    skipped; blanks are pushed to the end. Returns None if unusable.
    """
    if not team_planner:
        return None
    segs: list = []
    seen: set = set()
    for nm in unit_names:
        key = _norm_key(nm)
        if key in seen:
            continue
        code = team_planner.get(key)
        if code is None:
            continue
        seen.add(key)
        segs.append(format(code, "03x"))
        if len(segs) >= 10:
            break
    if not segs:
        return None
    code = "02" + "".join(segs) + "000" * (10 - len(segs)) + f"TFTSet{active_set}"
    return code if len(code) == 40 else None


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


def _compute_board_layout(units: list, catalog: dict, pos_overrides: Optional[dict] = None) -> list:
    """
    Infer a 4×7 board layout for a comp. The Riot API does not expose unit
    coordinates, so placement is heuristic, in priority order:
      • EXACT tftactics positions (``pos_overrides`` = {norm(name): (row, col)})
        for units the matched meta comp positions — this is real, hand-authored
        positioning and beats any heuristic.
      • otherwise front (melee, range ≤ 1) vs back (ranged), preferring the
        tftactics average-row hint over raw CDragon range.
      • front-row tanks cluster center (branching out); a damage carry stuck in
        the front row goes to the top-left corner.
      • back-row carries fill from the back-left (highest cost / main holder) right.
    Returns a list of 4 rows × 7 cells, each cell a unit name or None.
    """
    item_roles = catalog.get("itemRoles", {})
    unit_ranges = catalog.get("unitRanges", {})
    pos_overrides = pos_overrides or {}

    def cost_of(u: dict) -> float:
        c = u.get("cost")
        return c if isinstance(c, (int, float)) and c > 0 else 99

    grid = [[None] * 7 for _ in range(4)]

    # ── 0. Exact tftactics positions ────────────────────────────────────────────
    placed = set()
    for u in units:
        key = _norm_key(u.get("name", ""))
        rc = pos_overrides.get(key)
        if not rc:
            continue
        r, c = rc
        if 0 <= r <= 3 and 0 <= c <= 6 and grid[r][c] is None:
            grid[r][c] = u.get("name")
            placed.add(key)

    remaining = [u for u in units if _norm_key(u.get("name", "")) not in placed]

    # ── Front / back split for the rest ──────────────────────────────────────────
    pos_hints = _tft_position_hints()
    front, back = [], []
    for u in remaining:
        key = _norm_key(u.get("name", ""))
        hint = pos_hints.get(key)
        if hint is not None:
            (back if hint >= 1.5 else front).append(u)
        else:
            rng = unit_ranges.get(key)
            (back if isinstance(rng, (int, float)) and rng > 1 else front).append(u)

    def free_in(row_idx: int, order) -> Optional[int]:
        return next((c for c in order if grid[row_idx][c] is None), None)

    # ── Front row (row 0), collision-aware around already-placed units ───────────
    fc = sorted([u for u in front if _classify_board_role(u, item_roles) == "carry"], key=lambda u: -cost_of(u))
    ft = sorted([u for u in front if _classify_board_role(u, item_roles) == "tank"], key=lambda u: -cost_of(u))
    ff = sorted([u for u in front if _classify_board_role(u, item_roles) == "filler"], key=lambda u: -cost_of(u))
    for u in fc:  # damage carry → top-left
        c = free_in(0, [0, 1, 2, 3, 4, 5, 6])
        if c is not None:
            grid[0][c] = u.get("name")
        else:
            o = free_in(1, range(7))
            if o is not None:
                grid[1][o] = u.get("name")
    center_order = [3, 2, 4, 1, 5, 0, 6]

    def place_center(lst):
        for u in lst:
            p = free_in(0, center_order)
            if p is not None:
                grid[0][p] = u.get("name")
            else:
                o = free_in(1, range(7))
                if o is not None:
                    grid[1][o] = u.get("name")

    place_center(ft)   # main tank dead center, branching out
    place_center(ff)   # secondary melee fill around the center

    # ── Back row (row 3), collision-aware ────────────────────────────────────────
    bh = sorted([u for u in back if _classify_board_role(u, item_roles) != "filler"], key=lambda u: -cost_of(u))
    bf = sorted([u for u in back if _classify_board_role(u, item_roles) == "filler"], key=lambda u: -cost_of(u))

    def place_back(lst):
        for u in lst:
            c = free_in(3, [0, 1, 2, 3, 4, 5, 6])  # back-left → right, main carry first
            if c is not None:
                grid[3][c] = u.get("name")
            else:
                o = free_in(2, range(7))
                if o is not None:
                    grid[2][o] = u.get("name")

    place_back(bh)
    place_back(bf)
    return grid


# TFT Academy serves champion/summon art keyed by apiName — the only public
# source for non-champion synergy pieces (they aren't in CDragon at all).
TFTA_CHAMPION_ICON_BASE = "https://assets.tftacademy.com/champions/champion_icons/"


# A summon is only real if the comp actually fields the trait that spawns it.
# This gates out false positives from imperfect comp matches (e.g. an Elderwood
# tree attached to a comp that runs no Elderwood). Keys/values are norm'd traits.
SUMMON_REQUIRED_TRAIT = {
    "sentry": {"invoker"},
    "crimson raptor": {"riftbeast", "ravager"},
    # Elderwood pieces (Stonebark Tree / Lifeblossom / Protector) → Elderwood,
    # matched by the "elderwood" name prefix below.
}


def _summon_allowed(name: str, trait_keys: set) -> bool:
    """True if the comp's active traits can actually spawn this summon."""
    k = _norm_key(name)
    if k.startswith("elderwood"):
        return "elderwood" in trait_keys
    req = SUMMON_REQUIRED_TRAIT.get(k)
    if req is None:
        return True  # unknown summon → keep (fail-open)
    return bool(req & trait_keys)


def _board_summons(match: Optional[dict], catalog: dict) -> list:
    """Non-champion synergy pieces a comp places on its board (Elderwood
    Stonebark Tree / Lifeblossom / Protector, Crimson Raptor, Sentry, …).

    These aren't Riot champions and don't exist in CDragon, but TFT Academy
    authors their board position and serves their art keyed by apiName. Returns
    ``[{name, iconUrl, row, col}]`` so they can be placed on the suggested board
    and rendered by the frontend just like a real unit."""
    out, seen = [], set()
    units_cat = catalog.get("units") or {}
    for ch in (match or {}).get("characters") or []:
        nm, api = ch.get("name"), ch.get("apiName")
        row, col = ch.get("row"), ch.get("col")
        if not nm or not api or not isinstance(row, int) or not isinstance(col, int):
            continue
        # Real champions render from the catalog; only summons need this path.
        # Match by BOTH display name AND apiName: some pieces are real CDragon
        # units under a *different* display name than TFT Academy uses (e.g.
        # DA_18_Sentry is "Pebbles" in CDragon but "Sentry" on TFTA; DA_CrimsonRaptor18
        # is "Mama Beak" vs "Crimson Raptor"). Those already appear on the board as
        # the real unit from Riot match data, so emitting them here too would
        # duplicate the piece ("two Pebbles"). Skipping by apiName removes the dupe.
        if _unit_by_display_name(catalog, nm) or api in units_cat or api.lower() in units_cat:
            continue
        k = _norm_key(nm)
        if k in seen:
            continue
        seen.add(k)
        out.append({"name": nm, "iconUrl": f"{TFTA_CHAMPION_ICON_BASE}{api}.webp",
                    "row": row, "col": col})
    return out


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


# Units that are NEVER a primary carry, even when TFT Academy's authored build
# or our item heuristic assigns them offensive items. Enchanters / supports
# (Ivern, Nidalee) and non-champion monster/summon pieces (Elder Dragon) show up
# itemized in sample data but must not drive the "Main" carry designation or the
# comp NAME. Deliberately conservative — only units we're confident are never a
# primary carry, so we never strip a legit carry. normName keys.
NON_CARRY_UNITS = {"ivern", "nidalee", "elderdragon"}


def _is_non_carry(name: str) -> bool:
    """True if ``name`` is on the never-a-primary-carry denylist."""
    return _norm_key(name or "") in NON_CARRY_UNITS


def _name_starts_with_non_carry(name: str) -> bool:
    """True if a comp name begins with a denylisted unit (e.g. "Elder Dragon
    Fast 9", "Nidalee Aphelios") — such names imply a wrong primary carry."""
    words = (name or "").split()
    for i in range(1, min(3, len(words)) + 1):  # unit names are ≤ ~3 words
        if _norm_key("".join(words[:i])) in NON_CARRY_UNITS:
            return True
    return False


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

    carries = [u for u in units
               if _classify_board_role(u, item_roles) == "carry" and not _is_non_carry(u.get("name"))]
    tanks = [u for u in units if _classify_board_role(u, item_roles) == "tank"]
    carry = max(carries, key=holder_score) if carries else None
    tank = max(tanks, key=holder_score) if tanks else None

    # Reroll comps are defined by 3-starring a low-cost unit.
    reroll = [u for u in units
              if (u.get("threeStarPct") or 0) >= 35 and 1 <= cost(u) <= 3 and not _is_non_carry(u.get("name"))]
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


def _tfta_carry_tank(match: dict, catalog: dict, allowed_keys: set) -> tuple:
    """Derive the carry/tank ITEM HOLDERS from a matched TFT Academy comp's
    authored ``finalComp`` itemization. A comp can have several carries and/or
    tanks — classification is purely by the units' items (NOT ``mainChampion``,
    which is just the comp's name unit and is often a tank, e.g. Malphite).

    A unit whose items sum to an offensive role is a carry; a defensive sum is a
    tank. Returns ``(carries, tanks)`` — ordered lists of display names, each
    strongest-first. Only includes units in ``allowed_keys`` (our board's
    core+flex) so we never point at a unit we don't show.
    """
    item_roles = catalog.get("itemRoles", {})
    fc = match.get("finalComp") or []
    carries, tanks = [], []  # each: (name, magnitude, item_count)
    for u in fc:
        nm = u.get("name")
        items = u.get("items") or []
        if not nm or not items or _norm_key(nm) not in allowed_keys:
            continue
        # Skip non-champion synergy summons (Sentinel, Elderwood pieces, …): they
        # aren't real item holders, so they must never show as the "main tank".
        if not _unit_by_display_name(catalog, nm):
            continue
        score = sum(item_roles.get(_norm_key(it), 0) for it in items)
        if score < 0:
            tanks.append((nm, -score, len(items)))
        else:  # >0 offensive, ==0 utility/mixed → treat as a carry holder
            carries.append((nm, score, len(items)))

    carries.sort(key=lambda x: (-x[1], -x[2]))
    tanks.sort(key=lambda x: (-x[1], -x[2]))

    # Drop never-a-primary-carry units (Ivern/Nidalee/Elder Dragon) from the
    # carries list BEFORE anyone reads carries[0] — they must never be the Main
    # carry nor drive the comp name, even if TFTA gave them offensive items.
    carries = [c for c in carries if not _is_non_carry(c[0])]

    # Promote TFT Academy's headline ``mainChampion`` to the FRONT of whichever
    # list it lands in — it's the comp's designated PRIMARY carry (or primary
    # tank for reroll-tank comps like Malphite). This fixes comps that were named
    # after / led by a secondary carry with a higher raw item score. Skip when
    # the mainChampion is a denylisted non-carry (don't resurrect it as primary).
    main_k = _norm_key(match.get("mainChampion") or "")
    if main_k and main_k not in NON_CARRY_UNITS:
        for lst in (carries, tanks):
            for i, entry in enumerate(lst):
                if _norm_key(entry[0]) == main_k:
                    lst.insert(0, lst.pop(i))
                    break
    return [c[0] for c in carries], [t[0] for t in tanks]


# ── Early-game openers ──────────────────────────────────────────────────────────
# What to build toward in the early game before pivoting to the final board.
# Curated per-carry from community guides (BunnyMuffins meta + Set 18
# climbing/opener videos, patch 18.1). Keys are normalized carry names.
OPENER_LIBRARY = {
    "ahri": {
        "label": "Blossom opener", "streak": "win",
        "units": ["Karma", "Master Yi", "Veigar", "Rakan"],
        "detail": "Open 3 Blossom and hold your AP items on Karma to farm empowered Wisps; win-streak, add Rakan at level 4, then Fast 8 and move the items onto Ahri.",
    },
    "soraka": {
        "label": "AP / Blossom opener", "streak": "flex",
        "units": ["Karma", "Malphite"],
        "detail": "Open a strong AP board (Karma holds the items) around Blossom; slam pure tank items on Malphite and keep him alive. Don't over-slam AP — item quality matters here.",
    },
    "cassiopeia": {
        "label": "Cassio + Defenders / Coven", "streak": "loss",
        "units": ["Cassiopeia", "Diana"],
        "detail": "Open Cassio + Defenders, or lose-streak Coven for gold. Cassio needs BIS (Gunblade + Deathcap/Archangel's + Shojin/Blue Buff) — pivot if you can't realistically reach it.",
    },
    "kayle": {
        "label": "Kayle / Solar reroll", "streak": "flex",
        "units": ["Kayle", "Xayah"],
        "detail": "Open around a Kayle/Xayah + slammable items and save HP in stage 2. Don't press level early — slow-roll for 3-stars; if flooded with Solar units, lose-streak into the reroll.",
    },
    "master yi": {
        "label": "Blossom / Rengar opener", "streak": "flex",
        "units": ["Master Yi", "Rengar", "Karma"],
        "detail": "Open around an early Yi/Rengar orb or a 2-1 artifact + 3 Blossom. Yi+Rengar = AD, Yi solo = AP. Don't hard-commit without an artifact or an uncontested Rengar.",
    },
    "veigar": {
        "label": "Veigar / Blossom reroll", "streak": "flex",
        "units": ["Veigar", "Kobuko", "Rek'Sai"],
        "detail": "Open Veigar + a Blossom/Spriggan core and hold AP items; slow-roll to 3-star Veigar (Spriggan ramps his backline AP over the course of the fight).",
    },
    "cinderling": {
        "label": "Riftbeast / AD reroll", "streak": "flex",
        "units": ["Cinderling", "Pebbles", "Scuttlecrab"],
        "detail": "Open 3 Riftbeast (Cinderling + jungle monsters) for early Alpha-Mark tempo; slam AD/IE and slow-roll Cinderling, then put the Alpha Mark on him to make a super-carry.",
    },
    "draven": {
        "label": "AD Fast 9 opener", "streak": "win",
        "units": ["Xayah", "Rakan", "Camille"],
        "detail": "Open a strong AD board (Rageblade Xayah / Elderwood) and win-streak hard. Draven needs Guinsoo's; only force this if you can reliably reach level 9.",
    },
    "aphelios": {
        "label": "AD tempo opener", "streak": "flex",
        "units": ["Cinderling", "Camille", "Akali"],
        "detail": "Open around any AD/attack-speed slam (Guinsoo's is fine early) with AD holders Cinderling/Camille/Akali; Fast 8 and flex the frontline around Sentinel.",
    },
    "nidalee": {
        "label": "AP item-slam opener", "streak": "flex",
        "units": ["Karma", "Rakan"],
        "detail": "Very item-hungry: slam AP (Jeweled Gauntlet / Morello / Guinsoo's) early on Karma. Flex a 4-Vanguard frontline; needs ~3 rods for Nidalee + Morgana.",
    },
    "tristana": {
        "label": "Attack-speed reroll", "streak": "flex",
        "units": ["Tristana", "Cinderling"],
        "detail": "Open around bows / attack-speed slams (Guinsoo's); slow-roll to 3-star Tristana. Hunter AD line — stack bows for her attack-speed scaling.",
    },
}

_AP_COMPONENTS = {"needlessly large rod", "tear of the goddess"}
_AD_COMPONENTS = {"b.f. sword", "recurve bow"}


def _unit_by_display_name(catalog: dict, display_name: str) -> Optional[dict]:
    """Resolve a unit entry (name/iconUrl/cost) by its human display name."""
    target = _norm_key(display_name)
    if not target:
        return None
    for entry in (catalog.get("units") or {}).values():
        if _norm_key(entry.get("name", "")) == target:
            return entry
    return None


def _carry_dmg_type(arch: dict, catalog: dict) -> str:
    """Infer whether the carry itemizes AD or AP from its top items' components."""
    carry = arch.get("carryName")
    units = (arch.get("coreUnits") or []) + (arch.get("flexUnits") or [])
    cu = next((u for u in units if u.get("name") == carry), None)
    ap = ad = 0
    if cu:
        for it in (cu.get("topItems") or [])[:2]:
            for c in _item_components(it.get("name", ""), catalog):
                cn = _norm_key(c.get("name", ""))
                if cn in {_norm_key(x) for x in _AP_COMPONENTS}:
                    ap += 1
                elif cn in {_norm_key(x) for x in _AD_COMPONENTS}:
                    ad += 1
    return "AP" if ap > ad else "AD"


def _fallback_opener(arch: dict, dmg: str) -> dict:
    """Generic opener for comps whose carry isn't in the curated library."""
    cat = arch.get("category") or ""
    carry = arch.get("carryName") or "your carry"
    if "Reroll" in cat:
        if dmg == "AP":
            return {"label": "AP reroll opener", "streak": "flex", "units": ["Karma"],
                    "detail": f"Hold AP on Karma early and play copies of {carry}; win/loss-streak for econ, then slow-roll to 3-star {carry}."}
        return {"label": "AD reroll opener", "streak": "flex", "units": ["Cinderling", "Camille"],
                "detail": f"Slam an AD/attack-speed item and play copies of {carry}; win/loss-streak for econ, then slow-roll to 3-star {carry}."}
    if "Fast 9" in cat:
        return {"label": "AD Fast 9 opener", "streak": "win", "units": ["Xayah", "Rakan"],
                "detail": f"Win-streak a strong AD opener and push econ hard — you need level 9 to field {carry} + legendaries."}
    if dmg == "AP":
        return {"label": "AP tempo opener", "streak": "win", "units": ["Karma"],
                "detail": f"Open 3 Blossom and hold AP on Karma to farm Wisps; win-streak to Fast 8, then itemize {carry}."}
    return {"label": "AD tempo opener", "streak": "win", "units": ["Cinderling", "Camille"],
            "detail": f"Slam IE/Deathblade and hold AD on Cinderling/Camille; win-streak to Fast 8, then itemize {carry}."}


def _resolve_item_names(catalog: dict, names: list) -> list:
    """Resolve item display names → [{name, iconUrl}] (icon from catalog, keeps
    the given name even if the icon is unknown so the suggestion still shows)."""
    if not names:
        return []
    icons: dict = {}
    for v in (catalog.get("items") or {}).values():
        nm = (v or {}).get("name")
        if nm:
            icons.setdefault(_norm_key(nm), (v or {}).get("iconUrl"))
    out = []
    for nm in names:
        if not _norm_key(nm):
            continue
        out.append({"name": nm, "iconUrl": icons.get(_norm_key(nm))})
    return out


def _resolve_opener_units(catalog: dict, names: list,
                          items_by_name: Optional[dict] = None,
                          api_by_name: Optional[dict] = None) -> list:
    """Resolve unit display names → {name, iconUrl, cost[, items]} entries.

    ``items_by_name`` (norm(name) → [item display names]) attaches the early-game
    item suggestions the meta comp recommends holding on each opener unit, so the
    early board shows WHAT to slam and on WHOM, not just which units to play.

    ``api_by_name`` (norm(name) → apiName) lets us resolve a piece by its exact
    apiName first — the CDragon units map is keyed by apiName, so pieces TFTA
    humanizes differently than CDragon (e.g. ``DA_18_Sentry`` is "Pebbles" in
    CDragon but "Sentry" on TFTA) get the correct in-game name/icon instead of a
    broken TFTA humanization."""
    items_by_name = items_by_name or {}
    api_by_name = api_by_name or {}
    units_cat = catalog.get("units") or {}
    out = []
    for nm in names:
        api = api_by_name.get(_norm_key(nm))
        u = (units_cat.get(api) if api else None) or _unit_by_display_name(catalog, nm)
        entry = {"name": (u or {}).get("name") or nm,
                 "iconUrl": (u or {}).get("iconUrl"),
                 "cost": (u or {}).get("cost")}
        its = items_by_name.get(_norm_key(nm))
        if its:
            entry["items"] = _resolve_item_names(catalog, its)
        out.append(entry)
    return out


# tftactics.gg Set 18 meta comps (scraped): each has the mid/transition board
# ("mid") we surface as the early build target, plus playstyle + leveling note.
# Matched to our clustered archetypes by carry + core-unit overlap.
_TFT_COMPS = None


def _load_tft_comps() -> list:
    """Load the scraped meta-comp dataset that feeds positioning / tier / opener /
    carousel / stage tips.

    Prefers TFT Academy (scrape_tftacademy.py → tftacademy_set18.json): it's a
    strict SUPERSET of the tftactics schema (same name/tier/units/carries/mid/
    characters fields) PLUS exact boardIndex positioning for every unit,
    stage-by-stage roll/level tips, difficulty, a late-game max-cap, grouped
    augments, and an authored augments tip. Falls back to the older tftactics
    scrape if the Academy file is absent."""
    global _TFT_COMPS
    if _TFT_COMPS is not None:
        return _TFT_COMPS
    here = os.path.dirname(os.path.abspath(__file__))
    for fname in ("tftacademy_set18.json", "tftactics_set18.json"):
        path = os.path.join(here, fname)
        try:
            with open(path) as f:
                _TFT_COMPS = json.load(f)
            print(f"[opener] loaded {len(_TFT_COMPS)} meta comps from {fname}")
            return _TFT_COMPS
        except Exception:
            continue
    print("[opener] no meta-comp dataset found (tftacademy/tftactics)")
    _TFT_COMPS = []
    return _TFT_COMPS


_TFT_POS_HINTS: Optional[dict] = None


def _tft_position_hints() -> dict:
    """Map norm(unit name) → average board row (0 = front … 3 = back) across all
    tftactics comps.

    Lets board layout tell front-liners from back-liners by how a unit is
    ACTUALLY played rather than by raw attack range, which misclassifies
    melee-range divers/casters (e.g. Kennen has range 2 but dives the front).
    Requires the scraped dataset to include per-character positions
    (scrape_tftactics.py emits row/col); returns {} otherwise.
    """
    global _TFT_POS_HINTS
    if _TFT_POS_HINTS is not None:
        return _TFT_POS_HINTS
    agg: dict = {}
    for c in _load_tft_comps():
        for ch in c.get("characters") or []:
            r = ch.get("row")
            nm = ch.get("name")
            if nm and isinstance(r, (int, float)):
                e = agg.setdefault(_norm_key(nm), [0.0, 0])
                e[0] += r
                e[1] += 1
    _TFT_POS_HINTS = {k: tot / n for k, (tot, n) in agg.items() if n}
    return _TFT_POS_HINTS


def _tftactics_tier_letter(t) -> Optional[str]:
    """Map a comp tier index (1 = best / S) to a letter grade. 6 = X = the
    'Situational' bucket (TFT Academy's X tier) — surfaced as a distinct badge."""
    return {1: "S", 2: "A", 3: "B", 4: "C", 5: "D", 6: "X"}.get(t) if isinstance(t, int) else None


def _comp_tier_letter(arch: dict, match: Optional[dict]) -> Optional[str]:
    """Tier grade (S/A/B/C/D) for a comp, from the curated tftactics tier list.

    Prefers the confident metaName match; otherwise borrows the tier of the
    nearest comp by core+flex unit overlap (a ballpark "closest meta comp"
    grade) so every comp still gets a rating. We deliberately do NOT grade from
    our own board placements — those are harvested TOP boards, so their average
    finish is biased and would rate everything S.
    """
    if match and isinstance(match.get("tier"), int):
        return _tftactics_tier_letter(match["tier"])
    comps = _load_tft_comps()
    if not comps:
        return None
    ours = {_norm_key(u["name"]) for u in arch.get("coreUnits", [])} | \
           {_norm_key(u["name"]) for u in arch.get("flexUnits", [])}
    if not ours:
        return None
    best, best_jac = None, 0.0
    for c in comps:
        cu = {_norm_key(x) for x in c.get("units", [])}
        if not cu:
            continue
        jac = len(ours & cu) / (len(ours | cu) or 1)
        if jac > best_jac:
            best, best_jac = c, jac
    if best and best_jac >= 0.15 and isinstance(best.get("tier"), int):
        return _tftactics_tier_letter(best["tier"])
    return None


def _match_tft_comp(arch: dict) -> Optional[dict]:
    """Best-matching tftactics comp by core-unit Jaccard + carry match."""
    comps = _load_tft_comps()
    if not comps:
        return None
    ours = {_norm_key(u["name"]) for u in arch.get("coreUnits", [])} | \
           {_norm_key(u["name"]) for u in arch.get("flexUnits", [])}
    if not ours:
        return None
    carry = _norm_key(arch.get("carryName") or "")
    best, best_score, best_jac, best_carry = None, -1.0, 0.0, False
    for c in comps:
        cu = {_norm_key(x) for x in c.get("units", [])}
        if not cu:
            continue
        jac = len(ours & cu) / (len(ours | cu) or 1)
        carry_match = carry in {_norm_key(x) for x in c.get("carries", [])}
        score = jac + (0.45 if carry_match else 0)
        if score > best_score:
            best, best_score, best_jac, best_carry = c, score, jac, carry_match
    # Accept only confident matches so we don't attach a wrong transition board.
    if best and ((best_carry and best_jac >= 0.2) or best_jac >= 0.4):
        return best
    return None


def _comp_primary_carry(match: dict) -> str:
    """Norm-key of a TFT comp's primary carry: its first listed carry, else its
    ``mainChampion``. Used to gate whether the comp's NAME may be applied."""
    carries = match.get("carries") or []
    if carries:
        return _norm_key(carries[0] or "")
    return _norm_key(match.get("mainChampion") or "")


def _arch_contains_unit(arch: dict, unit_norm: str) -> bool:
    """True if the archetype's core/flex units include ``unit_norm``."""
    if not unit_norm:
        return False
    for u in (arch.get("coreUnits") or []) + (arch.get("flexUnits") or []):
        if _norm_key(u.get("name") or "") == unit_norm:
            return True
    return False


def _apply_meta_name(arch: dict, match: Optional[dict], used_names: set) -> None:
    """Apply the matched comp's board NAME to ``arch`` — but only if the archetype
    actually fields that comp's PRIMARY carry, and no other archetype already
    claimed the name. A high support-unit overlap alone must NOT rename a board
    (e.g. a carry-less Vanguard/Riftbeast board mislabelled "Draven Fast 9");
    such boards fall back to the carry+category name (metaName left unset)."""
    name = (match or {}).get("name")
    if not name:
        arch.pop("metaName", None)
        return
    pc = _comp_primary_carry(match)
    # Never title a comp after a denylisted non-carry: reject if the comp's
    # headline carry is denylisted OR the authored name simply begins with one
    # (e.g. "Elder Dragon Fast 9", "Nidalee Aphelios"). Such comps fall back to
    # the carry+category name built from the real (denylist-filtered) carry.
    if pc in NON_CARRY_UNITS or _name_starts_with_non_carry(name):
        arch.pop("metaName", None)
        return
    if pc and _arch_contains_unit(arch, pc) and name not in used_names:
        arch["metaName"] = name
        used_names.add(name)
    else:
        arch.pop("metaName", None)


def _join_names(names: list) -> str:
    """Human-readable ' A, B, and C' join."""
    names = [n for n in names if n]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return ", ".join(names[:-1]) + f", and {names[-1]}"


def _units_named_in_text(text: str, catalog: dict) -> set:
    """Champion display names (active-set catalog) explicitly named in ``text``.

    Matched on non-letter boundaries so short names (Vi, Sett) don't
    false-positive inside longer words, and apostrophe names (Rek'Sai) still
    match. Used both to gate curated nuance and to validate that opener guide
    text never references a unit off the board."""
    if not text:
        return set()
    found: set = set()
    for e in (catalog.get("units") or {}).values():
        dn = (e or {}).get("name") or ""
        if not dn:
            continue
        if re.search(r"(?<![A-Za-z])" + re.escape(dn) + r"(?![A-Za-z])", text, re.IGNORECASE):
            found.add(dn)
    return found


def _opener_allowed_norms(op_units: list, arch: dict) -> set:
    """Norm names allowed to appear in an opener guide: the early board plus the
    comp's final core + flex."""
    allowed = {_norm_key(u.get("name")) for u in (op_units or []) if u.get("name")}
    allowed |= {_norm_key(u.get("name")) for u in arch.get("coreUnits", []) if u.get("name")}
    allowed |= {_norm_key(u.get("name")) for u in arch.get("flexUnits", []) if u.get("name")}
    allowed.discard("")
    return allowed


def _data_driven_opener_detail(early_names: list, arch: dict, catalog: dict,
                               streak: str, play: str) -> str:
    """Build a coherent opener guide from ONLY real board data, so it can never
    reference an off-board unit.

    Structure: what to open (early-board units) + streak plan + who holds early
    items → then how it pivots (playstyle, the final CORE units you add, and the
    final carry + its AD/AP itemization). Every champion named here is by
    construction on the early board or the final core/flex.
    """
    early_names = [n for n in early_names if n]
    early_norm = {_norm_key(n) for n in early_names}
    core_names = [u.get("name") for u in arch.get("coreUnits", []) if u.get("name")]
    core_norm = {_norm_key(n) for n in core_names}
    carry = arch.get("carryName")
    carry_norm = _norm_key(carry or "")
    dmg = _carry_dmg_type(arch, catalog)

    # Per-unit signals from OUR final data: item-holder % (who actually held
    # items) and cost. Only units that also appear in the final core/flex carry
    # these; pure early-board bodies fall back to the catalog cost.
    final_by_norm: dict = {}
    for u in (arch.get("coreUnits") or []) + (arch.get("flexUnits") or []):
        nm = u.get("name")
        if nm:
            final_by_norm.setdefault(_norm_key(nm), u)

    def _cost(n: str) -> int:
        u = final_by_norm.get(_norm_key(n)) or {}
        return u.get("cost") or (_unit_by_display_name(catalog, n) or {}).get("cost") or 0

    def _ihp(n: str) -> float:
        return (final_by_norm.get(_norm_key(n)) or {}).get("itemHolderPct") or 0

    # Early item holder — you hold early items on the eventual carry / reroll
    # unit, NOT the priciest body. Priority (first match wins), always chosen
    # from units ON the early board so the guide never names an off-board unit:
    #   1. the comp's final carry, if it's already on the early board;
    #   2. the early unit with the highest itemHolderPct (empirical signal);
    #   3. a genuine reroll/transition carry on the early board — a curated early
    #      carry (OPENER_LIBRARY key, e.g. Veigar) or, failing that, a low-cost
    #      (1–2) unit kept into the final core — over a high-cost flex body;
    #   4. else the highest-cost early unit (a body, better than nothing).
    holder = None
    if carry_norm and carry_norm in early_norm:
        holder = next((n for n in early_names if _norm_key(n) == carry_norm), None)
    if not holder:
        ihp_cands = [(n, _ihp(n)) for n in early_names if _ihp(n) > 0]
        if ihp_cands:
            holder = max(ihp_cands, key=lambda t: t[1])[0]
    if not holder:
        lib_keys = {_norm_key(k) for k in OPENER_LIBRARY}
        lib_cands = [n for n in early_names if _norm_key(n) in lib_keys]
        if lib_cands:
            # Prefer one kept into the final core, then the cheaper (reroll) carry.
            holder = min(lib_cands, key=lambda n: (0 if _norm_key(n) in core_norm else 1, _cost(n)))
        else:
            reroll_cands = [n for n in early_names
                            if _norm_key(n) in core_norm and 1 <= _cost(n) <= 2]
            if reroll_cands:
                holder = max(reroll_cands, key=_cost)
    if not holder and early_names:
        holder = max(early_names, key=_cost)

    # Final CORE units you add (excluding what's already on the early board and
    # the carry, which gets its own clause). Cap for brevity.
    added = [n for n in core_names
             if _norm_key(n) not in early_norm and _norm_key(n) != carry_norm][:3]
    carry_named = carry if carry_norm and carry_norm in _opener_allowed_norms(
        [{"name": n} for n in early_names], arch) else None

    streak_phrase = {
        "win": "play for a win-streak",
        "loss": "lose-streak for econ",
    }.get(streak, "streak either way for econ")

    sentences = []
    if early_names:
        s = f"Open with {_join_names(early_names[:5])} and {streak_phrase}"
        if holder:
            s += f", holding your early items on {holder}"
        sentences.append(s + ".")

    play_ok = play if play and re.search(r"Fast|Roll|Reroll", play, re.IGNORECASE) else ""
    lead = f"{play_ok}: " if play_ok else ""
    pivot = None
    if added and carry_named:
        pivot = f"{lead}add {_join_names(added)} and move items onto {carry_named} ({dmg})"
    elif carry_named:
        pivot = f"{lead}itemize {carry_named} ({dmg}) as your main carry"
    elif added:
        pivot = f"{lead}add {_join_names(added)} to complete the board"
    elif play_ok:
        pivot = f"{play_ok} into your final board"
    if pivot:
        sentences.append(pivot[0].upper() + pivot[1:] + ".")

    return " ".join(sentences).strip()


def _sanitize_opener_detail(op: dict, arch: dict, catalog: dict) -> str:
    """Hard guarantee: an opener's guide text never names a unit that isn't on
    the early board or the final core/flex. If any off-board name slips in
    (e.g. a carry-keyed curated line), rebuild the detail purely data-driven."""
    detail = op.get("detail") or ""
    allowed = _opener_allowed_norms(op.get("units") or [], arch)
    bad = {n for n in _units_named_in_text(detail, catalog) if _norm_key(n) not in allowed}
    if detail and not bad:
        return detail
    return _data_driven_opener_detail(
        [u.get("name") for u in (op.get("units") or [])],
        arch, catalog, op.get("streak", "flex"), op.get("playstyle", ""))


def _opener_from_tft(comp: dict, arch: dict, catalog: dict) -> Optional[dict]:
    """Build an opener from a matched tftactics comp's mid/transition board.

    The guide text is generated data-driven from the REAL early + final boards
    (never the shared/boilerplate tftactics description). We only append a
    curated per-carry nuance line when every unit it names is actually on one of
    those boards — otherwise the carry-keyed library could describe a totally
    different comp (e.g. a Karma/Malphite AP line under a Veigar early board)."""
    # Early-game item suggestions the meta comp holds on each opener unit
    # (TFT Academy's earlyComp carries per-unit items; tftactics doesn't).
    early_items = {_norm_key(u.get("name")): (u.get("items") or [])
                   for u in (comp.get("earlyComp") or []) if u.get("name")}
    # norm(display) → apiName so pieces CDragon names differently than TFTA
    # (e.g. Sentry → DA_18_Sentry → "Pebbles") resolve to the real unit.
    api_by_name = {_norm_key(u.get("name")): u.get("apiName")
                   for u in (comp.get("earlyComp") or [])
                   if u.get("name") and u.get("apiName")}
    units = _resolve_opener_units(catalog, comp.get("mid") or [], early_items, api_by_name)
    if not units:
        return None
    play = (comp.get("playstyle") or "").strip()
    carry_norm = _norm_key(arch.get("carryName") or "")
    cur = next((v for k, v in OPENER_LIBRARY.items() if _norm_key(k) == carry_norm), None)
    streak = (cur or {}).get("streak", "flex")

    detail = _data_driven_opener_detail([u["name"] for u in units], arch, catalog, streak, play)

    # Append curated item-slam nuance ONLY if it stays on-board.
    if cur and cur.get("detail"):
        allowed = _opener_allowed_norms(units, arch)
        cur_named = {_norm_key(n) for n in _units_named_in_text(cur["detail"], catalog)}
        if cur_named <= allowed:
            detail = f"{detail} {cur['detail']}".strip()

    return {
        "label": comp.get("name") or "Early board",
        "streak": streak,
        "detail": detail,
        "units": units,
        "source": comp.get("source") or "tftactics",
        "playstyle": play,
    }


def _carousel_from_tft(comp: dict, catalog: dict) -> Optional[list]:
    """Carousel priority from a matched tftactics comp's curated component list.

    tftactics weights carousel priority toward the *key items* a comp wants
    (the carry/tank BIS), not the most-frequently-seen components. Its
    ``carrousel`` field is an ordered list of {item, component} pairs. We keep
    that priority order and, tftactics-style, surface each COMPONENT to grab
    together with the full ITEM it builds into (rendered as a mini badge).

    Output entries: {name, iconUrl, buildsInto: {name, iconUrl}}. Entries are
    deduped by (component, item) so the same pairing isn't shown twice; a
    component that builds two different items appears once per target.
    """
    car = (comp or {}).get("carrousel") or []
    if not car:
        return None
    # Item display-name → icon lookup from the catalog items map (covers both
    # components and full items — they're all "items" in CDragon).
    icons: dict = {}
    for v in (catalog.get("items") or {}).values():
        nm = (v or {}).get("name")
        if nm:
            icons.setdefault(_norm_key(nm), (v or {}).get("iconUrl"))
    out: list = []
    seen: set = set()
    for entry in car:
        cname = (entry or {}).get("component")
        if not cname:
            continue
        iname = (entry or {}).get("item") or ""
        key = (_norm_key(cname), _norm_key(iname))
        if key in seen:
            continue
        seen.add(key)
        row = {"name": cname, "iconUrl": icons.get(_norm_key(cname))}
        if iname:
            row["buildsInto"] = {"name": iname, "iconUrl": icons.get(_norm_key(iname))}
        out.append(row)
    return out or None


def _carousel_from_tfta(comp: dict, catalog: dict) -> Optional[list]:
    """Carousel priority from a TFT Academy comp's ``carousel`` field.

    Unlike tftactics' {component, item} pairs, TFT Academy lists the carousel
    priority as a flat, ordered mix of the exact components AND finished/artifact
    items to grab (highest priority first). We keep that authored order and emit
    each as its own icon (component or full item), resolving icons from the
    catalog. Output entries: {name, iconUrl}, deduped, priority order preserved."""
    car = (comp or {}).get("carousel") or []
    if not car:
        return None
    icons: dict = {}
    for v in (catalog.get("items") or {}).values():
        nm = (v or {}).get("name")
        if nm:
            icons.setdefault(_norm_key(nm), (v or {}).get("iconUrl"))
    out, seen = [], set()
    for name in car:
        k = _norm_key(name)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append({"name": name, "iconUrl": icons.get(k)})
    return out or None


def _emblems_from_comp(comp: dict, catalog: dict) -> list:
    """Extract authored emblem usage from a matched meta comp → [{unit, emblem}].

    TFT Academy encodes emblems as item entries containing 'Emblem' on specific
    units across the early/final/max-cap builds — the comp's *intended* emblem
    plan, a real strategic signal. This is far more meaningful than our harvested
    top-items (where an emblem often appears as incidental filler on some board),
    so it both powers a strict 'Emblem' playstyle bucket and gives us concrete
    info to show ('Invoker Emblem on Ahri')."""
    out, seen = [], set()
    for blk in ("finalComp", "earlyComp", "maxCap"):
        for u in comp.get(blk) or []:
            nm = u.get("name")
            if not nm:
                continue
            for it in (u.get("items") or []):
                itk = _norm_key(it)
                # Real trait emblems only; skip mis-parsed trait-augment hybrids
                # (e.g. "Flora Fatalis Augment Emblem") that aren't craftable items.
                if "emblem" not in itk or "augment" in itk:
                    continue
                key = (_norm_key(nm), _norm_key(it))
                if key in seen:
                    continue
                seen.add(key)
                ur = _unit_by_display_name(catalog, nm)
                out.append({"unit": (ur or {}).get("name") or nm, "emblem": it})
    return out


def _best_transition_comp(arch: dict) -> Optional[dict]:
    """Pick the tftactics comp whose early ("mid") board best transitions into
    OUR final board — i.e. shares the most units with it.

    The old approach matched purely on final-board similarity and then attached
    that comp's ``mid``; but tftactics' mid boards are comp-specific and often
    share ZERO units with a data-clustered board (e.g. a Kobuko/Rek'Sai/Teemo
    opener glued onto a Yorick/Azir/Spellweaver board). A real opener must
    cleanly pivot into the final board, so we rank every comp's early board by
    how many of its units are actually kept in our final board, then break ties
    by overall comp similarity + carry match.
    """
    comps = _load_tft_comps()
    if not comps:
        return None
    core_ours = {_norm_key(u["name"]) for u in arch.get("coreUnits", [])}
    all_ours = core_ours | {_norm_key(u["name"]) for u in arch.get("flexUnits", [])}
    if not all_ours:
        return None
    carry = _norm_key(arch.get("carryName") or "")
    best, best_key = None, None
    for c in comps:
        mid = {_norm_key(x) for x in (c.get("mid") or [])}
        if not mid:
            continue
        cu = {_norm_key(x) for x in c.get("units", [])}
        trans_core = len(mid & core_ours)              # defining units kept into final
        trans_all = len(mid & all_ours)                # any unit kept (incl. flex)
        jac = len(all_ours & cu) / (len(all_ours | cu) or 1)  # same-comp similarity
        carry_bonus = 0.3 if carry and carry in cu else 0.0
        # Prefer sharing our CORE units (a real carry-over) over generic early
        # units that merely show up in our flex; then total overlap; then
        # overall comp similarity + carry match.
        key = (trans_core, trans_all, round(jac + carry_bonus, 4))
        if best_key is None or key > best_key:
            best, best_key = c, key
    # Require the early board to carry ≥1 unit into our final board.
    if best is None or best_key[1] < 1:
        return None
    return best


def _classify_opener(arch: dict, catalog: dict, meta_comp: Optional[dict] = None) -> dict:
    """Attach an early-game opener plan.

    Priority: (1) the MATCHED meta comp's OWN early ("mid") board — so the comp
    title, early board, and guide all describe the SAME tftactics comp. (This
    fixes the mismatch where the early units used to come from a different comp
    than the guide text, e.g. a Riftbeast early board under a Karma/Ahri guide.)
    (2) the tftactics comp whose early board best pivots into our final board;
    (3) curated per-carry opener; (4) generic data-driven fallback.
    """
    op = None
    if meta_comp:
        op = _opener_from_tft(meta_comp, arch, catalog)
    if op is None:
        match = _best_transition_comp(arch)
        if match:
            op = _opener_from_tft(match, arch, catalog)
    if op is None:
        carry_norm = _norm_key(arch.get("carryName") or "")
        entry = next((v for k, v in OPENER_LIBRARY.items() if _norm_key(k) == carry_norm), None)
        if not entry:
            entry = _fallback_opener(arch, _carry_dmg_type(arch, catalog))
        op = {"label": entry["label"], "streak": entry.get("streak", "flex"),
              "detail": entry["detail"],
              "units": _resolve_opener_units(catalog, entry.get("units", [])),
              "source": "curated"}
    # Hard guarantee: guide text never names an off-board unit. Rebuilds the
    # detail purely data-driven if any curated line references a unit that isn't
    # on this comp's early board or final core/flex.
    op["detail"] = _sanitize_opener_detail(op, arch, catalog)
    return op


# ── MetaTFT recommended augments ───────────────────────────────────────────────
# MetaTFT publishes, per comp ("cluster"), a curated augment tier list plus the
# canonical unit set for each cluster. We match those clusters to OUR data-
# clustered archetypes (core-unit Jaccard, mirroring _match_tft_comp) and attach
# the comp's best augments. Public JSON, no auth; needs a browser-like UA + a
# metatft.com Referer. NON-FATAL by design: any failure just means no recommended
# augments this run, so a MetaTFT outage never breaks the cron.
METATFT_API = "https://api-hc.metatft.com/tft-comps-api"
# MetaTFT's augment icon CDN hosts an image for EVERY augment id (including
# current-set-only augments CDragon hasn't published yet), so it's the primary
# icon source for recommended augments — guaranteeing 100% coverage. The path is
# just the exact MetaTFT id lowercased, keeping the DA_ prefix.
METATFT_AUG_ICON_CDN = "https://cdn.metatft.com/file/metatft/augments/{id}.png"
METATFT_HEADERS = {
    "Referer": "https://www.metatft.com/",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
}

_METATFT_AUG: Optional[dict] = None


def _metatft_get(path: str) -> Optional[dict]:
    resp = requests.get(f"{METATFT_API}/{path}", headers=METATFT_HEADERS, timeout=FETCH_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _parse_metatft_unit(uid: str, active_set: int) -> str:
    """MetaTFT unit id → champion name-ish token. e.g. DA_KogMaw18_AD → 'KogMaw',
    DA_18_Aphelios → 'Aphelios'. Normalize with _norm_key before matching."""
    s = re.sub(r"^DA_", "", uid or "")
    s = re.sub(r"_(AP|AD|Tank|Health)$", "", s)
    s = s.replace(str(active_set), "")
    return s.strip("_").replace("_", "")


def _metatft_aug_base(aug_id: str, active_set: int) -> str:
    """Normalized base of a MetaTFT augment id: drop 'DA_', the set infix, the
    tier variant (I/II/III/1/2/3, Plus/PlusPlus) and _Silver/_Gold/_Prismatic."""
    s = re.sub(r"^DA_", "", aug_id or "")
    s = re.sub(rf"(^|_){active_set}(_|$)", "_", s)
    s = re.sub(r"_(Silver|Gold|Prismatic)$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"(PlusPlus|Plus)$", "", s)
    s = re.sub(r"_(I{1,3}|IV|V|1|2|3)$", "", s, flags=re.IGNORECASE)
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _metatft_augment_icon(aug_id: str) -> Optional[str]:
    """MetaTFT CDN icon URL for an augment id (exact id, lowercased)."""
    if not aug_id:
        return None
    return METATFT_AUG_ICON_CDN.format(id=aug_id.lower())


def _humanize_metatft_aug(aug_id: str, active_set: int) -> str:
    """Fallback display name from a MetaTFT augment id (de-camelCased).
    e.g. DA_BandOfThieves → 'Band Of Thieves'."""
    s = re.sub(r"^DA_", "", aug_id or "")
    s = re.sub(rf"(^|_){active_set}(_|$)", "_", s)
    s = s.replace("_", " ")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean_augment_desc(desc: str, effects: dict) -> str:
    """Turn a raw CDragon augment desc into readable tooltip text.

    CDragon descriptions embed effect placeholders like ``@NumGloves@`` or
    ``@Amount*100@`` plus TFT rich-text/markup tags. Substitute effect values
    where we have them, drop leftover tokens/markup, and tidy whitespace."""
    if not desc:
        return ""
    eff = {str(k).lower(): v for k, v in (effects or {}).items()}

    def _fmt(n):
        try:
            f = float(n)
        except (TypeError, ValueError):
            return str(n)
        return str(int(f)) if f == int(f) else f"{f:g}"

    def _sub(m):
        expr = m.group(1)
        mult = 1.0
        m2 = re.match(r"([A-Za-z0-9_]+)\s*\*\s*([0-9.]+)$", expr)
        base = expr
        if m2:
            base, mult = m2.group(1), float(m2.group(2))
        v = eff.get(base.lower())
        if v is None:
            return ""  # unknown placeholder → drop it
        try:
            return _fmt(float(v) * mult)
        except (TypeError, ValueError):
            return _fmt(v)

    s = re.sub(r"@([^@]+)@", _sub, desc)
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)   # line breaks → spaces
    s = re.sub(r"%i:[^%]*%", "", s)                # inline sprite tokens
    s = re.sub(r"<[^>]+>", "", s)                   # any remaining markup tags
    s = s.replace("@", "")                          # stray placeholder markers
    return re.sub(r"\s+", " ", s).strip()


# Keyword buckets for classifying an augment as Combat / Economy / Utility.
# Riot groups augments into these three families in-game; there's no clean data
# field, so we heuristically classify by name + description keywords. Combat is
# checked FIRST on distinctive stat phrases (many economy/utility augments also
# hand out a secondary component or gold, which would otherwise misfire), then
# Economy, else Utility. Tokens are chosen to avoid substring collisions (e.g.
# no bare "econ", which matches "seconds").
_AUG_COMBAT_KW = (
    "damage amp", "attack damage", "ability power", "attack speed", "magic resist",
    "armor", "durability", "critical", "omnivamp", "lifesteal", "shield",
    "max health", "gain health", "gains health", "health for each",
    "% health", "on-hit", "on hit", "sunder", "shred", "damage to",
    "deal 1", "deals 1", "deal 2", "deals 2", "bonus damage", "team gains",
    "units gain", "allies gain", "starts combat", "start combat",
    "b.f. sword", "tear of the goddess", "mana per",
)
_AUG_ECON_KW = (
    "gold", "interest", "income", "loot", "component", "anvil", "reforger",
    "reroll", "free shop", "shop reroll", "thief", "glove", "grab bag",
    "delivery", "rebate", "duplicator", "money", "gamble", "pilfer",
    "5-cost champion", "random component", "random 2-star", "random 5-cost",
    "silver augment", "prismatic",
)


def _augment_category(name: str, aug_id: str, desc: str) -> str:
    """Classify an augment into 'Combat' | 'Economy' | 'Utility'.

    Combat wins first (buffs units/board in fights), then Economy (gold,
    components, rerolls, duplicators — tempo/value), else Utility (leveling,
    emblems, champion copies, item crafting, scouting, trait flexibility)."""
    hay = f" {(name or '').lower()} {(aug_id or '').lower()} {(desc or '').lower()} "
    if any(k in hay for k in _AUG_COMBAT_KW):
        return "Combat"
    if any(k in hay for k in _AUG_ECON_KW):
        return "Economy"
    return "Utility"


def _cdragon_augment_index(catalog: dict) -> list:
    """Index CDragon augments (drawn from catalog items whose apiName contains
    'Augment') for MetaTFT id resolution. CDragon's own top-level augments array
    is empty, so augments live in the items list. Reuses the icon URLs the
    catalog already built."""
    active = catalog.get("activeSet") or 0
    cur_prefix = f"tft{active}_"
    idx, seen = [], set()
    for api, v in (catalog.get("items") or {}).items():
        low = api.lower()
        if "augment" not in low or api in seen:
            continue
        seen.add(api)
        name = (v or {}).get("name") or ""
        tail = re.sub(r"^tft\d*_augment_", "", low)
        idx.append({
            "iconUrl": (v or {}).get("iconUrl"),
            "name": name,
            "desc": _clean_augment_desc((v or {}).get("desc") or "", (v or {}).get("effects") or {}),
            "normName": re.sub(r"[^a-z0-9]", "", name.lower()),
            "normTail": re.sub(r"[^a-z0-9]", "", tail),
            # Prefer current-set ('TFT18_') or generic ('TFT_Augment_') variants.
            "isCurrent": low.startswith(cur_prefix) or low.startswith("tft_augment_"),
        })
    return idx


def _resolve_metatft_augment(aug_id: str, active_set: int, index: list) -> Optional[dict]:
    """Map a MetaTFT augment id → {name, iconUrl} via CDragon. Match the id's
    normalized base against the normalized TAIL of a CDragon augment apiName OR
    its normalized display name, preferring current-set variants."""
    target = _metatft_aug_base(aug_id, active_set)
    if not target:
        return None
    best, best_key = None, None
    for a in index:
        tail, nm = a["normTail"], a["normName"]
        if tail == target or nm == target:
            score = 3
        elif tail.startswith(target) or nm.startswith(target):
            score = 2
        elif target in tail or target in nm:
            score = 1
        else:
            continue
        # Prefer stronger match, then current-set, then the closest (shortest) tail.
        key = (score, 1 if a["isCurrent"] else 0, -len(tail))
        if best_key is None or key > best_key:
            best, best_key = a, key
    if not best:
        return None
    return {"name": best["name"], "iconUrl": best["iconUrl"], "desc": best.get("desc") or ""}


def _fetch_metatft_augments(active_set: int, catalog: dict) -> dict:
    """Pull MetaTFT per-comp augment tiers + canonical unit sets for the active set.

    Returns {"units": {cluster → set(norm unit names)},
             "tiers": {cluster → [{id, tier}]},
             "augIndex": [...]}. NON-FATAL: returns {} on any failure."""
    try:
        latest = _metatft_get("latest_cluster_id") or {}
        cluster_prefix = str(latest.get("cluster_id") or "")
        want = f"TFTSet{active_set}"
        if latest.get("tft_set") and latest.get("tft_set") != want:
            print(f"[metatft] set mismatch (MetaTFT={latest.get('tft_set')} ours={want}); skipping")
            return {}

        def in_set(cluster: str) -> bool:
            return not cluster_prefix or cluster.startswith(cluster_prefix)

        tiers_raw = (_metatft_get("comp_augment_tiers") or {}).get("results") or {}
        tiers: dict = {}
        for cl, v in tiers_raw.items():
            if not in_set(cl):
                continue
            augs = [{"id": a.get("id"), "tier": a.get("tier")}
                    for a in (v or {}).get("augments", []) if a.get("id")]
            if augs:
                tiers[cl] = augs

        options_raw = ((_metatft_get("comp_options") or {}).get("results") or {}).get("options") or {}
        units: dict = {}
        for cl, levels in options_raw.items():
            if not in_set(cl):
                continue
            # Canonical unit set = the option row with the highest count.
            best_row = None
            for rows in (levels or {}).values():
                for row in (rows or []):
                    if best_row is None or (row.get("count") or 0) > (best_row.get("count") or 0):
                        best_row = row
            if not best_row:
                continue
            names = {_norm_key(_parse_metatft_unit(u, active_set))
                     for u in (best_row.get("units_list") or "").split("&") if u}
            names.discard("")
            if names:
                units[cl] = names

        if not tiers or not units:
            print("[metatft] empty tiers/units; skipping recommended augments")
            return {}
        idx = _cdragon_augment_index(catalog)
        print(f"[metatft] {len(tiers)} comp augment lists, {len(units)} comp unit sets, "
              f"{len(idx)} CDragon augments indexed")
        return {"units": units, "tiers": tiers, "augIndex": idx}
    except Exception as e:
        print(f"[metatft] WARNING: fetch failed ({e}); no recommended augments this run")
        return {}


def _metatft_augments(active_set: int, catalog: dict) -> dict:
    """Memoized _fetch_metatft_augments (one fetch per process)."""
    global _METATFT_AUG
    if _METATFT_AUG is None:
        _METATFT_AUG = _fetch_metatft_augments(active_set, catalog)
    return _METATFT_AUG


def _curate_recommended_augments(entries: list, arch: dict, active_set: int, cap: int = 12) -> list:
    """Curate a digestible augment list for a comp from its MetaTFT tier list:
    comp/trait/hero-specific S-tier first, then general S-tier; fall back to
    A-tier to fill if there are few S. Deduped by base augment, capped."""
    trait_keys = {_norm_key(t.get("name", "")) for t in arch.get("traits", [])}
    unit_keys = {_norm_key(u.get("name", "")) for u in arch.get("coreUnits", [])}
    key_terms = {k for k in (trait_keys | unit_keys) if len(k) >= 4}

    def is_specific(aid: str) -> bool:
        low = (aid or "").lower()
        if f"_{active_set}_" in low:
            return True
        nid = re.sub(r"[^a-z0-9]", "", low)
        return any(k in nid for k in key_terms)

    def bucket(tier: str, spec: bool) -> list:
        return [e for e in entries if e.get("tier") == tier and is_specific(e["id"]) == spec]

    ordered = bucket("S", True) + bucket("S", False)
    if len(ordered) < 6:
        ordered += bucket("A", True) + bucket("A", False)

    seen, out = set(), []
    for e in ordered:
        base = _metatft_aug_base(e["id"], active_set)
        if base in seen:
            continue
        seen.add(base)
        out.append(e)
        if len(out) >= cap:
            break
    return out


def _attach_recommended_augments(arch: dict, catalog: dict) -> bool:
    """Attach MetaTFT-curated recommended augments to an archetype (live set only).

    Finds the MetaTFT cluster whose canonical unit set has the highest Jaccard
    overlap with the archetype's core units (require jaccard ≥ 0.3 or ≥ 3 shared
    units), then curates + resolves that cluster's augment tier list. Sets
    arch['recommendedAugments'] / arch['recommendedAugmentsSource']. Returns True
    if augments were attached."""
    active = catalog.get("activeSet") or 0
    meta = _metatft_augments(active, catalog)
    if not meta:
        return False
    units_by_cluster = meta.get("units") or {}
    tiers_by_cluster = meta.get("tiers") or {}
    index = meta.get("augIndex") or []
    ours = {_norm_key(u["name"]) for u in arch.get("coreUnits", []) if u.get("name")}
    if not ours:
        return False
    best_cl, best_jac, best_shared = None, 0.0, 0
    for cl, cu in units_by_cluster.items():
        if not cu:
            continue
        shared = len(ours & cu)
        jac = shared / (len(ours | cu) or 1)
        if jac > best_jac:
            best_cl, best_jac, best_shared = cl, jac, shared
    if best_cl is None or (best_jac < 0.3 and best_shared < 3):
        return False
    curated = _curate_recommended_augments(tiers_by_cluster.get(best_cl) or [], arch, active)
    # Signal for the "Augment" playstyle filter: does this comp have a dedicated
    # trait/hero augment among its recommendations (built around its identity)?
    trait_keys = {_norm_key(t.get("name", "")) for t in arch.get("traits", [])}
    unit_keys = {_norm_key(u.get("name", "")) for u in arch.get("coreUnits", [])}
    key_terms = {k for k in (trait_keys | unit_keys) if len(k) >= 4}
    reliant = False
    resolved = []
    for e in curated:
        info = _resolve_metatft_augment(e["id"], active, index)
        name = (info or {}).get("name") or _humanize_metatft_aug(e["id"], active)
        desc = (info or {}).get("desc") or ""
        resolved.append({
            # Display name via CDragon (with de-camelCase fallback); icon from
            # MetaTFT's CDN (covers current-set augments CDragon lacks), falling
            # back to a CDragon icon only if the id somehow yields no URL.
            "name": name,
            "iconUrl": _metatft_augment_icon(e["id"]) or (info or {}).get("iconUrl"),
            "tier": e.get("tier"),
            "id": e["id"],
            "category": _augment_category(name, e["id"], desc),
            "desc": desc,
        })
        nid = re.sub(r"[^a-z0-9]", "", (e["id"] or "").lower())
        if any(k in nid for k in key_terms):
            reliant = True
    if not resolved:
        return False
    arch["recommendedAugments"] = resolved
    arch["recommendedAugmentsSource"] = "metatft"
    arch["augmentReliant"] = reliant
    return True


def _attach_tfta_augments(arch: dict, match: dict, catalog: dict) -> bool:
    """Fallback: attach a matched TFT Academy comp's curated augments.

    Used only when MetaTFT can't match the comp (its observed per-comp tier list
    is preferred). TFT Academy lists augments grouped by rarity rather than a
    quality tier, so we surface the rarity (Silver/Gold/Prismatic) as the chip
    and classify each into Combat/Economy/Utility via the shared heuristic. Names
    and descriptions are resolved against the CDragon augment index; icons come
    from MetaTFT's CDN (100% coverage, keyed by the DA_ apiName), falling back to
    the CDragon icon. Sets recommendedAugments + recommendedAugmentsSource."""
    augs = match.get("augments") or []
    if not augs:
        return False
    active = catalog.get("activeSet") or 0
    index = _cdragon_augment_index(catalog)
    by_name: dict = {}
    for a in index:
        by_name.setdefault(a["normName"], a)
    trait_keys = {_norm_key(t.get("name", "")) for t in arch.get("traits", [])}
    unit_keys = {_norm_key(u.get("name", "")) for u in arch.get("coreUnits", [])}
    key_terms = {k for k in (trait_keys | unit_keys) if len(k) >= 4}
    resolved, seen, reliant = [], set(), False
    for a in augs:
        api = a.get("apiName") or ""
        # NOTE: TFT Academy's generic trait-augment markers
        # (DA_BlackthornTraitAugment, DA_18_ElderwoodTraitAugment, …) carry no
        # rarity/tier and TFTA doesn't render them as augment chips on its own
        # site. We deliberately KEEP them here (per user request) so the comp's
        # intended trait-augment plan is still surfaced.
        name = a.get("name") or _humanize_metatft_aug(api, active)
        nkey = re.sub(r"[^a-z0-9]", "", name.lower())
        if not nkey or nkey in seen:
            continue
        seen.add(nkey)
        info = by_name.get(nkey)
        if not info:
            info = next((v for k, v in by_name.items() if k.startswith(nkey) or nkey in k), None)
        disp = (info or {}).get("name") or name
        desc = (info or {}).get("desc") or ""
        resolved.append({
            "name": disp,
            # Prefer official CommunityDragon art (what TFT Academy and most
            # sites render); fall back to MetaTFT's CDN for the brand-new Set 18
            # augments CDragon hasn't published yet, so coverage stays 100%.
            "iconUrl": (info or {}).get("iconUrl") or _metatft_augment_icon(api),
            "tier": a.get("tier") or None,   # rarity: Silver/Gold/Prismatic
            "id": api,
            "category": _augment_category(disp, api, desc),
            "desc": desc,
        })
        if any(k in re.sub(r"[^a-z0-9]", "", api.lower()) for k in key_terms):
            reliant = True
    if not resolved:
        return False
    arch["recommendedAugments"] = resolved
    arch["recommendedAugmentsSource"] = "tftacademy"
    arch["augmentReliant"] = reliant
    return True


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
        # NEW: Track 3-item builds per unit (like tactics.tools)
        # Structure: { "unit_api_name": { "item1|item2|item3": {"games": N, "totalPl": M} } }
        "unitItemBuilds": {},
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
        
        # NEW: Track 3-item builds per unit (tactics.tools style)
        # Only track if unit has 2-3 completed items (meaningful build)
        if 2 <= len(unit_items) <= 3:
            # Sort items alphabetically so "A|B|C" == "C|B|A" 
            build_key = "|".join(sorted(unit_items))
            builds = acc["unitItemBuilds"].setdefault(cid, {})
            build_entry = builds.setdefault(build_key, {"games": 0, "totalPl": 0, "wins": 0})
            build_entry["games"] += 1
            build_entry["totalPl"] += pl
            if pl == 1:
                build_entry["wins"] += 1

    board_augs = [str(a) for a in participant.get("augments", []) if a]
    tb = acc["topBoards"]
    # Pick the slot to write this board into. While under the 50-board cap we
    # simply append. Once full, an augment-bearing board preferentially EVICTS a
    # legacy (augment-less) board so augment coverage backfills over refreshes
    # without shrinking the sample — older stored boards predate augment capture
    # and would otherwise permanently occupy every slot.
    slot = None
    if len(tb) < 50:
        slot = len(tb)
        tb.append(None)
    elif board_augs:
        slot = next((i for i, b in enumerate(tb) if not (b or {}).get("augments")), None)
    if slot is not None:
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
        tb[slot] = {
            "placement": pl, "units": board_units, "traits": active_traits,
            "augments": board_augs,
        }

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

    # NEW: Best item builds per unit (tactics.tools style tier list)
    # Structure: [ { unitName, unitIconUrl, builds: [ {items: [...], games, avgPl, winRate} ] } ]
    best_builds_by_unit = []
    for unit_api, builds_map in acc.get("unitItemBuilds", {}).items():
        if not builds_map:
            continue
        unit_builds = []
        for build_key, stats in builds_map.items():
            if stats["games"] < 2:  # Skip builds with too few games
                continue
            item_apis = build_key.split("|")
            items_list = [
                {"name": _map_name(catalog["items"], iapi), "iconUrl": _map_icon(catalog["items"], iapi)}
                for iapi in item_apis
            ]
            unit_builds.append({
                "items": items_list,
                "games": stats["games"],
                "avgPlacement": stats["totalPl"] / stats["games"],
                "winRate": stats["wins"] / stats["games"] if stats["games"] > 0 else 0,
            })
        if unit_builds:
            # Sort by games played (most popular), then by avg placement (best performing)
            unit_builds.sort(key=lambda x: (-x["games"], x["avgPlacement"]))
            best_builds_by_unit.append({
                "unitName": _map_name(catalog["units"], unit_api),
                "unitIconUrl": _map_icon(catalog["units"], unit_api),
                "unitCost": (catalog["units"].get(unit_api) or {}).get("cost"),
                "builds": unit_builds[:5],  # Top 5 builds per unit
            })
    # Sort units by total games across all builds
    best_builds_by_unit.sort(key=lambda x: -sum(b["games"] for b in x["builds"]))

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
        "bestBuilds": best_builds_by_unit[:20],  # Top 20 units with best builds (tactics.tools style)
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


def _cluster_boards(boards: list, min_jaccard: float = 0.45, min_size: int = 2, catalog: dict | None = None, with_opener: bool = True) -> list:
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
    used_meta_names: set = set()  # dedupe meta board names across archetypes
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
            # Leveling first: it sets carryName/category, which _match_tft_comp uses.
            arch.update(_classify_comp_leveling(core_units, flex_units, catalog))
            # Best-matching meta comp (TFT Academy, live set only). Used for the
            # prominent board NAME, exact board positioning, carousel, and the
            # authored enrichment below (stage tips, difficulty, late-game cap).
            match = _match_tft_comp(arch) if with_opener else None
            pos_overrides: dict = {}
            if match:
                # Name only if the arch fields this comp's primary carry (guards
                # against mislabelling a carry-less board), deduped across archs.
                _apply_meta_name(arch, match, used_meta_names)
                board_items = {}
                for ch in match.get("characters") or []:
                    nm, r, c = ch.get("name"), ch.get("row"), ch.get("col")
                    if nm and isinstance(r, int) and isinstance(c, int):
                        pos_overrides[_norm_key(nm)] = (r, c)
                    its = ch.get("items") or []
                    if nm and its:
                        board_items[nm] = its
                # Authored item build per board unit (TFT Academy finalComp) — the
                # main-priority items to show ON the board / prioritize in the
                # build finder, keyed by display name.
                if board_items:
                    arch["boardItems"] = board_items
                else:
                    arch.pop("boardItems", None)
                # Authored enrichment from TFT Academy (no-ops for the older
                # tftactics dataset, which lacks these fields):
                #  • stageTips  — comp-specific stage-by-stage roll/level guidance
                #    (far more actionable than our generic per-category curve),
                #  • difficulty — Easy/Medium/Hard rating,
                #  • lateGame   — max-cap additions and the unit each replaces,
                #  • augmentsTip — expert note on emblems/holders/late-game cap.
                tips = [t for t in (match.get("tips") or [])
                        if isinstance(t, dict) and t.get("tip")]
                if tips:
                    arch["stageTips"] = tips
                if match.get("difficulty"):
                    arch["difficulty"] = match["difficulty"]
                if match.get("maxCap"):
                    arch["lateGame"] = [
                        {"name": u.get("name"),
                         "replaces": (u.get("replaces") or [None])[0],
                         "items": u.get("items") or [],
                         "addLevel": u.get("addLevel")}
                        for u in match["maxCap"] if u.get("name")
                    ]
                if match.get("augmentsTip"):
                    arch["augmentsTip"] = match["augmentsTip"]
                embs = _emblems_from_comp(match, catalog)
                if embs:
                    arch["emblems"] = embs
                else:
                    arch.pop("emblems", None)
                # Carry/tank ITEM HOLDERS from TFT Academy's authored itemization
                # (classified by item type; a comp can have multiple carries and
                # tanks). Overrides our heuristic when the comp matches.
                allowed_keys = {_norm_key(u.get("name", "")) for u in (core_units + flex_units)}
                tfta_carries, tfta_tanks = _tfta_carry_tank(match, catalog, allowed_keys)
                if tfta_carries:
                    arch["carries"] = tfta_carries
                    arch["carryName"] = tfta_carries[0]
                if tfta_tanks:
                    arch["tanks"] = tfta_tanks
                    arch["tankName"] = tfta_tanks[0]
            # Tier rating from the curated tftactics tier list (confident match,
            # else nearest comp). Live set only — historical sets have no match.
            if with_opener:
                tier = _comp_tier_letter(arch, match)
                if tier:
                    arch["tier"] = tier
            # Also place the comp's FLEX units (the "Flex — seen in 20–70% of
            # boards" section) on the board, so the full comp is shown, not just
            # the core. They're positioned exactly where TFT Academy places them
            # (pos_overrides), else heuristically (front/back) by the layout
            # function, and tagged in ``boardFlex`` so the frontend highlights
            # them distinctly from core units.
            board_flex = []
            placed_keys = {_norm_key(u["name"]) for u in board_units}
            for u in flex_units:
                nm = u.get("name")
                if not nm or _is_hidden_board_unit(nm):
                    continue
                k = _norm_key(nm)
                if k in placed_keys:
                    continue
                placed_keys.add(k)
                board_units.append(u)
                board_flex.append(nm)
            if board_flex:
                arch["boardFlex"] = board_flex
            else:
                arch.pop("boardFlex", None)
            # Non-champion synergy summons the comp places on its board (from TFT
            # Academy). Positioned exactly via pos_overrides (already populated
            # above from the same characters) and rendered from TFTA art.
            summons = _board_summons(match, catalog) if with_opener else []
            # Gate summons by the comp's actual traits so we never show a piece
            # the comp can't spawn (removes false positives from loose matches).
            trait_keys = {_norm_key(t.get("name", "")) for t in (arch.get("traits") or [])}
            summons = [s for s in summons if _summon_allowed(s["name"], trait_keys)]
            if summons:
                arch["summonIcons"] = {s["name"]: s["iconUrl"] for s in summons}
            else:
                arch.pop("summonIcons", None)
            summon_units = [{"name": s["name"]} for s in summons]
            arch["board"] = _compute_board_layout(board_units[:18] + summon_units, catalog, pos_overrides or None)
            # Early-game opener: what to build toward before pivoting to the
            # final board (needs carryName/category from the leveling step above).
            # Openers are curated for the live set only; skip on historical sets.
            if with_opener:
                arch["opener"] = _classify_opener(arch, catalog, match)
                # In-game Team Planner code ("Copy team code") — live set only,
                # since CDragon only publishes codes for the current set.
                tp = catalog.get("teamPlanner") or {}
                aset = catalog.get("activeSet") or 0
                tc = _team_code([u["name"] for u in board_units], tp, aset)
                if tc:
                    arch["teamCode"] = tc
                # Same for the early-game opener board so users can import the
                # transition comp too.
                op = arch.get("opener") or {}
                op_tc = _team_code([u["name"] for u in op.get("units", [])], tp, aset)
                if op_tc:
                    op["teamCode"] = op_tc
                # Carousel priority: prefer the matched meta comp's curated
                # priority (TFT Academy's ordered component/item list, else
                # tftactics' {component,item} pairs), which is weighted toward the
                # comp's BIS carry/tank items; else fall back to the tally below.
                car = None
                if match:
                    car = _carousel_from_tfta(match, catalog) or _carousel_from_tft(match, catalog)
                if car:
                    arch["carouselPriority"] = car
                # Recommended augments: TFT Academy's expert-curated per-comp
                # shortlist is PRIMARY (authored specifically for this comp, and
                # what the user wants ported); MetaTFT's observed tier list is the
                # fallback when we have no confident TFTA match. The authored
                # augmentsTip is attached above regardless, as context alongside
                # whichever list shows.
                if not (match and _attach_tfta_augments(arch, match, catalog)):
                    _attach_recommended_augments(arch, catalog)
            # Carousel priority fallback: aggregate the components a comp's item
            # holders need across their BIS items, ranked by how many are required.
            arch.setdefault("carouselPriority", _compute_carousel_priority(core_units, catalog))
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
    top = results[:30]
    # Ensure archetype ids are unique within a run. The id is a hash of the core
    # unit set, so two distinct clusters that share identical core units (they
    # differ only in flex membership) collide — which violates the
    # archetype_boards primary key (platform, tier, set, arch_id, board_idx) and
    # crashes the whole run. Disambiguate deterministically after the final sort.
    seen_ids: dict = {}
    for arch in top:
        base = arch.get("id") or ""
        n = seen_ids.get(base, 0)
        if n:
            arch["id"] = f"{base}-{n}"
        seen_ids[base] = n + 1
    return top


def _store_archetype_boards(platform: str, tier: str, set_num: int, archetypes: list):
    """
    Replace the full per-archetype board list for a set in archetype_boards.
    Wipes the set first so archetypes removed between runs don't leave stragglers.
    """
    from psycopg2.extras import execute_values

    all_rows = []
    seen_keys: set = set()
    for arch in archetypes:
        arch_id = arch.get("id")
        boards = arch.get("_allBoards") or []
        if not arch_id or not boards:
            continue
        for idx, b in enumerate(boards):
            # Guard against duplicate (arch_id, board_idx) keys within the batch
            # so a single collision can't abort the whole insert.
            key = (arch_id, idx)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_rows.append((platform, tier, set_num, arch_id, idx,
                             b.get("placement"), json.dumps(b)))

    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM archetype_boards WHERE platform=%s AND tier=%s AND set_number=%s",
            [platform, tier, set_num],
        )
        # Batched multi-row inserts — one round-trip per chunk (vs. per row).
        # ON CONFLICT DO NOTHING is a belt-and-suspenders guard against any
        # residual key collision so the run can't crash on a duplicate.
        for i in range(0, len(all_rows), 500):
            execute_values(
                cur,
                "INSERT INTO archetype_boards "
                "(platform, tier, set_number, arch_id, board_idx, placement, board) VALUES %s "
                "ON CONFLICT (platform, tier, set_number, arch_id, board_idx) DO NOTHING",
                all_rows[i:i + 500],
            )
    conn.commit()
    print(f"[archetypes] Stored {len(all_rows)} boards for on-demand paging (set {set_num})")


def _top_augments(aug_map: dict, n: int = 12) -> list:
    """Rank aggregated augments by pick volume, with weighted avg placement.

    ``aug_map`` values carry {games, iconUrl, tier, totalPl} where totalPl is
    the games-weighted sum of per-player avg placements. Emitted entries match
    the per-archetype augment shape the frontend already renders.
    """
    out = []
    for name, v in aug_map.items():
        g = v.get("games", 0) or 0
        out.append({
            "name": name,
            "iconUrl": v.get("iconUrl"),
            "tier": v.get("tier"),
            "games": g,
            "avgPlacement": round(v["totalPl"] / g, 2) if g else None,
        })
    return sorted(out, key=lambda x: -x["games"])[:n]


def _cache_archetypes(platform: str, tier: str, active_set: int, target_set: int | None = None,
                      catalog: dict | None = None, with_opener: bool | None = None):
    """
    Compute and cache comp archetypes for a given set.

    For the active set, reads from challenger_players.
    For historical sets, reads from historical_insights (which may contain
    data pooled from multiple tiers — all stored as 'challenger').

    ``with_opener`` controls whether live-set enrichments (opener plan, in-game
    team code, tftactics carousel) are computed. It defaults to "not historical",
    but the current-set SEED reads from historical_insights (passing active_set=0)
    while still being the live set, so it must pass with_opener=True explicitly —
    otherwise the enrichments (and the Copy-team-code button) silently vanish.
    """
    set_num = target_set if target_set is not None else active_set
    is_historical = (set_num != active_set)
    if with_opener is None:
        with_opener = not is_historical

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
    archetypes = _cluster_boards(all_boards, catalog=catalog, with_opener=with_opener)
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
    aug_map: dict = {}
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
        for aug in (ins.get("topAugments") or []):
            n = aug.get("name")
            if not n:
                continue
            g = aug.get("games", 0) or 0
            e = aug_map.setdefault(n, {"games": 0, "iconUrl": aug.get("iconUrl"),
                                      "tier": aug.get("tier"), "totalPl": 0.0})
            e["games"] += g
            ap = aug.get("avgPlacement")
            if ap is not None:
                e["totalPl"] += ap * g

    def _top_n(d: dict, n: int = 20) -> list:
        return sorted(
            [{"name": k, **v} for k, v in d.items()],
            key=lambda x: -x.get("games", 0),
        )[:n]

    global_summary = {
        "topItems": _top_n(item_map),
        "topUnits": _top_n(unit_map),
        "topTraits": _top_n(trait_map),
        "topAugments": _top_augments(aug_map),
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
    aug_map: dict = {}
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
        for aug in (ins.get("topAugments") or []):
            n = aug.get("name")
            if not n:
                continue
            g = aug.get("games", 0) or 0
            e = aug_map.setdefault(n, {"games": 0, "iconUrl": aug.get("iconUrl"),
                                      "tier": aug.get("tier"), "totalPl": 0.0})
            e["games"] += g
            ap = aug.get("avgPlacement")
            if ap is not None:
                e["totalPl"] += ap * g

    def _top_n(d: dict, n: int = 20) -> list:
        return sorted([{"name": k, **v} for k, v in d.items()], key=lambda x: -x.get("games", 0))[:n]

    global_summary = {
        "topItems": _top_n(item_map),
        "topUnits": _top_n(unit_map),
        "topTraits": _top_n(trait_map),
        "topAugments": _top_augments(aug_map),
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
                flushed_state: dict[str, int] = {}

                def _flush(acc_map: dict[str, dict]) -> int:
                    """Delta + batched upsert of changed players, plus a seed-progress
                    checkpoint. Only rows whose board count changed since the last
                    flush are written (avoids re-upserting the whole accumulator each
                    checkpoint — the main RU sink)."""
                    from psycopg2.extras import execute_values
                    rows = []
                    for pid, a in acc_map.items():
                        mc = a["matchCount"]
                        if mc == 0 or flushed_state.get(pid) == mc:
                            continue
                        ins = _derive_insights(a, catalog)
                        ins["_raw"] = True
                        ins["patchStartTs"] = start_ts
                        rows.append((platform, run_tier, pid, target_set,
                                     names.get(pid), json.dumps(ins), int(time.time() * 1000)))
                        flushed_state[pid] = mc
                    conn = _get_conn()
                    with conn.cursor() as cur:
                        for i in range(0, len(rows), 500):
                            execute_values(
                                cur,
                                "INSERT INTO historical_insights "
                                "(platform, tier, puuid, set_number, summoner_name, insights, computed_at) "
                                "VALUES %s ON CONFLICT (platform, tier, puuid, set_number) DO UPDATE SET "
                                "insights = EXCLUDED.insights, computed_at = EXCLUDED.computed_at",
                                rows[i:i + 500],
                            )
                        # Persist seed progress so a timeout resumes instead of restarting.
                        cur.execute(
                            "INSERT INTO meta_cache (cache_key, payload, computed_at) VALUES (%s,%s,%s) "
                            "ON CONFLICT (cache_key) DO UPDATE SET payload=EXCLUDED.payload, "
                            "computed_at=EXCLUDED.computed_at",
                            [seed_key, json.dumps(sorted(processed_seeds)), int(time.time() * 1000)],
                        )
                    conn.commit()
                    print(f"\n[backfill]   flushed {len(rows)} players "
                          f"({len(processed_seeds)} seeds done)")
                    return len(rows)

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

    flushed_state: dict[str, int] = {}

    def _flush() -> int:
        """Persist accumulated players to historical_insights so a long crawl is
        checkpointed and survives interruption.

        Delta + batched: only players whose board count changed since the last
        flush are written, in one batched multi-row upsert. Previously this
        re-upserted the ENTIRE accumulator on every checkpoint, which (with a
        6-hourly cron) produced hundreds of thousands of redundant writes — the
        dominant RU cost. Correctness is preserved: the final _flush after the
        crawl writes every player's final state.
        """
        from psycopg2.extras import execute_values
        rows = []
        for ppid, a in accs.items():
            mc = a["matchCount"]
            if mc == 0 or flushed_state.get(ppid) == mc:
                continue
            ins = _derive_insights(a, catalog)
            ins["_raw"] = True
            ins["patchStartTs"] = start_ts
            rows.append((platform, "challenger", ppid, active_set,
                         names.get(ppid), json.dumps(ins), int(time.time() * 1000)))
            flushed_state[ppid] = mc
        if not rows:
            return 0
        conn = _get_conn()
        with conn.cursor() as cur:
            for i in range(0, len(rows), 500):
                execute_values(
                    cur,
                    "INSERT INTO historical_insights "
                    "(platform, tier, puuid, set_number, summoner_name, insights, computed_at) "
                    "VALUES %s ON CONFLICT (platform, tier, puuid, set_number) DO UPDATE SET "
                    "insights = EXCLUDED.insights, computed_at = EXCLUDED.computed_at",
                    rows[i:i + 500],
                )
        conn.commit()
        return len(rows)

    while queue and _boards() < target_boards:
        # Gather-then-commit: no DB writes mid-crawl. Everything is accumulated in
        # memory and only persisted once the crawl completes (single _flush below),
        # so a failed/timed-out crawl spends ZERO write RU. At these targets the
        # crawl finishes in ~30 min (well within the job limit), so DB checkpoints
        # aren't needed for resumption.
        if processed > 0 and processed % 10 == 0:
            print(f"\n[seed]   progress: {len(accs)} players in memory ({_boards()} boards)")
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
        # active_set=0 forces the historical_insights read path (where the seed
        # wrote), but this IS the live set, so compute openers/team codes/carousel.
        _cache_archetypes(platform, "challenger", active_set=0, target_set=active_set,
                          catalog=catalog, with_opener=True)
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
