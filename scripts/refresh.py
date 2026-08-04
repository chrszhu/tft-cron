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
PATCH_WINDOW_DAYS = 7   # rolling window for current-set refresh; also used for historical backfills
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

    traits: dict = {}
    for trait in set_data.get("traits", []):
        api = trait.get("apiName") or ""
        name = trait.get("name") or ""
        icon = trait.get("icon") or trait.get("iconPath") or ""
        if api and name:
            traits[api] = {"name": name, "iconUrl": _normalize_icon(icon)}
            traits[api.lower()] = {"name": name, "iconUrl": _normalize_icon(icon)}

    units: dict = {}
    for unit in set_data.get("champions", []):
        api = unit.get("apiName") or ""
        name = unit.get("name") or ""
        # CDragon uses "tileIcon" / "squareIcon" (not the *Path variants)
        icon = unit.get("tileIcon") or unit.get("squareIcon") or unit.get("tileIconPath") or ""
        cost = unit.get("cost", 0)
        if api and name:
            entry = {"name": name, "iconUrl": _normalize_icon(icon), "cost": cost}
            units[api] = entry
            units[api.lower()] = entry

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
    return {"items": items, "traits": traits, "units": units, "augments": augments}


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
    17: (1776211200, 9999999999),   # Set 17 Space Gods:       Apr 15 2026 – present
}


def _fetch_match_ids(
    routing: str,
    puuid: str,
    api_key: str,
    since_ts_s: Optional[int],
    active_set: int,
    end_ts_s: Optional[int] = None,   # hard ceiling (used for backfill)
    ignore_patch_floor: bool = False,  # skip the 14-day rolling floor
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
    while len(all_ids) < 500:
        n = min(200, 500 - len(all_ids))
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
        acc["topBoards"].append({"placement": pl, "units": board_units, "traits": active_traits})

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


def _cluster_boards(boards: list, min_jaccard: float = 0.45, min_size: int = 2) -> list:
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
                    "count": 0, "itemBoardCount": 0, "items": {},
                })
                ud["count"] += 1
                unit_items = unit.get("items", [])
                if len(unit_items) >= 2:
                    ud["itemBoardCount"] += 1
                for it in unit_items:
                    iname = it.get("name")
                    if iname:
                        ie = ud["items"].setdefault(iname, {"iconUrl": it.get("iconUrl"), "count": 0})
                        ie["count"] += 1
        trait_counts: dict = {}
        for board in cluster_boards:
            for t in board.get("traits", []):
                name = t.get("name")
                if name:
                    trait_counts[name] = trait_counts.get(name, 0) + 1
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
        results.append({"boardCount": total, "coreUnits": core_units, "flexUnits": flex_units, "traits": traits})

    results.sort(key=lambda x: -x["boardCount"])
    return results[:30]


def _cache_archetypes(platform: str, tier: str, active_set: int, target_set: int | None = None):
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

    archetypes = _cluster_boards(all_boards)
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
    sets_rows = _execute(
        "SELECT DISTINCT set_number FROM challenger_players "
        "WHERE platform=%s AND tier=%s ORDER BY set_number",
        [platform, tier], fetch="all",
    ) or []
    available_sets = {
        "sets": [int(r.get("set_number", 0)) for r in sets_rows],
        "activeSet": active_set,
    }

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

    # Use the last PATCH_WINDOW_DAYS of the set — captures end-of-set meta
    # (most refined comps) while keeping the backfill fast enough to finish in one run.
    window_floor = effective_end - PATCH_WINDOW_DAYS * 86400
    start_ts = max(start_ts, window_floor)

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

        # Skip players already processed in a previous (possibly timed-out) run.
        # This makes re-runs resume from where they left off rather than restarting.
        done_rows = _execute(
            "SELECT puuid FROM historical_insights WHERE platform=%s AND tier=%s AND set_number=%s",
            [platform, run_tier, target_set], fetch="all",
        ) or []
        done_puuids = {r["puuid"] for r in done_rows}
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
                found = 0

                for i, row in enumerate(rows):
                    puuid = row["puuid"]
                    time.sleep(REQUEST_DELAY)

                    match_ids = _fetch_match_ids(
                        routing, puuid, api_key,
                        since_ts_s=start_ts,
                        active_set=target_set,
                        end_ts_s=effective_end,
                        ignore_patch_floor=True,  # backfill ignores the rolling patch window
                    )

                    if not match_ids:
                        print(f"\r[backfill]   {i+1}/{len(rows)} — {puuid[:12]}… no matches", end="", flush=True)
                        continue

                    acc = _empty_acc()
                    for mid in match_ids:
                        time.sleep(REQUEST_DELAY)
                        resp = _fetch(f"https://{routing}.api.riotgames.com/tft/match/v1/matches/{mid}", api_key)
                        if not resp or not resp.ok:
                            continue
                        match = resp.json()
                        match_set = match.get("info", {}).get("tft_set_number")
                        if match_set is not None and match_set != target_set:
                            continue
                        participant = next(
                            (p for p in match.get("info", {}).get("participants", []) if p.get("puuid") == puuid),
                            None,
                        )
                        if not participant:
                            continue
                        match_ts_s = match.get("info", {}).get("game_datetime", 0) // 1000
                        _accumulate(acc, participant, catalog, match_ts_s)

                    if acc["matchCount"] == 0:
                        continue

                    found += 1
                    insights = _derive_insights(acc, catalog)
                    insights["_raw"] = True
                    insights["patchStartTs"] = start_ts

                    # Write to historical_insights (separate table keyed by set_number).
                    _execute(
                        """INSERT INTO historical_insights
                            (platform, tier, puuid, set_number, summoner_name, insights, computed_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (platform, tier, puuid, set_number) DO UPDATE SET
                             insights = EXCLUDED.insights,
                             computed_at = EXCLUDED.computed_at""",
                        [platform, run_tier, puuid, target_set,
                         row.get("summoner_name"), json.dumps(insights), int(time.time() * 1000)],
                    )
                    print(f"\r[backfill]   {i+1}/{len(rows)} — {found} with Set {target_set} data", end="", flush=True)

                print()
                print(f"[backfill] {found}/{len(rows)} players had Set {target_set} match data.")

                if found > 0:
                    print(f"[backfill] Computing archetypes for Set {target_set}…")
                    active_set_num = int(os.environ.get("TFT_ACTIVE_SET", "17"))
                    _cache_archetypes(platform, run_tier, active_set=active_set_num, target_set=target_set)
                    _export_historical_snapshot(platform, run_tier, active_set_num, target_set)

    print(f"\n[backfill] Done! Set {target_set} data is now in the DB.")
    print("[backfill] The UI set-selector will show it automatically on next page load.")


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
    _cache_archetypes(platform, tier, active_set)

    # ── Step 5: Export static snapshot ───────────────────────────────────────
    # Writes public/data/snapshot_{platform}_{tier}.json so Vercel serves it
    # from the CDN edge — no serverless cold-start, no DB query on page load.
    if not ladder_only:
        _export_static_snapshot(platform, tier, active_set)

    print("\n[refresh] Done! Data is live.")
    print("[refresh] Push public/data/ to git so Vercel picks up the new snapshot.")


if __name__ == "__main__":
    main()
