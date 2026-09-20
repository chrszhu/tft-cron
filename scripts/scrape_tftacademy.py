#!/usr/bin/env python3
"""Scrape tftacademy.com team comps into scripts/tftacademy_set18.json.

tftacademy.com is a SvelteKit app. Every comp-tierlist route exposes a
dehydrated data endpoint at ``<route>/__data.json`` whose payload is a *flat*
array of values where objects reference other entries by integer index. One
request to any comp route returns the ENTIRE ``guides`` array (all comps for the
set), so we fetch a single stable route and reconstruct the objects.

Compared to the tftactics scrape this dataset is strictly richer — for each comp
it carries expert-authored:

  • exact board positioning (``boardIndex`` 0..27 on a 4×7 hex grid),
  • stage-by-stage roll/level tips (``tips`` = [{stage, tip}]),
  • real Set 18 augment picks grouped ECON / ITEMS / COMBAT (+ an ``augmentsTip``),
  • early → final boards with per-unit items, a late-game ``maxCap`` (with the
    unit each addition replaces), a curated carousel priority, plus a tier and a
    difficulty rating.

We normalize this into a SUPERSET of the tftactics schema our refresh pipeline
already consumes (name / tier(int) / units / carries / mid / carrousel /
characters[{name,row,col,items}]) so existing positioning/tier/opener/carousel
logic keeps working, and we add the new fields (style / difficulty / tips /
augments / augmentsTip / maxCap / earlyComp) for the enrichment layer to pick up.

Usage:
    python scripts/scrape_tftacademy.py            # set 18 → tftacademy_set18.json
    python scripts/scrape_tftacademy.py --set 18 --out /tmp/s18.json
"""
from __future__ import annotations

import argparse
import json
import os
import re

import requests

BASE = "https://tftacademy.com"
# Any valid comp route returns the full guides array in its __data.json; the
# tierlist index route works too and is the most stable.
DATA_URL = f"{BASE}/tierlist/comps/__data.json"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

BOARD_COLS = 7  # 4 rows × 7 cols; boardIndex row-major, FRONT row (0..6) first.

# TFT Academy tier letters → tftactics-style integer (1 = best) so downstream
# tier mapping / sorting is unchanged. X = "Situational".
_TIER_TO_INT = {"S": 1, "A": 2, "B": 3, "C": 4, "D": 5, "X": 6}


def _fetch_json(url: str) -> dict:
    resp = requests.get(url, headers=UA, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _resolve_svelte(payload: dict) -> dict:
    """Rehydrate the SvelteKit flat-array node with the largest data table.

    Each node's ``data`` is a flat list; index 0 is the root object whose values
    are integer indices into the same list. We deref recursively (guarding
    against cycles) and return the resolved root.
    """
    best = None
    for node in payload.get("nodes", []):
        if isinstance(node, dict) and isinstance(node.get("data"), list):
            if best is None or len(node["data"]) > len(best):
                best = node["data"]
    if not best:
        raise RuntimeError("no data node found in __data.json")

    def deref(i, seen):
        if not isinstance(i, int):
            return i
        if i < 0 or i >= len(best) or i in seen:
            return None
        v = best[i]
        if isinstance(v, dict):
            return {k: deref(idx, seen | {i}) for k, idx in v.items()}
        if isinstance(v, list):
            return [deref(x, seen | {i}) for x in v]
        return v

    return deref(0, set())


# ── apiName → human display name ─────────────────────────────────────────────
# TFTA api names look like: DA_18_Ahri, DA_Karma18, DA_JeweledGauntlet,
# DA_Component_GiantsBelt, DA_Artifact_Dawncore, DA_18_EmblemInvoker,
# DA_GlassCannon_Silver (augment, tiered). We strip the DA_ prefix, the set
# token (leading "18_" or trailing "18"), category prefixes, and split CamelCase
# into words. refresh.py re-canonicalizes against the live catalog (normalization
# ignores spaces/apostrophes), so "RekSai" → "Rek Sai" still matches "Rek'Sai".
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
# Trailing build/variant descriptors TFTA appends to a champion apiName; our
# catalog uses the base champion name, so we drop these to match.
_VARIANT_WORDS = {"ad", "ap", "base", "small", "melee", "ranged", "big"}


def _humanize(api: str) -> str:
    if not api:
        return ""
    s = api
    if s.startswith("DA_"):
        s = s[3:]
    for pref in ("Component_", "Artifact_"):
        if s.startswith(pref):
            s = s[len(pref):]
    # Strip set tokens anywhere ("18_", trailing/embedded digits): no TFT unit or
    # item name contains a digit, so removing all digit runs is safe and handles
    # mid-string tokens like "Gromp18 AP" / "Elderwood18 Lifeblossom".
    s = re.sub(r"(?:Set)?\d+", "", s)
    s = s.replace("_", " ")
    s = _CAMEL.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Drop a trailing build-variant descriptor ("Master Yi AD" → "Master Yi").
    parts = s.split()
    if len(parts) > 1 and parts[-1].lower() in _VARIANT_WORDS:
        s = " ".join(parts[:-1])
    # Emblems: catalog names them "<Trait> Emblem", not "Emblem <Trait>".
    if s.startswith("Emblem ") and len(s) > len("Emblem "):
        s = f"{s[len('Emblem '):]} Emblem"
    return s


def _aug_display(api: str) -> tuple:
    """(name, tier) for an augment apiName; tier ∈ {Silver,Gold,Prismatic} or ''."""
    tier = ""
    parts = (api or "").split("_")
    if parts and parts[-1].lower() in ("silver", "gold", "prismatic"):
        tier = parts[-1].capitalize()
    name = _humanize(api)
    # Drop a trailing tier word left over from humanize (e.g. "Glass Cannon Silver").
    if tier:
        name = re.sub(rf"\s*{tier}$", "", name, flags=re.IGNORECASE).strip()
    return name, tier


def _rc(board_index) -> dict:
    if not isinstance(board_index, int) or not (0 <= board_index <= 27):
        return {"row": None, "col": None}
    return {"row": board_index // BOARD_COLS, "col": board_index % BOARD_COLS}


def _units_block(block) -> list:
    """Normalize an earlyComp/finalComp/maxCap entry list."""
    out = []
    for u in block or []:
        if not isinstance(u, dict):
            continue
        name = _humanize(u.get("apiName"))
        if not name:
            continue
        rc = _rc(u.get("boardIndex"))
        entry = {
            "name": name,
            # Raw apiName kept so downstream can resolve icons for non-champion
            # synergy summons (Elderwood Stonebark Tree, Crimson Raptor, …) that
            # aren't in CDragon — TFT Academy serves their art keyed by apiName.
            "apiName": u.get("apiName"),
            "items": [_humanize(it) for it in (u.get("items") or []) if it],
            "stars": u.get("stars") or 1,
            "row": rc["row"],
            "col": rc["col"],
        }
        # Predecessors are either the unit(s) this addition replaces (real champ
        # apiNames) OR a level token like ``TFT_Flex_Lv9`` meaning "add at level
        # 9". Split them: keep real replacements, and surface the level context.
        replaces, add_level = [], None
        for p in (u.get("predecessors") or []):
            if not p:
                continue
            m = re.search(r"Lv(\d+)", p)
            if p.startswith("TFT") or m:
                if m:
                    add_level = int(m.group(1))
                continue
            replaces.append(_humanize(p))
        if replaces:
            entry["replaces"] = replaces
        if add_level is not None:
            entry["addLevel"] = add_level
        out.append(entry)
    return out


def _normalize(g: dict) -> dict:
    final = _units_block(g.get("finalComp"))
    early = _units_block(g.get("earlyComp"))
    maxcap = _units_block(g.get("maxCap"))

    units = [u["name"] for u in final]
    carries = [u["name"] for u in final if u["items"]]
    main = g.get("mainChampion") or {}
    main_name = _humanize(main.get("apiName"))
    if main_name and main_name not in carries:
        carries.insert(0, main_name)

    # Augments grouped by type; TFTA gives a parallel augmentTypes list that maps
    # 1:1 to the augments in order? No — augmentTypes are section labels and the
    # augments list is flat. We infer group by tftactics-independent heuristic in
    # refresh.py; here we just carry name/tier/apiName + the section labels.
    augments = []
    for a in g.get("augments") or []:
        if not isinstance(a, dict) or a.get("disabled"):
            continue
        nm, tier = _aug_display(a.get("apiName"))
        if nm:
            augments.append({"name": nm, "tier": tier, "apiName": a.get("apiName")})

    tips = []
    for t in g.get("tips") or []:
        if isinstance(t, dict) and t.get("tip"):
            tips.append({"stage": t.get("stage") or "", "tip": t.get("tip")})

    carousel = [_humanize(c.get("apiName")) for c in (g.get("carousel") or [])
                if isinstance(c, dict) and c.get("apiName")]

    return {
        # tftactics-compatible fields (existing consumers) ───────────────────
        "name": g.get("metaTitle") or g.get("title") or "",
        "tier": _TIER_TO_INT.get((g.get("tier") or "").upper()),
        "playstyle": g.get("style") or "",
        "description": g.get("augmentsTip") or "",
        "units": units,
        "carries": carries,
        "mid": [u["name"] for u in early],
        "carrousel": [],  # legacy tftactics pair-shape; superseded by `carousel`
        "characters": [{"name": u["name"], "apiName": u.get("apiName"),
                        "items": u["items"],
                        "row": u["row"], "col": u["col"]} for u in final],
        # new TFT Academy enrichment fields ──────────────────────────────────
        "source": "tftacademy",
        "slug": g.get("compSlug"),
        "style": g.get("style") or "",
        "difficulty": (g.get("difficulty") or "").title(),
        "mainChampion": main_name,
        "earlyComp": early,
        "finalComp": final,
        "maxCap": maxcap,
        "augments": augments,
        "augmentsTip": g.get("augmentsTip") or "",
        "tips": tips,
        "carousel": carousel,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape tftacademy.com team comps.")
    ap.add_argument("--set", type=int, default=18, help="TFT set number to keep")
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "tftacademy_set18.json"))
    args = ap.parse_args()

    payload = _fetch_json(DATA_URL)
    root = _resolve_svelte(payload)
    guides = root.get("guides") or []
    if not guides:
        raise SystemExit("no guides array found in __data.json")

    selected = [_normalize(g) for g in guides if (g.get("set") == args.set)]
    # Best tier first (1 = S) so downstream matching prefers stronger comps;
    # comps with no tier / situational (X) sink to the bottom.
    selected.sort(key=lambda c: c.get("tier") if isinstance(c.get("tier"), int) else 99)
    print(f"[scrape] {len(guides)} comps in payload, {len(selected)} for set {args.set}")
    if not selected:
        raise SystemExit(f"no comps found for set {args.set}")
    with open(args.out, "w") as f:
        json.dump(selected, f, ensure_ascii=False, indent=2)
    print(f"[scrape] wrote {len(selected)} comps → {args.out}")


if __name__ == "__main__":
    main()
