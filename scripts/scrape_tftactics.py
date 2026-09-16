#!/usr/bin/env python3
"""Scrape tftactics.gg team-comps into scripts/tftactics_set18.json.

tftactics.gg is a Create-React-App that bakes its entire comp dataset into
``main.<hash>.chunk.js`` as a ``JSON.parse('…')`` blob. There is no public JSON
API, so we:

  1. Fetch the team-comps page and locate the hashed ``main`` chunk.
  2. Fetch that chunk and pull out the ``JSON.parse('…')`` payload that contains
     the comps (identified by the tell-tale ``carrousel`` field).
  3. Decode the JS string literal → ``json.loads`` → list of comp objects.
  4. Filter to the target set and emit the normalized fields our refresh
     pipeline consumes.

Beyond the fields the old scrape captured (name / playstyle / description /
units / carries / mid / carrousel) we now also keep each character's board
POSITION and ITEMS. tftactics stores positions as ``pN`` on a 4-row × 7-col
board (row-major, front row first), which we convert to {row, col} so the
refresh pipeline can borrow real positioning instead of guessing from range.

Usage:
    python scripts/scrape_tftactics.py            # set 18 → tftactics_set18.json
    python scripts/scrape_tftactics.py --set 17 --out /tmp/s17.json
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re

import requests

BASE = "https://tftactics.gg"
PAGE = f"{BASE}/tierlist/team-comps/"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

BOARD_COLS = 7  # tftactics renders a 4×7 hex board; positions are p1..p28.


def _fetch(url: str) -> str:
    resp = requests.get(url, headers=UA, timeout=30)
    resp.raise_for_status()
    return resp.text


def _main_chunk_url(html: str) -> str:
    m = re.findall(r'src="(/static/js/main\.[0-9a-f]+\.chunk\.js)"', html)
    if not m:
        raise RuntimeError("could not find main.*.chunk.js in team-comps page")
    return BASE + m[0]


def _extract_comps(js: str) -> list:
    """Pull the comp dataset out of a ``JSON.parse('…')`` call in the bundle."""
    for m in re.finditer(r"JSON\.parse\(", js):
        q = m.end()
        if q >= len(js):
            continue
        quote = js[q]
        if quote not in "'\"":
            continue
        # Walk the JS string literal, honoring backslash escapes, to its close.
        k = q + 1
        buf: list = []
        while k < len(js):
            ch = js[k]
            if ch == "\\":
                buf.append(js[k:k + 2])
                k += 2
                continue
            if ch == quote:
                break
            buf.append(ch)
            k += 1
        body = "".join(buf)
        if "carrousel" not in body:
            continue
        try:
            # ast.literal_eval decodes the JS/Python string escapes (\', \", \n,
            # \uXXXX) into the raw JSON text, which we then parse.
            text = ast.literal_eval(quote + body + quote)
            data = json.loads(text)
        except Exception:
            continue
        if isinstance(data, list) and data and isinstance(data[0], dict) and "carrousel" in data[0]:
            return data
    raise RuntimeError("comp dataset (carrousel) not found in bundle")


def _pos_rc(pos) -> dict | None:
    """Map a tftactics position code (``pN``, 1..28) to {row, col}.

    The board is 4 rows × 7 cols, numbered row-major from the FRONT row
    (p1..p7 = front, p22..p28 = back). Verified against known units
    (front-line tanks like Sett at low N, back-line carries like Ahri at high N).
    """
    try:
        n = int(str(pos).lstrip("p"))
    except (TypeError, ValueError):
        return None
    if not (1 <= n <= 28):
        return None
    return {"row": (n - 1) // BOARD_COLS, "col": (n - 1) % BOARD_COLS}


def _normalize(comp: dict) -> dict:
    chars = comp.get("characters") or []
    units = [c.get("name") for c in chars if c.get("name")]
    # Carries / item holders = characters that tftactics shows holding items.
    carries = [c.get("name") for c in chars if c.get("items")]
    out_chars = []
    for c in chars:
        rc = _pos_rc(c.get("position"))
        out_chars.append({
            "name": c.get("name"),
            "items": c.get("items") or [],
            "row": rc["row"] if rc else None,
            "col": rc["col"] if rc else None,
        })
    return {
        "name": comp.get("name"),
        "tier": comp.get("tier"),
        "playstyle": comp.get("playstyle"),
        "description": comp.get("description"),
        "units": units,
        "carries": carries,
        "mid": comp.get("mid") or [],
        "carrousel": comp.get("carrousel") or [],
        "characters": out_chars,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape tftactics.gg team comps.")
    ap.add_argument("--set", type=int, default=18, help="TFT set number to keep")
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "tftactics_set18.json"))
    args = ap.parse_args()

    html = _fetch(PAGE)
    js = _fetch(_main_chunk_url(html))
    comps = _extract_comps(js)
    selected = [_normalize(c) for c in comps if args.set in (c.get("set") or [])]
    # Best tier first (1 = S) so downstream matching prefers stronger comps.
    selected.sort(key=lambda c: c.get("tier") if isinstance(c.get("tier"), int) else 99)
    print(f"[scrape] {len(comps)} total comps in bundle, {len(selected)} for set {args.set}")
    if not selected:
        raise SystemExit(f"no comps found for set {args.set}")
    with open(args.out, "w") as f:
        json.dump(selected, f, ensure_ascii=False, indent=2)
    print(f"[scrape] wrote {len(selected)} comps → {args.out}")


if __name__ == "__main__":
    main()
