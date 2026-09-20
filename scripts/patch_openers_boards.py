#!/usr/bin/env python3
"""One-off: re-enrich committed snapshots with the improved opener matching and
tftactics-based board positioning WITHOUT a full cron/DB refresh.

The weekly cron bakes these fields, but code changes only take effect on the
next run. This patches the live snapshots in place so the fixes are visible
immediately: (1) openers now use the tftactics early board that best transitions
into each final board; (2) board layout borrows real front/back positioning
from tftactics (fixes e.g. Kennen shown as a back-liner).
"""
import importlib.util
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

spec = importlib.util.spec_from_file_location("refresh", os.path.join(HERE, "refresh.py"))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)

SNAPSHOTS = [
    os.path.join(ROOT, "public/data/snapshot_na1_challenger.json"),
    os.path.join(ROOT, "public/data/snapshot_na1_challenger_18.json"),
]

ACTIVE_SET = 18


def _board_units(arch: dict) -> list:
    """Core units only, hidden filtered, deduped, capped — mirrors clustering."""
    out, seen = [], set()
    for u in arch.get("coreUnits", []):
        nm = u.get("name")
        if not nm or r._is_hidden_board_unit(nm) or nm in seen:
            continue
        seen.add(nm)
        out.append(u)
    return out[:12]


def main() -> None:
    catalog = r._fetch_catalog(ACTIVE_SET)
    tp = catalog.get("teamPlanner") or {}
    aset = catalog.get("activeSet") or ACTIVE_SET

    for path in SNAPSHOTS:
        if not os.path.exists(path):
            print(f"[patch] skip missing {path}")
            continue
        snap = json.load(open(path))
        wb = snap.get("winningBoards") or {}
        archs = wb.get("archetypes") or []
        changed_open = changed_board = named = aug_attached = 0
        used_meta_names: set = set()  # dedupe meta board names across archetypes
        for arch in archs:
            # 1. Prominent meta board name + exact positions from the matched
            #    comp. Computed first so the opener can reuse the SAME comp.
            match = r._match_tft_comp(arch)
            pos_overrides: dict = {}
            if match:
                # Name only if the arch fields this comp's primary carry (guards
                # against mislabelling a carry-less board), deduped across archs.
                r._apply_meta_name(arch, match, used_meta_names)
                if arch.get("metaName"):
                    named += 1
                board_items = {}
                for ch in match.get("characters") or []:
                    nm, row, col = ch.get("name"), ch.get("row"), ch.get("col")
                    if nm and isinstance(row, int) and isinstance(col, int):
                        pos_overrides[r._norm_key(nm)] = (row, col)
                    its = ch.get("items") or []
                    if nm and its:
                        board_items[nm] = its
                # Authored per-unit item build (TFT Academy finalComp) — shown on
                # the board and prioritized in the build finder.
                if board_items:
                    arch["boardItems"] = board_items
                else:
                    arch.pop("boardItems", None)
                # Authored TFT Academy enrichment (no-op on the tftactics dataset).
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
                embs = r._emblems_from_comp(match, catalog)
                if embs:
                    arch["emblems"] = embs
                else:
                    arch.pop("emblems", None)
                # Carry/tank item holders from TFT Academy's authored itemization
                # (multiple possible; classified by item type). Mirrors _cluster_boards.
                allowed_keys = {r._norm_key(u.get("name", ""))
                                for u in (arch.get("coreUnits", []) + arch.get("flexUnits", []))}
                tfta_carries, tfta_tanks = r._tfta_carry_tank(match, catalog, allowed_keys)
                if tfta_carries:
                    arch["carries"] = tfta_carries
                    arch["carryName"] = tfta_carries[0]
                else:
                    arch.pop("carries", None)
                if tfta_tanks:
                    arch["tanks"] = tfta_tanks
                    arch["tankName"] = tfta_tanks[0]
                else:
                    arch.pop("tanks", None)

            # 1a. Recommended augments: TFT Academy primary (authored per-comp
            #     shortlist), MetaTFT fallback when there's no confident match.
            if match and r._attach_tfta_augments(arch, match, catalog):
                aug_attached += 1
            elif r._attach_recommended_augments(arch, catalog):
                aug_attached += 1

            # 1b. Carousel priority from the matched comp (TFTA order, else
            #     tftactics pairs); leave any existing value if no match.
            if match:
                car = r._carousel_from_tfta(match, catalog) or r._carousel_from_tft(match, catalog)
                if car:
                    arch["carouselPriority"] = car

            # 1c. Tier rating from the curated meta tier list (confident match,
            #     else nearest comp) so every comp gets a grade.
            tier = r._comp_tier_letter(arch, match)
            if tier:
                arch["tier"] = tier

            # 2. Opener from the SAME matched comp's early board (so title, early
            #    board, and guide are internally consistent).
            new_op = r._classify_opener(arch, catalog, match)
            if new_op:
                op_units = [u.get("name") for u in new_op.get("units", [])]
                op_tc = r._team_code(op_units, tp, aset)
                if op_tc:
                    new_op["teamCode"] = op_tc
                if (arch.get("opener") or {}).get("units") != new_op.get("units"):
                    changed_open += 1
                arch["opener"] = new_op

            # 3. Re-derive board layout with tftactics positioning (exact first).
            bu = _board_units(arch)
            # Also place the comp's FLEX units (the Flex-section units) on the
            # board — positioned by TFT Academy where available, else
            # heuristically — and tag them so the frontend highlights them.
            # Mirrors _cluster_boards in refresh.py.
            board_flex = []
            placed_keys = {r._norm_key(u["name"]) for u in bu}
            for u in arch.get("flexUnits", []):
                nm = u.get("name")
                if not nm or r._is_hidden_board_unit(nm):
                    continue
                k = r._norm_key(nm)
                if k in placed_keys:
                    continue
                placed_keys.add(k)
                bu.append(u)
                board_flex.append(nm)
            if board_flex:
                arch["boardFlex"] = board_flex
            else:
                arch.pop("boardFlex", None)
            # Non-champion synergy summons (Elderwood Stonebark Tree, Crimson
            # Raptor, Sentry, …) placed on the board via TFT Academy positions
            # + art (they aren't in CDragon).
            summons = r._board_summons(match, catalog)
            # Gate by the comp's actual traits (drops summons the comp can't spawn).
            trait_keys = {r._norm_key(t.get("name", "")) for t in (arch.get("traits") or [])}
            summons = [s for s in summons if r._summon_allowed(s["name"], trait_keys)]
            if summons:
                arch["summonIcons"] = {s["name"]: s["iconUrl"] for s in summons}
            else:
                arch.pop("summonIcons", None)
            summon_units = [{"name": s["name"]} for s in summons]
            new_board = r._compute_board_layout(bu[:18] + summon_units, catalog, pos_overrides or None)
            if new_board != arch.get("board"):
                changed_board += 1
            arch["board"] = new_board
            tc = r._team_code([u.get("name") for u in bu], tp, aset)
            if tc:
                arch["teamCode"] = tc

        with open(path, "w") as f:
            json.dump(snap, f, ensure_ascii=False, separators=(",", ":"))
        print(f"[patch] {os.path.basename(path)}: {len(archs)} archetypes, "
              f"{changed_open} openers changed, {changed_board} boards changed, "
              f"{named} named from tftactics, {aug_attached} with recommended augments")

        _validate_openers(path, archs, catalog)


def _validate_openers(path: str, archs: list, catalog: dict) -> None:
    """Flag any opener whose guide text names a champion that is NOT on that
    comp's early board or final core/flex. Target: ZERO contradictions."""
    offenders = []
    for arch in archs:
        op = arch.get("opener") or {}
        detail = op.get("detail") or ""
        allowed = r._opener_allowed_norms(op.get("units") or [], arch)
        bad = sorted({n for n in r._units_named_in_text(detail, catalog)
                      if r._norm_key(n) not in allowed})
        if bad:
            offenders.append((arch.get("metaName") or arch.get("carryName") or "?", bad, detail))
    tag = os.path.basename(path)
    print(f"[validate] {tag}: {len(offenders)} opener contradiction(s) "
          f"across {len(archs)} archetypes")
    for name, bad, detail in offenders:
        print(f"  ✗ {name}: off-board {bad}\n      detail: {detail}")


if __name__ == "__main__":
    main()
