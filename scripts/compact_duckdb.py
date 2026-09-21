#!/usr/bin/env python3
"""
Compact the DuckDB file for committing back to the cron repo.

Rewrites $DUCKDB_PATH (default <repo>/data/tft.duckdb) into a fresh, compacted
file that:
  • keeps the operationally-needed tables (ladder_meta, challenger_players,
    meta_cache, archetype_boards) — everything the weekly current-set refresh
    reads/writes and everything the static snapshots are built from, and
  • DROPS historical_insights (the ~160 MB bulk). That table is a transient
    staging area for the current-set crawl (its rows are copied into
    challenger_players.insights each run) and the historical set snapshots are
    already baked as static files in the app repo's public/data. Excluding it
    keeps the committed DuckDB file well under GitHub's 100 MB file limit, so we
    can version it in git WITHOUT Git LFS.

Uses 16 KiB blocks to minimise free-space waste. The schema is recreated via
refresh._ensure_schema() so historical_insights still exists (empty) — the next
run's crawl repopulates it and _available_sets() derives the historical tabs
from meta_cache regardless.
"""

import importlib.util
import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

KEEP_TABLES = ["ladder_meta", "challenger_players", "meta_cache", "archetype_boards"]


def main() -> int:
    import duckdb

    orig = os.environ.get("DUCKDB_PATH") or str(REPO / "data" / "tft.duckdb")
    orig = str(Path(orig).resolve())
    if not os.path.exists(orig):
        print(f"[compact] {orig} does not exist — nothing to compact.")
        return 0

    tmp = orig + ".compact"
    for p in (tmp, tmp + ".wal"):
        if os.path.exists(p):
            os.remove(p)

    # Import refresh to reuse its exact schema (all 5 tables).
    spec = importlib.util.spec_from_file_location("refresh", str(HERE / "refresh.py"))
    refresh = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(refresh)

    con = duckdb.connect(tmp, config={"default_block_size": "16384"})
    refresh._conn = con
    refresh._ensure_schema()  # creates all 5 tables; historical_insights stays empty

    con.execute(f"ATTACH '{orig}' AS src (READ_ONLY)")
    for t in KEEP_TABLES:
        try:
            con.execute(f"INSERT INTO {t} SELECT * FROM src.{t}")
        except Exception as exc:
            print(f"[compact] WARN copying {t}: {exc}")
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in KEEP_TABLES}
    con.close()

    os.replace(tmp, orig)
    size = os.path.getsize(orig)
    print(f"[compact] Wrote {orig} — {size/1_048_576:.1f} MiB, rows={counts}")
    if size >= 100 * 1_048_576:
        print("[compact] WARNING: file >= 100 MiB — GitHub will reject a non-LFS push!")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
