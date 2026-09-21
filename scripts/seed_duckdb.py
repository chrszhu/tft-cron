#!/usr/bin/env python3
"""
One-time seeder: load scripts/_dbexport/*.jsonl into the local DuckDB file.

This migrates the exported CockroachDB tables into DuckDB (env DUCKDB_PATH,
default <repo>/data/tft.duckdb). It reuses refresh.py's _ensure_schema() and
_bulk_upsert() so the schema + upsert semantics match the live refresh path
exactly. JSON columns (insights/payload/board) are json.dumps'd into VARCHAR.

Usage:
    python scripts/seed_duckdb.py            # -> <repo>/data/tft.duckdb
    DUCKDB_PATH=/tmp/x.duckdb python scripts/seed_duckdb.py

Idempotent: re-running upserts on the primary keys (DO UPDATE).
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
EXPORT_DIR = HERE / "_dbexport"

# Default the DuckDB path before importing refresh (its _get_conn reads it).
os.environ.setdefault("DUCKDB_PATH", str(REPO / "data" / "tft.duckdb"))

# Import refresh.py as a module to reuse its schema + upsert helpers.
_spec = importlib.util.spec_from_file_location("refresh", str(HERE / "refresh.py"))
refresh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(refresh)


# table -> (jsonl file, column order matching the schema, PK cols, JSON cols)
TABLES = {
    "ladder_meta": {
        "file": "ladder_meta.jsonl",
        "cols": ["platform", "tier", "fetched_at", "total_entries", "set_number"],
        "pk": ["platform", "tier"],
        "json": [],
    },
    "challenger_players": {
        "file": "challenger_players.jsonl",
        "cols": ["platform", "tier", "puuid", "league_points", "summoner_id",
                 "summoner_name", "wins", "losses", "rank_val", "inactive",
                 "fresh_blood", "hot_streak", "ladder_position", "ladder_fetched_at",
                 "insights", "insights_error", "insights_fetched_at",
                 "profile_icon_id", "insights_cursor", "set_number"],
        "pk": ["platform", "tier", "puuid"],
        "json": ["insights"],
    },
    "historical_insights": {
        "file": "historical_insights.jsonl",
        "cols": ["platform", "tier", "puuid", "set_number", "summoner_name",
                 "insights", "computed_at"],
        "pk": ["platform", "tier", "puuid", "set_number"],
        "json": ["insights"],
    },
    "meta_cache": {
        "file": "meta_cache.jsonl",
        "cols": ["cache_key", "payload", "computed_at"],
        "pk": ["cache_key"],
        "json": ["payload"],
    },
    "archetype_boards": {
        "file": "archetype_boards.jsonl",
        "cols": ["platform", "tier", "set_number", "arch_id", "board_idx",
                 "placement", "board"],
        "pk": ["platform", "tier", "set_number", "arch_id", "board_idx"],
        "json": ["board"],
    },
}


def _seed_table(name: str, cfg: dict) -> int:
    path = EXPORT_DIR / cfg["file"]
    if not path.exists():
        print(f"[seed] {name}: MISSING {path} — skipped")
        return 0

    cols = cfg["cols"]
    json_cols = set(cfg["json"])
    pk = cfg["pk"]
    pk_idx = [cols.index(c) for c in pk]

    # De-dupe on PK within the file (DuckDB rejects updating the same row twice
    # in one INSERT statement). Last occurrence wins.
    by_pk: dict = {}
    n_lines = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n_lines += 1
            obj = json.loads(line)
            tup = []
            for c in cols:
                v = obj.get(c)
                if c in json_cols and v is not None and not isinstance(v, str):
                    v = json.dumps(v)
                tup.append(v)
            key = tuple(tup[i] for i in pk_idx)
            by_pk[key] = tuple(tup)

    rows = list(by_pk.values())
    update_cols = [c for c in cols if c not in pk]
    refresh._bulk_upsert(name, cols, rows, conflict_cols=pk, update_cols=update_cols)

    cnt = refresh._get_conn().execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    dropped = n_lines - len(rows)
    extra = f" ({dropped} dup PKs collapsed)" if dropped else ""
    print(f"[seed] {name}: read {n_lines} lines -> {len(rows)} rows -> table now {cnt}{extra}")
    return cnt


def main() -> int:
    print(f"[seed] DUCKDB_PATH = {os.environ['DUCKDB_PATH']}")
    print(f"[seed] export dir  = {EXPORT_DIR}")
    refresh._ensure_schema()

    expected = {
        "ladder_meta": 2,
        "challenger_players": 1200,
        "historical_insights": 7476,
        "meta_cache": 12,
        "archetype_boards": 16770,
    }
    ok = True
    for name, cfg in TABLES.items():
        cnt = _seed_table(name, cfg)
        exp = expected.get(name)
        if exp is not None and cnt != exp:
            print(f"[seed]   WARNING: {name} count {cnt} != expected {exp}")
            ok = False

    size = Path(os.environ["DUCKDB_PATH"]).stat().st_size
    print(f"[seed] Done. DuckDB file = {size/1_048_576:.1f} MiB ({size} bytes)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
