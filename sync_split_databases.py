from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

from unificati_manager.config import (
    get_commerciali_db_path,
    get_legacy_db_path,
    get_materiali_db_path,
    get_normati_db_path,
)
from unificati_manager.db import COMMERCIALI_TABLES, MATERIALI_TABLES, NORMATI_TABLES, Database


def _table_count(db_path: str, table: str) -> int:
    if not os.path.isfile(db_path):
        return -1
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        exists = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if exists is None:
            return -1
        return int(cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def _print_summary(label: str, db_path: str, tables: Iterable[str]) -> None:
    print(f"\n[{label}] {db_path}")
    for table in tables:
        n = _table_count(db_path, table)
        if n >= 0:
            print(f"  {table}: {n}")


def _default_legacy_path() -> str:
    return str(Path(get_legacy_db_path()).resolve())


def _default_normati_path() -> str:
    return str(Path(get_normati_db_path()).resolve())


def _default_commerciali_path() -> str:
    return str(Path(get_commerciali_db_path()).resolve())


def _default_materiali_path() -> str:
    return str(Path(get_materiali_db_path()).resolve())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-sync da DB legacy unico verso i 3 DB area-specifici.",
    )
    parser.add_argument("--legacy", default=_default_legacy_path(), help="Path DB legacy sorgente.")
    parser.add_argument("--normati", default=_default_normati_path(), help="Path DB destinazione normati.")
    parser.add_argument("--commerciali", default=_default_commerciali_path(), help="Path DB destinazione commerciali.")
    parser.add_argument("--materiali", default=_default_materiali_path(), help="Path DB destinazione materiali.")
    parser.add_argument("--dry-run", action="store_true", help="Mostra solo i path senza eseguire sync.")
    args = parser.parse_args()

    legacy = str(Path(args.legacy).resolve())
    normati = str(Path(args.normati).resolve())
    commerciali = str(Path(args.commerciali).resolve())
    materiali = str(Path(args.materiali).resolve())

    print("Legacy:", legacy)
    print("Normati:", normati)
    print("Commerciali:", commerciali)
    print("Materiali:", materiali)

    if args.dry_run:
        return 0

    Database.resync_split_databases(
        legacy_path=legacy,
        normati_path=normati,
        commerciali_path=commerciali,
        materiali_path=materiali,
    )
    print("\nSync completato.")

    _print_summary("NORMATI", normati, ("category", "standard", "subcategory", "item", "manual_version"))
    _print_summary("COMMERCIALI", commerciali, ("comm_category", "comm_subcategory", "supplier", "comm_item"))
    _print_summary("MATERIALI", materiali, ("material", "material_property", "semi_item", "semi_item_dimension"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
