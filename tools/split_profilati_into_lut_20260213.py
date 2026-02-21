from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


TARGET_TYPES = {
    "L": ("PRFL", "PROFILO L", "L - SERIE STD"),
    "U": ("PRFU", "PROFILO U", "U - SERIE STD"),
    "T": ("PRFT", "PROFILO T", "T - SERIE STD"),
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm(text: str) -> str:
    return (text or "").strip().upper()


def backup_db(conn: sqlite3.Connection) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_split_profilati.db"
    dst = sqlite3.connect(out)
    try:
        conn.backup(dst)
        dst.commit()
    finally:
        dst.close()
    return out


def ensure_type_ids(cur: sqlite3.Cursor) -> Dict[str, int]:
    type_ids: Dict[str, int] = {}
    for key, (code, desc, _summary) in TARGET_TYPES.items():
        cur.execute(
            "INSERT OR IGNORE INTO semi_type(code, description) VALUES(?, ?)",
            (norm(code), norm(desc)),
        )
        cur.execute("SELECT id FROM semi_type WHERE code=?", (norm(code),))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Tipo semilavorato non trovato per code={code}")
        type_ids[key] = int(row["id"])
    return type_ids


def split_dimensions(rows: List[sqlite3.Row]) -> Dict[str, List[sqlite3.Row]]:
    out: Dict[str, List[sqlite3.Row]] = {"L": [], "U": [], "T": [], "OTHER": []}
    for r in rows:
        dim = norm(str(r["dimension"] or ""))
        if dim.startswith("L"):
            out["L"].append(r)
        elif dim.startswith("U"):
            out["U"].append(r)
        elif dim.startswith("T"):
            out["T"].append(r)
        else:
            out["OTHER"].append(r)
    return out


def insert_item_clone(
    cur: sqlite3.Cursor,
    src: sqlite3.Row,
    dst_type_id: int,
    summary: str,
) -> int:
    cur.execute(
        """
        INSERT INTO semi_item(
            type_id, state_id, material_id, description, dimensions, standard, notes, is_active, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(dst_type_id),
            int(src["state_id"]),
            int(src["material_id"]) if src["material_id"] is not None else None,
            norm(str(src["description"] or "")),
            norm(summary),
            norm(str(src["standard"] or "")),
            norm(str(src["notes"] or "")),
            int(src["is_active"] or 1),
            str(src["created_at"] or now_str()),
            now_str(),
        ),
    )
    return int(cur.lastrowid)


def replace_item_dimensions(cur: sqlite3.Cursor, item_id: int, dims: List[sqlite3.Row]) -> int:
    cur.execute("DELETE FROM semi_item_dimension WHERE semi_item_id=?", (int(item_id),))
    sort_order = 10
    inserted = 0
    for r in dims:
        cur.execute(
            """
            INSERT OR IGNORE INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order)
            VALUES(?, ?, ?, ?)
            """,
            (
                int(item_id),
                norm(str(r["dimension"] or "")),
                norm(str(r["weight_per_m"] or "")),
                int(sort_order),
            ),
        )
        if cur.rowcount > 0:
            inserted += 1
        sort_order += 10
    return inserted


def run(apply_changes: bool) -> int:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    cur = conn.cursor()

    backup_path = backup_db(conn)
    print(f"Backup: {backup_path}")

    type_ids = ensure_type_ids(cur)
    cur.execute("SELECT id FROM semi_type WHERE code='PROF' OR description='PROFILATI' ORDER BY id LIMIT 1")
    old_type_row = cur.fetchone()
    if old_type_row is None:
        print("Nessun tipo PROFILATI trovato.")
        conn.close()
        return 0
    old_type_id = int(old_type_row["id"])

    cur.execute(
        """
        SELECT id, type_id, state_id, material_id, description, dimensions, standard, notes, is_active, created_at, updated_at
        FROM semi_item
        WHERE type_id=?
        ORDER BY id
        """,
        (old_type_id,),
    )
    src_items = cur.fetchall()

    split_items = 0
    created_items = 0
    deleted_old_items = 0
    kept_old_items = 0
    inserted_dims_total = 0

    # Allinea eventuale transazione implicita prima della transazione esplicita.
    conn.commit()
    if apply_changes:
        conn.execute("BEGIN")

    for src in src_items:
        src_id = int(src["id"])
        cur.execute(
            """
            SELECT id, dimension, weight_per_m, sort_order
            FROM semi_item_dimension
            WHERE semi_item_id=?
            ORDER BY sort_order, id
            """,
            (src_id,),
        )
        dims = cur.fetchall()
        if not dims:
            kept_old_items += 1
            continue

        buckets = split_dimensions(dims)
        lut_count = len(buckets["L"]) + len(buckets["U"]) + len(buckets["T"])
        if lut_count == 0:
            kept_old_items += 1
            continue

        split_items += 1
        for key in ("L", "U", "T"):
            part = buckets[key]
            if not part:
                continue
            code, desc, summary = TARGET_TYPES[key]
            _ = code, desc  # explicit unpack for readability; ids are resolved earlier
            new_item_id = insert_item_clone(cur, src, type_ids[key], summary)
            inserted = replace_item_dimensions(cur, new_item_id, part)
            inserted_dims_total += inserted
            created_items += 1

        if buckets["OTHER"]:
            # Mantieni il record PROFILATI solo con eventuali dimensioni non L/U/T.
            replace_item_dimensions(cur, src_id, buckets["OTHER"])
            cur.execute(
                "UPDATE semi_item SET dimensions=?, updated_at=? WHERE id=?",
                ("PROFILATI - ALTRO", now_str(), src_id),
            )
            kept_old_items += 1
        else:
            cur.execute("DELETE FROM semi_item WHERE id=?", (src_id,))
            deleted_old_items += 1

    if apply_changes:
        conn.commit()
    else:
        conn.rollback()

    print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
    print(f"Source PROFILATI items found: {len(src_items)}")
    print(f"Items split: {split_items}")
    print(f"New items created: {created_items}")
    print(f"Old PROFILATI deleted: {deleted_old_items}")
    print(f"Old PROFILATI kept: {kept_old_items}")
    print(f"Dimension rows inserted on new items: {inserted_dims_total}")

    conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Split PROFILATI items into PROFILO L / U / T items.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return run(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
