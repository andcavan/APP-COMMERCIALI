from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import sqlite3
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


NORMATI_CAT_KNOWN: Dict[str, str] = {
    "VIT": "001",
    "ROS": "002",
    "DAD": "003",
    "GHI": "004",
    "SPI": "005",
    "PRI": "006",
    "AEL": "007",
    "CUS": "008",
    "LIN": "009",
    "CHI": "010",
}

COMM_CAT_KNOWN: Dict[str, str] = {
    "COMV": "1000",
    "LAVO": "2000",
    "CONS": "3000",
}

NORMATI_SUB_KNOWN: Dict[Tuple[str, str], str] = {
    ("VIT", "TESA"): "0001",
    ("VIT", "HEPZ"): "0002",
    ("VIT", "HEPA"): "0003",
    ("VIT", "HETZ"): "0004",
    ("VIT", "HETA"): "0005",
    ("VIT", "TCEI"): "0006",
    ("ROS", "PLAN"): "0001",
    ("DAD", "ESAG"): "0001",
    ("CUS", "R625"): "0001",
    ("LIN", "D685"): "0001",
    ("CHI", "D688"): "0001",
}

COMM_SUB_KNOWN: Dict[Tuple[str, str], str] = {
    ("COMV", "PIAS"): "0001",
    ("LAVO", "FRES"): "0001",
    ("CONS", "COLL"): "0001",
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def backup_db() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    out = BACKUP_DIR / f"unificati_manager_backup_{now_stamp()}_recode_numeric_and_clear_commerciali.db"
    src = sqlite3.connect(str(DB_PATH))
    dst = sqlite3.connect(str(out))
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()
    return out


def _next_free_code(used: set[str], width: int, start: int) -> str:
    n = max(0, int(start))
    while True:
        code = f"{n:0{width}d}"
        if code not in used:
            return code
        n += 1


def _assign_category_targets(
    rows: List[sqlite3.Row],
    known_map: Dict[str, str],
    width: int,
    auto_start: int,
) -> Dict[int, str]:
    targets: Dict[int, str] = {}
    used: set[str] = set()

    # Known legacy code -> target code has priority to preserve semantics.
    for r in rows:
        old = str(r["code"] or "").strip().upper()
        target = known_map.get(old)
        if target and target not in used:
            targets[int(r["id"])] = target
            used.add(target)

    next_n = int(auto_start)
    for r in rows:
        rid = int(r["id"])
        if rid in targets:
            continue
        old = str(r["code"] or "").strip().upper()
        if re.fullmatch(rf"[0-9]{{{width}}}", old) and old not in used:
            target = old
        else:
            target = _next_free_code(used, width=width, start=next_n)
            next_n = int(target) + 1
        targets[rid] = target
        used.add(target)

    return targets


def _assign_subcategory_targets(
    rows: List[sqlite3.Row],
    old_category_codes: Dict[int, str],
    known_map: Dict[Tuple[str, str], str],
) -> Dict[int, str]:
    by_cat: Dict[int, List[sqlite3.Row]] = {}
    for r in rows:
        by_cat.setdefault(int(r["category_id"]), []).append(r)

    targets: Dict[int, str] = {}
    for cat_id, cat_rows in by_cat.items():
        ordered = sorted(cat_rows, key=lambda x: (str(x["code"] or ""), int(x["id"])))
        used: set[str] = set()
        old_cat_code = old_category_codes.get(cat_id, "")

        # Known legacy pairs first.
        for r in ordered:
            rid = int(r["id"])
            old_sub = str(r["code"] or "").strip().upper()
            target = known_map.get((old_cat_code, old_sub))
            if target and target not in used:
                targets[rid] = target
                used.add(target)

        next_n = 1
        for r in ordered:
            rid = int(r["id"])
            if rid in targets:
                continue
            old_sub = str(r["code"] or "").strip().upper()
            if re.fullmatch(r"[0-9]{4}", old_sub) and old_sub not in used:
                target = old_sub
            else:
                target = _next_free_code(used, width=4, start=next_n)
                next_n = int(target) + 1
            targets[rid] = target
            used.add(target)

    return targets


def _apply_temp_then_final(
    cur: sqlite3.Cursor,
    table: str,
    updates: Dict[int, str],
) -> None:
    if not updates:
        return
    ordered_ids = sorted(updates.keys())
    for i, rid in enumerate(ordered_ids, start=1):
        cur.execute(f"UPDATE {table} SET code=? WHERE id=?", (f"TMP-{table[:3].upper()}-{i:06d}", int(rid)))
    for rid in ordered_ids:
        cur.execute(f"UPDATE {table} SET code=? WHERE id=?", (updates[rid], int(rid)))


def _rewrite_normati_item_codes(cur: sqlite3.Cursor) -> Tuple[int, int]:
    cur.execute("SELECT id FROM item ORDER BY id")
    ids = [int(r["id"]) for r in cur.fetchall()]
    for rid in ids:
        cur.execute("UPDATE item SET code=? WHERE id=?", (f"TMP-ITEM-{rid:08d}", rid))

    cur.execute(
        """
        SELECT i.id, i.seq, c.code AS cat_code, sc.code AS sub_code
        FROM item i
        JOIN category c ON c.id=i.category_id
        JOIN subcategory sc ON sc.id=i.subcategory_id
        ORDER BY i.id
        """
    )
    rewritten = 0
    max_seq = 0
    for r in cur.fetchall():
        seq = int(r["seq"])
        max_seq = max(max_seq, seq)
        if seq < 0 or seq > 9999:
            raise ValueError(f"SEQ fuori range 0000-9999 per item id={int(r['id'])}: {seq}")
        code = f"{r['cat_code']}-{r['sub_code']}-{seq:04d}"
        cur.execute("UPDATE item SET code=? WHERE id=?", (code, int(r["id"])))
        rewritten += 1
    return rewritten, max_seq


def _rows_to_old_code_map(rows: Iterable[sqlite3.Row]) -> Dict[int, str]:
    return {int(r["id"]): str(r["code"] or "").strip().upper() for r in rows}


def run(apply_changes: bool) -> int:
    backup = backup_db()
    print(f"Backup: {backup}")

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        con.execute("BEGIN")

        # Snapshot old codes before recoding.
        cur.execute("SELECT id, code FROM category ORDER BY id")
        norm_cat_rows = cur.fetchall()
        old_norm_cat_codes = _rows_to_old_code_map(norm_cat_rows)

        cur.execute("SELECT id, category_id, code FROM subcategory ORDER BY category_id, code, id")
        norm_sub_rows = cur.fetchall()

        cur.execute("SELECT id, code FROM comm_category ORDER BY id")
        comm_cat_rows = cur.fetchall()
        old_comm_cat_codes = _rows_to_old_code_map(comm_cat_rows)

        cur.execute("SELECT id, category_id, code FROM comm_subcategory ORDER BY category_id, code, id")
        comm_sub_rows = cur.fetchall()

        norm_cat_targets = _assign_category_targets(
            rows=norm_cat_rows,
            known_map=NORMATI_CAT_KNOWN,
            width=3,
            auto_start=11,
        )
        comm_cat_targets = _assign_category_targets(
            rows=comm_cat_rows,
            known_map=COMM_CAT_KNOWN,
            width=4,
            auto_start=4000,
        )

        norm_sub_targets = _assign_subcategory_targets(
            rows=norm_sub_rows,
            old_category_codes=old_norm_cat_codes,
            known_map=NORMATI_SUB_KNOWN,
        )
        comm_sub_targets = _assign_subcategory_targets(
            rows=comm_sub_rows,
            old_category_codes=old_comm_cat_codes,
            known_map=COMM_SUB_KNOWN,
        )

        # Delete all commercial items as requested.
        cur.execute("DELETE FROM comm_item")
        deleted_comm_items = int(cur.rowcount)

        _apply_temp_then_final(cur, "category", norm_cat_targets)
        _apply_temp_then_final(cur, "subcategory", norm_sub_targets)
        _apply_temp_then_final(cur, "comm_category", comm_cat_targets)
        _apply_temp_then_final(cur, "comm_subcategory", comm_sub_targets)
        norm_items_rewritten, max_seq = _rewrite_normati_item_codes(cur)

        if apply_changes:
            con.commit()
        else:
            con.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Commercial items deleted: {deleted_comm_items}")
        print(f"Normati categories recoded: {len(norm_cat_targets)}")
        print(f"Normati subcategories recoded: {len(norm_sub_targets)}")
        print(f"Normati item codes rewritten: {norm_items_rewritten}")
        print(f"Commercial categories recoded: {len(comm_cat_targets)}")
        print(f"Commercial subcategories recoded: {len(comm_sub_targets)}")
        print(f"Max seq observed in normati items: {max_seq}")
        return 0
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recode normati/commerciali codes to numeric format and delete all commercial items.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag runs in dry-run mode.",
    )
    args = parser.parse_args()
    return run(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())

