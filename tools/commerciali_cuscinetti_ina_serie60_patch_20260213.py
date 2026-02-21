from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.codifica import normalize_cccc, normalize_ssss
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


@dataclass(frozen=True)
class SubCfg:
    code: str
    series: str
    description: str
    suffix_desc: str
    supplier_suffix: str
    notes: str


CATEGORY_CODE = "CUSC"
CATEGORY_DESC = "CUSCINETTI"
SUPPLIER_CODE = "INA"
SUPPLIER_DESC = "INA / SCHAEFFLER"

SUBS: List[SubCfg] = [
    SubCfg(
        code="SEZZ",
        series="60",
        description="SERIE 60 ZZ",
        suffix_desc="ZZ",
        supplier_suffix="2Z",
        notes="CATALOGO INA SERIE 60. SCHERMATURA METALLICA SU DUE LATI (ZZ / 2Z).",
    ),
    SubCfg(
        code="SERS",
        series="60",
        description="SERIE 60 2RS",
        suffix_desc="2RS",
        supplier_suffix="2RSR",
        notes="CATALOGO INA SERIE 60. TENUTA IN GOMMA SU DUE LATI (2RS / 2RSR).",
    ),
    SubCfg(
        code="SBZZ",
        series="62",
        description="SERIE 62 ZZ",
        suffix_desc="ZZ",
        supplier_suffix="2Z",
        notes="CATALOGO INA SERIE 62. SCHERMATURA METALLICA SU DUE LATI (ZZ / 2Z).",
    ),
    SubCfg(
        code="SBRS",
        series="62",
        description="SERIE 62 2RS",
        suffix_desc="2RS",
        supplier_suffix="2RSR",
        notes="CATALOGO INA SERIE 62. TENUTA IN GOMMA SU DUE LATI (2RS / 2RSR).",
    ),
    SubCfg(
        code="SCZZ",
        series="63",
        description="SERIE 63 ZZ",
        suffix_desc="ZZ",
        supplier_suffix="2Z",
        notes="CATALOGO INA SERIE 63. SCHERMATURA METALLICA SU DUE LATI (ZZ / 2Z).",
    ),
    SubCfg(
        code="SCRS",
        series="63",
        description="SERIE 63 2RS",
        suffix_desc="2RS",
        supplier_suffix="2RSR",
        notes="CATALOGO INA SERIE 63. TENUTA IN GOMMA SU DUE LATI (2RS / 2RSR).",
    ),
]

# Serie 60 extra-light (ISO 15) - estesa con taglie minori e maggiori.
SERIE_60_DIMENSIONS: List[Tuple[str, int, int, int]] = [
    # Minori (serie 6xx)
    ("605", 5, 14, 5),
    ("606", 6, 17, 6),
    ("607", 7, 19, 6),
    ("608", 8, 22, 7),
    ("609", 9, 24, 7),
    ("600", 10, 26, 8),
    ("601", 12, 28, 8),
    ("602", 15, 32, 9),
    ("603", 17, 35, 10),

    # Serie 6000...
    ("6000", 10, 26, 8),
    ("6001", 12, 28, 8),
    ("6002", 15, 32, 9),
    ("6003", 17, 35, 10),
    ("6004", 20, 42, 12),
    ("6005", 25, 47, 12),
    ("6006", 30, 55, 13),
    ("6007", 35, 62, 14),
    ("6008", 40, 68, 15),
    ("6009", 45, 75, 16),
    ("6010", 50, 80, 16),
    ("6011", 55, 90, 18),
    ("6012", 60, 95, 18),

    # Maggiori
    ("6013", 65, 100, 18),
    ("6014", 70, 110, 20),
    ("6015", 75, 115, 20),
    ("6016", 80, 125, 22),
    ("6017", 85, 130, 22),
    ("6018", 90, 140, 24),
    ("6019", 95, 145, 24),
    ("6020", 100, 150, 24),
    ("6021", 105, 160, 26),
    ("6022", 110, 170, 28),
    ("6024", 120, 180, 28),
    ("6026", 130, 200, 33),
    ("6028", 140, 210, 33),
    ("6030", 150, 225, 35),
    ("6032", 160, 240, 38),
    ("6034", 170, 260, 42),
    ("6036", 180, 280, 46),
]

SERIE_62_DIMENSIONS: List[Tuple[str, int, int, int]] = [
    # Minori (serie 62x / 62xx ridotta)
    ("623", 3, 10, 4),
    ("624", 4, 13, 5),
    ("625", 5, 16, 5),
    ("626", 6, 19, 6),
    ("627", 7, 22, 7),
    ("628", 8, 24, 8),
    ("629", 9, 26, 8),
    ("620", 10, 30, 9),
    ("621", 12, 32, 10),
    ("622", 15, 35, 11),

    # Serie 6200...
    ("6200", 10, 30, 9),
    ("6201", 12, 32, 10),
    ("6202", 15, 35, 11),
    ("6203", 17, 40, 12),
    ("6204", 20, 47, 14),
    ("6205", 25, 52, 15),
    ("6206", 30, 62, 16),
    ("6207", 35, 72, 17),
    ("6208", 40, 80, 18),
    ("6209", 45, 85, 19),
    ("6210", 50, 90, 20),
    ("6211", 55, 100, 21),
    ("6212", 60, 110, 22),
    ("6213", 65, 120, 23),
    ("6214", 70, 125, 24),
    ("6215", 75, 130, 25),
    ("6216", 80, 140, 26),
    ("6217", 85, 150, 28),
    ("6218", 90, 160, 30),
    ("6219", 95, 170, 32),
    ("6220", 100, 180, 34),
    ("6221", 105, 190, 36),
    ("6222", 110, 200, 38),
    ("6224", 120, 215, 40),
    ("6226", 130, 230, 40),
    ("6228", 140, 250, 42),
    ("6230", 150, 270, 45),
    ("6232", 160, 290, 48),
    ("6234", 170, 310, 52),
    ("6236", 180, 320, 52),
]

SERIE_63_DIMENSIONS: List[Tuple[str, int, int, int]] = [
    # Minori (serie 63x / 63xx ridotta)
    ("633", 3, 13, 5),
    ("634", 4, 16, 5),
    ("635", 5, 19, 6),
    ("636", 6, 22, 7),
    ("637", 7, 26, 9),
    ("638", 8, 28, 9),
    ("639", 9, 30, 10),
    ("630", 10, 35, 11),
    ("631", 12, 37, 12),
    ("632", 15, 42, 13),

    # Serie 6300...
    ("6300", 10, 35, 11),
    ("6301", 12, 37, 12),
    ("6302", 15, 42, 13),
    ("6303", 17, 47, 14),
    ("6304", 20, 52, 15),
    ("6305", 25, 62, 17),
    ("6306", 30, 72, 19),
    ("6307", 35, 80, 21),
    ("6308", 40, 90, 23),
    ("6309", 45, 100, 25),
    ("6310", 50, 110, 27),
    ("6311", 55, 120, 29),
    ("6312", 60, 130, 31),
    ("6313", 65, 140, 33),
    ("6314", 70, 150, 35),
    ("6315", 75, 160, 37),
    ("6316", 80, 170, 39),
    ("6317", 85, 180, 41),
    ("6318", 90, 190, 43),
    ("6319", 95, 200, 45),
    ("6320", 100, 215, 47),
    ("6321", 105, 225, 49),
    ("6322", 110, 240, 50),
    ("6324", 120, 260, 55),
    ("6326", 130, 280, 58),
    ("6328", 140, 300, 62),
    ("6330", 150, 320, 65),
    ("6332", 160, 340, 68),
    ("6334", 170, 360, 72),
    ("6336", 180, 380, 75),
]

SERIES_DIMENSIONS: Dict[str, List[Tuple[str, int, int, int]]] = {
    "60": SERIE_60_DIMENSIONS,
    "62": SERIE_62_DIMENSIONS,
    "63": SERIE_63_DIMENSIONS,
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_commerciali_cuscinetti_ina606263.db"
    db.backup_to_path(str(out))
    return out


def ensure_category(cur, code: str, description: str) -> Tuple[int, str]:
    code_n = normalize_cccc(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM comm_category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is not None:
        cid = int(row["id"])
        cur.execute("UPDATE comm_category SET description=? WHERE id=?", (desc_n, cid))
        return cid, code_n
    cur.execute("INSERT INTO comm_category(code, description) VALUES(?, ?)", (code_n, desc_n))
    return int(cur.lastrowid), code_n


def ensure_subcategory(cur, category_id: int, code: str, description: str) -> int:
    code_n = normalize_ssss(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM comm_subcategory WHERE category_id=? AND code=?", (int(category_id), code_n))
    row = cur.fetchone()
    if row is not None:
        sid = int(row["id"])
        cur.execute("UPDATE comm_subcategory SET description=? WHERE id=?", (desc_n, sid))
        return sid
    cur.execute(
        "INSERT INTO comm_subcategory(category_id, code, description) VALUES(?, ?, ?)",
        (int(category_id), code_n, desc_n),
    )
    return int(cur.lastrowid)


def ensure_supplier(cur, code: str, description: str) -> int:
    code_n = normalize_upper(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM supplier WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is not None:
        sid = int(row["id"])
        cur.execute("UPDATE supplier SET description=? WHERE id=?", (desc_n, sid))
        return sid
    cur.execute("INSERT INTO supplier(code, description) VALUES(?, ?)", (code_n, desc_n))
    return int(cur.lastrowid)


def get_next_seq(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM comm_item WHERE category_id=? AND subcategory_id=?",
        (int(category_id), int(subcategory_id)),
    )
    return int(cur.fetchone()["next_seq"])


def find_free_code(cur, cat_code: str, sub_code: str, seq_start: int) -> Tuple[str, int]:
    seq = int(seq_start)
    while True:
        code = f"{cat_code}_{sub_code}-{seq:04d}"
        cur.execute("SELECT id FROM comm_item WHERE code=?", (code,))
        if cur.fetchone() is None:
            return code, seq
        seq += 1


def ensure_item(
    cur,
    category_id: int,
    subcategory_id: int,
    supplier_id: Optional[int],
    cat_code: str,
    sub_code: str,
    description: str,
    supplier_item_code: str,
    supplier_item_desc: str,
    notes: str,
) -> bool:
    desc_n = normalize_upper(description)
    sup_code_n = normalize_upper(supplier_item_code)
    sup_desc_n = normalize_upper(supplier_item_desc)
    notes_n = normalize_upper(notes)

    cur.execute(
        """
        SELECT id
        FROM comm_item
        WHERE category_id=? AND subcategory_id=? AND description=?
        LIMIT 1
        """,
        (int(category_id), int(subcategory_id), desc_n),
    )
    row = cur.fetchone()
    if row is not None:
        cur.execute(
            """
            UPDATE comm_item
            SET supplier_id=?, supplier_item_code=?, supplier_item_desc=?, notes=?, is_active=1, updated_at=?
            WHERE id=?
            """,
            (
                int(supplier_id) if supplier_id is not None else None,
                sup_code_n,
                sup_desc_n,
                notes_n,
                now_str(),
                int(row["id"]),
            ),
        )
        return False

    seq = get_next_seq(cur, category_id, subcategory_id)
    code, seq = find_free_code(cur, cat_code, sub_code, seq)
    cur.execute(
        """
        INSERT INTO comm_item(
            code, category_id, subcategory_id, supplier_id, seq, description,
            supplier_item_code, supplier_item_desc, file_folder, notes, preferred, is_active, created_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, '', ?, 0, 1, ?, ?)
        """,
        (
            code,
            int(category_id),
            int(subcategory_id),
            int(supplier_id) if supplier_id is not None else None,
            int(seq),
            desc_n,
            sup_code_n,
            sup_desc_n,
            notes_n,
            now_str(),
            now_str(),
        ),
    )
    return True


def dedupe_sub_items(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        """
        SELECT description, COUNT(*) AS c
        FROM comm_item
        WHERE category_id=? AND subcategory_id=?
        GROUP BY description
        HAVING COUNT(*) > 1
        """,
        (int(category_id), int(subcategory_id)),
    )
    groups = cur.fetchall()
    removed = 0
    for g in groups:
        desc = str(g["description"])
        cur.execute(
            """
            SELECT id
            FROM comm_item
            WHERE category_id=? AND subcategory_id=? AND description=?
            ORDER BY id
            """,
            (int(category_id), int(subcategory_id), desc),
        )
        ids = [int(r["id"]) for r in cur.fetchall()]
        for did in ids[1:]:
            cur.execute("DELETE FROM comm_item WHERE id=?", (did,))
            removed += int(cur.rowcount)
    return removed


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        backup = backup_db(db)
        print(f"Backup: {backup}")

        cur = db.conn.cursor()
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        cat_id, cat_code = ensure_category(cur, CATEGORY_CODE, CATEGORY_DESC)
        supplier_id = ensure_supplier(cur, SUPPLIER_CODE, SUPPLIER_DESC)

        sub_ids: Dict[str, int] = {}
        for s in SUBS:
            sub_ids[s.code] = ensure_subcategory(cur, cat_id, s.code, s.description)

        created_total = 0
        updated_total = 0
        deduped_total = 0
        per_sub_created: Dict[str, int] = {}
        per_sub_updated: Dict[str, int] = {}

        for s in SUBS:
            sub_code = normalize_ssss(s.code)
            sub_id = int(sub_ids[s.code])
            c_created = 0
            c_updated = 0
            series_dims = SERIES_DIMENSIONS.get(s.series, [])
            if not series_dims:
                raise RuntimeError(f"Serie non configurata: {s.series}")
            for base_code, d, D, B in series_dims:
                description = normalize_upper(
                    f"CUSCINETTO INA {base_code} {s.suffix_desc} SERIE {s.series} {d}X{D}X{B}"
                )
                supplier_item_code = normalize_upper(f"{base_code}-{s.supplier_suffix}")
                supplier_item_desc = normalize_upper(f"INA {base_code}-{s.supplier_suffix} {d}X{D}X{B}")
                created = ensure_item(
                    cur=cur,
                    category_id=cat_id,
                    subcategory_id=sub_id,
                    supplier_id=supplier_id,
                    cat_code=cat_code,
                    sub_code=sub_code,
                    description=description,
                    supplier_item_code=supplier_item_code,
                    supplier_item_desc=supplier_item_desc,
                    notes=s.notes,
                )
                if created:
                    c_created += 1
                else:
                    c_updated += 1

            deduped_total += dedupe_sub_items(cur, cat_id, sub_id)
            per_sub_created[sub_code] = c_created
            per_sub_updated[sub_code] = c_updated
            created_total += c_created
            updated_total += c_updated

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Category ensured: {CATEGORY_CODE}")
        print(f"Supplier ensured: {SUPPLIER_CODE}")
        print(f"Subcategories ensured: {len(SUBS)}")
        print(f"Items created total: {created_total}")
        print(f"Items updated total: {updated_total}")
        print(f"Duplicate items removed: {deduped_total}")
        print("Details by subcategory:")
        for k in sorted(per_sub_created.keys()):
            print(f"  - {k}: created={per_sub_created[k]} updated={per_sub_updated[k]}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Populate commerciali CUSC with INA serie 60/62/63 bearings, ZZ and 2RS variants."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
