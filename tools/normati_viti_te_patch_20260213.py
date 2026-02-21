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
from unificati_manager.codifica import normalize_mmm, normalize_gggg_normati
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


@dataclass(frozen=True)
class StdDef:
    code: str
    description: str


@dataclass(frozen=True)
class SubDef:
    code: str
    description: str
    primary_std_code: str
    desc_template: str


@dataclass(frozen=True)
class ItemDef:
    sub_code: str
    description: str
    notes: str


CATEGORY_CODE = "VIT"
CATEGORY_DESC = "VITI"

STANDARDS: List[StdDef] = [
    StdDef("ISO 4014", "VITE TE PARZIALMENTE FILETTATA"),
    StdDef("UNI EN ISO 4014", "VITE TE PARZIALMENTE FILETTATA"),
    StdDef("DIN 931", "VITE TE PARZIALMENTE FILETTATA"),
    StdDef("ISO 4017", "VITE TE TOTALMENTE FILETTATA"),
    StdDef("UNI EN ISO 4017", "VITE TE TOTALMENTE FILETTATA"),
    StdDef("DIN 933", "VITE TE TOTALMENTE FILETTATA"),
    StdDef("ISO 898-1", "CARATTERISTICHE MECCANICHE VITERIA ACCIAIO"),
    StdDef("ISO 4042", "RIVESTIMENTI ELETTROLITICI VITERIA"),
    StdDef("ISO 3506-1", "CARATTERISTICHE MECCANICHE VITERIA INOX"),
]

SUBCATEGORIES: List[SubDef] = [
    SubDef(
        code="HEPZ",
        description="TE PARZ FILETT ZINCATA",
        primary_std_code="ISO 4014",
        desc_template="VITE TE PARZ FILETT ISO 4014 M__X__ ACCIAIO ZINCATO CL 8.8",
    ),
    SubDef(
        code="HEPA",
        description="TE PARZ FILETT INOX A2",
        primary_std_code="ISO 4014",
        desc_template="VITE TE PARZ FILETT ISO 4014 M__X__ INOX A2-70",
    ),
    SubDef(
        code="HETZ",
        description="TE TOT FILETT ZINCATA",
        primary_std_code="ISO 4017",
        desc_template="VITE TE TOT FILETT ISO 4017 M__X__ ACCIAIO ZINCATO CL 8.8",
    ),
    SubDef(
        code="HETA",
        description="TE TOT FILETT INOX A2",
        primary_std_code="ISO 4017",
        desc_template="VITE TE TOT FILETT ISO 4017 M__X__ INOX A2-70",
    ),
]

ITEMS: List[ItemDef] = [
    ItemDef(
        sub_code="HEPZ",
        description="VITE TE PARZ FILETT ISO 4014 M8X40 ACCIAIO ZINCATO CL 8.8",
        notes="GERARCHIA NORME: ISO 4014 > UNI EN ISO 4014 > DIN 931. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
    ),
    ItemDef(
        sub_code="HETZ",
        description="VITE TE TOT FILETT ISO 4017 M8X30 ACCIAIO ZINCATO CL 8.8",
        notes="GERARCHIA NORME: ISO 4017 > UNI EN ISO 4017 > DIN 933. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
    ),
    ItemDef(
        sub_code="HEPA",
        description="VITE TE PARZ FILETT ISO 4014 M8X40 INOX A2-70",
        notes="GERARCHIA NORME: ISO 4014 > UNI EN ISO 4014 > DIN 931. INOX A2: ISO 3506-1.",
    ),
    ItemDef(
        sub_code="HETA",
        description="VITE TE TOT FILETT ISO 4017 M8X30 INOX A2-70",
        notes="GERARCHIA NORME: ISO 4017 > UNI EN ISO 4017 > DIN 933. INOX A2: ISO 3506-1.",
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_viti_te.db"
    db.backup_to_path(str(out))
    return out


def ensure_category(cur, code: str, description: str) -> int:
    code_n = normalize_mmm(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is not None:
        cat_id = int(row["id"])
        cur.execute("UPDATE category SET description=? WHERE id=?", (desc_n, cat_id))
        return cat_id
    cur.execute("INSERT INTO category(code, description) VALUES(?, ?)", (code_n, desc_n))
    return int(cur.lastrowid)


def ensure_standard(cur, category_id: int, code: str, description: str) -> int:
    code_n = normalize_upper(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM standard WHERE category_id=? AND code=?", (int(category_id), code_n))
    row = cur.fetchone()
    if row is not None:
        sid = int(row["id"])
        cur.execute("UPDATE standard SET description=? WHERE id=?", (desc_n, sid))
        return sid
    cur.execute(
        "INSERT INTO standard(category_id, code, description) VALUES(?, ?, ?)",
        (int(category_id), code_n, desc_n),
    )
    return int(cur.lastrowid)


def ensure_subcategory(cur, category_id: int, sub: SubDef, std_id: int) -> int:
    code_n = normalize_gggg_normati(sub.code)
    desc_n = normalize_upper(sub.description)
    tpl_n = normalize_upper(sub.desc_template)
    cur.execute("SELECT id FROM subcategory WHERE category_id=? AND code=?", (int(category_id), code_n))
    row = cur.fetchone()
    if row is not None:
        scid = int(row["id"])
        cur.execute(
            "UPDATE subcategory SET description=?, standard_id=?, desc_template=? WHERE id=?",
            (desc_n, int(std_id), tpl_n, scid),
        )
        return scid
    cur.execute(
        """
        INSERT INTO subcategory(category_id, code, description, standard_id, desc_template)
        VALUES(?, ?, ?, ?, ?)
        """,
        (int(category_id), code_n, desc_n, int(std_id), tpl_n),
    )
    return int(cur.lastrowid)


def next_seq(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM item WHERE category_id=? AND subcategory_id=?",
        (int(category_id), int(subcategory_id)),
    )
    return int(cur.fetchone()["next_seq"])


def next_code(cur, cat_code: str, sub_code: str, start_seq: int) -> Tuple[str, int]:
    seq = int(start_seq)
    while True:
        code = f"{cat_code}_{sub_code}-{seq:04d}"
        cur.execute("SELECT id FROM item WHERE code=?", (code,))
        if cur.fetchone() is None:
            return code, seq
        seq += 1


def ensure_item(
    cur,
    category_id: int,
    cat_code: str,
    subcategory_id: int,
    sub_code: str,
    standard_id: int,
    description: str,
    notes: str,
) -> Tuple[int, bool]:
    desc_n = normalize_upper(description)
    notes_n = normalize_upper(notes)
    cur.execute(
        """
        SELECT id, code, seq
        FROM item
        WHERE category_id=? AND subcategory_id=? AND description=?
        ORDER BY id
        LIMIT 1
        """,
        (int(category_id), int(subcategory_id), desc_n),
    )
    row = cur.fetchone()
    if row is not None:
        item_id = int(row["id"])
        cur.execute(
            """
            UPDATE item
            SET standard_id=?, notes=?, is_active=1, updated_at=?
            WHERE id=?
            """,
            (int(standard_id), notes_n, now_str(), item_id),
        )
        return item_id, False

    seq = next_seq(cur, category_id, subcategory_id)
    code, seq = next_code(cur, cat_code, sub_code, seq)
    cur.execute(
        """
        INSERT INTO item(code, category_id, subcategory_id, standard_id, seq, description, notes, is_active, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            code,
            int(category_id),
            int(subcategory_id),
            int(standard_id),
            int(seq),
            desc_n,
            notes_n,
            now_str(),
            now_str(),
        ),
    )
    return int(cur.lastrowid), True


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        backup_path = backup_db(db)
        print(f"Backup: {backup_path}")
        cur = db.conn.cursor()

        # allinea eventuale transazione implicita
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        cat_id = ensure_category(cur, CATEGORY_CODE, CATEGORY_DESC)
        std_ids: Dict[str, int] = {}
        for s in STANDARDS:
            std_ids[s.code] = ensure_standard(cur, cat_id, s.code, s.description)

        sub_ids: Dict[str, int] = {}
        for sc in SUBCATEGORIES:
            std_id = std_ids[sc.primary_std_code]
            sub_ids[sc.code] = ensure_subcategory(cur, cat_id, sc, std_id)

        created_items = 0
        updated_items = 0
        created_codes: List[str] = []
        for item in ITEMS:
            sub_id = sub_ids[item.sub_code]
            std_code = next(sc.primary_std_code for sc in SUBCATEGORIES if sc.code == item.sub_code)
            std_id = std_ids[std_code]
            item_id, created = ensure_item(
                cur=cur,
                category_id=cat_id,
                cat_code=normalize_mmm(CATEGORY_CODE),
                subcategory_id=sub_id,
                sub_code=normalize_gggg_normati(item.sub_code),
                standard_id=std_id,
                description=item.description,
                notes=item.notes,
            )
            if created:
                created_items += 1
                cur.execute("SELECT code FROM item WHERE id=?", (item_id,))
                created_codes.append(str(cur.fetchone()["code"]))
            else:
                updated_items += 1

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Category ensured: {CATEGORY_CODE}")
        print(f"Standards ensured: {len(STANDARDS)}")
        print(f"Subcategories ensured: {len(SUBCATEGORIES)}")
        print(f"Items created: {created_items}")
        print(f"Items updated: {updated_items}")
        if created_codes:
            print("Created codes:")
            for c in created_codes:
                print(f"  - {c}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Populate commerciali normati for hex-head screws full/partial thread, zinced steel and inox A2."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
