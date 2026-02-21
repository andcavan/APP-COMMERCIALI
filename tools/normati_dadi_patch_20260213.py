from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.codifica import normalize_gggg_normati, normalize_mmm
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


@dataclass(frozen=True)
class StdDef:
    code: str
    description: str


@dataclass(frozen=True)
class SubCfg:
    sub_code: str
    description: str
    primary_std_code: str
    desc_template: str
    material_label: str
    notes: str


CATEGORY_CODE = "DAD"
CATEGORY_DESC = "DADI"

# Placeholder legacy subcategory from seed defaults.
LEGACY_SUBS = ("ESAG",)

SIZES: List[int] = [3, 4, 5, 6, 8, 10, 12, 14, 16, 20, 24]

STANDARDS: List[StdDef] = [
    StdDef("ISO 4032", "DADO ESAGONALE MEDIO"),
    StdDef("UNI EN ISO 4032", "DADO ESAGONALE MEDIO"),
    StdDef("DIN 934", "DADO ESAGONALE MEDIO"),
    StdDef("ISO 4035", "DADO ESAGONALE BASSO"),
    StdDef("UNI EN ISO 4035", "DADO ESAGONALE BASSO"),
    StdDef("DIN 439", "DADO ESAGONALE BASSO"),
    StdDef("ISO 4033", "DADO ESAGONALE ALTO"),
    StdDef("UNI EN ISO 4033", "DADO ESAGONALE ALTO"),
    StdDef("DIN 6330", "DADO ESAGONALE ALTO"),
    StdDef("ISO 7040", "DADO AUTOBLOCCANTE ALTO"),
    StdDef("UNI EN ISO 7040", "DADO AUTOBLOCCANTE ALTO"),
    StdDef("DIN 982", "DADO AUTOBLOCCANTE ALTO"),
    StdDef("ISO 7041", "DADO AUTOBLOCCANTE BASSO"),
    StdDef("UNI EN ISO 7041", "DADO AUTOBLOCCANTE BASSO"),
    StdDef("DIN 985", "DADO AUTOBLOCCANTE BASSO"),
    StdDef("ISO 898-2", "CARATTERISTICHE MECCANICHE DADI ACCIAIO"),
    StdDef("ISO 3506-2", "CARATTERISTICHE MECCANICHE DADI INOX"),
    StdDef("ISO 4042", "RIVESTIMENTI ELETTROLITICI VITERIA"),
]

SUBS: List[SubCfg] = [
    SubCfg(
        sub_code="M10Z",
        description="DADO ESAGONALE MEDIO ZINCATO CL 10",
        primary_std_code="ISO 4032",
        desc_template="DADO ESAGONALE MEDIO ISO 4032 M__ ACCIAIO ZINCATO CL 10",
        material_label="ACCIAIO ZINCATO CL 10",
        notes="GERARCHIA NORME: ISO 4032 > UNI EN ISO 4032 > DIN 934. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
    ),
    SubCfg(
        sub_code="MA2X",
        description="DADO ESAGONALE MEDIO INOX A2",
        primary_std_code="ISO 4032",
        desc_template="DADO ESAGONALE MEDIO ISO 4032 M__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 4032 > UNI EN ISO 4032 > DIN 934. INOX A2: ISO 3506-2.",
    ),
    SubCfg(
        sub_code="B10Z",
        description="DADO ESAGONALE BASSO ZINCATO CL 10",
        primary_std_code="ISO 4035",
        desc_template="DADO ESAGONALE BASSO ISO 4035 M__ ACCIAIO ZINCATO CL 10",
        material_label="ACCIAIO ZINCATO CL 10",
        notes="GERARCHIA NORME: ISO 4035 > UNI EN ISO 4035 > DIN 439. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
    ),
    SubCfg(
        sub_code="BA2X",
        description="DADO ESAGONALE BASSO INOX A2",
        primary_std_code="ISO 4035",
        desc_template="DADO ESAGONALE BASSO ISO 4035 M__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 4035 > UNI EN ISO 4035 > DIN 439. INOX A2: ISO 3506-2.",
    ),
    SubCfg(
        sub_code="H10Z",
        description="DADO ESAGONALE ALTO ZINCATO CL 10",
        primary_std_code="ISO 4033",
        desc_template="DADO ESAGONALE ALTO ISO 4033 M__ ACCIAIO ZINCATO CL 10",
        material_label="ACCIAIO ZINCATO CL 10",
        notes="GERARCHIA NORME: ISO 4033 > UNI EN ISO 4033 > DIN 6330. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
    ),
    SubCfg(
        sub_code="HA2X",
        description="DADO ESAGONALE ALTO INOX A2",
        primary_std_code="ISO 4033",
        desc_template="DADO ESAGONALE ALTO ISO 4033 M__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 4033 > UNI EN ISO 4033 > DIN 6330. INOX A2: ISO 3506-2.",
    ),
    SubCfg(
        sub_code="AH10",
        description="DADO AUTOBLOCCANTE ALTO ZINCATO CL 10",
        primary_std_code="ISO 7040",
        desc_template="DADO AUTOBLOCCANTE ALTO ISO 7040 M__ ACCIAIO ZINCATO CL 10",
        material_label="ACCIAIO ZINCATO CL 10",
        notes="GERARCHIA NORME: ISO 7040 > UNI EN ISO 7040 > DIN 982. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
    ),
    SubCfg(
        sub_code="AH2X",
        description="DADO AUTOBLOCCANTE ALTO INOX A2",
        primary_std_code="ISO 7040",
        desc_template="DADO AUTOBLOCCANTE ALTO ISO 7040 M__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 7040 > UNI EN ISO 7040 > DIN 982. INOX A2: ISO 3506-2.",
    ),
    SubCfg(
        sub_code="AL10",
        description="DADO AUTOBLOCCANTE BASSO ZINCATO CL 10",
        primary_std_code="ISO 7041",
        desc_template="DADO AUTOBLOCCANTE BASSO ISO 7041 M__ ACCIAIO ZINCATO CL 10",
        material_label="ACCIAIO ZINCATO CL 10",
        notes="GERARCHIA NORME: ISO 7041 > UNI EN ISO 7041 > DIN 985. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
    ),
    SubCfg(
        sub_code="AL2X",
        description="DADO AUTOBLOCCANTE BASSO INOX A2",
        primary_std_code="ISO 7041",
        desc_template="DADO AUTOBLOCCANTE BASSO ISO 7041 M__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 7041 > UNI EN ISO 7041 > DIN 985. INOX A2: ISO 3506-2.",
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_dadi.db"
    db.backup_to_path(str(out))
    return out


def ensure_category(cur, code: str, description: str) -> Tuple[int, str]:
    code_n = normalize_mmm(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is not None:
        cid = int(row["id"])
        cur.execute("UPDATE category SET description=? WHERE id=?", (desc_n, cid))
        return cid, code_n
    cur.execute("INSERT INTO category(code, description) VALUES(?, ?)", (code_n, desc_n))
    return int(cur.lastrowid), code_n


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


def ensure_subcategory(cur, category_id: int, cfg: SubCfg, standard_id: int) -> int:
    code_n = normalize_gggg_normati(cfg.sub_code)
    desc_n = normalize_upper(cfg.description)
    tpl_n = normalize_upper(cfg.desc_template)
    cur.execute("SELECT id FROM subcategory WHERE category_id=? AND code=?", (int(category_id), code_n))
    row = cur.fetchone()
    if row is not None:
        scid = int(row["id"])
        cur.execute(
            """
            UPDATE subcategory
            SET description=?, standard_id=?, desc_template=?
            WHERE id=?
            """,
            (desc_n, int(standard_id), tpl_n, scid),
        )
        return scid
    cur.execute(
        """
        INSERT INTO subcategory(category_id, code, description, standard_id, desc_template)
        VALUES(?, ?, ?, ?, ?)
        """,
        (int(category_id), code_n, desc_n, int(standard_id), tpl_n),
    )
    return int(cur.lastrowid)


def delete_legacy_subcategory(cur, category_id: int, sub_code: str) -> Tuple[int, bool]:
    code_n = normalize_gggg_normati(sub_code)
    cur.execute("SELECT id FROM subcategory WHERE category_id=? AND code=?", (int(category_id), code_n))
    row = cur.fetchone()
    if row is None:
        return 0, False
    sid = int(row["id"])
    cur.execute("DELETE FROM item WHERE category_id=? AND subcategory_id=?", (int(category_id), sid))
    removed_items = int(cur.rowcount)
    cur.execute("DELETE FROM subcategory WHERE id=?", (sid,))
    return removed_items, True


def get_next_seq(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM item WHERE category_id=? AND subcategory_id=?",
        (int(category_id), int(subcategory_id)),
    )
    return int(cur.fetchone()["next_seq"])


def find_free_code(cur, cat_code: str, sub_code: str, seq_start: int) -> Tuple[str, int]:
    seq = int(seq_start)
    while True:
        code = f"{cat_code}_{sub_code}-{seq:04d}"
        cur.execute("SELECT id FROM item WHERE code=?", (code,))
        if cur.fetchone() is None:
            return code, seq
        seq += 1


def build_desc(cfg: SubCfg, size: int) -> str:
    return normalize_upper(
        f"{cfg.desc_template.replace('M__', f'M{size}').replace('__', str(size))}".replace("  ", " ")
    )


def ensure_item(
    cur,
    category_id: int,
    cat_code: str,
    subcategory_id: int,
    sub_code: str,
    standard_id: int,
    description: str,
    notes: str,
) -> bool:
    desc_n = normalize_upper(description)
    notes_n = normalize_upper(notes)
    cur.execute(
        """
        SELECT id
        FROM item
        WHERE category_id=? AND subcategory_id=? AND description=?
        LIMIT 1
        """,
        (int(category_id), int(subcategory_id), desc_n),
    )
    row = cur.fetchone()
    if row is not None:
        cur.execute(
            """
            UPDATE item
            SET standard_id=?, notes=?, is_active=1, updated_at=?
            WHERE id=?
            """,
            (int(standard_id), notes_n, now_str(), int(row["id"])),
        )
        return False

    seq = get_next_seq(cur, category_id, subcategory_id)
    code, seq = find_free_code(cur, cat_code, sub_code, seq)
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
    return True


def dedupe_sub_items(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        """
        SELECT description, COUNT(*) AS c
        FROM item
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
            FROM item
            WHERE category_id=? AND subcategory_id=? AND description=?
            ORDER BY id
            """,
            (int(category_id), int(subcategory_id), desc),
        )
        ids = [int(r["id"]) for r in cur.fetchall()]
        for did in ids[1:]:
            cur.execute("DELETE FROM item WHERE id=?", (did,))
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

        removed_legacy_subs = 0
        removed_legacy_items = 0
        for legacy in LEGACY_SUBS:
            removed_items, removed_sub = delete_legacy_subcategory(cur, cat_id, legacy)
            if removed_sub:
                removed_legacy_subs += 1
            removed_legacy_items += removed_items

        std_ids: Dict[str, int] = {}
        for s in STANDARDS:
            std_ids[s.code] = ensure_standard(cur, cat_id, s.code, s.description)

        sub_ids: Dict[str, int] = {}
        for cfg in SUBS:
            sub_ids[cfg.sub_code] = ensure_subcategory(cur, cat_id, cfg, std_ids[cfg.primary_std_code])

        created_total = 0
        updated_total = 0
        deduped_total = 0
        per_sub_created: Dict[str, int] = {}
        per_sub_updated: Dict[str, int] = {}

        for cfg in SUBS:
            sub_code = normalize_gggg_normati(cfg.sub_code)
            sub_id = int(sub_ids[cfg.sub_code])
            std_id = int(std_ids[cfg.primary_std_code])
            created_sub = 0
            updated_sub = 0

            for size in SIZES:
                desc = build_desc(cfg, int(size))
                created = ensure_item(
                    cur=cur,
                    category_id=cat_id,
                    cat_code=cat_code,
                    subcategory_id=sub_id,
                    sub_code=sub_code,
                    standard_id=std_id,
                    description=desc,
                    notes=cfg.notes,
                )
                if created:
                    created_sub += 1
                else:
                    updated_sub += 1

            deduped_total += dedupe_sub_items(cur, cat_id, sub_id)
            per_sub_created[sub_code] = created_sub
            per_sub_updated[sub_code] = updated_sub
            created_total += created_sub
            updated_total += updated_sub

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Legacy subcategories removed: {removed_legacy_subs}")
        print(f"Legacy items removed: {removed_legacy_items}")
        print(f"Standards ensured: {len(STANDARDS)}")
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
        description="Populate DAD nuts: class 10 zincated and inox A2, medium/low/high plus prevailing-torque high/low."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
