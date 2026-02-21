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


TCEI_SERIES: Dict[int, List[int]] = {
    3: [6, 8, 10, 12, 16, 20, 25, 30],
    4: [8, 10, 12, 16, 20, 25, 30, 35, 40, 45, 50],
    5: [10, 12, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60],
    6: [12, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100],
    8: [16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120],
    10: [20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150],
    12: [25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
    14: [30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180],
    16: [30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
    20: [40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
    24: [50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200, 220],
}

TSEI_SERIES: Dict[int, List[int]] = {
    3: [6, 8, 10, 12, 16, 20],
    4: [8, 10, 12, 16, 20, 25, 30],
    5: [10, 12, 16, 20, 25, 30, 35, 40],
    6: [12, 16, 20, 25, 30, 35, 40, 45, 50, 60],
    8: [16, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80],
    10: [20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100],
    12: [25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120],
    14: [30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120, 130],
    16: [30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150],
    20: [40, 45, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
}


@dataclass(frozen=True)
class StdDef:
    code: str
    description: str


@dataclass(frozen=True)
class SubCfg:
    sub_code: str
    head_type: str
    primary_std_code: str
    description: str
    desc_template: str
    material_label: str
    notes: str
    series: Dict[int, List[int]]


CATEGORY_CODE = "VIT"
LEGACY_SUBS = ("TCEI",)

STANDARDS: List[StdDef] = [
    StdDef("ISO 4762", "VITE TCEI"),
    StdDef("UNI EN ISO 4762", "VITE TCEI"),
    StdDef("DIN 912", "VITE TCEI"),
    StdDef("ISO 10642", "VITE TSEI"),
    StdDef("UNI EN ISO 10642", "VITE TSEI"),
    StdDef("DIN 7991", "VITE TSEI"),
    StdDef("ISO 898-1", "CARATTERISTICHE MECCANICHE VITERIA ACCIAIO"),
    StdDef("ISO 4042", "RIVESTIMENTI ELETTROLITICI VITERIA"),
    StdDef("ISO 3506-1", "CARATTERISTICHE MECCANICHE VITERIA INOX"),
]

SUBS: List[SubCfg] = [
    SubCfg(
        sub_code="TC8Z",
        head_type="TCEI",
        primary_std_code="ISO 4762",
        description="TCEI ZINCATA CL 8.8",
        desc_template="VITE TCEI ISO 4762 M__X__ ACCIAIO ZINCATO CL 8.8",
        material_label="ACCIAIO ZINCATO CL 8.8",
        notes="GERARCHIA NORME: ISO 4762 > UNI EN ISO 4762 > DIN 912. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        series=TCEI_SERIES,
    ),
    SubCfg(
        sub_code="TC9Z",
        head_type="TCEI",
        primary_std_code="ISO 4762",
        description="TCEI ZINCATA CL 10.9",
        desc_template="VITE TCEI ISO 4762 M__X__ ACCIAIO ZINCATO CL 10.9",
        material_label="ACCIAIO ZINCATO CL 10.9",
        notes="GERARCHIA NORME: ISO 4762 > UNI EN ISO 4762 > DIN 912. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        series=TCEI_SERIES,
    ),
    SubCfg(
        sub_code="TCA2",
        head_type="TCEI",
        primary_std_code="ISO 4762",
        description="TCEI INOX A2",
        desc_template="VITE TCEI ISO 4762 M__X__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 4762 > UNI EN ISO 4762 > DIN 912. INOX A2: ISO 3506-1.",
        series=TCEI_SERIES,
    ),
    SubCfg(
        sub_code="TS8Z",
        head_type="TSEI",
        primary_std_code="ISO 10642",
        description="TSEI ZINCATA CL 8.8",
        desc_template="VITE TSEI ISO 10642 M__X__ ACCIAIO ZINCATO CL 8.8",
        material_label="ACCIAIO ZINCATO CL 8.8",
        notes="GERARCHIA NORME: ISO 10642 > UNI EN ISO 10642 > DIN 7991. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        series=TSEI_SERIES,
    ),
    SubCfg(
        sub_code="TS9Z",
        head_type="TSEI",
        primary_std_code="ISO 10642",
        description="TSEI ZINCATA CL 10.9",
        desc_template="VITE TSEI ISO 10642 M__X__ ACCIAIO ZINCATO CL 10.9",
        material_label="ACCIAIO ZINCATO CL 10.9",
        notes="GERARCHIA NORME: ISO 10642 > UNI EN ISO 10642 > DIN 7991. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        series=TSEI_SERIES,
    ),
    SubCfg(
        sub_code="TSA2",
        head_type="TSEI",
        primary_std_code="ISO 10642",
        description="TSEI INOX A2",
        desc_template="VITE TSEI ISO 10642 M__X__ INOX A2-70",
        material_label="INOX A2-70",
        notes="GERARCHIA NORME: ISO 10642 > UNI EN ISO 10642 > DIN 7991. INOX A2: ISO 3506-1.",
        series=TSEI_SERIES,
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_viti_tcei_tsei.db"
    db.backup_to_path(str(out))
    return out


def fetch_category(cur, code: str) -> Tuple[int, str]:
    code_n = normalize_mmm(code)
    cur.execute("SELECT id, code FROM category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Categoria non trovata: {code_n}")
    return int(row["id"]), str(row["code"])


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


def build_desc(cfg: SubCfg, diameter: int, length: int) -> str:
    return normalize_upper(
        f"VITE {cfg.head_type} {cfg.primary_std_code} M{diameter}X{length} {cfg.material_label}"
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

        cat_id, cat_code = fetch_category(cur, CATEGORY_CODE)
        cat_code = normalize_mmm(cat_code)

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
            code_n = normalize_gggg_normati(cfg.sub_code)
            sub_id = int(sub_ids[cfg.sub_code])
            std_id = int(std_ids[cfg.primary_std_code])
            created_sub = 0
            updated_sub = 0
            for dia in sorted(cfg.series.keys()):
                for length in cfg.series[dia]:
                    desc = build_desc(cfg, int(dia), int(length))
                    created = ensure_item(
                        cur=cur,
                        category_id=cat_id,
                        cat_code=cat_code,
                        subcategory_id=sub_id,
                        sub_code=code_n,
                        standard_id=std_id,
                        description=desc,
                        notes=cfg.notes,
                    )
                    if created:
                        created_sub += 1
                    else:
                        updated_sub += 1

            deduped_total += dedupe_sub_items(cur, cat_id, sub_id)
            per_sub_created[code_n] = created_sub
            per_sub_updated[code_n] = updated_sub
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
        description="Populate VIT TCEI/TSEI (8.8 zincated, 10.9 zincated, inox A2) with ISO>UNI>DIN hierarchy."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
