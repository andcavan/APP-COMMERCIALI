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
    category_code: str
    code: str
    description: str


@dataclass(frozen=True)
class SubDef:
    category_code: str
    code: str
    description: str
    primary_std_code: str
    desc_template: str
    notes: str
    kind: str  # linguetta / km / guk
    material_label: str


CATEGORY_DESCRIPTIONS: Dict[str, str] = {
    "LIN": "LINGUETTE",
    "GHI": "GHIERE",
}

LEGACY_SUBS_TO_REMOVE: List[Tuple[str, str]] = [
    ("LIN", "D685"),
]

# Serie tipica linguette parallele UNI 6604 / DIN 6885 (b x h) e lunghezze tipiche.
KEY_SECTIONS: List[Tuple[int, int]] = [
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5),
    (6, 6),
    (8, 7),
    (10, 8),
    (12, 8),
    (14, 9),
    (16, 10),
    (18, 11),
    (20, 12),
    (22, 14),
    (25, 14),
    (28, 16),
    (32, 18),
    (36, 20),
    (40, 22),
    (45, 25),
    (50, 28),
]

KEY_LENGTHS_BY_B: Dict[int, List[int]] = {
    2: [6, 8, 10, 12],
    3: [8, 10, 12, 14, 16],
    4: [10, 12, 14, 16, 18, 20],
    5: [12, 14, 16, 18, 20, 22, 25],
    6: [16, 18, 20, 22, 25, 28, 32],
    8: [18, 20, 22, 25, 28, 32, 36, 40],
    10: [22, 25, 28, 32, 36, 40, 45, 50],
    12: [28, 32, 36, 40, 45, 50, 56, 63],
    14: [36, 40, 45, 50, 56, 63, 70],
    16: [45, 50, 56, 63, 70, 80],
    18: [50, 56, 63, 70, 80, 90],
    20: [56, 63, 70, 80, 90, 100],
    22: [63, 70, 80, 90, 100, 112],
    25: [70, 80, 90, 100, 112, 125],
    28: [80, 90, 100, 112, 125, 140],
    32: [90, 100, 112, 125, 140, 160],
    36: [100, 112, 125, 140, 160, 180],
    40: [112, 125, 140, 160, 180, 200],
    45: [125, 140, 160, 180, 200, 224],
    50: [140, 160, 180, 200, 224, 250],
}

KM_SIZES: List[int] = list(range(0, 41))   # KM0..KM40
GUK_SIZES: List[int] = list(range(1, 41))  # GUK1..GUK40


STANDARDS: List[StdDef] = [
    # LIN
    StdDef("LIN", "UNI 6604", "LINGUETTA PARALLELA"),
    StdDef("LIN", "DIN 6885", "LINGUETTA PARALLELA"),
    StdDef("LIN", "ISO 773", "LINGUETTA PARALLELA"),
    # GHI
    StdDef("GHI", "ISO 2982-2", "GHIERA KM"),
    StdDef("GHI", "DIN 981", "GHIERA KM"),
    StdDef("GHI", "TIPO GUK", "GHIERA GUK"),
    StdDef("GHI", "ISO 898-2", "CARATTERISTICHE MECCANICHE GHIERE ACCIAIO"),
    StdDef("GHI", "ISO 3506-2", "CARATTERISTICHE MECCANICHE GHIERE INOX"),
    StdDef("GHI", "ISO 4042", "RIVESTIMENTI ELETTROLITICI VITERIA"),
]

SUBCATEGORIES: List[SubDef] = [
    # Linguette
    SubDef(
        category_code="LIN",
        code="LUAC",
        description="LINGUETTE UNI ACCIAIO",
        primary_std_code="UNI 6604",
        desc_template="LINGUETTA PARALLELA UNI 6604 __X__X__ ACCIAIO",
        notes="GERARCHIA NORME: ISO 773 > UNI 6604 > DIN 6885.",
        kind="linguetta",
        material_label="ACCIAIO",
    ),
    SubDef(
        category_code="LIN",
        code="LUIX",
        description="LINGUETTE UNI INOX",
        primary_std_code="UNI 6604",
        desc_template="LINGUETTA PARALLELA UNI 6604 __X__X__ INOX A2",
        notes="GERARCHIA NORME: ISO 773 > UNI 6604 > DIN 6885. INOX.",
        kind="linguetta",
        material_label="INOX A2",
    ),
    # Ghiere KM
    SubDef(
        category_code="GHI",
        code="KMZA",
        description="GHIERA KM ACCIAIO ZINCATO",
        primary_std_code="ISO 2982-2",
        desc_template="GHIERA KM ISO 2982-2 KM__ ACCIAIO ZINCATO",
        notes="GERARCHIA NORME: ISO 2982-2 > UNI N/A > DIN 981. ACCIAIO: ISO 898-2. ZINCATURA: ISO 4042.",
        kind="km",
        material_label="ACCIAIO ZINCATO",
    ),
    SubDef(
        category_code="GHI",
        code="KMIX",
        description="GHIERA KM INOX",
        primary_std_code="ISO 2982-2",
        desc_template="GHIERA KM ISO 2982-2 KM__ INOX A2",
        notes="GERARCHIA NORME: ISO 2982-2 > UNI N/A > DIN 981. INOX: ISO 3506-2.",
        kind="km",
        material_label="INOX A2",
    ),
    # Ghiere GUK
    SubDef(
        category_code="GHI",
        code="GUZA",
        description="GHIERA GUK ACCIAIO ZINCATO",
        primary_std_code="TIPO GUK",
        desc_template="GHIERA GUK GUK__ ACCIAIO ZINCATO",
        notes="GERARCHIA NORME: ISO N/A > UNI N/A > TIPO GUK (COMMERCIALE). ZINCATURA: ISO 4042.",
        kind="guk",
        material_label="ACCIAIO ZINCATO",
    ),
    SubDef(
        category_code="GHI",
        code="GUIX",
        description="GHIERA GUK INOX",
        primary_std_code="TIPO GUK",
        desc_template="GHIERA GUK GUK__ INOX A2",
        notes="GERARCHIA NORME: ISO N/A > UNI N/A > TIPO GUK (COMMERCIALE).",
        kind="guk",
        material_label="INOX A2",
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_linguette_ghiere.db"
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


def ensure_subcategory(cur, category_id: int, sub: SubDef, standard_id: int) -> int:
    code_n = normalize_gggg_normati(sub.code)
    desc_n = normalize_upper(sub.description)
    tpl_n = normalize_upper(sub.desc_template)
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


def build_descriptions(sub: SubDef) -> List[str]:
    out: List[str] = []
    if sub.kind == "linguetta":
        for b, h in KEY_SECTIONS:
            lengths = KEY_LENGTHS_BY_B.get(int(b), [])
            for l in lengths:
                out.append(normalize_upper(f"LINGUETTA PARALLELA UNI 6604 {b}X{h}X{int(l)} {sub.material_label}"))
        return out
    if sub.kind == "km":
        for n in KM_SIZES:
            out.append(normalize_upper(f"GHIERA KM ISO 2982-2 KM{int(n)} {sub.material_label}"))
        return out
    if sub.kind == "guk":
        for n in GUK_SIZES:
            out.append(normalize_upper(f"GHIERA GUK GUK{int(n)} {sub.material_label}"))
        return out
    raise RuntimeError(f"Tipo subcategoria non gestito: {sub.kind}")


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        backup = backup_db(db)
        print(f"Backup: {backup}")

        cur = db.conn.cursor()
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        category_ids: Dict[str, int] = {}
        category_codes: Dict[str, str] = {}
        for cat_code, cat_desc in CATEGORY_DESCRIPTIONS.items():
            cid, ccode = ensure_category(cur, cat_code, cat_desc)
            category_ids[cat_code] = cid
            category_codes[cat_code] = ccode

        removed_legacy_subs = 0
        removed_legacy_items = 0
        for cat_code, legacy_sub in LEGACY_SUBS_TO_REMOVE:
            rem_items, rem_sub = delete_legacy_subcategory(cur, category_ids[cat_code], legacy_sub)
            removed_legacy_items += rem_items
            if rem_sub:
                removed_legacy_subs += 1

        std_ids: Dict[Tuple[str, str], int] = {}
        for s in STANDARDS:
            sid = ensure_standard(cur, category_ids[s.category_code], s.code, s.description)
            std_ids[(s.category_code, s.code)] = sid

        sub_ids: Dict[Tuple[str, str], int] = {}
        for sub in SUBCATEGORIES:
            sid = std_ids[(sub.category_code, sub.primary_std_code)]
            scid = ensure_subcategory(cur, category_ids[sub.category_code], sub, sid)
            sub_ids[(sub.category_code, sub.code)] = scid

        created_total = 0
        updated_total = 0
        deduped_total = 0
        per_sub_created: Dict[str, int] = {}
        per_sub_updated: Dict[str, int] = {}

        for sub in SUBCATEGORIES:
            cat_code = sub.category_code
            cat_id = category_ids[cat_code]
            cat_code_norm = category_codes[cat_code]
            sub_code_norm = normalize_gggg_normati(sub.code)
            sub_id = sub_ids[(cat_code, sub.code)]
            std_id = std_ids[(cat_code, sub.primary_std_code)]
            created_sub = 0
            updated_sub = 0

            for desc in build_descriptions(sub):
                created = ensure_item(
                    cur=cur,
                    category_id=cat_id,
                    cat_code=cat_code_norm,
                    subcategory_id=sub_id,
                    sub_code=sub_code_norm,
                    standard_id=std_id,
                    description=desc,
                    notes=sub.notes,
                )
                if created:
                    created_sub += 1
                else:
                    updated_sub += 1

            deduped_total += dedupe_sub_items(cur, cat_id, sub_id)
            k = f"{cat_code}:{sub_code_norm}"
            per_sub_created[k] = created_sub
            per_sub_updated[k] = updated_sub
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
        print(f"Subcategories ensured: {len(SUBCATEGORIES)}")
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
        description="Populate LIN (linguette UNI acciaio/inox) and GHI (ghiere KM/GUK acciaio zincato/inox)."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
