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
    item_description: str
    notes: str


CATEGORY_CODE = "AEL"
CATEGORY_DESC = "ANELLI ELASTICI"

STANDARDS: List[StdDef] = [
    StdDef("DIN 471", "ANELLI ELASTICI ESTERNI PER ALBERI"),
    StdDef("DIN 472", "ANELLI ELASTICI INTERNI PER FORI"),
    StdDef("UNI 7435 / DIN 471", "SERIE A PER ALBERI"),
    StdDef("UNI 7436 / DIN 471", "SERIE AS PER ALBERI RINFORZATA"),
    StdDef("UNI 7437 / DIN 472", "SERIE J PER FORI"),
    StdDef("UNI 7438 / DIN 472", "SERIE JS PER FORI RINFORZATA"),
    StdDef("TIPO AV", "SERIE AV CONCENTRICA PER ALBERI"),
    StdDef("TIPO JV", "SERIE JV CONCENTRICA PER FORI"),
    StdDef("DIN 983", "SERIE AK PER ALBERI"),
    StdDef("DIN 984", "SERIE JK PER FORI"),
]

SUBCATEGORIES: List[SubDef] = [
    SubDef(
        code="AABR",
        description="SERIE A ALBERO ACCIAIO BRUNITO",
        primary_std_code="UNI 7435 / DIN 471",
        desc_template="ANELLO ELASTICO SERIE A PER ALBERO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE A PER ALBERO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7435 -> DIN 471. TIPO: ALBERO.",
    ),
    SubDef(
        code="AAIX",
        description="SERIE A ALBERO INOX",
        primary_std_code="UNI 7435 / DIN 471",
        desc_template="ANELLO ELASTICO SERIE A PER ALBERO D__ INOX",
        item_description="ANELLO ELASTICO SERIE A PER ALBERO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7435 -> DIN 471. TIPO: ALBERO.",
    ),
    SubDef(
        code="ASBR",
        description="SERIE AS ALBERO ACCIAIO BRUNITO",
        primary_std_code="UNI 7436 / DIN 471",
        desc_template="ANELLO ELASTICO SERIE AS PER ALBERO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE AS PER ALBERO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7436 -> DIN 471. TIPO: ALBERO RINFORZATO.",
    ),
    SubDef(
        code="ASIX",
        description="SERIE AS ALBERO INOX",
        primary_std_code="UNI 7436 / DIN 471",
        desc_template="ANELLO ELASTICO SERIE AS PER ALBERO D__ INOX",
        item_description="ANELLO ELASTICO SERIE AS PER ALBERO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7436 -> DIN 471. TIPO: ALBERO RINFORZATO. DISPONIBILITA INOX DA VERIFICARE.",
    ),
    SubDef(
        code="AVBR",
        description="SERIE AV ALBERO ACCIAIO BRUNITO",
        primary_std_code="TIPO AV",
        desc_template="ANELLO ELASTICO SERIE AV PER ALBERO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE AV PER ALBERO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO AV (COMMERCIALE). TIPO: ALBERO CONCENTRICO.",
    ),
    SubDef(
        code="AVIX",
        description="SERIE AV ALBERO INOX",
        primary_std_code="TIPO AV",
        desc_template="ANELLO ELASTICO SERIE AV PER ALBERO D__ INOX",
        item_description="ANELLO ELASTICO SERIE AV PER ALBERO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO AV (COMMERCIALE). TIPO: ALBERO CONCENTRICO.",
    ),
    SubDef(
        code="AKBR",
        description="SERIE AK ALBERO ACCIAIO BRUNITO",
        primary_std_code="DIN 983",
        desc_template="ANELLO ELASTICO SERIE AK PER ALBERO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE AK PER ALBERO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 983. TIPO: ALBERO.",
    ),
    SubDef(
        code="AKIX",
        description="SERIE AK ALBERO INOX",
        primary_std_code="DIN 983",
        desc_template="ANELLO ELASTICO SERIE AK PER ALBERO D__ INOX",
        item_description="ANELLO ELASTICO SERIE AK PER ALBERO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 983. TIPO: ALBERO.",
    ),
    SubDef(
        code="JFBR",
        description="SERIE J FORO ACCIAIO BRUNITO",
        primary_std_code="UNI 7437 / DIN 472",
        desc_template="ANELLO ELASTICO SERIE J PER FORO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE J PER FORO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7437 -> DIN 472. TIPO: FORO.",
    ),
    SubDef(
        code="JFIX",
        description="SERIE J FORO INOX",
        primary_std_code="UNI 7437 / DIN 472",
        desc_template="ANELLO ELASTICO SERIE J PER FORO D__ INOX",
        item_description="ANELLO ELASTICO SERIE J PER FORO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7437 -> DIN 472. TIPO: FORO.",
    ),
    SubDef(
        code="JSBR",
        description="SERIE JS FORO ACCIAIO BRUNITO",
        primary_std_code="UNI 7438 / DIN 472",
        desc_template="ANELLO ELASTICO SERIE JS PER FORO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE JS PER FORO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7438 -> DIN 472. TIPO: FORO RINFORZATO.",
    ),
    SubDef(
        code="JSIX",
        description="SERIE JS FORO INOX",
        primary_std_code="UNI 7438 / DIN 472",
        desc_template="ANELLO ELASTICO SERIE JS PER FORO D__ INOX",
        item_description="ANELLO ELASTICO SERIE JS PER FORO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI 7438 -> DIN 472. TIPO: FORO RINFORZATO. DISPONIBILITA INOX DA VERIFICARE.",
    ),
    SubDef(
        code="JVBR",
        description="SERIE JV FORO ACCIAIO BRUNITO",
        primary_std_code="TIPO JV",
        desc_template="ANELLO ELASTICO SERIE JV PER FORO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE JV PER FORO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO JV (COMMERCIALE). TIPO: FORO CONCENTRICO.",
    ),
    SubDef(
        code="JVIX",
        description="SERIE JV FORO INOX",
        primary_std_code="TIPO JV",
        desc_template="ANELLO ELASTICO SERIE JV PER FORO D__ INOX",
        item_description="ANELLO ELASTICO SERIE JV PER FORO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO JV (COMMERCIALE). TIPO: FORO CONCENTRICO.",
    ),
    SubDef(
        code="JKBR",
        description="SERIE JK FORO ACCIAIO BRUNITO",
        primary_std_code="DIN 984",
        desc_template="ANELLO ELASTICO SERIE JK PER FORO D__ ACCIAIO BRUNITO",
        item_description="ANELLO ELASTICO SERIE JK PER FORO ACCIAIO BRUNITO",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 984. TIPO: FORO.",
    ),
    SubDef(
        code="JKIX",
        description="SERIE JK FORO INOX",
        primary_std_code="DIN 984",
        desc_template="ANELLO ELASTICO SERIE JK PER FORO D__ INOX",
        item_description="ANELLO ELASTICO SERIE JK PER FORO INOX",
        notes="GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 984. TIPO: FORO.",
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_anelli.db"
    db.backup_to_path(str(out))
    return out


def ensure_category(cur, code: str, description: str) -> int:
    code_n = normalize_mmm(code)
    desc_n = normalize_upper(description)
    cur.execute("SELECT id FROM category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is not None:
        cid = int(row["id"])
        cur.execute("UPDATE category SET description=? WHERE id=?", (desc_n, cid))
        return cid
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


def get_next_seq(cur, category_id: int, subcategory_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM item WHERE category_id=? AND subcategory_id=?",
        (int(category_id), int(subcategory_id)),
    )
    return int(cur.fetchone()["next_seq"])


def find_free_code(cur, cat_code: str, sub_code: str, start_seq: int) -> Tuple[str, int]:
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


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        backup_path = backup_db(db)
        print(f"Backup: {backup_path}")
        cur = db.conn.cursor()

        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        category_id = ensure_category(cur, CATEGORY_CODE, CATEGORY_DESC)
        cat_code = normalize_mmm(CATEGORY_CODE)

        std_ids: Dict[str, int] = {}
        for s in STANDARDS:
            std_ids[s.code] = ensure_standard(cur, category_id, s.code, s.description)

        sub_ids: Dict[str, int] = {}
        for sub in SUBCATEGORIES:
            sid = std_ids[sub.primary_std_code]
            sub_ids[sub.code] = ensure_subcategory(cur, category_id, sub, sid)

        created_items = 0
        updated_items = 0
        for sub in SUBCATEGORIES:
            sub_code = normalize_gggg_normati(sub.code)
            sub_id = sub_ids[sub.code]
            std_id = std_ids[sub.primary_std_code]
            created = ensure_item(
                cur=cur,
                category_id=category_id,
                cat_code=cat_code,
                subcategory_id=sub_id,
                sub_code=sub_code,
                standard_id=std_id,
                description=sub.item_description,
                notes=sub.notes,
            )
            if created:
                created_items += 1
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
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Populate normati retaining rings series A/AS/J/JS/AV/JV/AK/JK, split by shaft/bore and material."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
