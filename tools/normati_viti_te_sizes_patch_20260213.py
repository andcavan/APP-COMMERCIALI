from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
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


PARTIAL_SERIES: Dict[int, List[int]] = {
    3: [16, 20, 25, 30],
    4: [16, 20, 25, 30, 35, 40],
    5: [16, 20, 25, 30, 35, 40, 45, 50],
    6: [20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100],
    8: [25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120],
    10: [30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150],
    12: [35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
    14: [40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180],
    16: [45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
    20: [55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200, 220],
    24: [70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200, 220, 240],
}

FULL_SERIES: Dict[int, List[int]] = {
    3: [6, 8, 10, 12, 16, 20, 25, 30],
    4: [8, 10, 12, 16, 20, 25, 30, 35, 40, 45, 50],
    5: [10, 12, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60],
    6: [10, 12, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80],
    8: [12, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100],
    10: [16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120],
    12: [20, 25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150],
    14: [25, 30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
    16: [30, 35, 40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
    20: [40, 45, 50, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200],
    24: [50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200, 220],
}


@dataclass(frozen=True)
class SubConfig:
    sub_code: str
    thread_kind: str  # PARZ or TOT
    standard_code: str
    notes: str
    material_label: str
    series: Dict[int, List[int]]


SUBS: List[SubConfig] = [
    SubConfig(
        sub_code="HEPZ",
        thread_kind="PARZ",
        standard_code="ISO 4014",
        notes="GERARCHIA NORME: ISO 4014 > UNI EN ISO 4014 > DIN 931. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        material_label="ACCIAIO ZINCATO CL 8.8",
        series=PARTIAL_SERIES,
    ),
    SubConfig(
        sub_code="HEPA",
        thread_kind="PARZ",
        standard_code="ISO 4014",
        notes="GERARCHIA NORME: ISO 4014 > UNI EN ISO 4014 > DIN 931. INOX A2: ISO 3506-1.",
        material_label="INOX A2-70",
        series=PARTIAL_SERIES,
    ),
    SubConfig(
        sub_code="HETZ",
        thread_kind="TOT",
        standard_code="ISO 4017",
        notes="GERARCHIA NORME: ISO 4017 > UNI EN ISO 4017 > DIN 933. ACCIAIO: ISO 898-1. ZINCATURA: ISO 4042.",
        material_label="ACCIAIO ZINCATO CL 8.8",
        series=FULL_SERIES,
    ),
    SubConfig(
        sub_code="HETA",
        thread_kind="TOT",
        standard_code="ISO 4017",
        notes="GERARCHIA NORME: ISO 4017 > UNI EN ISO 4017 > DIN 933. INOX A2: ISO 3506-1.",
        material_label="INOX A2-70",
        series=FULL_SERIES,
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_viti_sizes.db"
    db.backup_to_path(str(out))
    return out


def fetch_vit_ids(cur) -> Tuple[int, str]:
    cur.execute("SELECT id, code FROM category WHERE code='VIT'")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Categoria VIT non trovata. Esegui prima la patch base viti TE.")
    return int(row["id"]), str(row["code"])


def fetch_standard_id(cur, category_id: int, std_code: str) -> int:
    cur.execute(
        "SELECT id FROM standard WHERE category_id=? AND code=?",
        (int(category_id), normalize_upper(std_code)),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Norma non trovata in VIT: {std_code}")
    return int(row["id"])


def fetch_subcategory_id(cur, category_id: int, sub_code: str) -> int:
    cur.execute(
        "SELECT id FROM subcategory WHERE category_id=? AND code=?",
        (int(category_id), normalize_gggg_normati(sub_code)),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Sotto-categoria non trovata in VIT: {sub_code}")
    return int(row["id"])


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


def pick_primary_norm(standard_code: str) -> str:
    s = normalize_upper(standard_code or "")
    if not s:
        return ""
    for prefix in ("ISO", "UNI", "DIN"):
        m = re.search(rf"\b{prefix}\s*[A-Z0-9./-]+", s)
        if m:
            return normalize_upper(m.group(0))
    return s


def build_desc_base(thread_kind: str, diameter: int, length: int, material_label: str) -> str:
    kind = "PARZ FILETT" if thread_kind == "PARZ" else "TOT FILETT"
    return normalize_upper(f"VITE TE {kind} M{diameter}X{length} {material_label}")


def build_desc(thread_kind: str, diameter: int, length: int, material_label: str, norm_ref: str) -> str:
    base = build_desc_base(thread_kind, diameter, length, material_label)
    n = normalize_upper(norm_ref or "")
    if not n:
        return base
    return normalize_upper(base.replace(" M", f" {n} M", 1))


def ensure_item(
    cur,
    category_id: int,
    cat_code: str,
    subcategory_id: int,
    sub_code: str,
    standard_id: int,
    base_description: str,
    final_description: str,
    notes: str,
) -> bool:
    desc_base_n = normalize_upper(base_description)
    desc_n = normalize_upper(final_description)
    cur.execute(
        """
        SELECT id
        FROM item
        WHERE category_id=? AND subcategory_id=?
          AND (
              description=?
              OR description=?
              OR description LIKE ?
          )
        LIMIT 1
        """,
        (
            int(category_id),
            int(subcategory_id),
            desc_n,
            desc_base_n,
            f"% {desc_base_n} %",
        ),
    )
    row = cur.fetchone()
    if row is not None:
        cur.execute(
            """
            UPDATE item
            SET standard_id=?, description=?, notes=?, is_active=1, updated_at=?
            WHERE id=?
            """,
            (int(standard_id), desc_n, normalize_upper(notes), now_str(), int(row["id"])),
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
            normalize_upper(notes),
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

        cat_id, cat_code = fetch_vit_ids(cur)
        cat_code = normalize_mmm(cat_code)

        created_total = 0
        updated_total = 0
        per_sub_created: Dict[str, int] = {}
        per_sub_updated: Dict[str, int] = {}

        for cfg in SUBS:
            sub_code = normalize_gggg_normati(cfg.sub_code)
            sub_id = fetch_subcategory_id(cur, cat_id, sub_code)
            std_id = fetch_standard_id(cur, cat_id, cfg.standard_code)
            norm_ref = pick_primary_norm(cfg.standard_code)
            c_created = 0
            c_updated = 0

            for dia in sorted(cfg.series.keys()):
                for length in cfg.series[dia]:
                    base_desc = build_desc_base(cfg.thread_kind, dia, int(length), cfg.material_label)
                    desc = build_desc(cfg.thread_kind, dia, int(length), cfg.material_label, norm_ref)
                    created = ensure_item(
                        cur=cur,
                        category_id=cat_id,
                        cat_code=cat_code,
                        subcategory_id=sub_id,
                        sub_code=sub_code,
                        standard_id=std_id,
                        base_description=base_desc,
                        final_description=desc,
                        notes=cfg.notes,
                    )
                    if created:
                        c_created += 1
                    else:
                        c_updated += 1

            per_sub_created[sub_code] = c_created
            per_sub_updated[sub_code] = c_updated
            created_total += c_created
            updated_total += c_updated

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Items created total: {created_total}")
        print(f"Items updated total: {updated_total}")
        print("Details by subcategory:")
        for k in sorted(per_sub_created.keys()):
            print(f"  - {k}: created={per_sub_created[k]} updated={per_sub_updated[k]}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Populate VIT hex-head screws with size and length series.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
