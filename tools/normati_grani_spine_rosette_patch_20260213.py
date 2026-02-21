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
    kind: str  # grano / spina / rosetta
    prefix: str
    suffix: str
    fixed_d: int = 0


CATEGORY_DESCRIPTIONS: Dict[str, str] = {
    "VIT": "VITI",
    "SPI": "SPINE",
    "ROS": "RONDELLE / ROSETTE",
}

# Placeholder subcategories from defaults to avoid visual duplication.
LEGACY_SUBS_TO_REMOVE: List[Tuple[str, str]] = [
    ("ROS", "PLAN"),
]


GRANI_SERIES: Dict[int, List[int]] = {
    3: [3, 4, 5, 6, 8, 10, 12, 16, 20],
    4: [4, 5, 6, 8, 10, 12, 16, 20, 25],
    5: [5, 6, 8, 10, 12, 16, 20, 25, 30],
    6: [6, 8, 10, 12, 16, 20, 25, 30, 35, 40],
    8: [8, 10, 12, 16, 20, 25, 30, 35, 40, 45, 50],
    10: [10, 12, 16, 20, 25, 30, 35, 40, 45, 50, 60],
    12: [12, 16, 20, 25, 30, 35, 40, 45, 50, 60],
    14: [14, 16, 20, 25, 30, 35, 40, 45, 50, 60],
    16: [16, 20, 25, 30, 35, 40, 45, 50, 60],
}

SPINA_LENGTHS_D6: List[int] = [10, 12, 16, 20, 25, 30, 35, 40, 45, 50, 60]
WASHER_SIZES: List[int] = [3, 4, 5, 6, 8, 10, 12, 14, 16, 20, 24]


STANDARDS: List[StdDef] = [
    # VIT - grani
    StdDef("VIT", "ISO 4026", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA PIANA"),
    StdDef("VIT", "UNI EN ISO 4026", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA PIANA"),
    StdDef("VIT", "DIN 913", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA PIANA"),
    StdDef("VIT", "ISO 4027", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CONICA"),
    StdDef("VIT", "UNI EN ISO 4027", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CONICA"),
    StdDef("VIT", "DIN 914", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CONICA"),
    StdDef("VIT", "ISO 4028", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CILINDRICA"),
    StdDef("VIT", "UNI EN ISO 4028", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CILINDRICA"),
    StdDef("VIT", "DIN 915", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA CILINDRICA"),
    StdDef("VIT", "ISO 4029", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA A COPPA"),
    StdDef("VIT", "UNI EN ISO 4029", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA A COPPA"),
    StdDef("VIT", "DIN 916", "VITE SENZA TESTA CAVA ESAGONALE ESTREMITA A COPPA"),
    StdDef("VIT", "ISO 898-5", "CARATTERISTICHE MECCANICHE VITI SENZA TESTA ACCIAIO"),
    StdDef("VIT", "ISO 3506-3", "CARATTERISTICHE MECCANICHE VITI SENZA TESTA INOX"),

    # SPI - spine
    StdDef("SPI", "ISO 8734", "SPINE CILINDRICHE TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "UNI EN ISO 8734", "SPINE CILINDRICHE TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "DIN 6325", "SPINE CILINDRICHE TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "ISO 8735", "SPINE CILINDRICHE CON FORO FILETTATO TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "UNI EN ISO 8735", "SPINE CILINDRICHE CON FORO FILETTATO TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "DIN 7979", "SPINE CILINDRICHE CON FORO FILETTATO TEMPRATE E RETTIFICATE"),
    StdDef("SPI", "ISO 2338", "SPINE CILINDRICHE NON TEMPRATE"),
    StdDef("SPI", "UNI EN ISO 2338", "SPINE CILINDRICHE NON TEMPRATE"),
    StdDef("SPI", "DIN 7", "SPINE CILINDRICHE NON TEMPRATE"),
    StdDef("SPI", "ISO 8733", "SPINE CILINDRICHE CON FORO FILETTATO NON TEMPRATE"),
    StdDef("SPI", "UNI EN ISO 8733", "SPINE CILINDRICHE CON FORO FILETTATO NON TEMPRATE"),
    StdDef("SPI", "DIN EN ISO 8733", "SPINE CILINDRICHE CON FORO FILETTATO NON TEMPRATE"),

    # ROS - rosette
    StdDef("ROS", "ISO 7089", "ROSETTA PIANA NORMALE"),
    StdDef("ROS", "UNI EN ISO 7089", "ROSETTA PIANA NORMALE"),
    StdDef("ROS", "DIN 125-A", "ROSETTA PIANA NORMALE"),
    StdDef("ROS", "ISO 7093-1", "ROSETTA PIANA LARGA"),
    StdDef("ROS", "UNI EN ISO 7093-1", "ROSETTA PIANA LARGA"),
    StdDef("ROS", "DIN 9021", "ROSETTA PIANA LARGA"),
    StdDef("ROS", "UNI 1751", "ROSETTA ELASTICA"),
    StdDef("ROS", "DIN 127-B", "ROSETTA ELASTICA"),
]


SUBCATEGORIES: List[SubDef] = [
    # -------- Grani (VIT) --------
    SubDef(
        category_code="VIT",
        code="GPIA",
        description="GRANI ESTREMITA PIANA ACCIAIO",
        primary_std_code="ISO 4026",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA PIANA ISO 4026 M__X__ ACCIAIO",
        notes="GERARCHIA NORME: ISO 4026 > UNI EN ISO 4026 > DIN 913. ACCIAIO: ISO 898-5.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA PIANA ISO 4026",
        suffix="ACCIAIO",
    ),
    SubDef(
        category_code="VIT",
        code="GPIX",
        description="GRANI ESTREMITA PIANA INOX",
        primary_std_code="ISO 4026",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA PIANA ISO 4026 M__X__ INOX A2-70",
        notes="GERARCHIA NORME: ISO 4026 > UNI EN ISO 4026 > DIN 913. INOX: ISO 3506-3.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA PIANA ISO 4026",
        suffix="INOX A2-70",
    ),
    SubDef(
        category_code="VIT",
        code="GCIA",
        description="GRANI ESTREMITA CILINDRICA ACCIAIO",
        primary_std_code="ISO 4028",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA CILINDRICA ISO 4028 M__X__ ACCIAIO",
        notes="GERARCHIA NORME: ISO 4028 > UNI EN ISO 4028 > DIN 915. ACCIAIO: ISO 898-5.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA CILINDRICA ISO 4028",
        suffix="ACCIAIO",
    ),
    SubDef(
        category_code="VIT",
        code="GCIX",
        description="GRANI ESTREMITA CILINDRICA INOX",
        primary_std_code="ISO 4028",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA CILINDRICA ISO 4028 M__X__ INOX A2-70",
        notes="GERARCHIA NORME: ISO 4028 > UNI EN ISO 4028 > DIN 915. INOX: ISO 3506-3.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA CILINDRICA ISO 4028",
        suffix="INOX A2-70",
    ),
    SubDef(
        category_code="VIT",
        code="GCOA",
        description="GRANI ESTREMITA CONICA ACCIAIO",
        primary_std_code="ISO 4027",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA CONICA ISO 4027 M__X__ ACCIAIO",
        notes="GERARCHIA NORME: ISO 4027 > UNI EN ISO 4027 > DIN 914. ACCIAIO: ISO 898-5.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA CONICA ISO 4027",
        suffix="ACCIAIO",
    ),
    SubDef(
        category_code="VIT",
        code="GCOX",
        description="GRANI ESTREMITA CONICA INOX",
        primary_std_code="ISO 4027",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA CONICA ISO 4027 M__X__ INOX A2-70",
        notes="GERARCHIA NORME: ISO 4027 > UNI EN ISO 4027 > DIN 914. INOX: ISO 3506-3.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA CONICA ISO 4027",
        suffix="INOX A2-70",
    ),
    SubDef(
        category_code="VIT",
        code="GCPA",
        description="GRANI ESTREMITA A COPPA ACCIAIO",
        primary_std_code="ISO 4029",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA A COPPA ISO 4029 M__X__ ACCIAIO",
        notes="GERARCHIA NORME: ISO 4029 > UNI EN ISO 4029 > DIN 916. ACCIAIO: ISO 898-5.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA A COPPA ISO 4029",
        suffix="ACCIAIO",
    ),
    SubDef(
        category_code="VIT",
        code="GCPX",
        description="GRANI ESTREMITA A COPPA INOX",
        primary_std_code="ISO 4029",
        desc_template="VITE GRANO CAVA ESAGONALE ESTREMITA A COPPA ISO 4029 M__X__ INOX A2-70",
        notes="GERARCHIA NORME: ISO 4029 > UNI EN ISO 4029 > DIN 916. INOX: ISO 3506-3.",
        kind="grano",
        prefix="VITE GRANO CAVA ESAGONALE ESTREMITA A COPPA ISO 4029",
        suffix="INOX A2-70",
    ),

    # -------- Spine (SPI) --------
    SubDef(
        category_code="SPI",
        code="TM6A",
        description="SPINE TEMPRATE RETTIFICATE M6 ACCIAIO",
        primary_std_code="ISO 8734",
        desc_template="SPINA CILINDRICA TEMPRATA RETTIFICATA ISO 8734 D6X__ M6 ACCIAIO",
        notes="GERARCHIA NORME: ISO 8734 > UNI EN ISO 8734 > DIN 6325. TOLLERANZA M6.",
        kind="spina",
        prefix="SPINA CILINDRICA TEMPRATA RETTIFICATA ISO 8734",
        suffix="M6 ACCIAIO",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="TF6A",
        description="SPINE TEMPRATE RETTIFICATE M6 CON FORO FILETTATO ACCIAIO",
        primary_std_code="ISO 8735",
        desc_template="SPINA CILINDRICA CON FORO FILETTATO ISO 8735 D6X__ M6 ACCIAIO",
        notes="GERARCHIA NORME: ISO 8735 > UNI EN ISO 8735 > DIN 7979. TOLLERANZA M6.",
        kind="spina",
        prefix="SPINA CILINDRICA CON FORO FILETTATO ISO 8735",
        suffix="M6 ACCIAIO",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NM6A",
        description="SPINE NON TEMPRATE M6 ACCIAIO",
        primary_std_code="ISO 2338",
        desc_template="SPINA CILINDRICA ISO 2338 D6X__ M6 ACCIAIO",
        notes="GERARCHIA NORME: ISO 2338 > UNI EN ISO 2338 > DIN 7. TOLLERANZA M6.",
        kind="spina",
        prefix="SPINA CILINDRICA ISO 2338",
        suffix="M6 ACCIAIO",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NM6X",
        description="SPINE NON TEMPRATE M6 INOX",
        primary_std_code="ISO 2338",
        desc_template="SPINA CILINDRICA ISO 2338 D6X__ M6 INOX",
        notes="GERARCHIA NORME: ISO 2338 > UNI EN ISO 2338 > DIN 7. TOLLERANZA M6. INOX.",
        kind="spina",
        prefix="SPINA CILINDRICA ISO 2338",
        suffix="M6 INOX",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NH8A",
        description="SPINE NON TEMPRATE H8 ACCIAIO",
        primary_std_code="ISO 2338",
        desc_template="SPINA CILINDRICA ISO 2338 D6X__ H8 ACCIAIO",
        notes="GERARCHIA NORME: ISO 2338 > UNI EN ISO 2338 > DIN 7. TOLLERANZA H8.",
        kind="spina",
        prefix="SPINA CILINDRICA ISO 2338",
        suffix="H8 ACCIAIO",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NH8X",
        description="SPINE NON TEMPRATE H8 INOX",
        primary_std_code="ISO 2338",
        desc_template="SPINA CILINDRICA ISO 2338 D6X__ H8 INOX",
        notes="GERARCHIA NORME: ISO 2338 > UNI EN ISO 2338 > DIN 7. TOLLERANZA H8. INOX.",
        kind="spina",
        prefix="SPINA CILINDRICA ISO 2338",
        suffix="H8 INOX",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NF6A",
        description="SPINE NON TEMPRATE M6 CON FORO FILETTATO ACCIAIO",
        primary_std_code="ISO 8733",
        desc_template="SPINA CILINDRICA CON FORO FILETTATO ISO 8733 D6X__ M6 ACCIAIO",
        notes="GERARCHIA NORME: ISO 8733 > UNI EN ISO 8733 > DIN EN ISO 8733. TOLLERANZA M6.",
        kind="spina",
        prefix="SPINA CILINDRICA CON FORO FILETTATO ISO 8733",
        suffix="M6 ACCIAIO",
        fixed_d=6,
    ),
    SubDef(
        category_code="SPI",
        code="NF6X",
        description="SPINE NON TEMPRATE M6 CON FORO FILETTATO INOX",
        primary_std_code="ISO 8733",
        desc_template="SPINA CILINDRICA CON FORO FILETTATO ISO 8733 D6X__ M6 INOX",
        notes="GERARCHIA NORME: ISO 8733 > UNI EN ISO 8733 > DIN EN ISO 8733. TOLLERANZA M6. INOX.",
        kind="spina",
        prefix="SPINA CILINDRICA CON FORO FILETTATO ISO 8733",
        suffix="M6 INOX",
        fixed_d=6,
    ),

    # -------- Rosette (ROS) --------
    SubDef(
        category_code="ROS",
        code="RNZA",
        description="ROSETTE PIANE NORMALI ACCIAIO ZINCATO",
        primary_std_code="ISO 7089",
        desc_template="ROSETTA PIANA NORMALE ISO 7089 M__ ACCIAIO ZINCATO",
        notes="GERARCHIA NORME: ISO 7089 > UNI EN ISO 7089 > DIN 125-A.",
        kind="rosetta",
        prefix="ROSETTA PIANA NORMALE ISO 7089",
        suffix="ACCIAIO ZINCATO",
    ),
    SubDef(
        category_code="ROS",
        code="RNIX",
        description="ROSETTE PIANE NORMALI INOX",
        primary_std_code="ISO 7089",
        desc_template="ROSETTA PIANA NORMALE ISO 7089 M__ INOX A2",
        notes="GERARCHIA NORME: ISO 7089 > UNI EN ISO 7089 > DIN 125-A.",
        kind="rosetta",
        prefix="ROSETTA PIANA NORMALE ISO 7089",
        suffix="INOX A2",
    ),
    SubDef(
        category_code="ROS",
        code="RLZA",
        description="ROSETTE PIANE LARGHE ACCIAIO ZINCATO",
        primary_std_code="ISO 7093-1",
        desc_template="ROSETTA PIANA LARGA ISO 7093-1 M__ ACCIAIO ZINCATO",
        notes="GERARCHIA NORME: ISO 7093-1 > UNI EN ISO 7093-1 > DIN 9021.",
        kind="rosetta",
        prefix="ROSETTA PIANA LARGA ISO 7093-1",
        suffix="ACCIAIO ZINCATO",
    ),
    SubDef(
        category_code="ROS",
        code="RLIX",
        description="ROSETTE PIANE LARGHE INOX",
        primary_std_code="ISO 7093-1",
        desc_template="ROSETTA PIANA LARGA ISO 7093-1 M__ INOX A2",
        notes="GERARCHIA NORME: ISO 7093-1 > UNI EN ISO 7093-1 > DIN 9021.",
        kind="rosetta",
        prefix="ROSETTA PIANA LARGA ISO 7093-1",
        suffix="INOX A2",
    ),
    SubDef(
        category_code="ROS",
        code="REZA",
        description="ROSETTE ELASTICHE ACCIAIO ZINCATO",
        primary_std_code="UNI 1751",
        desc_template="ROSETTA ELASTICA UNI 1751 M__ ACCIAIO ZINCATO",
        notes="GERARCHIA NORME: ISO N/A > UNI 1751 > DIN 127-B.",
        kind="rosetta",
        prefix="ROSETTA ELASTICA UNI 1751",
        suffix="ACCIAIO ZINCATO",
    ),
    SubDef(
        category_code="ROS",
        code="REIX",
        description="ROSETTE ELASTICHE INOX",
        primary_std_code="UNI 1751",
        desc_template="ROSETTA ELASTICA UNI 1751 M__ INOX A2",
        notes="GERARCHIA NORME: ISO N/A > UNI 1751 > DIN 127-B.",
        kind="rosetta",
        prefix="ROSETTA ELASTICA UNI 1751",
        suffix="INOX A2",
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_grani_spine_rosette.db"
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
    if sub.kind == "grano":
        for d in sorted(GRANI_SERIES.keys()):
            for l in GRANI_SERIES[d]:
                out.append(normalize_upper(f"{sub.prefix} M{d}X{int(l)} {sub.suffix}"))
        return out
    if sub.kind == "spina":
        d = int(sub.fixed_d) if int(sub.fixed_d) > 0 else 6
        for l in SPINA_LENGTHS_D6:
            out.append(normalize_upper(f"{sub.prefix} D{d}X{int(l)} {sub.suffix}"))
        return out
    if sub.kind == "rosetta":
        for m in WASHER_SIZES:
            out.append(normalize_upper(f"{sub.prefix} M{int(m)} {sub.suffix}"))
        return out
    raise RuntimeError(f"Tipo sottocategoria non gestito: {sub.kind}")


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
            cid = category_ids[cat_code]
            rem_items, rem_sub = delete_legacy_subcategory(cur, cid, legacy_sub)
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
            key = f"{cat_code}:{sub_code_norm}"
            per_sub_created[key] = created_sub
            per_sub_updated[key] = updated_sub
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
        description=(
            "Populate VIT (grani), SPI (spine), ROS (rosette) with requested variants and "
            "ISO>UNI>DIN hierarchy."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
