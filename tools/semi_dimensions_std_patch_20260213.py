from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.utils import normalize_upper

DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


TONDI_STD = [
    6, 8, 10, 12, 14, 15, 16, 18, 19, 20, 22, 24, 25, 26, 27, 28, 30, 32, 35, 36,
    40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 120, 130, 140,
    150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290, 300,
    310, 340,
]

ESAGONI_STD = [10, 12, 13, 14, 15, 17, 19, 22, 24, 27, 30, 32, 36, 41, 46, 50, 80]

PIATTI_B_STD = [
    10, 12, 15, 16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 90, 100,
    110, 120, 130, 135, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250,
    260, 270, 280, 300, 320, 330, 340, 350, 360, 380, 400, 450, 500, 600,
]
PIATTI_S_STD = [2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 35, 40, 50, 60, 70, 80, 100, 120, 150, 200]

TUBI_D_STD = [
    12, 15, 16, 18, 20, 21.3, 22, 25, 26.9, 30, 32, 33.7, 35, 38, 40, 42, 42.4, 45,
    48.3, 50, 55, 60, 60.3, 70, 76.1, 80, 88.9, 90, 101.6, 108, 114.3, 120, 121, 127,
    133, 139.7, 152.4, 159, 168.3, 177.8, 193.7, 219.1, 244.5, 273, 323.9,
]
TUBI_S_STD = [1.5, 1.75, 2, 2.5, 2.6, 2.9, 3, 3.2, 3.6, 4, 4.5, 5, 6, 6.3, 7.1, 8, 10, 16]

TUBOLARI_SIDE_STD = [
    10, 12, 15, 16, 20, 25, 30, 34, 35, 40, 45, 50, 60, 70, 80, 90, 100, 110, 120,
    140, 150, 160, 180, 200, 220, 250, 260, 300, 350, 400,
]
TUBOLARI_S_STD = [1, 1.5, 2, 2.5, 3, 3.2, 3.5, 4, 5, 6, 6.3, 7.1, 8, 10, 12.5, 16]

LAMIERE_STD = sorted(
    {
        0.5, 0.63, 0.75, 0.88, 1, 1.25, 1.5, 2, 2.5, 2.99, 3, 4, 5, 6, 8, 10, 12, 15, 18, 20, 25,
        30, 35, 40, 50,
    }
)

TRAVI_STD = [
    "IPE80", "IPE100", "IPE120", "IPE140", "IPE160", "IPE180", "IPE200", "IPE220", "IPE240", "IPE270",
    "IPE300", "IPE330", "IPE360", "IPE400", "IPE450", "IPE500", "IPE550", "IPE600",
    "HEA100", "HEA120", "HEA140", "HEA160", "HEA180", "HEA200", "HEA220", "HEA240", "HEA260", "HEA280",
    "HEA300", "HEA320", "HEA340", "HEA360", "HEA400", "HEA450", "HEA500", "HEA600", "HEA700",
    "HEB100", "HEB120", "HEB140", "HEB160", "HEB180", "HEB200", "HEB220", "HEB240", "HEB260", "HEB280",
    "HEB300", "HEB320", "HEB340", "HEB360", "HEB400", "HEB450", "HEB500", "HEB550", "HEB600", "HEB650",
    "HEB700", "HEB800", "HEB900", "HEB1000",
]

PROFILATI_L_STD = [
    "L20X20X3", "L25X25X3", "L30X30X3", "L30X30X4", "L35X35X4", "L40X40X4", "L45X45X5",
    "L50X50X5", "L60X60X6", "L70X70X7", "L80X80X8", "L90X90X9", "L100X100X10",
    "L120X120X12", "L150X150X15",
    "L40X20X3", "L50X30X4", "L60X40X5", "L80X50X6", "L100X65X8", "L120X80X8",
    "L150X100X12", "L200X100X12", "L250X90X16",
]

PROFILATI_U_STD = [
    "U50X25X5", "U60X30X5", "U80X45X6", "U100X50X6", "U120X55X7", "U140X60X7",
    "U160X65X7.5", "U180X70X8", "U200X75X8.5", "U220X80X9", "U240X85X9.5",
    "U270X90X10", "U300X100X10.5", "U330X105X11.5", "U360X110X12.5", "U400X115X13.5",
]

PROFILATI_T_STD = [
    "T30X30X4", "T40X40X5", "T50X50X6", "T60X60X7", "T70X70X8", "T80X80X9",
    "T100X100X11", "T120X120X13",
    "T50X30X5", "T60X40X6", "T70X50X7", "T80X60X8",
]


def extract_numbers(text: str) -> List[float]:
    out: List[float] = []
    for tok in re.findall(r"\d+(?:[.,]\d+)?", text or ""):
        try:
            out.append(float(tok.replace(",", ".")))
        except ValueError:
            pass
    return out


def fmt_num(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def between(values: Iterable[float], lo: float, hi: float) -> List[float]:
    return [v for v in values if (v + 1e-9) >= lo and (v - 1e-9) <= hi]


def is_legacy_ambiguous(text: str) -> bool:
    s = normalize_upper(text or "")
    if not s:
        return True
    if re.search(r"\d+\s*-\s*\d+", s):
        return True
    if "VARIE" in s:
        return True
    return False


def dedupe_keep_order(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for v in values:
        vv = normalize_upper(v)
        if not vv or vv in seen:
            continue
        seen.add(vv)
        out.append(vv)
    return out


def gen_tondi(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    lo, hi = min(TONDI_STD), max(TONDI_STD)
    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
    vals = between(TONDI_STD, lo, hi)
    return [f"D{fmt_num(v)}" for v in vals]


def gen_esagoni(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    lo, hi = min(ESAGONI_STD), max(ESAGONI_STD)
    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
    vals = between(ESAGONI_STD, lo, hi)
    return [f"CH{fmt_num(v)}" for v in vals]


def gen_lamiere(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    lo, hi = min(LAMIERE_STD), max(LAMIERE_STD)
    if len(nums) >= 2:
        lo, hi = nums[0], nums[1]
    vals = between(LAMIERE_STD, lo, hi)
    return [f"SP{fmt_num(v)}" for v in vals]


def gen_tubi(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    d_lo, d_hi = min(TUBI_D_STD), max(TUBI_D_STD)
    s_lo, s_hi = min(TUBI_S_STD), max(TUBI_S_STD)
    if len(nums) >= 2:
        d_lo, d_hi = nums[0], nums[1]
    if len(nums) >= 4:
        s_lo, s_hi = nums[2], nums[3]
    d_vals = between(TUBI_D_STD, d_lo, d_hi)
    s_vals = between(TUBI_S_STD, s_lo, s_hi)
    out: List[str] = []
    for d in d_vals:
        for s in s_vals:
            if (2.0 * s) < d:
                out.append(f"{fmt_num(d)}X{fmt_num(s)}")
    return out


def gen_tubolari(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    side_lo, side_hi = min(TUBOLARI_SIDE_STD), max(TUBOLARI_SIDE_STD)
    s_lo, s_hi = min(TUBOLARI_S_STD), max(TUBOLARI_S_STD)

    if len(nums) >= 6:
        side_lo = min(nums[0], nums[1])
        side_hi = max(nums[3], nums[4])
        s_lo = min(nums[2], nums[5])
        s_hi = max(nums[2], nums[5])

    side_vals = between(TUBOLARI_SIDE_STD, side_lo, side_hi)
    s_vals = between(TUBOLARI_S_STD, s_lo, s_hi)

    out: List[str] = []
    for a in side_vals:
        for s in s_vals:
            if (2.0 * s) < a:
                out.append(f"{fmt_num(a)}X{fmt_num(a)}X{fmt_num(s)}")
    return out


def gen_piatti(dim_txt: str) -> List[str]:
    nums = extract_numbers(dim_txt)
    s_lo, s_hi = min(PIATTI_S_STD), max(PIATTI_S_STD)
    if len(nums) >= 2:
        s_lo, s_hi = nums[0], nums[1]
    s_vals = between(PIATTI_S_STD, s_lo, s_hi)

    out: List[str] = []
    for b in PIATTI_B_STD:
        for s in s_vals:
            if s < b:
                out.append(f"{fmt_num(b)}X{fmt_num(s)}")
    return out


def gen_travi() -> List[str]:
    return TRAVI_STD.copy()


def gen_profilati_l() -> List[str]:
    return PROFILATI_L_STD.copy()


def gen_profilati_u() -> List[str]:
    return PROFILATI_U_STD.copy()


def gen_profilati_t() -> List[str]:
    return PROFILATI_T_STD.copy()


def generate_dimensions(type_desc: str, legacy_dimensions: str) -> List[str]:
    t = normalize_upper(type_desc or "")
    d = normalize_upper(legacy_dimensions or "")
    if t == "TONDI":
        return dedupe_keep_order(gen_tondi(d))
    if t == "ESAGONI":
        return dedupe_keep_order(gen_esagoni(d))
    if t == "PIATTI":
        return dedupe_keep_order(gen_piatti(d))
    if t == "TUBI":
        return dedupe_keep_order(gen_tubi(d))
    if t == "TUBOLARI":
        return dedupe_keep_order(gen_tubolari(d))
    if t == "LAMIERE":
        return dedupe_keep_order(gen_lamiere(d))
    if t == "TRAVI":
        return dedupe_keep_order(gen_travi())
    if t == "PROFILO L":
        return dedupe_keep_order(gen_profilati_l())
    if t == "PROFILO U":
        return dedupe_keep_order(gen_profilati_u())
    if t == "PROFILO T":
        return dedupe_keep_order(gen_profilati_t())
    if t == "PROFILATI":
        # Legacy generico: popola con serie standard L/U/T.
        combo = gen_profilati_l() + gen_profilati_u() + gen_profilati_t()
        return dedupe_keep_order(combo)
    return []


def auto_weight(db: Database, type_desc: str, density: Optional[float], dimension: str) -> str:
    if density is None or density <= 0:
        return ""
    t = normalize_upper(type_desc or "")
    if t in {"PROFILO L", "PROFILO U", "PROFILO T"}:
        t = "PROFILATI"
    if t == "LAMIERE":
        sp = db._lamiera_thickness_mm(dimension)
        if sp is None or sp <= 0:
            return ""
        return f"{sp * density:.3f}"
    area = db._section_area_mm2(t, dimension)
    if area is None or area <= 0:
        return ""
    return f"{(area * density) / 1000.0:.3f}"


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = BACKUP_DIR / f"unificati_manager_backup_{stamp}_semi_std.db"
        db.backup_to_path(str(backup_path))
        print(f"Backup: {backup_path}")

        cur = db.conn.cursor()
        cur.execute(
            """
            SELECT
                si.id,
                si.material_id,
                si.dimensions AS legacy_dimensions,
                st.description AS type_desc,
                (SELECT COUNT(*) FROM semi_item_dimension d WHERE d.semi_item_id=si.id) AS dim_count,
                (SELECT d.dimension FROM semi_item_dimension d WHERE d.semi_item_id=si.id ORDER BY d.id LIMIT 1) AS first_dim
            FROM semi_item si
            JOIN semi_type st ON st.id=si.type_id
            ORDER BY si.id
            """
        )
        rows = cur.fetchall()

        density_cache: Dict[int, Optional[float]] = {}
        updated_items = 0
        inserted_rows = 0
        deleted_rows = 0
        computed_weights = 0
        skipped_custom = 0
        skipped_no_generator = 0
        skipped_not_legacy = 0
        per_type = defaultdict(int)

        if apply_changes:
            db.conn.execute("BEGIN")

        for r in rows:
            semi_id = int(r["id"])
            type_desc = normalize_upper(str(r["type_desc"] or ""))
            legacy = normalize_upper(str(r["legacy_dimensions"] or ""))
            first_dim = normalize_upper(str(r["first_dim"] or ""))
            dim_count = int(r["dim_count"] or 0)
            material_id = r["material_id"]

            if dim_count != 1:
                skipped_not_legacy += 1
                continue
            if legacy != first_dim:
                skipped_custom += 1
                continue
            if not is_legacy_ambiguous(legacy):
                skipped_not_legacy += 1
                continue

            new_dims = generate_dimensions(type_desc, legacy)
            if not new_dims:
                skipped_no_generator += 1
                continue

            if material_id is not None:
                mid = int(material_id)
                if mid not in density_cache:
                    density_cache[mid] = db.read_material_density_g_cm3(mid)
                density = density_cache[mid]
            else:
                density = None

            to_insert: List[Tuple[int, str, str, int]] = []
            sort_order = 10
            for dim in new_dims:
                w = auto_weight(db, type_desc, density, dim)
                if w:
                    computed_weights += 1
                to_insert.append((semi_id, dim, w, sort_order))
                sort_order += 10

            if apply_changes:
                db.conn.execute("DELETE FROM semi_item_dimension WHERE semi_item_id=?", (semi_id,))
                db.conn.executemany(
                    """
                    INSERT INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order)
                    VALUES(?, ?, ?, ?)
                    """,
                    to_insert,
                )

            updated_items += 1
            inserted_rows += len(to_insert)
            deleted_rows += dim_count
            per_type[type_desc] += 1

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Updated items: {updated_items}")
        print(f"Rows deleted: {deleted_rows}")
        print(f"Rows inserted: {inserted_rows}")
        print(f"Auto-weights computed: {computed_weights}")
        print(f"Skipped custom rows: {skipped_custom}")
        print(f"Skipped no generator: {skipped_no_generator}")
        print(f"Skipped non-legacy rows: {skipped_not_legacy}")
        print("Updated by type:")
        for t in sorted(per_type):
            print(f"  - {t}: {per_type[t]}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Populate STD semilavorati dimensions from legacy range rows.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
