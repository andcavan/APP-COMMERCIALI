from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


IPE_SERIES = [80, 100, 120, 140, 160, 180, 200, 220, 240, 270, 300, 330, 360, 400, 450, 500, 550, 600]
HE_SERIES = [100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 400, 450, 500, 550, 600, 650, 700, 800, 900, 1000]
UPN_SERIES = [50, 65, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 350, 400]
UPE_SERIES = [80, 100, 120, 140, 160, 180, 200, 220, 240, 270, 300, 330, 360, 400]

L_TRAFILATO_DIMS = [
    "L20X20X2",
    "L20X20X3",
    "L30X20X2",
    "L30X30X2",
    "L30X30X3",
    "L35X35X3",
    "L35X35X5",
    "L40X20X2",
    "L40X30X3",
    "L40X40X3",
    "L40X40X5",
    "L50X30X3",
    "L50X40X3",
    "L50X50X3",
    "L50X50X5",
    "L60X40X3",
    "L60X50X5",
    "L60X60X3",
    "L60X60X5",
]

U_TRAFILATO_DIMS = [
    "U30X15X3",
    "U40X20X3",
    "U50X25X4",
    "U60X30X5",
    "U80X40X5",
    "U100X50X6",
    "U120X60X6",
    "U140X60X6",
]

T_TRAFILATO_DIMS = [
    "T20X20X3",
    "T25X25X3.5",
    "T30X30X4",
    "T35X35X4.5",
    "T40X40X5",
    "T45X45X5.5",
    "T50X50X6",
    "T60X60X7",
    "T70X70X8",
    "T80X80X9",
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fmt_int_series(prefix: str, values: Iterable[int]) -> List[str]:
    return [f"{prefix}{int(v)}" for v in values]


BEAM_FAMILIES: Dict[str, Dict[str, object]] = {
    "TRAVE IPE": {
        "code": "TVIP",
        "summary": "IPE - EN 10365",
        "dims": fmt_int_series("IPE", IPE_SERIES),
    },
    "TRAVE HEA": {
        "code": "TVHA",
        "summary": "HEA - EN 10365",
        "dims": fmt_int_series("HEA", HE_SERIES),
    },
    "TRAVE HEB": {
        "code": "TVHB",
        "summary": "HEB - EN 10365",
        "dims": fmt_int_series("HEB", HE_SERIES),
    },
    "TRAVE HEM": {
        "code": "TVHM",
        "summary": "HEM - EN 10365",
        "dims": fmt_int_series("HEM", HE_SERIES),
    },
    "TRAVE HE": {
        "code": "TVHE",
        "summary": "HE (A/B/M) - EN 10365",
        "dims": fmt_int_series("HEA", HE_SERIES) + fmt_int_series("HEB", HE_SERIES) + fmt_int_series("HEM", HE_SERIES),
    },
    "TRAVE UPN": {
        "code": "TVUN",
        "summary": "UPN - EN 10365",
        "dims": fmt_int_series("UPN", UPN_SERIES),
    },
    "TRAVE UPE": {
        "code": "TVUE",
        "summary": "UPE - EN 10365",
        "dims": fmt_int_series("UPE", UPE_SERIES),
    },
    # UPS non risulta una serie EN indipendente nelle fonti trovate:
    # viene gestita come alias commerciale della serie UPE.
    "TRAVE UPS": {
        "code": "TVUS",
        "summary": "UPS (ALIAS UPE) - EN 10365",
        "dims": fmt_int_series("UPS", UPE_SERIES),
    },
}


TRAFILATO_TYPES: Dict[str, Dict[str, object]] = {
    "PROFILO L TRAFILATO": {
        "code": "PLTR",
        "source_type": "PROFILO L",
        "summary": "L TRAFILATO - SPIGOLI VIVI",
        "standard": "DIN 59370 / EN 10278",
        "dims": L_TRAFILATO_DIMS,
    },
    "PROFILO U TRAFILATO": {
        "code": "PUTR",
        "source_type": "PROFILO U",
        "summary": "U TRAFILATO - SPIGOLI VIVI",
        "standard": "EN 10278 / EN 10056 (RANGE COMMERCIALE)",
        "dims": U_TRAFILATO_DIMS,
    },
    "PROFILO T TRAFILATO": {
        "code": "PTTR",
        "source_type": "PROFILO T",
        "summary": "T TRAFILATO - SPIGOLI VIVI",
        "standard": "EN 10055 / EN 10278 (RANGE COMMERCIALE)",
        "dims": T_TRAFILATO_DIMS,
    },
}


def ensure_type(cur, code: str, description: str) -> int:
    cur.execute(
        "INSERT OR IGNORE INTO semi_type(code, description) VALUES(?, ?)",
        (normalize_upper(code), normalize_upper(description)),
    )
    cur.execute("SELECT id FROM semi_type WHERE code=?", (normalize_upper(code),))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Tipo non trovato per code={code}")
    return int(row["id"])


def get_state_id(cur, description: str) -> int:
    cur.execute("SELECT id FROM semi_state WHERE description=?", (normalize_upper(description),))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Stato semilavorato non trovato: {description}")
    return int(row["id"])


def find_or_create_item(
    cur,
    type_id: int,
    state_id: int,
    material_id: Optional[int],
    description: str,
    summary_dimensions: str,
    standard: str,
    notes: str,
    is_active: int,
) -> int:
    cur.execute(
        """
        SELECT id
        FROM semi_item
        WHERE type_id=? AND state_id=? AND COALESCE(material_id,0)=COALESCE(?,0) AND description=?
        ORDER BY id
        LIMIT 1
        """,
        (int(type_id), int(state_id), material_id, normalize_upper(description)),
    )
    row = cur.fetchone()
    if row is not None:
        item_id = int(row["id"])
        cur.execute(
            """
            UPDATE semi_item
            SET dimensions=?, standard=?, notes=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                normalize_upper(summary_dimensions),
                normalize_upper(standard),
                normalize_upper(notes),
                int(is_active),
                now_str(),
                item_id,
            ),
        )
        return item_id

    cur.execute(
        """
        INSERT INTO semi_item(type_id, state_id, material_id, description, dimensions, standard, notes, is_active, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(type_id),
            int(state_id),
            int(material_id) if material_id is not None else None,
            normalize_upper(description),
            normalize_upper(summary_dimensions),
            normalize_upper(standard),
            normalize_upper(notes),
            int(is_active),
            now_str(),
            now_str(),
        ),
    )
    return int(cur.lastrowid)


def replace_dimensions(cur, item_id: int, rows: List[Tuple[str, str]]) -> int:
    cur.execute("DELETE FROM semi_item_dimension WHERE semi_item_id=?", (int(item_id),))
    sort_order = 10
    inserted = 0
    for dim, weight in rows:
        cur.execute(
            """
            INSERT OR IGNORE INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order)
            VALUES(?, ?, ?, ?)
            """,
            (int(item_id), normalize_upper(dim), normalize_upper(weight), int(sort_order)),
        )
        if cur.rowcount > 0:
            inserted += 1
        sort_order += 10
    return inserted


def beam_rows(dims: List[str]) -> List[Tuple[str, str]]:
    return [(d, "") for d in dims]


def trafilato_rows(db: Database, type_desc: str, material_id: Optional[int], dims: List[str]) -> List[Tuple[str, str]]:
    density: Optional[float] = None
    if material_id is not None:
        density = db.read_material_density_g_cm3(int(material_id))
    out: List[Tuple[str, str]] = []
    for d in dims:
        w = ""
        if density is not None and density > 0:
            area = db._section_area_mm2(type_desc, d)
            if area is not None and area > 0:
                w = f"{(area * density) / 1000.0:.3f}"
        out.append((d, w))
    return out


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = BACKUP_DIR / f"unificati_manager_backup_{stamp}_beam_trafilati.db"
        db.backup_to_path(str(backup_path))
        print(f"Backup: {backup_path}")

        cur = db.conn.cursor()

        type_ids: Dict[str, int] = {}
        # Ensure all target types
        for t_desc, meta in BEAM_FAMILIES.items():
            type_ids[t_desc] = ensure_type(cur, str(meta["code"]), t_desc)
        for t_desc, meta in TRAFILATO_TYPES.items():
            type_ids[t_desc] = ensure_type(cur, str(meta["code"]), t_desc)
        # Existing source types
        type_ids["TRAVI"] = ensure_type(cur, "TRAV", "TRAVI")
        type_ids["PROFILO L"] = ensure_type(cur, "PRFL", "PROFILO L")
        type_ids["PROFILO U"] = ensure_type(cur, "PRFU", "PROFILO U")
        type_ids["PROFILO T"] = ensure_type(cur, "PRFT", "PROFILO T")

        state_trafilato = get_state_id(cur, "TRAFILATO")

        # close implicit transaction before explicit BEGIN
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        travi_src_count = 0
        beam_items_created_or_updated = 0
        beam_dims_written = 0
        travi_deleted = 0

        cur.execute(
            """
            SELECT id, state_id, material_id, description, standard, notes, is_active
            FROM semi_item
            WHERE type_id=?
            ORDER BY id
            """,
            (int(type_ids["TRAVI"]),),
        )
        travi_rows = cur.fetchall()
        travi_src_count = len(travi_rows)

        for src in travi_rows:
            src_id = int(src["id"])
            for beam_type_desc, meta in BEAM_FAMILIES.items():
                dst_id = find_or_create_item(
                    cur=cur,
                    type_id=type_ids[beam_type_desc],
                    state_id=int(src["state_id"]),
                    material_id=int(src["material_id"]) if src["material_id"] is not None else None,
                    description=str(src["description"] or "TRAVE"),
                    summary_dimensions=str(meta["summary"]),
                    standard=str(src["standard"] or ""),
                    notes=str(src["notes"] or ""),
                    is_active=int(src["is_active"] or 1),
                )
                rows = beam_rows(list(meta["dims"]))  # type: ignore[arg-type]
                beam_dims_written += replace_dimensions(cur, dst_id, rows)
                beam_items_created_or_updated += 1

            cur.execute("DELETE FROM semi_item WHERE id=?", (src_id,))
            travi_deleted += 1

        # Create/refresh trafilato profile items (derived from LAMINATO structural items)
        trafilato_items_created_or_updated = 0
        trafilato_dims_written = 0

        for trf_type_desc, meta in TRAFILATO_TYPES.items():
            source_type_desc = str(meta["source_type"])
            source_type_id = type_ids[source_type_desc]
            target_type_id = type_ids[trf_type_desc]
            dims = list(meta["dims"])  # type: ignore[arg-type]
            std = str(meta["standard"])
            summary = str(meta["summary"])

            cur.execute(
                """
                SELECT si.id, si.material_id, si.description, si.is_active
                FROM semi_item si
                JOIN semi_state ss ON ss.id=si.state_id
                WHERE si.type_id=? AND ss.description='LAMINATO'
                ORDER BY si.id
                """,
                (int(source_type_id),),
            )
            src_rows = cur.fetchall()
            for src in src_rows:
                dst_id = find_or_create_item(
                    cur=cur,
                    type_id=target_type_id,
                    state_id=state_trafilato,
                    material_id=int(src["material_id"]) if src["material_id"] is not None else None,
                    description=str(src["description"] or "PROFILO TRAFILATO"),
                    summary_dimensions=summary,
                    standard=std,
                    notes="SPIGOLI VIVI",
                    is_active=int(src["is_active"] or 1),
                )
                rows = trafilato_rows(
                    db=db,
                    type_desc=trf_type_desc,
                    material_id=int(src["material_id"]) if src["material_id"] is not None else None,
                    dims=dims,
                )
                trafilato_dims_written += replace_dimensions(cur, dst_id, rows)
                trafilato_items_created_or_updated += 1

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"TRAVI source items: {travi_src_count}")
        print(f"TRAVI deleted after split: {travi_deleted}")
        print(f"Beam items created/updated: {beam_items_created_or_updated}")
        print(f"Beam dimension rows written: {beam_dims_written}")
        print(f"Trafilato items created/updated: {trafilato_items_created_or_updated}")
        print(f"Trafilato dimension rows written: {trafilato_dims_written}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Split TRAVI into IPE/HE/UPN/UPE/UPS families and add L/U/T trafilati families.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
