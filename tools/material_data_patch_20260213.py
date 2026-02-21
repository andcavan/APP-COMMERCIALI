from __future__ import annotations

import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


DEFAULT_MATERIAL_PROPERTY_TEMPLATE = [
    ("PHYS", "RESISTIVITA ELETTRICA", "UOHM*CM", "", "", "", 10),
    ("PHYS", "RESISTIVITA VOLUMICA", "OHM*CM", "", "", "", 20),
    ("PHYS", "DENSITA", "G/CM3", "", "", "", 30),
    ("PHYS", "COEFF. DILATAZIONE", "UM/MK", "", "", "", 40),
    ("PHYS", "INTERVALLO DI FUSIONE", "C", "", "", "", 50),
    ("PHYS", "CALORE SPECIFICO", "J/KGK", "", "", "", 60),
    ("PHYS", "CONDUCIBILITA TERMICA", "W/MK", "", "", "", 70),
    ("MECH", "CARICO DI ROTTURA RM", "MPA", "", "", "", 10),
    ("MECH", "SNERVAMENTO RP0.2", "MPA", "", "", "", 20),
    ("MECH", "ALLUNGAMENTO A", "%", "", "", "", 30),
    ("MECH", "COEFFICIENTE DI POISSON", "-", "", "", "", 40),
    ("MECH", "DUREZZA BRINELL", "HB", "", "", "", 50),
    ("MECH", "DUREZZA ROCKWELL C", "HRC", "", "", "", 60),
    ("MECH", "MODULO ELASTICO E", "GPA", "", "", "", 70),
    ("MECH", "LIMITE DI FATICA", "MPA", "", "", "", 80),
    ("MECH", "RESILIENZA CHARPY", "J", "", "", "", 90),
    ("MECH", "RESISTENZA A COMPRESSIONE", "MPA", "", "", "", 100),
]

CHEM_SORT_ORDER = {
    "C": 10,
    "SI": 20,
    "MN": 30,
    "P": 40,
    "S": 50,
    "CR": 60,
    "NI": 70,
    "MO": 80,
    "N": 90,
    "CU": 100,
    "AL": 110,
    "SN": 120,
    "ZN": 130,
    "MATRICE": 140,
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def norm(value: Optional[str]) -> str:
    return (value or "").strip().upper()


@dataclass
class PatchStats:
    derived_values: int = 0
    properties_updated: int = 0
    new_materials: int = 0


class MaterialPatcher:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.stats = PatchStats()

    def backup(self) -> Path:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_materials_patch.db"
        shutil.copy2(DB_PATH, out)
        return out

    def get_material(self, description: str) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, code, family, description, standard, notes FROM material WHERE description=?",
            (norm(description),),
        )
        return cur.fetchone()

    def ensure_taxonomy(self, family: str, description: str) -> None:
        fam = norm(family)
        sub = norm(description)
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO material_family(description) VALUES(?)", (fam,))
        cur.execute("SELECT id FROM material_family WHERE description=?", (fam,))
        row = cur.fetchone()
        if row is not None:
            cur.execute(
                "INSERT OR IGNORE INTO material_subfamily(family_id, description) VALUES(?, ?)",
                (int(row["id"]), sub),
            )

    def ensure_material(
        self,
        family: str,
        description: str,
        standard: str,
        notes: str = "",
    ) -> int:
        fam = norm(family)
        desc = norm(description)
        row = self.get_material(desc)
        if row is not None:
            cur = self.conn.cursor()
            cur.execute(
                """
                UPDATE material
                SET family=?, standard=CASE WHEN TRIM(COALESCE(standard,''))='' THEN ? ELSE standard END,
                    notes=CASE WHEN TRIM(COALESCE(notes,''))='' THEN ? ELSE notes END, updated_at=?
                WHERE id=?
                """,
                (fam, norm(standard), norm(notes), now_str(), int(row["id"])),
            )
            self.ensure_taxonomy(fam, desc)
            self.ensure_default_template(int(row["id"]))
            return int(row["id"])

        code = f"MAT_{uuid.uuid4().hex[:10].upper()}"
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO material(code, family, description, standard, notes, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (code, fam, desc, norm(standard), norm(notes), now_str(), now_str()),
        )
        material_id = int(cur.lastrowid)
        self.ensure_taxonomy(fam, desc)
        self.ensure_default_template(material_id)
        self.stats.new_materials += 1
        return material_id

    def ensure_default_template(self, material_id: int) -> None:
        cur = self.conn.cursor()
        for group_code, name, unit, value, min_value, max_value, sort_order in DEFAULT_MATERIAL_PROPERTY_TEMPLATE:
            cur.execute(
                """
                INSERT OR IGNORE INTO material_property(
                    material_id, prop_group, state_code, name, unit, value, min_value, max_value, notes, sort_order
                )
                VALUES(?, ?, '', ?, ?, ?, ?, ?, '', ?)
                """,
                (
                    int(material_id),
                    norm(group_code),
                    norm(name),
                    norm(unit),
                    norm(value),
                    norm(min_value),
                    norm(max_value),
                    int(sort_order),
                ),
            )

    def touch_material(self, material_id: int) -> None:
        self.conn.execute("UPDATE material SET updated_at=? WHERE id=?", (now_str(), int(material_id)))

    def _load_prop(self, material_id: int, group_code: str, name: str) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, unit, value, min_value, max_value, notes, sort_order
            FROM material_property
            WHERE material_id=? AND prop_group=? AND state_code='' AND name=?
            """,
            (int(material_id), norm(group_code), norm(name)),
        )
        return cur.fetchone()

    def set_prop(
        self,
        material_id: int,
        group_code: str,
        name: str,
        unit: Optional[str] = None,
        value: Optional[str] = None,
        min_value: Optional[str] = None,
        max_value: Optional[str] = None,
        notes: Optional[str] = None,
        sort_order: Optional[int] = None,
    ) -> None:
        row = self._load_prop(material_id, group_code, name)
        grp = norm(group_code)
        nm = norm(name)

        if row is None:
            so = sort_order
            if so is None:
                if grp == "CHEM":
                    so = CHEM_SORT_ORDER.get(nm, 999)
                else:
                    so = 0
            self.conn.execute(
                """
                INSERT INTO material_property(
                    material_id, prop_group, state_code, name, unit, value, min_value, max_value, notes, sort_order
                )
                VALUES(?, ?, '', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(material_id),
                    grp,
                    nm,
                    norm(unit),
                    norm(value),
                    norm(min_value),
                    norm(max_value),
                    norm(notes),
                    int(so),
                ),
            )
            self.stats.properties_updated += 1
            self.touch_material(material_id)
            return

        new_unit = norm(unit) if unit is not None else norm(str(row["unit"] or ""))
        new_value = norm(value) if value is not None else norm(str(row["value"] or ""))
        new_min = norm(min_value) if min_value is not None else norm(str(row["min_value"] or ""))
        new_max = norm(max_value) if max_value is not None else norm(str(row["max_value"] or ""))
        new_notes = norm(notes) if notes is not None else norm(str(row["notes"] or ""))
        new_sort = int(sort_order) if sort_order is not None else int(row["sort_order"] or 0)

        if (
            new_unit == norm(str(row["unit"] or ""))
            and new_value == norm(str(row["value"] or ""))
            and new_min == norm(str(row["min_value"] or ""))
            and new_max == norm(str(row["max_value"] or ""))
            and new_notes == norm(str(row["notes"] or ""))
            and new_sort == int(row["sort_order"] or 0)
        ):
            return

        self.conn.execute(
            """
            UPDATE material_property
            SET unit=?, value=?, min_value=?, max_value=?, notes=?, sort_order=?
            WHERE id=?
            """,
            (new_unit, new_value, new_min, new_max, new_notes, new_sort, int(row["id"])),
        )
        self.stats.properties_updated += 1
        self.touch_material(material_id)

    def derive_values_from_limits(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, min_value, max_value
            FROM material_property
            WHERE TRIM(COALESCE(value, ''))=''
              AND (TRIM(COALESCE(min_value, ''))<>'' OR TRIM(COALESCE(max_value, ''))<>'')
            """
        )
        rows = cur.fetchall()
        for r in rows:
            min_v = norm(str(r["min_value"] or ""))
            max_v = norm(str(r["max_value"] or ""))
            if min_v and max_v:
                val = f"{min_v}-{max_v}"
            elif min_v:
                val = f">={min_v}"
            elif max_v:
                val = f"<={max_v}"
            else:
                continue
            self.conn.execute(
                "UPDATE material_property SET value=? WHERE id=?",
                (val, int(r["id"])),
            )
            self.stats.derived_values += 1


def apply_updates(p: MaterialPatcher) -> None:
    steel_phys_note = (
        "OVAKO C45 / EN 1.0503, OTHER PROPERTIES: DENSITY 7.85 G/CM3, THERMAL "
        "EXPANSION 12, SPECIFIC HEAT 460-480 J/KGK, THERMAL CONDUCTIVITY 40-45 W/MK, "
        "ELECTRICAL RESISTIVITY 0.20-0.25 UOHM*M (CONVERSION APPLIED)."
    )
    steel_vol_note = (
        "CONVERTITO DA 20-25 UOHM*CM: 1 UOHM*CM = 1E-6 OHM*CM."
    )

    for mat in ("S235JR", "S275JR", "S355JR"):
        row = p.get_material(mat)
        if row is None:
            continue
        mid = int(row["id"])
        p.set_prop(mid, "PHYS", "RESISTIVITA ELETTRICA", value="20-25", min_value="20", max_value="25", notes=steel_phys_note)
        p.set_prop(
            mid,
            "PHYS",
            "RESISTIVITA VOLUMICA",
            value="0.000020-0.000025",
            min_value="0.000020",
            max_value="0.000025",
            notes=steel_vol_note,
        )
        p.set_prop(mid, "PHYS", "COEFF. DILATAZIONE", value="12", notes=steel_phys_note)
        p.set_prop(mid, "PHYS", "CALORE SPECIFICO", value="460-480", min_value="460", max_value="480", notes=steel_phys_note)
        p.set_prop(mid, "PHYS", "CONDUCIBILITA TERMICA", value="40-45", min_value="40", max_value="45", notes=steel_phys_note)

    note_304 = (
        "ALLEIMA 3R12 (ASTM 304/304L) DATASHEET: PROOF >=230 MPA, TENSILE 540-750 MPA, "
        "A >=45%, HARDNESS <=215 HB, E 200 GPA, DENSITY 7.9, ALPHA 16.5, CP 500, "
        "LAMBDA 15, RHO_E 0.73 OHM*MM2/M (CONVERTITO 73 UOHM*CM)."
    )
    row_304 = p.get_material("AISI 304")
    if row_304 is not None:
        mid = int(row_304["id"])
        p.set_prop(mid, "CHEM", "C", unit="%", value="<=0.030", max_value="0.030", notes=note_304)
        p.set_prop(mid, "CHEM", "CR", unit="%", value="17.5-18.5", min_value="17.5", max_value="18.5", notes=note_304)
        p.set_prop(mid, "CHEM", "NI", unit="%", value="8.1-10.1", min_value="8.1", max_value="10.1", notes=note_304)
        p.set_prop(mid, "MECH", "CARICO DI ROTTURA RM", value="540-750", min_value="540", max_value="750", notes=note_304)
        p.set_prop(mid, "MECH", "SNERVAMENTO RP0.2", value=">=230", min_value="230", notes=note_304)
        p.set_prop(mid, "MECH", "ALLUNGAMENTO A", value=">=45", min_value="45", notes=note_304)
        p.set_prop(mid, "MECH", "DUREZZA BRINELL", value="<=215", max_value="215", notes=note_304)
        p.set_prop(mid, "MECH", "MODULO ELASTICO E", value="200", notes=note_304)
        p.set_prop(mid, "PHYS", "DENSITA", value="7.9", notes=note_304)
        p.set_prop(mid, "PHYS", "RESISTIVITA ELETTRICA", value="73", notes=note_304)
        p.set_prop(mid, "PHYS", "RESISTIVITA VOLUMICA", value="0.000073", notes="CONVERTITO DA 73 UOHM*CM.")
        p.set_prop(mid, "PHYS", "COEFF. DILATAZIONE", value="16.5", notes=note_304)
        p.set_prop(mid, "PHYS", "CALORE SPECIFICO", value="500", notes=note_304)
        p.set_prop(mid, "PHYS", "CONDUCIBILITA TERMICA", value="15", notes=note_304)

    note_316 = (
        "ALLEIMA 3R65 (ASTM 316/316L) DATASHEET: COMPOSIZIONE NOMINALE CR 17.3, NI 10.1, MO 2.1; "
        "PROOF >=240 MPA, TENSILE 530-680 MPA, A >=40%, HARDNESS <=217 HB, E 200 GPA, "
        "DENSITY 8.0, ALPHA 16.5, CP 500, LAMBDA 14, RHO_E 0.74 OHM*MM2/M (CONVERTITO 74 UOHM*CM)."
    )
    row_316 = p.get_material("AISI 316L")
    if row_316 is not None:
        mid = int(row_316["id"])
        p.set_prop(mid, "CHEM", "CR", unit="%", value="17.3", notes=note_316)
        p.set_prop(mid, "CHEM", "NI", unit="%", value="10.1", notes=note_316)
        p.set_prop(mid, "CHEM", "MO", unit="%", value="2.1", notes=note_316)
        p.set_prop(mid, "MECH", "CARICO DI ROTTURA RM", value="530-680", min_value="530", max_value="680", notes=note_316)
        p.set_prop(mid, "MECH", "SNERVAMENTO RP0.2", value=">=240", min_value="240", notes=note_316)
        p.set_prop(mid, "MECH", "ALLUNGAMENTO A", value=">=40", min_value="40", notes=note_316)
        p.set_prop(mid, "MECH", "DUREZZA BRINELL", value="<=217", max_value="217", notes=note_316)
        p.set_prop(mid, "MECH", "MODULO ELASTICO E", value="200", notes=note_316)
        p.set_prop(mid, "PHYS", "DENSITA", value="8.0", notes=note_316)
        p.set_prop(mid, "PHYS", "RESISTIVITA ELETTRICA", value="74", notes=note_316)
        p.set_prop(mid, "PHYS", "RESISTIVITA VOLUMICA", value="0.000074", notes="CONVERTITO DA 74 UOHM*CM.")
        p.set_prop(mid, "PHYS", "COEFF. DILATAZIONE", value="16.5", notes=note_316)
        p.set_prop(mid, "PHYS", "CALORE SPECIFICO", value="500", notes=note_316)
        p.set_prop(mid, "PHYS", "CONDUCIBILITA TERMICA", value="14", notes=note_316)

    note_2205 = (
        "ALLEIMA SAF 2205 DATASHEET: CR 21.5-23.0, NI 4.5-6.5, MO 3.0-3.5, N 0.14-0.20; "
        "PROOF >=550 MPA, TENSILE 750-950 MPA, A >=25%, HARDNESS <=290 HB; E 200 GPA, "
        "DENSITY 7.8, ALPHA 13, CP 480, LAMBDA 15, RHO_E 0.75 OHM*MM2/M (CONVERTITO 75 UOHM*CM)."
    )
    row_2205 = p.get_material("DUPLEX 2205")
    if row_2205 is not None:
        mid = int(row_2205["id"])
        p.set_prop(mid, "CHEM", "CR", unit="%", value="21.5-23.0", min_value="21.5", max_value="23.0", notes=note_2205)
        p.set_prop(mid, "CHEM", "NI", unit="%", value="4.5-6.5", min_value="4.5", max_value="6.5", notes=note_2205)
        p.set_prop(mid, "CHEM", "MO", unit="%", value="3.0-3.5", min_value="3.0", max_value="3.5", notes=note_2205)
        p.set_prop(mid, "CHEM", "N", unit="%", value="0.14-0.20", min_value="0.14", max_value="0.20", notes=note_2205)
        p.set_prop(mid, "MECH", "CARICO DI ROTTURA RM", value="750-950", min_value="750", max_value="950", notes=note_2205)
        p.set_prop(mid, "MECH", "SNERVAMENTO RP0.2", value=">=550", min_value="550", notes=note_2205)
        p.set_prop(mid, "MECH", "ALLUNGAMENTO A", value=">=25", min_value="25", notes=note_2205)
        p.set_prop(mid, "MECH", "DUREZZA BRINELL", value="<=290", max_value="290", notes=note_2205)
        p.set_prop(mid, "MECH", "MODULO ELASTICO E", value="200", notes=note_2205)
        p.set_prop(mid, "PHYS", "DENSITA", value="7.8", notes=note_2205)
        p.set_prop(mid, "PHYS", "RESISTIVITA ELETTRICA", value="75", notes=note_2205)
        p.set_prop(mid, "PHYS", "RESISTIVITA VOLUMICA", value="0.000075", notes="CONVERTITO DA 75 UOHM*CM.")
        p.set_prop(mid, "PHYS", "COEFF. DILATAZIONE", value="13", notes=note_2205)
        p.set_prop(mid, "PHYS", "CALORE SPECIFICO", value="480", notes=note_2205)
        p.set_prop(mid, "PHYS", "CONDUCIBILITA TERMICA", value="15", notes=note_2205)

    note_ti5 = (
        "ATI 6AL-4V (GRADE 5) DATASHEET: TENSILE >=895 MPA, YIELD >=825 MPA, ELONGATION >=10%, "
        "DENSITY 4.43 G/CM3."
    )
    row_ti5 = p.get_material("TI GRADE 5 (TI-6AL-4V)")
    if row_ti5 is not None:
        mid = int(row_ti5["id"])
        p.set_prop(mid, "MECH", "CARICO DI ROTTURA RM", value=">=895", min_value="895", notes=note_ti5)
        p.set_prop(mid, "MECH", "SNERVAMENTO RP0.2", value=">=825", min_value="825", notes=note_ti5)
        p.set_prop(mid, "MECH", "ALLUNGAMENTO A", value=">=10", min_value="10", notes=note_ti5)
        p.set_prop(mid, "PHYS", "DENSITA", value="4.43", notes=note_ti5)

    note_304l = (
        "ALLEIMA 3R12 (304L / 1.4307) DATASHEET: C<=0.030, CR 17.5-18.5, NI 8.1-10.1; "
        "PROOF >=230 MPA, TENSILE 540-750 MPA, A >=45%, HB <=215, E 200 GPA, "
        "DENSITY 7.9, ALPHA 16.5, CP 500, LAMBDA 15, RHO_E 0.73 OHM*MM2/M."
    )
    mid_304l = p.ensure_material(
        family="ACCIAIO INOX",
        description="AISI 304L",
        standard="EN 10088-1 / EN 10088-2 / 1.4307",
        notes="DATI DA DATASHEET ALLEIMA 3R12.",
    )
    p.set_prop(mid_304l, "CHEM", "C", unit="%", value="<=0.030", max_value="0.030", notes=note_304l)
    p.set_prop(mid_304l, "CHEM", "CR", unit="%", value="17.5-18.5", min_value="17.5", max_value="18.5", notes=note_304l)
    p.set_prop(mid_304l, "CHEM", "NI", unit="%", value="8.1-10.1", min_value="8.1", max_value="10.1", notes=note_304l)
    p.set_prop(mid_304l, "MECH", "CARICO DI ROTTURA RM", value="540-750", min_value="540", max_value="750", notes=note_304l)
    p.set_prop(mid_304l, "MECH", "SNERVAMENTO RP0.2", value=">=230", min_value="230", notes=note_304l)
    p.set_prop(mid_304l, "MECH", "ALLUNGAMENTO A", value=">=45", min_value="45", notes=note_304l)
    p.set_prop(mid_304l, "MECH", "DUREZZA BRINELL", value="<=215", max_value="215", notes=note_304l)
    p.set_prop(mid_304l, "MECH", "MODULO ELASTICO E", value="200", notes=note_304l)
    p.set_prop(mid_304l, "PHYS", "DENSITA", value="7.9", notes=note_304l)
    p.set_prop(mid_304l, "PHYS", "RESISTIVITA ELETTRICA", value="73", notes=note_304l)
    p.set_prop(mid_304l, "PHYS", "RESISTIVITA VOLUMICA", value="0.000073", notes="CONVERTITO DA 73 UOHM*CM.")
    p.set_prop(mid_304l, "PHYS", "COEFF. DILATAZIONE", value="16.5", notes=note_304l)
    p.set_prop(mid_304l, "PHYS", "CALORE SPECIFICO", value="500", notes=note_304l)
    p.set_prop(mid_304l, "PHYS", "CONDUCIBILITA TERMICA", value="15", notes=note_304l)

    note_303 = (
        "ALLEIMA SANMAC 4305 (AISI 303 / 1.4305) DATASHEET: C<=0.10, S 0.15-0.35, CR 17-19, NI 8-10; "
        "PROOF >=280 MPA, TENSILE 550-750 MPA, A >=35%, E 193 GPA, "
        "DENSITY 7.9, ALPHA 17, CP 500, LAMBDA 15, RHO_E 0.73 OHM*MM2/M."
    )
    mid_303 = p.ensure_material(
        family="ACCIAIO INOX",
        description="AISI 303",
        standard="EN 10088-3 / 1.4305 / ASTM 303",
        notes="DATI DA DATASHEET ALLEIMA SANMAC 4305.",
    )
    p.set_prop(mid_303, "CHEM", "C", unit="%", value="<=0.10", max_value="0.10", notes=note_303)
    p.set_prop(mid_303, "CHEM", "S", unit="%", value="0.15-0.35", min_value="0.15", max_value="0.35", notes=note_303)
    p.set_prop(mid_303, "CHEM", "CR", unit="%", value="17-19", min_value="17", max_value="19", notes=note_303)
    p.set_prop(mid_303, "CHEM", "NI", unit="%", value="8-10", min_value="8", max_value="10", notes=note_303)
    p.set_prop(mid_303, "MECH", "CARICO DI ROTTURA RM", value="550-750", min_value="550", max_value="750", notes=note_303)
    p.set_prop(mid_303, "MECH", "SNERVAMENTO RP0.2", value=">=280", min_value="280", notes=note_303)
    p.set_prop(mid_303, "MECH", "ALLUNGAMENTO A", value=">=35", min_value="35", notes=note_303)
    p.set_prop(mid_303, "MECH", "MODULO ELASTICO E", value="193", notes=note_303)
    p.set_prop(mid_303, "PHYS", "DENSITA", value="7.9", notes=note_303)
    p.set_prop(mid_303, "PHYS", "RESISTIVITA ELETTRICA", value="73", notes=note_303)
    p.set_prop(mid_303, "PHYS", "RESISTIVITA VOLUMICA", value="0.000073", notes="CONVERTITO DA 73 UOHM*CM.")
    p.set_prop(mid_303, "PHYS", "COEFF. DILATAZIONE", value="17", notes=note_303)
    p.set_prop(mid_303, "PHYS", "CALORE SPECIFICO", value="500", notes=note_303)
    p.set_prop(mid_303, "PHYS", "CONDUCIBILITA TERMICA", value="15", notes=note_303)


def print_summary(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM material")
    n_materials = int(cur.fetchone()["n"])

    cur.execute(
        """
        SELECT
            SUM(CASE WHEN TRIM(COALESCE(value,''))='' THEN 1 ELSE 0 END) AS miss_value,
            SUM(CASE WHEN TRIM(COALESCE(min_value,''))='' THEN 1 ELSE 0 END) AS miss_min,
            SUM(CASE WHEN TRIM(COALESCE(max_value,''))='' THEN 1 ELSE 0 END) AS miss_max,
            COUNT(*) AS total
        FROM material_property
        """
    )
    row = cur.fetchone()
    print(f"MATERIALI_TOTALI={n_materials}")
    print(f"MISSING_VALUE={int(row['miss_value'])}/{int(row['total'])}")
    print(f"MISSING_MIN={int(row['miss_min'])}/{int(row['total'])}")
    print(f"MISSING_MAX={int(row['miss_max'])}/{int(row['total'])}")

    cur.execute(
        """
        SELECT m.family, m.description,
               SUM(CASE WHEN TRIM(COALESCE(mp.value,''))='' THEN 1 ELSE 0 END) AS miss,
               COUNT(*) AS total
        FROM material m
        JOIN material_property mp ON mp.material_id=m.id
        GROUP BY m.id
        ORDER BY miss DESC, m.family, m.description
        LIMIT 10
        """
    )
    for r in cur.fetchall():
        print(f"TOP_MISS\t{r['family']}\t{r['description']}\t{int(r['miss'])}/{int(r['total'])}")


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database non trovato: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    patcher = MaterialPatcher(conn)
    backup_path = patcher.backup()
    print(f"BACKUP={backup_path}")

    patcher.derive_values_from_limits()
    apply_updates(patcher)
    conn.commit()

    print(f"DERIVED_VALUES={patcher.stats.derived_values}")
    print(f"PROPERTIES_UPDATED={patcher.stats.properties_updated}")
    print(f"NEW_MATERIALS={patcher.stats.new_materials}")
    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
