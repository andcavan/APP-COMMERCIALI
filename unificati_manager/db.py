from __future__ import annotations

import math
import os
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .utils import now_str, normalize_upper
from .codifica import normalize_mmm, normalize_gggg_normati, normalize_cccc, normalize_ssss
from .config import (
    DATE_FMT,
    SEED_COMMERCIALI_DEFAULTS,
    SEED_NORMATI_DEFAULTS,
    SEED_SUPPLIERS_DEFAULTS,
)

DEFAULT_NORMATI_CATEGORIES = [
    ("Viti", "001"),
    ("Rondelle / Rosette", "002"),
    ("Dadi", "003"),
    ("Ghiere", "004"),
    ("Spine", "005"),
    ("Prigionieri", "006"),
    ("Anelli elastici", "007"),
    ("Cuscinetti", "008"),
    ("Linguette", "009"),
    ("Chiavette", "010"),
]

DEFAULT_NORMATI_STANDARDS = [
    ("Vite TE Metrica", "ISO 4017", "001"),
    ("Vite TE Parzialmente filettata", "ISO 4014", "001"),
    ("Vite TE Parzialmente filettata", "UNI EN ISO 4014", "001"),
    ("Vite TE Parzialmente filettata", "DIN 931", "001"),
    ("Vite TE Totalmente filettata", "UNI EN ISO 4017", "001"),
    ("Vite TE Totalmente filettata", "DIN 933", "001"),
    ("Caratteristiche meccaniche viteria acciaio", "ISO 898-1", "001"),
    ("Rivestimenti elettrolitici viteria", "ISO 4042", "001"),
    ("Caratteristiche meccaniche viteria inox", "ISO 3506-1", "001"),
    ("Vite TCEI", "ISO 4762", "001"),
    ("Dado esagonale", "ISO 4032", "003"),
    ("Rondella piana", "ISO 7089", "002"),
    ("Anello elastico per alberi", "DIN 471", "007"),
    ("Anello elastico per fori", "DIN 472", "007"),
    ("Cuscinetto radiale a sfere", "DIN 625", "008"),
    ("Linguetta parallela", "DIN 6885", "009"),
    ("Chiavetta a mezzaluna", "DIN 6888", "010"),
]

DEFAULT_NORMATI_SUBCATEGORIES = [
    ("TE", "0001", "001", "ISO 4017"),
    ("TE PARZ FILETT ZINCATA", "0002", "001", "ISO 4014"),
    ("TE PARZ FILETT INOX A2", "0003", "001", "ISO 4014"),
    ("TE TOT FILETT ZINCATA", "0004", "001", "ISO 4017"),
    ("TE TOT FILETT INOX A2", "0005", "001", "ISO 4017"),
    ("TCEI", "0006", "001", "ISO 4762"),
    ("ESAGONALE", "0001", "003", "ISO 4032"),
    ("PIANE", "0001", "002", "ISO 7089"),
    ("RADIALE SFERE", "0001", "008", "DIN 625"),
    ("PARALLELA", "0001", "009", "DIN 6885"),
    ("MEZZALUNA", "0001", "010", "DIN 6888"),
]

DEFAULT_COMM_CATEGORIES = [
    ("COMMERCIALI VARI", "1000"),
    ("LAVORAZIONI", "2000"),
    ("CONSUMABILI", "3000"),
]

DEFAULT_COMM_SUBCATEGORIES = [
    ("COLLANTI", "0001", "3000"),
    ("FRESATURE", "0001", "2000"),
    ("PIASTRE", "0001", "1000"),
]

DEFAULT_SUPPLIERS = [
    ("F1", "FORNITORE 1"),
    ("F2", "FORNITORE 2"),
]

# Template proprieta standard materiale (senza legame a stati).
# Le righe vengono create vuote su ogni materiale per facilitare la compilazione.
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

# Alias legacy -> nome canonico template.
DEFAULT_MATERIAL_PROPERTY_ALIASES = [
    ("MECH", "RM", "CARICO DI ROTTURA RM"),
    ("MECH", "RES_TRAZIONE", "CARICO DI ROTTURA RM"),
]


class Database:
    def __init__(
        self,
        path: str,
        *,
        access_mode: str = "rw",
        session_role: str = "editor",
        writer_holder: str = "",
        writer_lock_token: Optional[str] = None,
        writer_lock_timeout_seconds: int = 120,
    ) -> None:
        self.path = os.path.abspath(path)
        self.access_mode = "ro" if (access_mode or "").strip().lower() == "ro" else "rw"
        self.is_read_only = self.access_mode == "ro"
        self.session_role = "reader" if self.is_read_only else ("reader" if (session_role or "").strip().lower() == "reader" else "editor")
        self.writer_holder = (writer_holder or "").strip()
        self.writer_lock_token = writer_lock_token
        self.writer_lock_timeout_seconds = max(15, int(writer_lock_timeout_seconds or 120))

        if self.is_read_only:
            uri = f"{Path(self.path).as_uri()}?mode=ro"
            self.conn = sqlite3.connect(uri, timeout=30, uri=True)
        else:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA busy_timeout=30000;")
        if not self.is_read_only:
            self._init_schema()
            self._seed_defaults()
            self._backfill_semi_dimensions_from_legacy_field()
            self._normalize_semi_dimension_preferred_flags()
            self._ensure_manual_v1000_entry()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _auto_code(prefix: str) -> str:
        return f"{normalize_upper(prefix)}_{uuid.uuid4().hex[:10].upper()}"

    def backup_to_path(self, target_path: str) -> None:
        target_dir = os.path.dirname(os.path.abspath(target_path))
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)
        dst = sqlite3.connect(target_path)
        try:
            self.conn.backup(dst)
            dst.commit()
        finally:
            dst.close()

    @staticmethod
    def _parse_lock_ts(text: str) -> Optional[datetime]:
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, DATE_FMT)
        except Exception:
            return None

    @staticmethod
    def _open_lock_connection(path: str) -> sqlite3.Connection:
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        conn = sqlite3.connect(abs_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    @staticmethod
    def _ensure_writer_lock_table(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_writer_lock (
                lock_key TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                token TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL
            );
            """
        )
        conn.commit()

    @staticmethod
    def try_acquire_writer_lock(path: str, holder: str, timeout_seconds: int = 120) -> Dict[str, Any]:
        timeout = max(15, int(timeout_seconds or 120))
        who = (holder or "").strip() or os.environ.get("USERNAME", "") or "EDITOR"
        token = uuid.uuid4().hex
        now_dt = datetime.now()
        now_val = now_dt.strftime(DATE_FMT)
        conn = Database._open_lock_connection(path)
        try:
            Database._ensure_writer_lock_table(conn)
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.cursor()
            cur.execute("SELECT holder, token, heartbeat_at FROM app_writer_lock WHERE lock_key='MAIN'")
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO app_writer_lock(lock_key, holder, token, acquired_at, heartbeat_at)
                    VALUES('MAIN', ?, ?, ?, ?)
                    """,
                    (who, token, now_val, now_val),
                )
                conn.commit()
                return {"acquired": True, "holder": who, "token": token, "heartbeat_at": now_val}

            lock_holder = str(row["holder"] or "")
            lock_hb = str(row["heartbeat_at"] or "")
            hb_dt = Database._parse_lock_ts(lock_hb)
            age = (now_dt - hb_dt).total_seconds() if hb_dt is not None else float("inf")
            is_expired = age > float(timeout)
            if is_expired:
                cur.execute(
                    """
                    UPDATE app_writer_lock
                    SET holder=?, token=?, acquired_at=?, heartbeat_at=?
                    WHERE lock_key='MAIN'
                    """,
                    (who, token, now_val, now_val),
                )
                conn.commit()
                return {"acquired": True, "holder": who, "token": token, "heartbeat_at": now_val}

            conn.rollback()
            return {"acquired": False, "holder": lock_holder, "token": None, "heartbeat_at": lock_hb}
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    @staticmethod
    def release_writer_lock_static(path: str, token: str) -> bool:
        tok = (token or "").strip()
        if not tok:
            return False
        conn = Database._open_lock_connection(path)
        try:
            Database._ensure_writer_lock_table(conn)
            cur = conn.cursor()
            cur.execute("DELETE FROM app_writer_lock WHERE lock_key='MAIN' AND token=?", (tok,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def heartbeat_writer_lock(self) -> bool:
        if self.is_read_only:
            return False
        tok = (self.writer_lock_token or "").strip()
        if not tok:
            return False
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE app_writer_lock
            SET heartbeat_at=?
            WHERE lock_key='MAIN' AND token=?
            """,
            (now_str(), tok),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def release_writer_lock(self) -> bool:
        if self.is_read_only:
            return False
        tok = (self.writer_lock_token or "").strip()
        if not tok:
            return False
        cur = self.conn.cursor()
        cur.execute("DELETE FROM app_writer_lock WHERE lock_key='MAIN' AND token=?", (tok,))
        self.conn.commit()
        return cur.rowcount > 0

    def _ensure_column(self, table: str, col: str, decl: str) -> None:
        """Aggiunge una colonna se manca (safe anche se la tabella non esiste)."""
        try:
            cur = self.conn.execute(f"PRAGMA table_info({table})")
            cols = {r["name"] for r in cur.fetchall()}
        except sqlite3.OperationalError:
            return
        if col not in cols:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
            self.conn.commit()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()

        # Normati
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS category (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS standard (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                description TEXT NOT NULL,
                UNIQUE(category_id, code),
                FOREIGN KEY(category_id) REFERENCES category(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subcategory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                description TEXT NOT NULL,
                standard_id INTEGER,
                desc_template TEXT NOT NULL DEFAULT '',
                UNIQUE(category_id, code),
                FOREIGN KEY(category_id) REFERENCES category(id),
                FOREIGN KEY(standard_id) REFERENCES standard(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                category_id INTEGER NOT NULL,
                subcategory_id INTEGER NOT NULL,
                standard_id INTEGER,
                seq INTEGER NOT NULL,
                description TEXT NOT NULL,
                notes TEXT,
                preferred INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES category(id),
                FOREIGN KEY(subcategory_id) REFERENCES subcategory(id),
                FOREIGN KEY(standard_id) REFERENCES standard(id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_cat_sub ON item(category_id, subcategory_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_code ON item(code)")

        # Commerciali non normati
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS comm_category (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS comm_subcategory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                description TEXT NOT NULL,
                UNIQUE(category_id, code),
                FOREIGN KEY(category_id) REFERENCES comm_category(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS supplier (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS comm_item (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                category_id INTEGER NOT NULL,
                subcategory_id INTEGER NOT NULL,
                supplier_id INTEGER,
                seq INTEGER NOT NULL,
                description TEXT NOT NULL,
                supplier_item_code TEXT,
                supplier_item_desc TEXT,
                file_folder TEXT,
                notes TEXT,
                preferred INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES comm_category(id),
                FOREIGN KEY(subcategory_id) REFERENCES comm_subcategory(id),
                FOREIGN KEY(supplier_id) REFERENCES supplier(id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_comm_item_cat_sub ON comm_item(category_id, subcategory_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_comm_item_code ON comm_item(code)")


        # Materiali / Trattamenti / Semilavorati
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS material (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                family TEXT NOT NULL,
                description TEXT NOT NULL,
                standard TEXT,
                notes TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS material_family (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT NOT NULL UNIQUE
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS material_subfamily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                family_id INTEGER NOT NULL,
                description TEXT NOT NULL,
                UNIQUE(family_id, description),
                FOREIGN KEY(family_id) REFERENCES material_family(id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS material_property (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                material_id INTEGER NOT NULL,
                prop_group TEXT NOT NULL,         -- CHEM / PHYS / MECH
                state_code TEXT NOT NULL DEFAULT '',  -- '' = generale, altrimenti codice stato (4 lettere)
                name TEXT NOT NULL,
                unit TEXT,
                value TEXT,
                min_value TEXT,
                max_value TEXT,
                notes TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0,
                UNIQUE(material_id, prop_group, name, state_code),
                FOREIGN KEY(material_id) REFERENCES material(id)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_material_code ON material(code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_material_prop_mid ON material_property(material_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_material_prop_grp ON material_property(material_id, prop_group)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_material_subfamily_family ON material_subfamily(family_id)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS heat_treatment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                characteristics TEXT,
                standard TEXT,
                notes TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS surface_treatment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                characteristics TEXT,
                standard TEXT,
                notes TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS semi_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS semi_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS semi_item (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type_id INTEGER NOT NULL,
                state_id INTEGER NOT NULL,
                material_id INTEGER,
                description TEXT NOT NULL,
                dimensions TEXT,
                standard TEXT,
                notes TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(type_id) REFERENCES semi_type(id),
                FOREIGN KEY(state_id) REFERENCES semi_state(id),
                FOREIGN KEY(material_id) REFERENCES material(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS semi_item_dimension (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                semi_item_id INTEGER NOT NULL,
                dimension TEXT NOT NULL,
                weight_per_m TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0,
                preferred INTEGER NOT NULL DEFAULT 0,
                UNIQUE(semi_item_id, dimension),
                FOREIGN KEY(semi_item_id) REFERENCES semi_item(id) ON DELETE CASCADE
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_semi_item_ts ON semi_item(type_id, state_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_semi_item_mat ON semi_item(material_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_semi_dim_item ON semi_item_dimension(semi_item_id)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version TEXT NOT NULL UNIQUE,
                release_date TEXT NOT NULL,
                updates TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_manual_version_release_date ON manual_version(release_date)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_writer_lock (
                lock_key TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                token TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL
            );
            """
        )

        self.conn.commit()

        # migrations for older DBs
        self._ensure_column("subcategory", "desc_template", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("comm_item", "supplier_item_code", "TEXT")
        self._ensure_column("comm_item", "supplier_item_desc", "TEXT")
        self._ensure_column("item", "preferred", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("comm_item", "preferred", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("material_property", "state_code", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("semi_item", "material_id", "INTEGER")
        self._ensure_column("semi_item_dimension", "preferred", "INTEGER NOT NULL DEFAULT 0")
        try:
            self.conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_semi_dim_one_pref
                ON semi_item_dimension(semi_item_id)
                WHERE preferred=1
                """
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

    def _seed_defaults(self) -> None:
        cur = self.conn.cursor()

        if SEED_NORMATI_DEFAULTS:
            # Normati
            for desc, mmm in DEFAULT_NORMATI_CATEGORIES:
                cur.execute("INSERT OR IGNORE INTO category(code, description) VALUES(?, ?)", (mmm, normalize_upper(desc)))
            self.conn.commit()

            cur.execute("SELECT id, code FROM category")
            cat_map = {r["code"]: int(r["id"]) for r in cur.fetchall()}

            for desc, code, mmm in DEFAULT_NORMATI_STANDARDS:
                cid = cat_map.get(mmm)
                if cid:
                    cur.execute(
                        "INSERT OR IGNORE INTO standard(category_id, code, description) VALUES(?, ?, ?)",
                        (cid, normalize_upper(code), normalize_upper(desc)),
                    )
            self.conn.commit()

            cur.execute("SELECT id, category_id, code FROM standard")
            std_map = {(int(r["category_id"]), r["code"]): int(r["id"]) for r in cur.fetchall()}

            for desc, gggg, mmm, std_code in DEFAULT_NORMATI_SUBCATEGORIES:
                cid = cat_map.get(mmm)
                if not cid:
                    continue
                sid = std_map.get((cid, normalize_upper(std_code))) if std_code else None
                cur.execute(
                    "INSERT OR IGNORE INTO subcategory(category_id, code, description, standard_id, desc_template) VALUES(?, ?, ?, ?, ?)",
                    (cid, normalize_gggg_normati(gggg), normalize_upper(desc), sid, ""),
                )
            self.conn.commit()

        if SEED_COMMERCIALI_DEFAULTS:
            # Commerciali
            for desc, code in DEFAULT_COMM_CATEGORIES:
                cur.execute("INSERT OR IGNORE INTO comm_category(code, description) VALUES(?, ?)", (normalize_cccc(code), normalize_upper(desc)))
            self.conn.commit()

            cur.execute("SELECT id, code FROM comm_category")
            comm_cat_map = {r["code"]: int(r["id"]) for r in cur.fetchall()}

            for desc, code, cccc in DEFAULT_COMM_SUBCATEGORIES:
                cid = comm_cat_map.get(normalize_cccc(cccc))
                if cid:
                    cur.execute(
                        "INSERT OR IGNORE INTO comm_subcategory(category_id, code, description) VALUES(?, ?, ?)",
                        (cid, normalize_ssss(code), normalize_upper(desc)),
                    )
            self.conn.commit()

        if SEED_SUPPLIERS_DEFAULTS:
            for code, desc in DEFAULT_SUPPLIERS:
                cur.execute("INSERT OR IGNORE INTO supplier(code, description) VALUES(?, ?)", (normalize_upper(code), normalize_upper(desc)))
            self.conn.commit()


        # Semilavorati: tipi e stati (esempi iniziali, modificabili)
        semi_types = [
            ("PIAT", "PIATTI"),
            ("TOND", "TONDI"),
            ("ESAG", "ESAGONI"),
            ("TUBO", "TUBI"),
            ("TUBL", "TUBOLARI"),
            ("LAMI", "LAMIERE"),
            ("TRAV", "TRAVI"),
            ("TVIP", "TRAVE IPE"),
            ("TVHE", "TRAVE HE"),
            ("TVHA", "TRAVE HEA"),
            ("TVHB", "TRAVE HEB"),
            ("TVHM", "TRAVE HEM"),
            ("TVUN", "TRAVE UPN"),
            ("TVUE", "TRAVE UPE"),
            ("TVUS", "TRAVE UPS"),
            ("PROF", "PROFILATI"),
            ("PRFL", "PROFILO L"),
            ("PRFU", "PROFILO U"),
            ("PRFT", "PROFILO T"),
            ("PLTR", "PROFILO L TRAFILATO"),
            ("PUTR", "PROFILO U TRAFILATO"),
            ("PTTR", "PROFILO T TRAFILATO"),
        ]
        for code, desc in semi_types:
            cur.execute(
                "INSERT OR IGNORE INTO semi_type(code, description) VALUES(?, ?)",
                (normalize_upper(code), normalize_upper(desc)),
            )

        semi_states = [
            ("LAMI", "LAMINATO"),
            ("TRAF", "TRAFILATO"),
            ("ESTR", "ESTRUSO"),
            ("RETT", "RETTIFICATO"),
            ("BONI", "BONIFICATO"),
            ("RIC0", "RICOTTO"),
        ]
        for code, desc in semi_states:
            cur.execute(
                "INSERT OR IGNORE INTO semi_state(code, description) VALUES(?, ?)",
                (normalize_upper(code), normalize_upper(desc)),
            )
        self.conn.commit()
        self._seed_material_taxonomy_from_materials()
        self.ensure_default_material_properties_all()

    def _seed_material_taxonomy_from_materials(self) -> None:
        """Populate family/subfamily master tables from existing materials."""
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT family FROM material WHERE TRIM(COALESCE(family,'')) <> ''")
        families = [normalize_upper(str(r["family"])) for r in cur.fetchall()]
        for fam in families:
            cur.execute("INSERT OR IGNORE INTO material_family(description) VALUES(?)", (fam,))
        self.conn.commit()

        cur.execute("SELECT id, description FROM material_family")
        fam_map = {normalize_upper(str(r["description"])): int(r["id"]) for r in cur.fetchall()}

        cur.execute(
            """
            SELECT DISTINCT family, description
            FROM material
            WHERE TRIM(COALESCE(family,'')) <> '' AND TRIM(COALESCE(description,'')) <> ''
            """
        )
        for r in cur.fetchall():
            fam = normalize_upper(str(r["family"]))
            sub = normalize_upper(str(r["description"]))
            fid = fam_map.get(fam)
            if fid:
                cur.execute(
                    "INSERT OR IGNORE INTO material_subfamily(family_id, description) VALUES(?, ?)",
                    (fid, sub),
                )
        self.conn.commit()

    def _backfill_semi_dimensions_from_legacy_field(self) -> int:
        """
        Migrazione soft:
        se un semilavorato ha il vecchio campo `dimensions` valorizzato ma non ha
        ancora righe in `semi_item_dimension`, crea una prima riga lista dimensionale.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, dimensions
            FROM semi_item
            WHERE TRIM(COALESCE(dimensions, '')) <> ''
            """
        )
        rows = cur.fetchall()
        touched = 0
        for r in rows:
            semi_id = int(r["id"])
            dim = normalize_upper(str(r["dimensions"] or ""))
            if not dim:
                continue
            cur.execute("SELECT COUNT(*) AS n FROM semi_item_dimension WHERE semi_item_id=?", (semi_id,))
            if int(cur.fetchone()["n"]) > 0:
                continue
            cur.execute(
                """
                INSERT INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order, preferred)
                VALUES(?, ?, '', 10, 1)
                """,
                (semi_id, dim),
            )
            touched += 1
        if touched:
            self.conn.commit()
        return touched

    def _normalize_semi_dimension_preferred_flags(self) -> int:
        """Mantiene al massimo una dimensione preferita per semilavorato."""
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT semi_item_id
            FROM semi_item_dimension
            WHERE COALESCE(preferred, 0)=1
            GROUP BY semi_item_id
            HAVING COUNT(*) > 1
            """
        )
        rows = cur.fetchall()
        touched = 0
        for r in rows:
            semi_item_id = int(r["semi_item_id"])
            cur.execute(
                """
                SELECT id
                FROM semi_item_dimension
                WHERE semi_item_id=? AND COALESCE(preferred, 0)=1
                ORDER BY sort_order, id
                """,
                (semi_item_id,),
            )
            pref_rows = cur.fetchall()
            if len(pref_rows) <= 1:
                continue
            keep_id = int(pref_rows[0]["id"])
            cur.execute(
                """
                UPDATE semi_item_dimension
                SET preferred=0
                WHERE semi_item_id=? AND id<>? AND COALESCE(preferred, 0)=1
                """,
                (semi_item_id, keep_id),
            )
            touched += cur.rowcount
        if touched:
            self.conn.commit()
        return touched

    def _ensure_manual_v1000_entry(self) -> None:
        """Registra la baseline corrente del manuale se non ancora presente."""
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO manual_version(version, release_date, updates, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                "v10.00",
                "2026-02-14",
                "BASELINE V10.00: TAB SEMILAVORATI CON PREFERITO LIVELLO DIMENSIONALE E COLONNA PREF IN LISTA.",
                now_str(),
                now_str(),
            ),
        )
        if cur.rowcount > 0:
            self.conn.commit()

    def _ensure_default_material_properties_with_cursor(self, cur: sqlite3.Cursor, material_id: int) -> int:
        """Insert template property rows and normalize common legacy aliases."""
        mid = int(material_id)
        touched = 0

        canonical_meta = {
            (normalize_upper(group_code), normalize_upper(name)): (normalize_upper(unit), int(sort_order))
            for group_code, name, unit, _val, _min, _max, sort_order in DEFAULT_MATERIAL_PROPERTY_TEMPLATE
        }

        for group_code, alias_name, canonical_name in DEFAULT_MATERIAL_PROPERTY_ALIASES:
            g = normalize_upper(group_code)
            alias = normalize_upper(alias_name)
            canonical = normalize_upper(canonical_name)

            cur.execute(
                """
                SELECT id, unit, value, min_value, max_value, notes, sort_order
                FROM material_property
                WHERE material_id=? AND prop_group=? AND state_code='' AND name=?
                """,
                (mid, g, canonical),
            )
            canonical_row = cur.fetchone()

            cur.execute(
                """
                SELECT id, unit, value, min_value, max_value, notes, sort_order
                FROM material_property
                WHERE material_id=? AND prop_group=? AND state_code='' AND name=?
                """,
                (mid, g, alias),
            )
            alias_row = cur.fetchone()
            if alias_row is None:
                continue

            unit_default, sort_default = canonical_meta.get((g, canonical), ("", 0))

            if canonical_row is None:
                cur.execute(
                    """
                    UPDATE material_property
                    SET name=?, unit=CASE WHEN TRIM(COALESCE(unit,''))='' THEN ? ELSE unit END, sort_order=?
                    WHERE id=?
                    """,
                    (canonical, unit_default, int(sort_default), int(alias_row["id"])),
                )
                if cur.rowcount > 0:
                    touched += 1
                continue

            def _pick(canonical_value: str, alias_value: str, fallback: str = "") -> str:
                cv = normalize_upper(canonical_value or "")
                av = normalize_upper(alias_value or "")
                if cv.strip():
                    return cv
                if av.strip():
                    return av
                return normalize_upper(fallback or "")

            merged_unit = _pick(str(canonical_row["unit"] or ""), str(alias_row["unit"] or ""), unit_default)
            merged_value = _pick(str(canonical_row["value"] or ""), str(alias_row["value"] or ""))
            merged_min = _pick(str(canonical_row["min_value"] or ""), str(alias_row["min_value"] or ""))
            merged_max = _pick(str(canonical_row["max_value"] or ""), str(alias_row["max_value"] or ""))
            merged_notes = _pick(str(canonical_row["notes"] or ""), str(alias_row["notes"] or ""))

            cur.execute(
                """
                UPDATE material_property
                SET unit=?, value=?, min_value=?, max_value=?, notes=?, sort_order=?
                WHERE id=?
                """,
                (
                    merged_unit,
                    merged_value,
                    merged_min,
                    merged_max,
                    merged_notes,
                    int(sort_default),
                    int(canonical_row["id"]),
                ),
            )
            if cur.rowcount > 0:
                touched += 1

            cur.execute("DELETE FROM material_property WHERE id=?", (int(alias_row["id"]),))
            if cur.rowcount > 0:
                touched += 1

        for group_code, name, unit, value, min_value, max_value, sort_order in DEFAULT_MATERIAL_PROPERTY_TEMPLATE:
            cur.execute(
                """
                INSERT OR IGNORE INTO material_property(
                    material_id, prop_group, state_code, name, unit, value, min_value, max_value, notes, sort_order
                )
                VALUES(?, ?, '', ?, ?, ?, ?, ?, '', ?)
                """,
                (
                    mid,
                    normalize_upper(group_code),
                    normalize_upper(name),
                    normalize_upper(unit),
                    normalize_upper(value),
                    normalize_upper(min_value),
                    normalize_upper(max_value),
                    int(sort_order),
                ),
            )
            if cur.rowcount > 0:
                touched += 1

        return touched

    def ensure_default_material_properties(self, material_id: int) -> int:
        cur = self.conn.cursor()
        touched = self._ensure_default_material_properties_with_cursor(cur, material_id)
        self.conn.commit()
        return touched

    def ensure_default_material_properties_all(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM material")
        rows = cur.fetchall()
        touched = 0
        for r in rows:
            touched += self._ensure_default_material_properties_with_cursor(cur, int(r["id"]))
        self.conn.commit()
        return touched

    # -------- Normati fetch --------
    def fetch_categories(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM category ORDER BY code")
        return cur.fetchall()

    def fetch_standards(self, category_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM standard WHERE category_id=? ORDER BY code", (int(category_id),))
        return cur.fetchall()

    def fetch_subcategories(self, category_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT sc.id, sc.code, sc.description, sc.standard_id, sc.desc_template,
                   st.code AS standard_code
            FROM subcategory sc
            LEFT JOIN standard st ON st.id=sc.standard_id
            WHERE sc.category_id=?
            ORDER BY sc.code
            """,
            (int(category_id),),
        )
        return cur.fetchall()

    def get_next_seq(self, category_id: int, subcategory_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM item WHERE category_id=? AND subcategory_id=?",
            (int(category_id), int(subcategory_id)),
        )
        return int(cur.fetchone()["next_seq"])

    @staticmethod
    def _parse_search_tokens(q: str) -> List[Tuple[str, bool]]:
        """
        Tokenizer query:
        - testo tra doppi apici => token quoted (ricerca "esatta")
        - resto => token standard
        """
        out: List[Tuple[str, bool]] = []
        for m in re.finditer(r'"([^"]+)"|(\S+)', q or ""):
            raw = m.group(1) if m.group(1) is not None else m.group(2)
            tok = normalize_upper((raw or "").strip())
            if tok:
                out.append((tok, bool(m.group(1) is not None)))
        return out

    @staticmethod
    def _escape_like(v: str) -> str:
        return (v or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    @staticmethod
    def _is_dimension_like_token(tok: str) -> bool:
        t = normalize_upper(tok or "")
        return bool(
            re.fullmatch(r"(?:M|D)\d+(?:[.,]\d+)?X\d+(?:[.,]\d+)?", t)
            or re.fullmatch(r"(?:KM|GUK)\d+", t)
        )

    @staticmethod
    def _normalized_search_expr(field_sql: str) -> str:
        """
        Normalizza separatori in spazio per permettere match a parola intera.
        """
        expr = f"UPPER(COALESCE({field_sql},''))"
        for ch in ["/", "-", ",", ";", ".", "(", ")", "[", "]", "{", "}", "\"", ":", "_"]:
            expr = f"REPLACE({expr}, '{ch}', ' ')"
        return f"(' ' || {expr} || ' ')"

    def _append_token_where(
        self,
        fields_sql: List[str],
        token: str,
        quoted: bool,
        where: List[str],
        params: List[Any],
    ) -> None:
        tok = normalize_upper(token or "")
        if not tok:
            return

        has_space = " " in tok
        # Regola 3: token quoted (senza spazi) => match esatto parola.
        # Regola 1: token dimensionale => match esatto parola.
        use_exact_word = (quoted and not has_space) or self._is_dimension_like_token(tok)
        esc = self._escape_like(tok)

        parts: List[str] = []
        if use_exact_word:
            for f in fields_sql:
                parts.append(f"{self._normalized_search_expr(f)} LIKE ? ESCAPE '\\'")
                params.append(f"% {esc} %")
        else:
            for f in fields_sql:
                parts.append(f"UPPER(COALESCE({f},'')) LIKE ? ESCAPE '\\'")
                params.append(f"%{esc}%")
        where.append("(" + " OR ".join(parts) + ")")

    def search_items(
        self,
        q: str = "",
        category_id: Optional[int] = None,
        subcategory_id: Optional[int] = None,
        only_preferred: bool = False,
    ):
        q = (q or "").strip()
        cur = self.conn.cursor()
        params: List[Any] = []
        where: List[str] = []
        sql = """
            SELECT i.id, i.code, i.description, i.updated_at,
                   c.code AS cat_code, sc.code AS sub_code,
                   COALESCE(i.preferred, 0) AS preferred
            FROM item i
            JOIN category c ON c.id=i.category_id
            JOIN subcategory sc ON sc.id=i.subcategory_id
        """
        tokens = self._parse_search_tokens(q)
        for tok, quoted in tokens:
            # Regola 2: token in AND (ogni token aggiunge una clausola).
            self._append_token_where(
                fields_sql=["i.code", "i.description", "c.code", "sc.code"],
                token=tok,
                quoted=quoted,
                where=where,
                params=params,
            )
        if category_id is not None:
            where.append("i.category_id=?")
            params.append(int(category_id))
        if subcategory_id is not None:
            where.append("i.subcategory_id=?")
            params.append(int(subcategory_id))
        if only_preferred:
            where.append("COALESCE(i.preferred, 0)=1")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY COALESCE(i.preferred, 0) DESC, i.updated_at DESC"
        cur.execute(sql, tuple(params))
        return cur.fetchall()

    def read_item(self, item_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT i.*, c.code AS cat_code, c.description AS cat_desc,
                   sc.code AS sub_code, sc.description AS sub_desc,
                   sc.desc_template AS sub_template,
                   st.code AS std_code
            FROM item i
            JOIN category c ON c.id=i.category_id
            JOIN subcategory sc ON sc.id=i.subcategory_id
            LEFT JOIN standard st ON st.id=i.standard_id
            WHERE i.id=?
            """,
            (int(item_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Articolo non trovato")
        return row

    # Normati CRUD (categorie/norme/sotto/articoli)
    def create_category(self, code: str, description: str) -> None:
        code_n = normalize_mmm(code)
        if not re.fullmatch(r"[0-9]{3}", code_n):
            raise ValueError("CODICE categoria normati non valido: servono 3 numeri.")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO category(code, description) VALUES(?, ?)", (code_n, normalize_upper(description)))
        self.conn.commit()

    def update_category(self, category_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE category SET description=? WHERE id=?", (normalize_upper(description), int(category_id)))
        self.conn.commit()

    def delete_category(self, category_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM category WHERE id=?", (int(category_id),))
        self.conn.commit()

    def create_standard(self, category_id: int, code: str, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO standard(category_id, code, description) VALUES(?, ?, ?)",
            (int(category_id), normalize_upper(code), normalize_upper(description)),
        )
        self.conn.commit()

    def update_standard(self, standard_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE standard SET description=? WHERE id=?", (normalize_upper(description), int(standard_id)))
        self.conn.commit()

    def delete_standard(self, standard_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM standard WHERE id=?", (int(standard_id),))
        self.conn.commit()

    def create_subcategory(self, category_id: int, code: str, description: str, standard_id: Optional[int], desc_template: str) -> None:
        code_n = normalize_gggg_normati(code)
        if not re.fullmatch(r"[0-9]{4}", code_n):
            raise ValueError("CODICE sotto-categoria normati non valido: servono 4 numeri.")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO subcategory(category_id, code, description, standard_id, desc_template) VALUES(?, ?, ?, ?, ?)",
            (int(category_id), code_n, normalize_upper(description), int(standard_id) if standard_id else None, normalize_upper(desc_template)),
        )
        self.conn.commit()

    def update_subcategory(self, subcategory_id: int, description: str, standard_id: Optional[int], desc_template: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE subcategory SET description=?, standard_id=?, desc_template=? WHERE id=?",
            (normalize_upper(description), int(standard_id) if standard_id else None, normalize_upper(desc_template), int(subcategory_id)),
        )
        self.conn.commit()

    def delete_subcategory(self, subcategory_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM subcategory WHERE id=?", (int(subcategory_id),))
        self.conn.commit()

    def create_item(self, payload: Dict[str, Any]) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO item(code, category_id, subcategory_id, standard_id, seq, description, notes, preferred, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["code"],
                int(payload["category_id"]),
                int(payload["subcategory_id"]),
                int(payload["standard_id"]) if payload.get("standard_id") else None,
                int(payload["seq"]),
                payload["description"],
                payload.get("notes") or "",
                int(payload.get("preferred", 0)),
                int(payload.get("is_active", 1)),
                now_str(),
                now_str(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_item(self, item_id: int, payload: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE item
            SET category_id=?, subcategory_id=?, standard_id=?, description=?, notes=?, preferred=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                int(payload["category_id"]),
                int(payload["subcategory_id"]),
                int(payload["standard_id"]) if payload.get("standard_id") else None,
                payload["description"],
                payload.get("notes") or "",
                int(payload.get("preferred", 0)),
                int(payload.get("is_active", 1)),
                now_str(),
                int(item_id),
            ),
        )
        self.conn.commit()

    def delete_item(self, item_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM item WHERE id=?", (int(item_id),))
        self.conn.commit()

    # -------- Commerciali fetch --------
    def fetch_comm_categories(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM comm_category ORDER BY code")
        return cur.fetchall()

    def fetch_comm_subcategories(self, category_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM comm_subcategory WHERE category_id=? ORDER BY code", (int(category_id),))
        return cur.fetchall()

    def fetch_suppliers(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM supplier ORDER BY code")
        return cur.fetchall()

    def get_next_comm_seq(self, category_id: int, subcategory_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM comm_item WHERE category_id=? AND subcategory_id=?",
            (int(category_id), int(subcategory_id)),
        )
        return int(cur.fetchone()["next_seq"])

    def search_comm_items(
        self,
        q: str = "",
        category_id: Optional[int] = None,
        subcategory_id: Optional[int] = None,
        supplier_id: Optional[int] = None,
        only_preferred: bool = False,
    ):
        q = (q or "").strip()
        cur = self.conn.cursor()
        params: List[Any] = []
        where: List[str] = []
        sql = """
            SELECT i.id, i.code, i.description, i.updated_at,
                   c.code AS cat_code, sc.code AS sub_code,
                   s.code AS sup_code,
                   i.supplier_item_code, i.supplier_item_desc,
                   COALESCE(i.preferred, 0) AS preferred
            FROM comm_item i
            JOIN comm_category c ON c.id=i.category_id
            JOIN comm_subcategory sc ON sc.id=i.subcategory_id
            LEFT JOIN supplier s ON s.id=i.supplier_id
        """
        tokens = self._parse_search_tokens(q)
        for tok, quoted in tokens:
            # Regola 2: token in AND (ogni token aggiunge una clausola).
            self._append_token_where(
                fields_sql=[
                    "i.code",
                    "i.description",
                    "c.code",
                    "sc.code",
                    "s.code",
                    "i.supplier_item_code",
                    "i.supplier_item_desc",
                ],
                token=tok,
                quoted=quoted,
                where=where,
                params=params,
            )
        if category_id is not None:
            where.append("i.category_id=?")
            params.append(int(category_id))
        if subcategory_id is not None:
            where.append("i.subcategory_id=?")
            params.append(int(subcategory_id))
        if supplier_id is not None:
            where.append("i.supplier_id=?")
            params.append(int(supplier_id))
        if only_preferred:
            where.append("COALESCE(i.preferred, 0)=1")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY COALESCE(i.preferred, 0) DESC, i.updated_at DESC"
        cur.execute(sql, tuple(params))
        return cur.fetchall()

    def read_comm_item(self, item_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT i.*, c.code AS cat_code, c.description AS cat_desc,
                   sc.code AS sub_code, sc.description AS sub_desc,
                   s.code AS sup_code, s.description AS sup_desc
            FROM comm_item i
            JOIN comm_category c ON c.id=i.category_id
            JOIN comm_subcategory sc ON sc.id=i.subcategory_id
            LEFT JOIN supplier s ON s.id=i.supplier_id
            WHERE i.id=?
            """,
            (int(item_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Commerciale non normato non trovato")
        return row

    # Commerciali CRUD: categorie/sotto/fornitori/articoli
    def create_comm_category(self, code: str, description: str) -> None:
        code_n = normalize_cccc(code)
        if not re.fullmatch(r"[0-9]{4}", code_n):
            raise ValueError("CODICE categoria commerciali non valido: servono 4 numeri.")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO comm_category(code, description) VALUES(?, ?)", (code_n, normalize_upper(description)))
        self.conn.commit()

    def update_comm_category(self, category_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE comm_category SET description=? WHERE id=?", (normalize_upper(description), int(category_id)))
        self.conn.commit()

    def delete_comm_category(self, category_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM comm_category WHERE id=?", (int(category_id),))
        self.conn.commit()

    def create_comm_subcategory(self, category_id: int, code: str, description: str) -> None:
        code_n = normalize_ssss(code)
        if not re.fullmatch(r"[0-9]{4}", code_n):
            raise ValueError("CODICE sotto-categoria commerciali non valido: servono 4 numeri.")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO comm_subcategory(category_id, code, description) VALUES(?, ?, ?)",
            (int(category_id), code_n, normalize_upper(description)),
        )
        self.conn.commit()

    def update_comm_subcategory(self, subcategory_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE comm_subcategory SET description=? WHERE id=?", (normalize_upper(description), int(subcategory_id)))
        self.conn.commit()

    def delete_comm_subcategory(self, subcategory_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM comm_subcategory WHERE id=?", (int(subcategory_id),))
        self.conn.commit()

    def create_supplier(self, code: str, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT INTO supplier(code, description) VALUES(?, ?)", (normalize_upper(code), normalize_upper(description)))
        self.conn.commit()

    def update_supplier(self, supplier_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE supplier SET description=? WHERE id=?", (normalize_upper(description), int(supplier_id)))
        self.conn.commit()

    def delete_supplier(self, supplier_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM supplier WHERE id=?", (int(supplier_id),))
        self.conn.commit()

    def create_comm_item(self, payload: Dict[str, Any]) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO comm_item(code, category_id, subcategory_id, supplier_id, seq, description,
                                  supplier_item_code, supplier_item_desc,
                                  file_folder, notes, preferred, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["code"],
                int(payload["category_id"]),
                int(payload["subcategory_id"]),
                int(payload["supplier_id"]) if payload.get("supplier_id") else None,
                int(payload["seq"]),
                payload["description"],
                payload.get("supplier_item_code") or "",
                payload.get("supplier_item_desc") or "",
                payload.get("file_folder") or "",
                payload.get("notes") or "",
                int(payload.get("preferred", 0)),
                int(payload.get("is_active", 1)),
                now_str(),
                now_str(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_comm_item(self, item_id: int, payload: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE comm_item
            SET category_id=?, subcategory_id=?, supplier_id=?, description=?,
                supplier_item_code=?, supplier_item_desc=?,
                file_folder=?, notes=?, preferred=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                int(payload["category_id"]),
                int(payload["subcategory_id"]),
                int(payload["supplier_id"]) if payload.get("supplier_id") else None,
                payload["description"],
                payload.get("supplier_item_code") or "",
                payload.get("supplier_item_desc") or "",
                payload.get("file_folder") or "",
                payload.get("notes") or "",
                int(payload.get("preferred", 0)),
                int(payload.get("is_active", 1)),
                now_str(),
                int(item_id),
            ),
        )
        self.conn.commit()

    def delete_comm_item(self, item_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM comm_item WHERE id=?", (int(item_id),))
        self.conn.commit()


    # -------- Materiali / Trattamenti / Semilavorati --------
    
    
    # -------- Materiali --------
    def fetch_material_families(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, description FROM material_family ORDER BY description")
        return cur.fetchall()

    def fetch_material_subfamilies(self, family_id: int):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, family_id, description FROM material_subfamily WHERE family_id=? ORDER BY description",
            (int(family_id),),
        )
        return cur.fetchall()

    def create_material_family(self, description: str) -> int:
        cur = self.conn.cursor()
        cur.execute("INSERT INTO material_family(description) VALUES(?)", (normalize_upper(description),))
        self.conn.commit()
        return int(cur.lastrowid)

    def update_material_family(self, family_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT description FROM material_family WHERE id=?", (int(family_id),))
        row = cur.fetchone()
        if row is None:
            raise ValueError("Famiglia materiale non trovata")
        old_desc = normalize_upper(str(row["description"]))
        new_desc = normalize_upper(description)
        cur.execute("UPDATE material_family SET description=? WHERE id=?", (new_desc, int(family_id)))
        cur.execute("UPDATE material SET family=? WHERE family=?", (new_desc, old_desc))
        self.conn.commit()

    def delete_material_family(self, family_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT description FROM material_family WHERE id=?", (int(family_id),))
        row = cur.fetchone()
        if row is None:
            return
        fam_desc = normalize_upper(str(row["description"]))

        cur.execute("SELECT COUNT(*) AS n FROM material WHERE family=?", (fam_desc,))
        in_use = int(cur.fetchone()["n"])
        if in_use > 0:
            raise ValueError("Impossibile eliminare: famiglia usata da materiali esistenti.")

        cur.execute("SELECT COUNT(*) AS n FROM material_subfamily WHERE family_id=?", (int(family_id),))
        has_sub = int(cur.fetchone()["n"])
        if has_sub > 0:
            raise ValueError("Impossibile eliminare: elimina prima le sottofamiglie.")

        cur.execute("DELETE FROM material_family WHERE id=?", (int(family_id),))
        self.conn.commit()

    def create_material_subfamily(self, family_id: int, description: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO material_subfamily(family_id, description) VALUES(?, ?)",
            (int(family_id), normalize_upper(description)),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_material_subfamily(self, subfamily_id: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT sf.id, sf.family_id, sf.description AS sub_desc, f.description AS fam_desc
            FROM material_subfamily sf
            JOIN material_family f ON f.id=sf.family_id
            WHERE sf.id=?
            """,
            (int(subfamily_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Sottofamiglia materiale non trovata")
        old_sub = normalize_upper(str(row["sub_desc"]))
        fam_desc = normalize_upper(str(row["fam_desc"]))
        new_sub = normalize_upper(description)
        cur.execute("UPDATE material_subfamily SET description=? WHERE id=?", (new_sub, int(subfamily_id)))
        cur.execute(
            "UPDATE material SET description=? WHERE family=? AND description=?",
            (new_sub, fam_desc, old_sub),
        )
        self.conn.commit()

    def delete_material_subfamily(self, subfamily_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT sf.id, sf.description AS sub_desc, f.description AS fam_desc
            FROM material_subfamily sf
            JOIN material_family f ON f.id=sf.family_id
            WHERE sf.id=?
            """,
            (int(subfamily_id),),
        )
        row = cur.fetchone()
        if row is None:
            return
        sub_desc = normalize_upper(str(row["sub_desc"]))
        fam_desc = normalize_upper(str(row["fam_desc"]))
        cur.execute(
            "SELECT COUNT(*) AS n FROM material WHERE family=? AND description=?",
            (fam_desc, sub_desc),
        )
        in_use = int(cur.fetchone()["n"])
        if in_use > 0:
            raise ValueError("Impossibile eliminare: sottofamiglia usata da materiali esistenti.")
        cur.execute("DELETE FROM material_subfamily WHERE id=?", (int(subfamily_id),))
        self.conn.commit()

    def ensure_material_taxonomy_entry(self, family: str, subfamily: str) -> None:
        fam = normalize_upper(family)
        sub = normalize_upper(subfamily)
        if not fam:
            return
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO material_family(description) VALUES(?)", (fam,))
        cur.execute("SELECT id FROM material_family WHERE description=?", (fam,))
        row = cur.fetchone()
        if row and sub:
            cur.execute(
                "INSERT OR IGNORE INTO material_subfamily(family_id, description) VALUES(?, ?)",
                (int(row["id"]), sub),
            )
        self.conn.commit()

    def search_materials(self, q: str = ""):
        q = (q or "").strip()
        cur = self.conn.cursor()
        if q:
            like = f"%{q}%"
            cur.execute(
                """
                SELECT id, code, family, description, updated_at
                FROM material
                WHERE code LIKE ? OR family LIKE ? OR description LIKE ?
                ORDER BY updated_at DESC
                """,
                (like, like, like),
            )
        else:
            cur.execute("SELECT id, code, family, description, updated_at FROM material ORDER BY updated_at DESC")
        return cur.fetchall()
    
    def read_material(self, material_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM material WHERE id=?", (int(material_id),))
        row = cur.fetchone()
        if row is None:
            raise ValueError("Materiale non trovato")
        return row
    
    def create_material(self, code: Optional[str], family: str, description: str, standard: str, notes: str) -> int:
        code = normalize_upper(code) if (code or "").strip() else self._auto_code("MAT")
        self.ensure_material_taxonomy_entry(family, description)
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO material(code, family, description, standard, notes, is_active, created_at, updated_at) VALUES(?, ?, ?, ?, ?, 1, ?, ?)",
            (normalize_upper(code), normalize_upper(family), normalize_upper(description), normalize_upper(standard), normalize_upper(notes), now_str(), now_str()),
        )
        new_id = int(cur.lastrowid)
        self._ensure_default_material_properties_with_cursor(cur, new_id)
        self.conn.commit()
        return new_id
    
    def update_material(self, material_id: int, family: str, description: str, standard: str, notes: str) -> None:
        self.ensure_material_taxonomy_entry(family, description)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE material SET family=?, description=?, standard=?, notes=?, updated_at=? WHERE id=?",
            (normalize_upper(family), normalize_upper(description), normalize_upper(standard), normalize_upper(notes), now_str(), int(material_id)),
        )
        self.conn.commit()
    
    def delete_material(self, material_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM material WHERE id=?", (int(material_id),))
        self.conn.commit()
    
    def fetch_material_properties(self, material_id: int, prop_group: str):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, state_code, name, unit, value, min_value, max_value, notes, sort_order
            FROM material_property
            WHERE material_id=? AND prop_group=?
            ORDER BY state_code, sort_order, name
            """,
            (int(material_id), normalize_upper(prop_group)),
        )
        return cur.fetchall()

    def read_material_property_notes(self, prop_id: int) -> str:
        cur = self.conn.cursor()
        cur.execute("SELECT notes FROM material_property WHERE id=?", (int(prop_id),))
        row = cur.fetchone()
        return "" if row is None else str(row["notes"] or "")
    
    def create_material_property(
        self,
        material_id: int,
        prop_group: str,
        state_code: str,
        name: str,
        unit: str,
        value: str,
        min_value: str,
        max_value: str,
        notes: str,
        sort_order: int = 0,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO material_property(material_id, prop_group, state_code, name, unit, value, min_value, max_value, notes, sort_order)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(material_id),
                normalize_upper(prop_group),
                normalize_upper(state_code or ""),
                normalize_upper(name),
                normalize_upper(unit),
                normalize_upper(value),
                normalize_upper(min_value),
                normalize_upper(max_value),
                normalize_upper(notes),
                int(sort_order),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_material_property(
        self,
        prop_id: int,
        state_code: str,
        unit: str,
        value: str,
        min_value: str,
        max_value: str,
        notes: str,
        sort_order: int = 0,
    ) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE material_property
            SET state_code=?, unit=?, value=?, min_value=?, max_value=?, notes=?, sort_order=?
            WHERE id=?
            """,
            (
                normalize_upper(state_code or ""),
                normalize_upper(unit),
                normalize_upper(value),
                normalize_upper(min_value),
                normalize_upper(max_value),
                normalize_upper(notes),
                int(sort_order),
                int(prop_id),
            ),
        )
        self.conn.commit()
    
    def delete_material_property(self, prop_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM material_property WHERE id=?", (int(prop_id),))
        self.conn.commit()
    
    # -------- Trattamenti --------
    def fetch_heat_treatments(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description, updated_at FROM heat_treatment ORDER BY updated_at DESC")
        return cur.fetchall()
    
    def fetch_surface_treatments(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description, updated_at FROM surface_treatment ORDER BY updated_at DESC")
        return cur.fetchall()
    
    def read_heat_treatment(self, tid: int):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM heat_treatment WHERE id=?", (int(tid),))
        row = cur.fetchone()
        if row is None:
            raise ValueError("Trattamento termico non trovato")
        return row
    
    def read_surface_treatment(self, tid: int):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM surface_treatment WHERE id=?", (int(tid),))
        row = cur.fetchone()
        if row is None:
            raise ValueError("Trattamento superficiale non trovato")
        return row
    
    def create_heat_treatment(self, code: Optional[str], description: str, characteristics: str, standard: str, notes: str) -> int:
        code = normalize_upper(code) if (code or "").strip() else self._auto_code("HEAT")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO heat_treatment(code, description, characteristics, standard, notes, is_active, created_at, updated_at) VALUES(?, ?, ?, ?, ?, 1, ?, ?)",
            (normalize_upper(code), normalize_upper(description), normalize_upper(characteristics), normalize_upper(standard), normalize_upper(notes), now_str(), now_str()),
        )
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_heat_treatment(self, tid: int, description: str, characteristics: str, standard: str, notes: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE heat_treatment SET description=?, characteristics=?, standard=?, notes=?, updated_at=? WHERE id=?",
            (normalize_upper(description), normalize_upper(characteristics), normalize_upper(standard), normalize_upper(notes), now_str(), int(tid)),
        )
        self.conn.commit()
    
    def delete_heat_treatment(self, tid: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM heat_treatment WHERE id=?", (int(tid),))
        self.conn.commit()
    
    def create_surface_treatment(self, code: Optional[str], description: str, characteristics: str, standard: str, notes: str) -> int:
        code = normalize_upper(code) if (code or "").strip() else self._auto_code("SURF")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO surface_treatment(code, description, characteristics, standard, notes, is_active, created_at, updated_at) VALUES(?, ?, ?, ?, ?, 1, ?, ?)",
            (normalize_upper(code), normalize_upper(description), normalize_upper(characteristics), normalize_upper(standard), normalize_upper(notes), now_str(), now_str()),
        )
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_surface_treatment(self, tid: int, description: str, characteristics: str, standard: str, notes: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE surface_treatment SET description=?, characteristics=?, standard=?, notes=?, updated_at=? WHERE id=?",
            (normalize_upper(description), normalize_upper(characteristics), normalize_upper(standard), normalize_upper(notes), now_str(), int(tid)),
        )
        self.conn.commit()
    
    def delete_surface_treatment(self, tid: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM surface_treatment WHERE id=?", (int(tid),))
        self.conn.commit()

    # -------- Manuale versioni --------
    def fetch_manual_versions(self, q: str = ""):
        cur = self.conn.cursor()
        qn = (q or "").strip()
        if qn:
            like = f"%{qn}%"
            cur.execute(
                """
                SELECT id, version, release_date, updates, updated_at
                FROM manual_version
                WHERE UPPER(COALESCE(version,'')) LIKE UPPER(?)
                   OR UPPER(COALESCE(updates,'')) LIKE UPPER(?)
                ORDER BY release_date DESC, updated_at DESC, id DESC
                """,
                (like, like),
            )
        else:
            cur.execute(
                """
                SELECT id, version, release_date, updates, updated_at
                FROM manual_version
                ORDER BY release_date DESC, updated_at DESC, id DESC
                """
            )
        return cur.fetchall()

    def read_manual_version(self, entry_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, version, release_date, updates, created_at, updated_at
            FROM manual_version
            WHERE id=?
            """,
            (int(entry_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Versione manuale non trovata")
        return row

    def create_manual_version(self, version: str, release_date: str, updates: str) -> int:
        ver = (version or "").strip()
        rel = (release_date or "").strip()
        upd = (updates or "").strip()
        if not ver:
            raise ValueError("Compila VERSIONE.")
        if not rel:
            raise ValueError("Compila DATA RILASCIO.")
        if not upd:
            raise ValueError("Compila AGGIORNAMENTI.")
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO manual_version(version, release_date, updates, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (ver, rel, upd, now_str(), now_str()),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_manual_version(self, entry_id: int, version: str, release_date: str, updates: str) -> None:
        ver = (version or "").strip()
        rel = (release_date or "").strip()
        upd = (updates or "").strip()
        if not ver:
            raise ValueError("Compila VERSIONE.")
        if not rel:
            raise ValueError("Compila DATA RILASCIO.")
        if not upd:
            raise ValueError("Compila AGGIORNAMENTI.")
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE manual_version
            SET version=?, release_date=?, updates=?, updated_at=?
            WHERE id=?
            """,
            (ver, rel, upd, now_str(), int(entry_id)),
        )
        self.conn.commit()

    def delete_manual_version(self, entry_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM manual_version WHERE id=?", (int(entry_id),))
        self.conn.commit()
    
    # -------- Semilavorati --------
    def fetch_semi_types(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM semi_type ORDER BY description")
        return cur.fetchall()
    
    def fetch_semi_states(self):
        cur = self.conn.cursor()
        cur.execute("SELECT id, code, description FROM semi_state ORDER BY description")
        return cur.fetchall()
    
    def create_semi_type(self, code: Optional[str], description: str) -> int:
        code = normalize_upper(code) if (code or "").strip() else self._auto_code("TYPE")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO semi_type(code, description) VALUES(?, ?)", (normalize_upper(code), normalize_upper(description)))
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_semi_type(self, tid: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE semi_type SET description=? WHERE id=?", (normalize_upper(description), int(tid)))
        self.conn.commit()
    
    def delete_semi_type(self, tid: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM semi_type WHERE id=?", (int(tid),))
        self.conn.commit()
    
    def create_semi_state(self, code: Optional[str], description: str) -> int:
        code = normalize_upper(code) if (code or "").strip() else self._auto_code("STATE")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO semi_state(code, description) VALUES(?, ?)", (normalize_upper(code), normalize_upper(description)))
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_semi_state(self, sid: int, description: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE semi_state SET description=? WHERE id=?", (normalize_upper(description), int(sid)))
        self.conn.commit()
    
    def delete_semi_state(self, sid: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM semi_state WHERE id=?", (int(sid),))
        self.conn.commit()
    
    def search_semi_items(self, q: str = "", only_preferred_dimension: bool = False):
        q = (q or "").strip()
        cur = self.conn.cursor()
        where: List[str] = []
        params: List[Any] = []
        if q:
            like = f"%{q}%"
            where.append(
                """
                (
                    si.description LIKE ? OR si.dimensions LIKE ? OR st.description LIKE ? OR ss.description LIKE ?
                    OR COALESCE(m.family,'') LIKE ? OR COALESCE(m.description,'') LIKE ?
                    OR EXISTS(
                        SELECT 1
                        FROM semi_item_dimension d2
                        WHERE d2.semi_item_id=si.id AND d2.dimension LIKE ?
                    )
                )
                """
            )
            params.extend([like, like, like, like, like, like, like])
        if only_preferred_dimension:
            where.append(
                """
                EXISTS(
                    SELECT 1
                    FROM semi_item_dimension p
                    WHERE p.semi_item_id=si.id AND COALESCE(p.preferred, 0)=1
                )
                """
            )
        sql = """
            SELECT si.id,
                   st.description AS type_desc, ss.description AS state_desc,
                   COALESCE(m.family || ' - ' || m.description, '') AS mat_label,
                   si.description, si.dimensions,
                   COALESCE(pd.dimension, si.dimensions, '') AS dim_display,
                   CASE WHEN pd.id IS NULL THEN 0 ELSE 1 END AS has_preferred_dimension,
                   si.updated_at
            FROM semi_item si
            JOIN semi_type st ON st.id=si.type_id
            JOIN semi_state ss ON ss.id=si.state_id
            LEFT JOIN material m ON m.id=si.material_id
            LEFT JOIN semi_item_dimension pd ON pd.id=(
                SELECT d.id
                FROM semi_item_dimension d
                WHERE d.semi_item_id=si.id AND COALESCE(d.preferred, 0)=1
                ORDER BY d.sort_order, d.id
                LIMIT 1
            )
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY CASE WHEN pd.id IS NULL THEN 0 ELSE 1 END DESC, si.updated_at DESC"
        cur.execute(sql, tuple(params))
        return cur.fetchall()
    
    def fetch_semis_by_material(self, material_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT si.id,
                   st.description AS type_desc, ss.description AS state_desc,
                   si.description, si.dimensions, si.updated_at
            FROM semi_item si
            JOIN semi_type st ON st.id=si.type_id
            JOIN semi_state ss ON ss.id=si.state_id
            WHERE si.material_id=?
            ORDER BY si.updated_at DESC
            """,
            (int(material_id),),
        )
        return cur.fetchall()
    
    def read_semi_item(self, item_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT si.*,
                   st.code AS type_code, st.description AS type_desc,
                   ss.code AS state_code, ss.description AS state_desc,
                   m.code AS mat_code, m.family AS mat_family, m.description AS mat_desc,
                   COALESCE(m.family || ' - ' || m.description, '') AS mat_label
            FROM semi_item si
            JOIN semi_type st ON st.id=si.type_id
            JOIN semi_state ss ON ss.id=si.state_id
            LEFT JOIN material m ON m.id=si.material_id
            WHERE si.id=?
            """,
            (int(item_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Semilavorato non trovato")
        return row
    
    def create_semi_item(self, payload: Dict[str, Any]) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO semi_item(type_id, state_id, material_id, description, dimensions, standard, notes, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(payload["type_id"]),
                int(payload["state_id"]),
                int(payload["material_id"]) if payload.get("material_id") else None,
                normalize_upper(payload["description"]),
                normalize_upper(payload.get("dimensions") or ""),
                normalize_upper(payload.get("standard") or ""),
                normalize_upper(payload.get("notes") or ""),
                int(payload.get("is_active", 1)),
                now_str(),
                now_str(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)
    
    def update_semi_item(self, item_id: int, payload: Dict[str, Any]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE semi_item
            SET type_id=?, state_id=?, material_id=?, description=?, dimensions=?, standard=?, notes=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                int(payload["type_id"]),
                int(payload["state_id"]),
                int(payload["material_id"]) if payload.get("material_id") else None,
                normalize_upper(payload["description"]),
                normalize_upper(payload.get("dimensions") or ""),
                normalize_upper(payload.get("standard") or ""),
                normalize_upper(payload.get("notes") or ""),
                int(payload.get("is_active", 1)),
                now_str(),
                int(item_id),
            ),
        )
        self.conn.commit()
    
    def delete_semi_item(self, item_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM semi_item WHERE id=?", (int(item_id),))
        self.conn.commit()

    def fetch_semi_dimensions(self, semi_item_id: int):
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, dimension, weight_per_m, sort_order, COALESCE(preferred, 0) AS preferred
            FROM semi_item_dimension
            WHERE semi_item_id=?
            ORDER BY sort_order, dimension
            """,
            (int(semi_item_id),),
        )
        return cur.fetchall()

    def create_semi_dimension(
        self,
        semi_item_id: int,
        dimension: str,
        weight_per_m: str,
        preferred: int = 0,
        sort_order: Optional[int] = None,
    ) -> int:
        cur = self.conn.cursor()
        if sort_order is None:
            cur.execute(
                "SELECT COALESCE(MAX(sort_order), 0) + 10 AS next_ord FROM semi_item_dimension WHERE semi_item_id=?",
                (int(semi_item_id),),
            )
            sort_order = int(cur.fetchone()["next_ord"])
        pref_val = 1 if int(preferred or 0) else 0
        if pref_val:
            cur.execute(
                "UPDATE semi_item_dimension SET preferred=0 WHERE semi_item_id=?",
                (int(semi_item_id),),
            )
        cur.execute(
            """
            INSERT INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order, preferred)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                int(semi_item_id),
                normalize_upper(dimension),
                normalize_upper(weight_per_m),
                int(sort_order),
                pref_val,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def update_semi_dimension(
        self,
        dim_id: int,
        dimension: str,
        weight_per_m: str,
        preferred: Optional[int] = None,
        sort_order: Optional[int] = None,
    ) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT semi_item_id, sort_order, COALESCE(preferred, 0) AS preferred FROM semi_item_dimension WHERE id=?",
            (int(dim_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Dimensione semilavorato non trovata")
        semi_item_id = int(row["semi_item_id"])
        if sort_order is None:
            sort_order = int(row["sort_order"] or 0)
        pref_val = int(row["preferred"] or 0) if preferred is None else (1 if int(preferred or 0) else 0)
        if pref_val:
            cur.execute(
                "UPDATE semi_item_dimension SET preferred=0 WHERE semi_item_id=? AND id<>?",
                (semi_item_id, int(dim_id)),
            )
        cur.execute(
            """
            UPDATE semi_item_dimension
            SET dimension=?, weight_per_m=?, sort_order=?, preferred=?
            WHERE id=?
            """,
            (
                normalize_upper(dimension),
                normalize_upper(weight_per_m),
                int(sort_order),
                pref_val,
                int(dim_id),
            ),
        )
        self.conn.commit()

    def delete_semi_dimension(self, dim_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM semi_item_dimension WHERE id=?", (int(dim_id),))
        self.conn.commit()

    def clone_semi_dimensions(self, src_item_id: int, dst_item_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT dimension, weight_per_m, sort_order, COALESCE(preferred, 0) AS preferred
            FROM semi_item_dimension
            WHERE semi_item_id=?
            ORDER BY sort_order, id
            """,
            (int(src_item_id),),
        )
        rows = cur.fetchall()
        copied = 0
        has_preferred = False
        for r in rows:
            pref_val = 1 if int(r["preferred"] or 0) and not has_preferred else 0
            if pref_val:
                has_preferred = True
            cur.execute(
                """
                INSERT OR IGNORE INTO semi_item_dimension(semi_item_id, dimension, weight_per_m, sort_order, preferred)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    int(dst_item_id),
                    normalize_upper(str(r["dimension"] or "")),
                    normalize_upper(str(r["weight_per_m"] or "")),
                    int(r["sort_order"] or 0),
                    pref_val,
                ),
            )
            if cur.rowcount > 0:
                copied += 1
        self.conn.commit()
        return copied

    @staticmethod
    def _extract_numbers(text: str) -> List[float]:
        vals: List[float] = []
        for tok in re.findall(r"\d+(?:[.,]\d+)?", text or ""):
            try:
                vals.append(float(tok.replace(",", ".")))
            except Exception:
                continue
        return vals

    @staticmethod
    def _is_dimension_ambiguous(text: str) -> bool:
        s = normalize_upper(text or "")
        if not s:
            return True
        if re.search(r"\d+\s*-\s*\d+", s):
            return True
        if "VARIE" in s:
            return True
        return False

    @staticmethod
    def _section_area_mm2(type_desc: str, dimension: str) -> Optional[float]:
        t = normalize_upper(type_desc or "")
        d = normalize_upper(dimension or "")
        if Database._is_dimension_ambiguous(d):
            return None
        nums = Database._extract_numbers(d)
        if not nums:
            return None

        if t == "TONDI":
            dia = nums[0]
            if dia <= 0:
                return None
            return math.pi * (dia ** 2) / 4.0

        if t == "ESAGONI":
            ch = nums[0]
            if ch <= 0:
                return None
            return (math.sqrt(3.0) / 2.0) * (ch ** 2)

        if t == "PIATTI":
            if len(nums) < 2:
                return None
            b = nums[0]
            s = nums[1]
            if b <= 0 or s <= 0:
                return None
            return b * s

        if t == "TUBI":
            if len(nums) < 2:
                return None
            d_ext = nums[0]
            sp = nums[1]
            if d_ext <= 0 or sp <= 0:
                return None
            d_int = d_ext - 2.0 * sp
            if d_int <= 0:
                return None
            return (math.pi / 4.0) * ((d_ext ** 2) - (d_int ** 2))

        if t == "TUBOLARI":
            if len(nums) < 3:
                return None
            b = nums[0]
            h = nums[1]
            sp = nums[2]
            if b <= 0 or h <= 0 or sp <= 0:
                return None
            b_int = b - 2.0 * sp
            h_int = h - 2.0 * sp
            if b_int <= 0 or h_int <= 0:
                return None
            return (b * h) - (b_int * h_int)

        if t in {
            "PROFILATI",
            "PROFILO L",
            "PROFILO U",
            "PROFILO T",
            "PROFILO L TRAFILATO",
            "PROFILO U TRAFILATO",
            "PROFILO T TRAFILATO",
        }:
            s = d.replace(" ", "")
            # Angolare: L AxBxS (oppure L AxS con ali uguali)
            if s.startswith("L"):
                if len(nums) < 2:
                    return None
                if len(nums) == 2:
                    a = nums[0]
                    b = nums[0]
                    sp = nums[1]
                else:
                    a = nums[0]
                    b = nums[1]
                    sp = nums[2]
                if a <= 0 or b <= 0 or sp <= 0:
                    return None
                if sp >= a or sp >= b:
                    return None
                return sp * (a + b - sp)

            # U: U HxBxS (spessore unico) oppure U HxBxTFxTW
            if s.startswith("U"):
                if len(nums) < 3:
                    return None
                h = nums[0]
                b = nums[1]
                if h <= 0 or b <= 0:
                    return None
                if len(nums) >= 4:
                    tf = nums[2]
                    tw = nums[3]
                    if tf <= 0 or tw <= 0 or (2.0 * tf) >= h or tw >= b:
                        return None
                    return (2.0 * b * tf) + ((h - 2.0 * tf) * tw)
                sp = nums[2]
                if sp <= 0 or (2.0 * sp) >= h or sp >= b:
                    return None
                return sp * (h + 2.0 * b - 2.0 * sp)

            # T: T BxHxS (spessore unico) oppure T BxHxTFxTW
            if s.startswith("T"):
                if len(nums) < 3:
                    return None
                b = nums[0]
                h = nums[1]
                if b <= 0 or h <= 0:
                    return None
                if len(nums) >= 4:
                    tf = nums[2]
                    tw = nums[3]
                    if tf <= 0 or tw <= 0 or tf >= h or tw >= b:
                        return None
                    return (b * tf) + ((h - tf) * tw)
                sp = nums[2]
                if sp <= 0 or sp >= h or sp >= b:
                    return None
                return (b * sp) + ((h - sp) * sp)

            return None

        # TRAVI (IPE/HEA/IPN/UPN...) richiedono tabelle dedicate.
        return None

    @staticmethod
    def _lamiera_thickness_mm(dimension: str) -> Optional[float]:
        d = normalize_upper(dimension or "")
        if Database._is_dimension_ambiguous(d):
            return None
        nums = Database._extract_numbers(d)
        if not nums:
            return None
        # Convenzione: ultimo valore numerico = spessore (es. SP3, 1000X2000X3, 40X10).
        sp = float(nums[-1])
        if sp <= 0:
            return None
        return sp

    def read_material_density_g_cm3(self, material_id: int) -> Optional[float]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT value, min_value, max_value
            FROM material_property
            WHERE material_id=? AND name='DENSITA'
            ORDER BY id
            """,
            (int(material_id),),
        )
        rows = cur.fetchall()
        for r in rows:
            for key in ("value", "min_value", "max_value"):
                raw = str(r[key] or "").strip()
                if not raw:
                    continue
                vals = self._extract_numbers(raw)
                for v in vals:
                    if v > 0:
                        return float(v)
        return None

    def calculate_semi_weight_per_m(self, semi_item_id: int, dimension: str) -> Optional[float]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT si.material_id, st.description AS type_desc
            FROM semi_item si
            JOIN semi_type st ON st.id=si.type_id
            WHERE si.id=?
            """,
            (int(semi_item_id),),
        )
        row = cur.fetchone()
        if row is None:
            return None
        mat_id = row["material_id"]
        if mat_id is None:
            return None

        density = self.read_material_density_g_cm3(int(mat_id))
        if density is None or density <= 0:
            return None

        type_desc = normalize_upper(str(row["type_desc"] or ""))
        if type_desc == "LAMIERE":
            sp = self._lamiera_thickness_mm(dimension)
            if sp is None:
                return None
            # Formula lamiera: kg/m^2 = spessore_mm * densita_g/cm^3
            return sp * density

        area = self._section_area_mm2(type_desc, dimension)
        if area is None or area <= 0:
            return None

        # Formula: kg/m = area_mm2 * density_g_cm3 / 1000
        return (area * density) / 1000.0
    
