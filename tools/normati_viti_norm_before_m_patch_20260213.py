from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Dict, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.codifica import normalize_gggg_normati, normalize_mmm
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"

CATEGORY_CODE = "VIT"
TARGET_SUBS = ("HEPZ", "HEPA", "HETZ", "HETA")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_viti_norm_before_m.db"
    db.backup_to_path(str(out))
    return out


def fetch_category(cur, code: str) -> Tuple[int, str]:
    code_n = normalize_mmm(code)
    cur.execute("SELECT id, code FROM category WHERE code=?", (code_n,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Categoria non trovata: {code_n}")
    return int(row["id"]), str(row["code"])


def pick_primary_norm(standard_code: str) -> str:
    s = normalize_upper(standard_code or "")
    if not s:
        return ""
    for prefix in ("ISO", "UNI", "DIN"):
        m = re.search(rf"\b{prefix}\s*[A-Z0-9./-]+", s)
        if m:
            return normalize_upper(m.group(0))
    return s


def ensure_norm_before_m(text: str, norm_ref: str) -> str:
    s = normalize_upper(text or "")
    n = normalize_upper(norm_ref or "")
    if not s or not n:
        return s

    # Rimuove eventuale norma già presente davanti al token M...
    s = re.sub(
        r"\b(?:ISO|UNI|DIN)\s+[A-Z0-9./-]+\s+(?=M(?:__|\d+(?:[.,]\d+)?)X(?:__|\d+(?:[.,]\d+)?))",
        "",
        s,
    )
    # Inserisce la norma prima della prima dimensione metrica M...
    return re.sub(
        r"\bM(?:__|\d+(?:[.,]\d+)?)X(?:__|\d+(?:[.,]\d+)?)(?:[.,]\d+)?\b",
        lambda m: f"{n} {m.group(0)}",
        s,
        count=1,
    )


def fetch_sub_map(cur, category_id: int) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for sub_code in TARGET_SUBS:
        code_n = normalize_gggg_normati(sub_code)
        cur.execute(
            """
            SELECT sc.id,
                   sc.code,
                   COALESCE(sc.desc_template, '') AS desc_template,
                   COALESCE(st.code, '') AS standard_code
            FROM subcategory sc
            LEFT JOIN standard st ON st.id=sc.standard_id
            WHERE sc.category_id=? AND sc.code=?
            """,
            (int(category_id), code_n),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Sotto-categoria non trovata: {code_n}")
        out[code_n] = {
            "id": int(row["id"]),
            "code": str(row["code"] or ""),
            "desc_template": str(row["desc_template"] or ""),
            "standard_code": str(row["standard_code"] or ""),
        }
    return out


def dedupe_sub_items(cur, category_id: int, sub_id: int) -> int:
    cur.execute(
        """
        SELECT description, COUNT(*) AS c
        FROM item
        WHERE category_id=? AND subcategory_id=?
        GROUP BY description
        HAVING COUNT(*) > 1
        """,
        (int(category_id), int(sub_id)),
    )
    groups = cur.fetchall()
    removed = 0
    for g in groups:
        desc = str(g["description"] or "")
        cur.execute(
            """
            SELECT id
            FROM item
            WHERE category_id=? AND subcategory_id=? AND description=?
            ORDER BY id
            """,
            (int(category_id), int(sub_id), desc),
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

        cat_id, _ = fetch_category(cur, CATEGORY_CODE)
        sub_map = fetch_sub_map(cur, cat_id)

        tpl_updated = 0
        item_updated = 0
        dedup_removed = 0

        for sub_code in sorted(sub_map.keys()):
            sub = sub_map[sub_code]
            sub_id = int(sub["id"])
            norm_ref = pick_primary_norm(str(sub["standard_code"] or ""))

            old_tpl = normalize_upper(str(sub["desc_template"] or ""))
            new_tpl = ensure_norm_before_m(old_tpl, norm_ref)
            if new_tpl != old_tpl:
                cur.execute("UPDATE subcategory SET desc_template=? WHERE id=?", (new_tpl, sub_id))
                tpl_updated += int(cur.rowcount)

            cur.execute(
                """
                SELECT id, description
                FROM item
                WHERE category_id=? AND subcategory_id=?
                ORDER BY id
                """,
                (int(cat_id), sub_id),
            )
            rows = cur.fetchall()
            for r in rows:
                iid = int(r["id"])
                old_desc = normalize_upper(str(r["description"] or ""))
                new_desc = ensure_norm_before_m(old_desc, norm_ref)
                if new_desc != old_desc:
                    cur.execute(
                        "UPDATE item SET description=?, updated_at=? WHERE id=?",
                        (new_desc, now_str(), iid),
                    )
                    item_updated += int(cur.rowcount)

            dedup_removed += dedupe_sub_items(cur, cat_id, sub_id)

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Templates updated: {tpl_updated}")
        print(f"Items updated: {item_updated}")
        print(f"Duplicate items removed: {dedup_removed}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Insert reference standard before M-size in VIT TE screw descriptions (ISO>UNI>DIN)."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
