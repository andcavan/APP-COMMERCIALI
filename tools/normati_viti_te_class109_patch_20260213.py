from __future__ import annotations

import argparse
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

TARGET_SUBS = ("HEPZ", "HETZ")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_viti_cl109.db"
    db.backup_to_path(str(out))
    return out


def fetch_vit(cur) -> Tuple[int, str]:
    cur.execute("SELECT id, code FROM category WHERE code='VIT'")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Categoria VIT non trovata.")
    return int(row["id"]), str(row["code"])


def fetch_subs(cur, category_id: int) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for code in TARGET_SUBS:
        code_n = normalize_gggg_normati(code)
        cur.execute(
            """
            SELECT sc.id,
                   sc.code,
                   sc.description,
                   sc.standard_id,
                   sc.desc_template,
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
            "code": str(row["code"]),
            "description": str(row["description"] or ""),
            "standard_id": int(row["standard_id"]) if row["standard_id"] is not None else None,
            "desc_template": str(row["desc_template"] or ""),
            "standard_code": str(row["standard_code"] or ""),
        }
    return out


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
    if not n:
        return s
    s = re.sub(
        r"\b(?:ISO|UNI|DIN)\s+[A-Z0-9./-]+\s+(?=M(?:__|\d+(?:[.,]\d+)?)X(?:__|\d+(?:[.,]\d+)?))",
        "",
        s,
    )
    return re.sub(
        r"\bM(?:__|\d+(?:[.,]\d+)?)X(?:__|\d+(?:[.,]\d+)?)(?:[.,]\d+)?\b",
        lambda m: f"{n} {m.group(0)}",
        s,
        count=1,
    )


def maybe_update_template(cur, sub_id: int, tpl: str, norm_ref: str) -> bool:
    old = normalize_upper(tpl)
    new = old.replace("CL 8.8", "CL __")
    new = ensure_norm_before_m(new, norm_ref)
    if new == old:
        return False
    cur.execute("UPDATE subcategory SET desc_template=? WHERE id=?", (new, int(sub_id)))
    return True


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
    subcategory_id: int,
    cat_code: str,
    sub_code: str,
    standard_id: int | None,
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
            (
                int(standard_id) if standard_id is not None else None,
                notes_n,
                now_str(),
                int(row["id"]),
            ),
        )
        return False

    next_seq = get_next_seq(cur, category_id, subcategory_id)
    code, seq = find_free_code(cur, cat_code, sub_code, next_seq)
    cur.execute(
        """
        INSERT INTO item(code, category_id, subcategory_id, standard_id, seq, description, notes, is_active, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            code,
            int(category_id),
            int(subcategory_id),
            int(standard_id) if standard_id is not None else None,
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
        backup = backup_db(db)
        print(f"Backup: {backup}")

        cur = db.conn.cursor()
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        category_id, cat_code = fetch_vit(cur)
        cat_code_n = normalize_mmm(cat_code)
        subs = fetch_subs(cur, category_id)

        template_updates = 0
        created = 0
        updated = 0
        per_sub_created: Dict[str, int] = {}
        per_sub_updated: Dict[str, int] = {}

        for sub_code in TARGET_SUBS:
            code_n = normalize_gggg_normati(sub_code)
            meta = subs[code_n]
            sub_id = int(meta["id"])  # type: ignore[index]
            std_id = meta["standard_id"]  # type: ignore[index]
            norm_ref = pick_primary_norm(str(meta["standard_code"]))  # type: ignore[index]
            if maybe_update_template(cur, sub_id, str(meta["desc_template"]), norm_ref):
                template_updates += 1

            cur.execute(
                """
                SELECT description, notes
                FROM item
                WHERE category_id=? AND subcategory_id=? AND description LIKE '%CL 8.8%'
                ORDER BY seq, id
                """,
                (int(category_id), int(sub_id)),
            )
            base_rows = cur.fetchall()

            c_sub = 0
            u_sub = 0
            for r in base_rows:
                base_desc = normalize_upper(str(r["description"] or ""))
                base_notes = normalize_upper(str(r["notes"] or ""))
                new_desc = base_desc.replace("CL 8.8", "CL 10.9")
                new_desc = ensure_norm_before_m(new_desc, norm_ref)
                new_notes = base_notes.replace("CL 8.8", "CL 10.9")
                if "ISO 898-1" in new_notes and "CL 10.9" not in new_notes:
                    new_notes = f"{new_notes} CL 10.9"
                was_created = ensure_item(
                    cur=cur,
                    category_id=category_id,
                    subcategory_id=sub_id,
                    cat_code=cat_code_n,
                    sub_code=code_n,
                    standard_id=int(std_id) if std_id is not None else None,
                    description=new_desc,
                    notes=new_notes,
                )
                if was_created:
                    created += 1
                    c_sub += 1
                else:
                    updated += 1
                    u_sub += 1

            per_sub_created[code_n] = c_sub
            per_sub_updated[code_n] = u_sub

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Subcategory templates updated: {template_updates}")
        print(f"Items created (CL 10.9): {created}")
        print(f"Items updated (already existed): {updated}")
        print("Details:")
        for k in sorted(per_sub_created.keys()):
            print(f"  - {k}: created={per_sub_created[k]} updated={per_sub_updated[k]}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Add class 10.9 variants to VIT zincated hex-head screws.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
