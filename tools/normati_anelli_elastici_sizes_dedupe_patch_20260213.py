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


@dataclass(frozen=True)
class SeriesDef:
    sub_code: str
    series: str
    seat: str
    material: str
    d_min: int
    d_max: int
    notes: str


CATEGORY_CODE = "AEL"

# Legacy seeded subcategories from defaults: replaced by detailed family split.
LEGACY_DUPLICATE_SUBS = ("S471", "S472")

# Generic seed items created in base patch (no diameter): replaced by tagged sizes.
GENERIC_ITEM_DESC_BY_SUB: Dict[str, str] = {
    "AABR": "ANELLO ELASTICO SERIE A PER ALBERO ACCIAIO BRUNITO",
    "AAIX": "ANELLO ELASTICO SERIE A PER ALBERO INOX",
    "ASBR": "ANELLO ELASTICO SERIE AS PER ALBERO ACCIAIO BRUNITO",
    "ASIX": "ANELLO ELASTICO SERIE AS PER ALBERO INOX",
    "AVBR": "ANELLO ELASTICO SERIE AV PER ALBERO ACCIAIO BRUNITO",
    "AVIX": "ANELLO ELASTICO SERIE AV PER ALBERO INOX",
    "AKBR": "ANELLO ELASTICO SERIE AK PER ALBERO ACCIAIO BRUNITO",
    "AKIX": "ANELLO ELASTICO SERIE AK PER ALBERO INOX",
    "JFBR": "ANELLO ELASTICO SERIE J PER FORO ACCIAIO BRUNITO",
    "JFIX": "ANELLO ELASTICO SERIE J PER FORO INOX",
    "JSBR": "ANELLO ELASTICO SERIE JS PER FORO ACCIAIO BRUNITO",
    "JSIX": "ANELLO ELASTICO SERIE JS PER FORO INOX",
    "JVBR": "ANELLO ELASTICO SERIE JV PER FORO ACCIAIO BRUNITO",
    "JVIX": "ANELLO ELASTICO SERIE JV PER FORO INOX",
    "JKBR": "ANELLO ELASTICO SERIE JK PER FORO ACCIAIO BRUNITO",
    "JKIX": "ANELLO ELASTICO SERIE JK PER FORO INOX",
}

SERIES: List[SeriesDef] = [
    SeriesDef(
        sub_code="AABR",
        series="A",
        seat="ALBERO",
        material="ACCIAIO BRUNITO",
        d_min=3,
        d_max=300,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7435 -> DIN 471. "
            "SERIE A PER ALBERO. RANGE IMPOSTATO D3-D300."
        ),
    ),
    SeriesDef(
        sub_code="AAIX",
        series="A",
        seat="ALBERO",
        material="INOX",
        d_min=3,
        d_max=300,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7435 -> DIN 471. "
            "SERIE A PER ALBERO INOX. RANGE IMPOSTATO D3-D300."
        ),
    ),
    SeriesDef(
        sub_code="ASBR",
        series="AS",
        seat="ALBERO",
        material="ACCIAIO BRUNITO",
        d_min=10,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7436 -> DIN 471 SPESSORE MAGGIORATO. "
            "SERIE AS PER ALBERO. RANGE IMPOSTATO D10-D100."
        ),
    ),
    SeriesDef(
        sub_code="ASIX",
        series="AS",
        seat="ALBERO",
        material="INOX",
        d_min=10,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7436 -> DIN 471 SPESSORE MAGGIORATO. "
            "SERIE AS PER ALBERO INOX. RANGE IMPOSTATO D10-D100."
        ),
    ),
    SeriesDef(
        sub_code="AVBR",
        series="AV",
        seat="ALBERO",
        material="ACCIAIO BRUNITO",
        d_min=12,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO AV (COMMERCIALE). "
            "SERIE AV PER ALBERO. RANGE IMPOSTATO D12-D100."
        ),
    ),
    SeriesDef(
        sub_code="AVIX",
        series="AV",
        seat="ALBERO",
        material="INOX",
        d_min=12,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO AV (COMMERCIALE). "
            "SERIE AV PER ALBERO INOX. RANGE IMPOSTATO D12-D100."
        ),
    ),
    SeriesDef(
        sub_code="AKBR",
        series="AK",
        seat="ALBERO",
        material="ACCIAIO BRUNITO",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 983. "
            "SERIE AK PER ALBERO. RANGE IMPOSTATO D16-D100."
        ),
    ),
    SeriesDef(
        sub_code="AKIX",
        series="AK",
        seat="ALBERO",
        material="INOX",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 983. "
            "SERIE AK PER ALBERO INOX. RANGE IMPOSTATO D16-D100."
        ),
    ),
    SeriesDef(
        sub_code="JFBR",
        series="J",
        seat="FORO",
        material="ACCIAIO BRUNITO",
        d_min=8,
        d_max=300,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7437 -> DIN 472. "
            "SERIE J PER FORO. RANGE IMPOSTATO D8-D300."
        ),
    ),
    SeriesDef(
        sub_code="JFIX",
        series="J",
        seat="FORO",
        material="INOX",
        d_min=8,
        d_max=300,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7437 -> DIN 472. "
            "SERIE J PER FORO INOX. RANGE IMPOSTATO D8-D300."
        ),
    ),
    SeriesDef(
        sub_code="JSBR",
        series="JS",
        seat="FORO",
        material="ACCIAIO BRUNITO",
        d_min=14,
        d_max=110,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7438 -> DIN 472 SPESSORE MAGGIORATO. "
            "SERIE JS PER FORO. RANGE IMPOSTATO D14-D110."
        ),
    ),
    SeriesDef(
        sub_code="JSIX",
        series="JS",
        seat="FORO",
        material="INOX",
        d_min=14,
        d_max=110,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI 7438 -> DIN 472 SPESSORE MAGGIORATO. "
            "SERIE JS PER FORO INOX. RANGE IMPOSTATO D14-D110."
        ),
    ),
    SeriesDef(
        sub_code="JVBR",
        series="JV",
        seat="FORO",
        material="ACCIAIO BRUNITO",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO JV (COMMERCIALE). "
            "SERIE JV PER FORO. RANGE IMPOSTATO D16-D100."
        ),
    ),
    SeriesDef(
        sub_code="JVIX",
        series="JV",
        seat="FORO",
        material="INOX",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> TIPO JV (COMMERCIALE). "
            "SERIE JV PER FORO INOX. RANGE IMPOSTATO D16-D100."
        ),
    ),
    SeriesDef(
        sub_code="JKBR",
        series="JK",
        seat="FORO",
        material="ACCIAIO BRUNITO",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 984. "
            "SERIE JK PER FORO. RANGE IMPOSTATO D16-D100."
        ),
    ),
    SeriesDef(
        sub_code="JKIX",
        series="JK",
        seat="FORO",
        material="INOX",
        d_min=16,
        d_max=100,
        notes=(
            "GERARCHIA NORME: ISO N/A -> UNI N/A -> DIN 984. "
            "SERIE JK PER FORO INOX. RANGE IMPOSTATO D16-D100."
        ),
    ),
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_db(db: Database) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"unificati_manager_backup_{stamp}_normati_anelli_sizes_dedupe.db"
    db.backup_to_path(str(out))
    return out


def fetch_category(cur, code: str) -> Tuple[int, str]:
    c = normalize_mmm(code)
    cur.execute("SELECT id, code FROM category WHERE code=?", (c,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Categoria non trovata: {code}")
    return int(row["id"]), str(row["code"])


def fetch_sub_map(cur, category_id: int) -> Dict[str, Dict[str, str]]:
    cur.execute(
        """
        SELECT sc.id,
               sc.code,
               COALESCE(sc.standard_id, 0) AS standard_id,
               COALESCE(st.code, '') AS standard_code
        FROM subcategory sc
        LEFT JOIN standard st ON st.id=sc.standard_id
        WHERE sc.category_id=?
        """,
        (int(category_id),),
    )
    out: Dict[str, Dict[str, str]] = {}
    for r in cur.fetchall():
        out[str(r["code"])] = {
            "id": int(r["id"]),
            "standard_id": int(r["standard_id"]),
            "standard_code": str(r["standard_code"] or ""),
        }
    return out


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

    # Gerarchia richiesta: ISO > UNI > DIN.
    for prefix in ("ISO", "UNI", "DIN"):
        m = re.search(rf"\b{prefix}\s*[A-Z0-9./-]+", s)
        if m:
            return normalize_upper(m.group(0))
    return s


def build_base_desc(cfg: SeriesDef, diameter: int) -> str:
    return normalize_upper(f"ANELLO ELASTICO SERIE {cfg.series} PER {cfg.seat} D{diameter} {cfg.material}")


def build_desc(cfg: SeriesDef, diameter: int, norm_ref: str) -> str:
    base = build_base_desc(cfg, diameter)
    n = normalize_upper(norm_ref or "")
    if not n:
        return base
    return normalize_upper(f"{base} [{n}]")


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
    desc_base = normalize_upper(base_description)
    desc_n = normalize_upper(final_description)
    notes_n = normalize_upper(notes)
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
            desc_base,
            f"{desc_base} [%]",
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
            (int(standard_id), desc_n, notes_n, now_str(), int(row["id"])),
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
            int(standard_id) if int(standard_id) > 0 else None,
            int(seq),
            desc_n,
            notes_n,
            now_str(),
            now_str(),
        ),
    )
    return True


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


def delete_generic_item(cur, category_id: int, subcategory_id: int, description: str) -> int:
    cur.execute(
        """
        DELETE FROM item
        WHERE category_id=? AND subcategory_id=? AND description=?
        """,
        (int(category_id), int(subcategory_id), normalize_upper(description)),
    )
    return int(cur.rowcount)


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
    dup_groups = cur.fetchall()
    removed = 0
    for g in dup_groups:
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


def patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        backup_path = backup_db(db)
        print(f"Backup: {backup_path}")

        cur = db.conn.cursor()
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        cat_id, cat_code = fetch_category(cur, CATEGORY_CODE)
        cat_code = normalize_mmm(cat_code)

        removed_legacy_subs = 0
        removed_legacy_items = 0
        for sub in LEGACY_DUPLICATE_SUBS:
            removed_items, removed_sub = delete_legacy_subcategory(cur, cat_id, sub)
            if removed_sub:
                removed_legacy_subs += 1
            removed_legacy_items += removed_items

        sub_map = fetch_sub_map(cur, cat_id)

        # Remove generic no-size items before loading dimensional series.
        removed_generic = 0
        for code, generic_desc in GENERIC_ITEM_DESC_BY_SUB.items():
            sub = sub_map.get(normalize_gggg_normati(code))
            if not sub:
                continue
            removed_generic += delete_generic_item(cur, cat_id, int(sub["id"]), generic_desc)

        created_total = 0
        updated_total = 0
        deduped_total = 0
        per_sub: Dict[str, Tuple[int, int]] = {}

        for cfg in SERIES:
            scode = normalize_gggg_normati(cfg.sub_code)
            sub = sub_map.get(scode)
            if not sub:
                raise RuntimeError(f"Sotto-categoria mancante in AEL: {cfg.sub_code}")
            sub_id = int(sub["id"])
            std_id = int(sub["standard_id"])
            norm_ref = pick_primary_norm(str(sub.get("standard_code") or ""))

            # Aggiorna template descrizione sotto-categoria con norma in coda.
            tpl = normalize_upper(
                f"ANELLO ELASTICO SERIE {cfg.series} PER {cfg.seat} D__ {cfg.material}"
            )
            if norm_ref:
                tpl = normalize_upper(f"{tpl} [{norm_ref}]")
            cur.execute(
                "UPDATE subcategory SET desc_template=? WHERE id=?",
                (tpl, sub_id),
            )

            created_sub = 0
            updated_sub = 0

            for d in range(int(cfg.d_min), int(cfg.d_max) + 1):
                base_desc = build_base_desc(cfg, d)
                desc = build_desc(cfg, d, norm_ref)
                created = ensure_item(
                    cur=cur,
                    category_id=cat_id,
                    cat_code=cat_code,
                    subcategory_id=sub_id,
                    sub_code=scode,
                    standard_id=std_id,
                    base_description=base_desc,
                    final_description=desc,
                    notes=cfg.notes,
                )
                if created:
                    created_sub += 1
                else:
                    updated_sub += 1

            deduped_sub = dedupe_sub_items(cur, cat_id, sub_id)
            deduped_total += deduped_sub
            created_total += created_sub
            updated_total += updated_sub
            per_sub[scode] = (created_sub, updated_sub)

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Legacy subcategories removed: {removed_legacy_subs}")
        print(f"Legacy items removed: {removed_legacy_items}")
        print(f"Generic no-size items removed: {removed_generic}")
        print(f"Items created total: {created_total}")
        print(f"Items updated total: {updated_total}")
        print(f"Duplicate items removed: {deduped_total}")
        print("Details by subcategory:")
        for k in sorted(per_sub.keys()):
            c, u = per_sub[k]
            print(f"  - {k}: created={c} updated={u}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Populate AEL retaining rings with diameter series and remove legacy duplicates."
    )
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
