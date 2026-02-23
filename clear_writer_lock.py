"""
Script per verificare e liberare lock writer nei database area-specifici.
Usare questo script quando non si riesce ad entrare come EDITOR.
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from unificati_manager.config import (
    get_commerciali_db_path,
    get_legacy_db_path,
    get_materiali_db_path,
    get_normati_db_path,
)

DATE_FMT = "%Y-%m-%d %H:%M:%S"

DB_TARGETS = {
    "N": ("NORMATI", Path(get_normati_db_path())),
    "C": ("COMMERCIALI", Path(get_commerciali_db_path())),
    "M": ("MATERIALI", Path(get_materiali_db_path())),
    "L": ("LEGACY", Path(get_legacy_db_path())),
}


def _load_lock_rows(db_path: Path) -> List[sqlite3.Row]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='app_writer_lock'")
        if not cur.fetchone():
            return []
        cur.execute(
            """
            SELECT lock_key, holder, token, acquired_at, heartbeat_at
            FROM app_writer_lock
            ORDER BY lock_key
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def _print_lock_rows(db_code: str, db_name: str, db_path: Path, rows: List[sqlite3.Row]) -> None:
    if not rows:
        return
    print(f"\n[{db_code}] {db_name} -> {db_path}")
    for row in rows:
        lock_key = str(row["lock_key"] or "")
        holder = str(row["holder"] or "")
        acquired_at = str(row["acquired_at"] or "")
        heartbeat_at = str(row["heartbeat_at"] or "")
        token = str(row["token"] or "")
        print(f"  Lock [{lock_key}] holder={holder} acquired={acquired_at} hb={heartbeat_at} token={token}")
        try:
            hb_dt = datetime.strptime(heartbeat_at, DATE_FMT)
            age_seconds = (datetime.now() - hb_dt).total_seconds()
            print(f"    Eta: {age_seconds:.1f} sec")
        except Exception:
            pass


def _delete_lock(db_path: Path, lock_key: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        if lock_key.upper() == "ALL":
            cur.execute("DELETE FROM app_writer_lock")
        else:
            cur.execute("DELETE FROM app_writer_lock WHERE UPPER(lock_key)=UPPER(?)", (lock_key,))
        conn.commit()
        return int(cur.rowcount)
    finally:
        conn.close()


def _existing_targets() -> Dict[str, Tuple[str, Path]]:
    out: Dict[str, Tuple[str, Path]] = {}
    for code, (name, path) in DB_TARGETS.items():
        if path.exists():
            out[code] = (name, path)
    return out


def main():
    targets = _existing_targets()
    if not targets:
        print("Nessun database trovato.")
        return

    found_any = False
    for code, (name, path) in targets.items():
        rows = _load_lock_rows(path)
        _print_lock_rows(code, name, path, rows)
        if rows:
            found_any = True

    if not found_any:
        print("Nessun lock writer attivo.")
        return

    print("\nComandi:")
    print("  ALL                       -> rimuove tutti i lock da tutti i DB trovati")
    print("  <DB>:ALL                  -> rimuove tutti i lock del DB (es. N:ALL)")
    print("  <DB>:<LOCK_KEY>           -> rimuove una lock_key nel DB (es. C:MAIN)")
    print("  Invio vuoto               -> annulla")
    cmd = input("\nInserisci comando: ").strip()
    if not cmd:
        print("Operazione annullata.")
        return

    if cmd.upper() == "ALL":
        total = 0
        for _code, (_name, path) in targets.items():
            total += _delete_lock(path, "ALL")
        print(f"Lock rimossi: {total}")
        return

    if ":" not in cmd:
        print("Formato non valido.")
        return
    db_code, lock_key = [p.strip() for p in cmd.split(":", 1)]
    db_code = db_code.upper()
    if db_code not in targets:
        print(f"DB non valido: {db_code}")
        return
    _name, path = targets[db_code]
    removed = _delete_lock(path, lock_key or "ALL")
    print(f"Lock rimossi su {db_code}: {removed}")


if __name__ == "__main__":
    main()
