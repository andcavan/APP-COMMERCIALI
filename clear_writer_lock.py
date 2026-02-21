"""
Script per verificare e liberare il lock writer nel database.
Usare questo script quando non si riesce ad entrare come EDITOR.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "unificati_manager" / "database" / "unificati_manager.db"
DATE_FMT = "%Y-%m-%d %H:%M:%S"

def main():
    if not DB_PATH.exists():
        print(f"Database non trovato: {DB_PATH}")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    try:
        # Verifica se esiste la tabella
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='app_writer_lock'")
        if not cur.fetchone():
            print("Tabella app_writer_lock non esiste ancora. Nessun lock da pulire.")
            return
        
        # Controlla il lock attuale
        cur.execute("SELECT holder, token, acquired_at, heartbeat_at FROM app_writer_lock WHERE lock_key='MAIN'")
        row = cur.fetchone()
        
        if not row:
            print("Nessun lock writer attivo.")
            return
        
        holder = row["holder"]
        acquired_at = row["acquired_at"]
        heartbeat_at = row["heartbeat_at"]
        token = row["token"]
        
        print(f"\nLock writer attivo:")
        print(f"  Holder: {holder}")
        print(f"  Acquisito: {acquired_at}")
        print(f"  Ultimo heartbeat: {heartbeat_at}")
        print(f"  Token: {token}")
        
        # Calcola età del lock
        try:
            hb_dt = datetime.strptime(heartbeat_at, DATE_FMT)
            age_seconds = (datetime.now() - hb_dt).total_seconds()
            print(f"  Età: {age_seconds:.1f} secondi")
        except Exception:
            print("  Età: non calcolabile")
        
        # Chiedi conferma per rimuovere
        print("\nVuoi rimuovere il lock? (s/n): ", end="")
        risposta = input().strip().lower()
        
        if risposta == 's':
            cur.execute("DELETE FROM app_writer_lock WHERE lock_key='MAIN'")
            conn.commit()
            print("\n✓ Lock rimosso con successo!")
            print("Ora puoi accedere come EDITOR.")
        else:
            print("\nOperazione annullata.")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
