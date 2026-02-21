# Unificati Manager — V9

App Python con interfaccia grafica (CustomTkinter) per la gestione e codifica di:
- Commerciali **normati** (categorie MMM, sotto-categorie GGGG, progressivo 0000)
- Commerciali **non normati** (categorie CCCC, sotto-categorie SSSS, progressivo 0000)
- **Materiali - Semilavorati** (gestione materiali, trattamenti, semilavorati)

## Novità principali
- Tab principale **"Materiali - Semilavorati"** con sotto-tab:
  - **Materiali**: anagrafica + proprietà parametriche (chimiche/fisiche/meccaniche)
  - **Trattamenti termici e superficiali** (2 box separati)
  - **Semilavorati**: gestione semilavorati **senza codifica** (per ora)
- Link **opzionale** tra **Semilavorati ↔ Materiali**
- Proprietà materiali con **valori multipli per stato** (stati semilavorati)
- Tutti i DB dell’app sono in `unificati_manager/database/`
- Refactoring: `app.py` è entrypoint, logica divisa in moduli (`db.py`, `ui_*.py`, ecc.)

## Avvio
```bash
pip install -r requirements.txt
python -m unificati_manager.app
```

In alternativa:
```bash
python unificati_manager/app.py
```

## Struttura
- `unificati_manager/database/` → contiene `unificati_manager.db` (creato al primo avvio)
- `unificati_manager/main_app.py` → costruzione GUI e tab principali
- `unificati_manager/db.py` → schema + CRUD
- `unificati_manager/ui_normati.py` → Commerciali Normati
- `unificati_manager/ui_commerciali.py` → Commerciali (non normati)
- `unificati_manager/ui_materiali.py` → Materiali / Trattamenti / Semilavorati

## Future Multiuser Prep
- Service layer: `unificati_manager/services.py` is now the UI access point to data operations.
- Local backups: automatic DB backups are stored in `unificati_manager/backups/`.
- Backup policy is configurable in `unificati_manager/config.py` via `BACKUP_*` and `AUTO_BACKUP_*` settings.
- Materiali, trattamenti e semilavorati: nessun codice manuale richiesto (codice interno automatico).
- Materiali gestiti per FAMIGLIA + SOTTOFAMIGLIA/STATO.
- Semilavorati con materiale selezionabile dai materiali e gestione FAMIGLIA + STATO.
