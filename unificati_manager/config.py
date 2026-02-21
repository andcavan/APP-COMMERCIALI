from __future__ import annotations

import os

APP_NAME = "Unificati Manager"

DB_FILENAME = "unificati_manager.db"
DB_FOLDER = "database"

BACKUP_FOLDER = "backups"
BACKUP_FILE_PREFIX = "unificati_manager_backup"
BACKUP_KEEP_LAST = 30
BACKUP_INTERVAL_HOURS = 24
AUTO_BACKUP_ON_STARTUP = True
AUTO_BACKUP_ON_CLOSE = False

# Modalita multiutente (writer unico con lock heartbeat).
WRITER_LOCK_TIMEOUT_SECONDS = 120
WRITER_HEARTBEAT_SECONDS = 20

# Seed automatico anagrafiche all'avvio DB.
# Per lasciare vuoti normati/commerciali impostare a False.
SEED_NORMATI_DEFAULTS = False
SEED_COMMERCIALI_DEFAULTS = False
SEED_SUPPLIERS_DEFAULTS = False

DATE_FMT = "%Y-%m-%d %H:%M:%S"


def get_app_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def get_db_dir() -> str:
    return os.path.join(get_app_dir(), DB_FOLDER)


def get_db_path() -> str:
    return os.path.join(get_db_dir(), DB_FILENAME)


def get_backup_dir() -> str:
    return os.path.join(get_app_dir(), BACKUP_FOLDER)
