from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, List, Optional

from .config import (
    AUTO_BACKUP_ON_CLOSE,
    AUTO_BACKUP_ON_STARTUP,
    BACKUP_FILE_PREFIX,
    BACKUP_INTERVAL_HOURS,
    BACKUP_KEEP_LAST,
    get_backup_dir,
)
from .db import Database
from .utils import ensure_dir


class AppService:
    """Service layer to keep UI decoupled from the storage implementation."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def __getattr__(self, name: str) -> Any:
        # Delegate existing DB methods to keep current UI code stable.
        return getattr(self._db, name)

    @property
    def db_path(self) -> str:
        return self._db.path

    def close(self) -> None:
        self._db.close()

    def create_periodic_backup(self, reason: str, force: bool = False) -> Optional[str]:
        reason_key = (reason or "").strip().lower()
        if not force:
            if reason_key == "startup" and not AUTO_BACKUP_ON_STARTUP:
                return None
            if reason_key in {"close", "shutdown"} and not AUTO_BACKUP_ON_CLOSE:
                return None

            last = self._latest_backup_path()
            if last:
                min_hours = max(1, int(BACKUP_INTERVAL_HOURS))
                age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(last))
                if age < timedelta(hours=min_hours):
                    return None

        return self.create_backup(reason_key or "auto")

    def create_backup(self, reason: str = "manual") -> str:
        ensure_dir(get_backup_dir())
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        tag = re.sub(r"[^a-z0-9_-]+", "_", (reason or "manual").lower()).strip("_") or "manual"
        filename = f"{BACKUP_FILE_PREFIX}_{stamp}_{tag}.db"
        out_path = os.path.join(get_backup_dir(), filename)
        self._db.backup_to_path(out_path)
        self._prune_backups()
        return out_path

    def _list_backup_paths(self) -> List[str]:
        bdir = get_backup_dir()
        if not os.path.isdir(bdir):
            return []
        names = [
            n for n in os.listdir(bdir)
            if n.startswith(BACKUP_FILE_PREFIX + "_") and n.endswith(".db")
        ]
        return [os.path.join(bdir, n) for n in names]

    def _latest_backup_path(self) -> Optional[str]:
        paths = self._list_backup_paths()
        if not paths:
            return None
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return paths[0]

    def _prune_backups(self) -> None:
        keep = max(1, int(BACKUP_KEEP_LAST))
        paths = self._list_backup_paths()
        if len(paths) <= keep:
            return
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for old_path in paths[keep:]:
            try:
                os.remove(old_path)
            except OSError:
                pass
