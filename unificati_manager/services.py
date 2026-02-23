from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Set

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

_SCOPE_MAIN = "MAIN"
_SCOPE_NORMATI = "NORMATI"
_SCOPE_COMMERCIALI = "COMMERCIALI"
_SCOPE_MATERIALI = "MATERIALI"

_SCOPE_LABELS = {
    _SCOPE_MAIN: "TUTTE LE AREE",
    _SCOPE_NORMATI: "COMMERCIALI NORMATI",
    _SCOPE_COMMERCIALI: "COMMERCIALI",
    _SCOPE_MATERIALI: "MATERIALI - SEMILAVORATI",
}

_METHOD_SCOPE_MAP: Dict[str, str] = {
    # Normati
    "fetch_categories": _SCOPE_NORMATI,
    "fetch_standards": _SCOPE_NORMATI,
    "fetch_subcategories": _SCOPE_NORMATI,
    "get_next_seq": _SCOPE_NORMATI,
    "search_items": _SCOPE_NORMATI,
    "read_item": _SCOPE_NORMATI,
    "create_category": _SCOPE_NORMATI,
    "update_category": _SCOPE_NORMATI,
    "delete_category": _SCOPE_NORMATI,
    "create_standard": _SCOPE_NORMATI,
    "update_standard": _SCOPE_NORMATI,
    "delete_standard": _SCOPE_NORMATI,
    "create_subcategory": _SCOPE_NORMATI,
    "update_subcategory": _SCOPE_NORMATI,
    "delete_subcategory": _SCOPE_NORMATI,
    "create_item": _SCOPE_NORMATI,
    "update_item": _SCOPE_NORMATI,
    "delete_item": _SCOPE_NORMATI,
    # Commerciali
    "fetch_comm_categories": _SCOPE_COMMERCIALI,
    "fetch_comm_subcategories": _SCOPE_COMMERCIALI,
    "fetch_suppliers": _SCOPE_COMMERCIALI,
    "get_next_comm_seq": _SCOPE_COMMERCIALI,
    "search_comm_items": _SCOPE_COMMERCIALI,
    "read_comm_item": _SCOPE_COMMERCIALI,
    "create_comm_category": _SCOPE_COMMERCIALI,
    "update_comm_category": _SCOPE_COMMERCIALI,
    "delete_comm_category": _SCOPE_COMMERCIALI,
    "create_comm_subcategory": _SCOPE_COMMERCIALI,
    "update_comm_subcategory": _SCOPE_COMMERCIALI,
    "delete_comm_subcategory": _SCOPE_COMMERCIALI,
    "create_supplier": _SCOPE_COMMERCIALI,
    "update_supplier": _SCOPE_COMMERCIALI,
    "delete_supplier": _SCOPE_COMMERCIALI,
    "create_comm_item": _SCOPE_COMMERCIALI,
    "update_comm_item": _SCOPE_COMMERCIALI,
    "delete_comm_item": _SCOPE_COMMERCIALI,
    # Materiali / Semilavorati / Trattamenti
    "fetch_material_families": _SCOPE_MATERIALI,
    "fetch_material_subfamilies": _SCOPE_MATERIALI,
    "create_material_family": _SCOPE_MATERIALI,
    "update_material_family": _SCOPE_MATERIALI,
    "delete_material_family": _SCOPE_MATERIALI,
    "create_material_subfamily": _SCOPE_MATERIALI,
    "update_material_subfamily": _SCOPE_MATERIALI,
    "delete_material_subfamily": _SCOPE_MATERIALI,
    "ensure_default_material_properties": _SCOPE_MATERIALI,
    "ensure_default_material_properties_all": _SCOPE_MATERIALI,
    "ensure_material_taxonomy_entry": _SCOPE_MATERIALI,
    "search_materials": _SCOPE_MATERIALI,
    "read_material": _SCOPE_MATERIALI,
    "create_material": _SCOPE_MATERIALI,
    "update_material": _SCOPE_MATERIALI,
    "delete_material": _SCOPE_MATERIALI,
    "fetch_material_properties": _SCOPE_MATERIALI,
    "read_material_property_notes": _SCOPE_MATERIALI,
    "create_material_property": _SCOPE_MATERIALI,
    "update_material_property": _SCOPE_MATERIALI,
    "delete_material_property": _SCOPE_MATERIALI,
    "fetch_heat_treatments": _SCOPE_MATERIALI,
    "fetch_surface_treatments": _SCOPE_MATERIALI,
    "read_heat_treatment": _SCOPE_MATERIALI,
    "read_surface_treatment": _SCOPE_MATERIALI,
    "create_heat_treatment": _SCOPE_MATERIALI,
    "update_heat_treatment": _SCOPE_MATERIALI,
    "delete_heat_treatment": _SCOPE_MATERIALI,
    "create_surface_treatment": _SCOPE_MATERIALI,
    "update_surface_treatment": _SCOPE_MATERIALI,
    "delete_surface_treatment": _SCOPE_MATERIALI,
    "fetch_semi_types": _SCOPE_MATERIALI,
    "fetch_semi_states": _SCOPE_MATERIALI,
    "create_semi_type": _SCOPE_MATERIALI,
    "update_semi_type": _SCOPE_MATERIALI,
    "delete_semi_type": _SCOPE_MATERIALI,
    "create_semi_state": _SCOPE_MATERIALI,
    "update_semi_state": _SCOPE_MATERIALI,
    "delete_semi_state": _SCOPE_MATERIALI,
    "search_semi_items": _SCOPE_MATERIALI,
    "fetch_semis_by_material": _SCOPE_MATERIALI,
    "read_semi_item": _SCOPE_MATERIALI,
    "create_semi_item": _SCOPE_MATERIALI,
    "update_semi_item": _SCOPE_MATERIALI,
    "delete_semi_item": _SCOPE_MATERIALI,
    "fetch_semi_dimensions": _SCOPE_MATERIALI,
    "create_semi_dimension": _SCOPE_MATERIALI,
    "update_semi_dimension": _SCOPE_MATERIALI,
    "delete_semi_dimension": _SCOPE_MATERIALI,
    "clone_semi_dimensions": _SCOPE_MATERIALI,
    "read_material_density_g_cm3": _SCOPE_MATERIALI,
    "calculate_semi_weight_per_m": _SCOPE_MATERIALI,
    # Manuale (ospitato su DB Normati)
    "fetch_manual_versions": _SCOPE_NORMATI,
    "read_manual_version": _SCOPE_NORMATI,
    "create_manual_version": _SCOPE_NORMATI,
    "update_manual_version": _SCOPE_NORMATI,
    "delete_manual_version": _SCOPE_NORMATI,
}

_WRITE_METHODS: Set[str] = {
    name
    for name in _METHOD_SCOPE_MAP
    if name.startswith("create_") or name.startswith("update_") or name.startswith("delete_")
}
_WRITE_METHODS.update(
    {
        "ensure_default_material_properties",
        "ensure_default_material_properties_all",
        "ensure_material_taxonomy_entry",
        "clone_semi_dimensions",
    }
)


def _normalize_scope(scope: Optional[str]) -> str:
    raw = (scope or _SCOPE_MAIN).strip().upper()
    if raw in {_SCOPE_MAIN, _SCOPE_NORMATI, _SCOPE_COMMERCIALI, _SCOPE_MATERIALI}:
        return raw
    return _SCOPE_MAIN


def _scope_label(scope: str) -> str:
    key = _normalize_scope(scope)
    return _SCOPE_LABELS.get(key, key)


class AppService:
    """Service layer che instrada le chiamate verso il DB corretto per area."""

    def __init__(
        self,
        db_normati: Database,
        db_commerciali: Database,
        db_materiali: Database,
        editor_scope: Optional[str] = None,
    ) -> None:
        self._db_normati = db_normati
        self._db_commerciali = db_commerciali
        self._db_materiali = db_materiali
        self._db_by_scope = {
            _SCOPE_NORMATI: self._db_normati,
            _SCOPE_COMMERCIALI: self._db_commerciali,
            _SCOPE_MATERIALI: self._db_materiali,
        }
        self._editor_scope = _normalize_scope(editor_scope)
        self._active_db = self._db_by_scope.get(self._editor_scope, self._db_normati)

    def _db_for_scope(self, scope: str) -> Database:
        key = _normalize_scope(scope)
        if key == _SCOPE_NORMATI:
            return self._db_normati
        if key == _SCOPE_COMMERCIALI:
            return self._db_commerciali
        if key == _SCOPE_MATERIALI:
            return self._db_materiali
        return self._active_db

    def __getattr__(self, name: str) -> Any:
        target_scope = _METHOD_SCOPE_MAP.get(name)
        target_db = self._db_for_scope(target_scope or _SCOPE_MAIN)
        target = getattr(target_db, name)
        if not callable(target):
            return target

        if name not in _WRITE_METHODS:
            return target

        @wraps(target)
        def guarded(*args, **kwargs):
            self._assert_scope_for_write(name, target_scope or _SCOPE_MAIN)
            return target(*args, **kwargs)

        return guarded

    @property
    def db_path(self) -> str:
        return self._active_db.path

    @property
    def db_paths(self) -> Dict[str, str]:
        return {
            _SCOPE_NORMATI: self._db_normati.path,
            _SCOPE_COMMERCIALI: self._db_commerciali.path,
            _SCOPE_MATERIALI: self._db_materiali.path,
        }

    @property
    def editor_scope(self) -> str:
        return self._editor_scope

    def close(self) -> None:
        seen: Set[int] = set()
        for db in (self._db_normati, self._db_commerciali, self._db_materiali):
            if id(db) in seen:
                continue
            seen.add(id(db))
            db.close()

    def _assert_scope_for_write(self, method_name: str, required_scope: str) -> None:
        target_db = self._db_for_scope(required_scope)
        if target_db.is_read_only:
            raise PermissionError("Sessione in sola lettura: scrittura non consentita.")
        if self._editor_scope in {_SCOPE_MAIN, required_scope}:
            return
        raise PermissionError(
            "Permesso negato per area editor. "
            f"Sessione: {_scope_label(self._editor_scope)}. "
            f"Operazione {method_name} consentita solo su {_scope_label(required_scope)}."
        )

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

        outputs: List[str] = []
        plan = [
            ("normati", self._db_normati),
            ("commerciali", self._db_commerciali),
            ("materiali", self._db_materiali),
        ]
        for suffix, db in plan:
            filename = f"{BACKUP_FILE_PREFIX}_{stamp}_{tag}_{suffix}.db"
            out_path = os.path.join(get_backup_dir(), filename)
            db.backup_to_path(out_path)
            outputs.append(out_path)

        self._prune_backups()
        return ";".join(outputs)

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
        # Ogni backup logico produce 3 file (normati/commerciali/materiali).
        keep = max(1, int(BACKUP_KEEP_LAST)) * 3
        paths = self._list_backup_paths()
        if len(paths) <= keep:
            return
        paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for old_path in paths[keep:]:
            try:
                os.remove(old_path)
            except OSError:
                pass
