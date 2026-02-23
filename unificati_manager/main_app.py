from __future__ import annotations

import os

try:
    import customtkinter as ctk
except Exception as e:
    raise SystemExit(
        "Questa app usa 'customtkinter'. Installa con:\n\n"
        "  pip install -r requirements.txt\n\n"
        f"Dettagli import: {e}"
    )
from tkinter import messagebox

from .my_style_01.style import STYLE_NAME, apply_style
from .my_style_01.ttk import configure_treeview_style

from .config import (
    APP_NAME,
    WRITER_HEARTBEAT_SECONDS,
    WRITER_LOCK_TIMEOUT_SECONDS,
    get_backup_dir,
    get_commerciali_db_path,
    get_db_dir,
    get_legacy_db_path,
    get_materiali_db_path,
    get_normati_db_path,
)
from .db import Database
from .services import AppService
from .ui_commerciali import CommercialArticlesTab, CommercialCodingTab, SuppliersTab
from .ui_manuale import ManualeTab
from .ui_materiali import MaterialsTab, SemilavoratiTab, TreatmentsTab
from .ui_normati import NormatiArticlesTab, NormatiCodingTab
from .utils import ensure_dir

EDITOR_SCOPE_CHOICES = (
    ("NORMATI", "COMMERCIALI NORMATI"),
    ("COMMERCIALI", "COMMERCIALI"),
    ("MATERIALI", "MATERIALI - SEMILAVORATI"),
)
EDITOR_SCOPE_LABELS = {code: label for code, label in EDITOR_SCOPE_CHOICES}
EDITOR_SCOPE_VALUES = [code for code, _label in EDITOR_SCOPE_CHOICES]


def _scope_label(scope: str) -> str:
    key = (scope or "").strip().upper()
    if key == "MAIN":
        return "TUTTE LE AREE"
    return EDITOR_SCOPE_LABELS.get(key, key or "SCONOSCIUTO")


class RoleLoginDialog(ctk.CTkToplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Accesso")
        self.geometry("520x260")
        self.minsize(520, 260)
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        default_user = (os.environ.get("USERNAME") or os.environ.get("USER") or "").strip()
        self.var_user = ctk.StringVar(value=default_user)
        self.var_role = ctk.StringVar(value="READER")
        self.var_writer_scope = ctk.StringVar(value=EDITOR_SCOPE_VALUES[0])
        self.result = None

        self.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(self, text="Login ruolo", font=ctk.CTkFont(size=17, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=14, pady=(14, 8)
        )

        form = ctk.CTkFrame(self)
        form.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 8))
        form.grid_columnconfigure(0, weight=1)
        form.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(form, text="Utente").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 2))
        self.ent_user = ctk.CTkEntry(form, textvariable=self.var_user)
        self.ent_user.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))

        ctk.CTkLabel(form, text="Ruolo").grid(row=0, column=1, sticky="w", padx=8, pady=(8, 2))
        self.om_role = ctk.CTkOptionMenu(
            form,
            variable=self.var_role,
            values=["READER", "EDITOR"],
            command=self._on_role_changed,
        )
        self.om_role.grid(row=1, column=1, sticky="ew", padx=8, pady=(0, 8))

        ctk.CTkLabel(form, text="Area editor").grid(row=2, column=0, sticky="w", padx=8, pady=(2, 2))
        self.om_scope = ctk.CTkOptionMenu(form, variable=self.var_writer_scope, values=EDITOR_SCOPE_VALUES)
        self.om_scope.grid(row=3, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 8))
        ctk.CTkLabel(
            form,
            text="NORMATI = Commerciali Normati | COMMERCIALI = Commerciali | MATERIALI = Materiali-Semilavorati",
        ).grid(row=4, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=2, column=0, sticky="e", padx=14, pady=(0, 14))
        ctk.CTkButton(btns, text="Annulla", width=90, command=self._cancel).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Entra", width=90, command=self._confirm).pack(side="left", padx=4)

        self.bind("<Return>", lambda _e: self._confirm())
        self.bind("<Escape>", lambda _e: self._cancel())
        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self.after(20, self.ent_user.focus_set)
        self._on_role_changed(self.var_role.get())

    def _confirm(self):
        user = (self.var_user.get() or "").strip()
        role = (self.var_role.get() or "READER").strip().upper()
        if not user:
            messagebox.showwarning("Accesso", "Compila utente.", parent=self)
            return
        is_editor = role == "EDITOR"
        self.result = {
            "user": user,
            "role": "editor" if is_editor else "reader",
            "writer_scope": self.var_writer_scope.get() if is_editor else None,
        }
        self.destroy()

    def _on_role_changed(self, role_value: str) -> None:
        role = (role_value or "").strip().upper()
        state = "normal" if role == "EDITOR" else "disabled"
        self.om_scope.configure(state=state)

    def _cancel(self):
        self.result = None
        self.destroy()


class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        self.is_dark_mode = True
        self.palette = apply_style(dark=self.is_dark_mode)
        self.title(f"{APP_NAME} - {STYLE_NAME}")
        self.geometry("1550x900")
        self.minsize(1250, 720)
        configure_treeview_style(self, self.palette)

        self.session_user = ""
        self.session_role = "reader"
        self.writer_scope = "MAIN"
        self._writer_heartbeat_job = None
        self._writer_heartbeat_seconds = max(5, int(WRITER_HEARTBEAT_SECONDS))

        ensure_dir(get_db_dir())
        ensure_dir(get_backup_dir())
        self._bootstrap_split_databases()

        session = self._resolve_session_mode()
        if session is None:
            self.after(0, self.destroy)
            return
        self.session_user = session["user"]
        self.session_role = session["role"]
        self.writer_scope = str(session.get("writer_scope") or "MAIN")
        writer_token = session.get("writer_token")
        writer_db_path = str(session.get("writer_db_path") or "")

        mode_normati = "ro"
        mode_commerciali = "ro"
        mode_materiali = "ro"
        if self.session_role == "editor":
            if self.writer_scope == "NORMATI":
                mode_normati = "rw"
            elif self.writer_scope == "COMMERCIALI":
                mode_commerciali = "rw"
            elif self.writer_scope == "MATERIALI":
                mode_materiali = "rw"

        try:
            self.db_normati = Database(
                get_normati_db_path(),
                db_profile="NORMATI",
                access_mode=mode_normati,
                session_role="editor" if mode_normati == "rw" else "reader",
                writer_holder=self.session_user,
                writer_lock_token=writer_token if writer_db_path and os.path.abspath(writer_db_path) == os.path.abspath(get_normati_db_path()) else None,
                writer_lock_scope="MAIN",
                writer_lock_timeout_seconds=WRITER_LOCK_TIMEOUT_SECONDS,
            )
            self.db_commerciali = Database(
                get_commerciali_db_path(),
                db_profile="COMMERCIALI",
                access_mode=mode_commerciali,
                session_role="editor" if mode_commerciali == "rw" else "reader",
                writer_holder=self.session_user,
                writer_lock_token=writer_token if writer_db_path and os.path.abspath(writer_db_path) == os.path.abspath(get_commerciali_db_path()) else None,
                writer_lock_scope="MAIN",
                writer_lock_timeout_seconds=WRITER_LOCK_TIMEOUT_SECONDS,
            )
            self.db_materiali = Database(
                get_materiali_db_path(),
                db_profile="MATERIALI",
                access_mode=mode_materiali,
                session_role="editor" if mode_materiali == "rw" else "reader",
                writer_holder=self.session_user,
                writer_lock_token=writer_token if writer_db_path and os.path.abspath(writer_db_path) == os.path.abspath(get_materiali_db_path()) else None,
                writer_lock_scope="MAIN",
                writer_lock_timeout_seconds=WRITER_LOCK_TIMEOUT_SECONDS,
            )
        except Exception as e:
            messagebox.showerror("Avvio", f"Impossibile aprire il database: {e}", parent=self)
            self.after(0, self.destroy)
            return

        if self.writer_scope == "COMMERCIALI":
            self.db = self.db_commerciali
        elif self.writer_scope == "MATERIALI":
            self.db = self.db_materiali
        else:
            self.db = self.db_normati

        self.service = AppService(
            self.db_normati,
            self.db_commerciali,
            self.db_materiali,
            editor_scope=self.writer_scope,
        )
        if self.session_role == "editor":
            try:
                self.service.create_periodic_backup("startup")
            except Exception:
                pass

        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        self._build_ui()
        self._apply_read_only_ui()
        self._start_writer_heartbeat()

    def toggle_theme(self) -> None:
        """Cambia tra modalità dark e light."""
        self.is_dark_mode = not self.is_dark_mode
        self.palette = apply_style(dark=self.is_dark_mode)
        configure_treeview_style(self, self.palette)

        # Distrugge i widget esistenti prima di ricostruire
        for child in self.winfo_children():
            child.destroy()

        # Ricostruisce l'interfaccia per applicare il nuovo tema
        self._build_ui()
        self._apply_read_only_ui()

    def _ask_login(self):
        dlg = RoleLoginDialog(self)
        self.wait_window(dlg)
        return dlg.result

    def _db_path_for_scope(self, scope: str) -> str:
        key = (scope or "").strip().upper()
        if key == "COMMERCIALI":
            return get_commerciali_db_path()
        if key == "MATERIALI":
            return get_materiali_db_path()
        return get_normati_db_path()

    def _bootstrap_split_databases(self) -> None:
        Database.bootstrap_split_databases(
            get_legacy_db_path(),
            get_normati_db_path(),
            get_commerciali_db_path(),
            get_materiali_db_path(),
        )

    def _resolve_session_mode(self):
        while True:
            login = self._ask_login()
            if login is None:
                return None
            if login["role"] == "reader":
                return {
                    "user": login["user"],
                    "role": "reader",
                    "writer_token": None,
                    "writer_scope": None,
                    "writer_db_path": None,
                }
            requested_scope = str(login.get("writer_scope") or EDITOR_SCOPE_VALUES[0]).upper()
            db_path = self._db_path_for_scope(requested_scope)
            try:
                lock = Database.try_acquire_writer_lock(
                    db_path,
                    holder=login["user"],
                    timeout_seconds=WRITER_LOCK_TIMEOUT_SECONDS,
                    lock_scope="MAIN",
                )
            except Exception as e:
                messagebox.showerror("Accesso", f"Errore lock writer: {e}", parent=self)
                continue
            if bool(lock.get("acquired")):
                return {
                    "user": login["user"],
                    "role": "editor",
                    "writer_token": lock.get("token"),
                    "writer_scope": requested_scope,
                    "writer_db_path": db_path,
                }
            holder = str(lock.get("holder") or "SCONOSCIUTO")
            hb = str(lock.get("heartbeat_at") or "-")
            to_reader = messagebox.askyesno(
                "Accesso",
                "Modalita EDITOR non disponibile per l'area selezionata.\n"
                f"Area richiesta: {_scope_label(requested_scope)}\n"
                f"Writer attivo: {holder}\n"
                f"Ultimo heartbeat: {hb}\n\n"
                "Aprire in sola lettura?",
                parent=self,
            )
            if to_reader:
                return {
                    "user": login["user"],
                    "role": "reader",
                    "writer_token": None,
                    "writer_scope": None,
                    "writer_db_path": None,
                }

    def _apply_read_only_ui(self) -> None:
        role_label = "READ-ONLY" if self.db.is_read_only else f"EDITOR:{_scope_label(self.writer_scope)}"
        self.title(f"{APP_NAME} - {STYLE_NAME} - {self.session_user} ({role_label})")
        if self.db.is_read_only:
            self._disable_write_buttons_recursive(self)
            return
        self._apply_editor_scope_restrictions()

    def _apply_editor_scope_restrictions(self) -> None:
        scope = (self.writer_scope or "").strip().upper()
        if scope in {"", "MAIN"}:
            return
        blocked_roots = []
        if scope != "NORMATI":
            blocked_roots.append(getattr(self, "tab_normati_root", None))
            blocked_roots.append(getattr(self, "tab_man_root", None))
        if scope != "COMMERCIALI":
            blocked_roots.append(getattr(self, "tab_comm_root", None))
        if scope != "MATERIALI":
            blocked_roots.append(getattr(self, "tab_mat_root", None))
        for root in blocked_roots:
            if root is not None:
                self._disable_write_buttons_recursive(root)

    def _is_area_visible(self, area_key: str) -> bool:
        if self.db.is_read_only:
            return True
        scope = (self.writer_scope or "").strip().upper()
        if scope in {"", "MAIN"}:
            return True
        key = (area_key or "").strip().upper()
        if key == "MANUALE":
            return True
        return key == scope

    def _disable_write_buttons_recursive(self, root):
        write_tokens = ("SALVA", "ELIMINA", "GESTISCI FAMIGLIE")
        for child in root.winfo_children():
            try:
                text = str(child.cget("text") or "").strip().upper()
            except Exception:
                text = ""
            if isinstance(child, ctk.CTkButton) and any(tok in text for tok in write_tokens):
                try:
                    child.configure(state="disabled")
                except Exception:
                    pass
            self._disable_write_buttons_recursive(child)

    def _start_writer_heartbeat(self) -> None:
        if self.db.is_read_only or self.session_role != "editor":
            return
        self._writer_heartbeat_job = self.after(
            self._writer_heartbeat_seconds * 1000,
            self._writer_heartbeat_tick,
        )

    def _writer_heartbeat_tick(self) -> None:
        self._writer_heartbeat_job = None
        ok = False
        try:
            ok = bool(self.db.heartbeat_writer_lock())
        except Exception:
            ok = False
        if not ok:
            messagebox.showerror(
                "Accesso",
                "Lock editor perso. L'app verra chiusa per evitare scritture concorrenti.",
                parent=self,
            )
            self.on_close()
            return
        self._start_writer_heartbeat()

    def _cancel_writer_heartbeat(self) -> None:
        if self._writer_heartbeat_job is None:
            return
        try:
            self.after_cancel(self._writer_heartbeat_job)
        except Exception:
            pass
        self._writer_heartbeat_job = None

    def _build_ui(self) -> None:
        # Frame superiore con pulsante tema
        header_frame = ctk.CTkFrame(self, fg_color="transparent")
        header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))

        theme_btn = ctk.CTkButton(
            header_frame,
            text="☀️ Light" if self.is_dark_mode else "🌙 Dark",
            width=100,
            command=self.toggle_theme
        )
        theme_btn.pack(side="right", padx=5)

        self.main_tabs = ctk.CTkTabview(self)
        self.main_tabs.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 10))

        self.tab_normati_root = None
        self.tab_comm_root = None
        self.tab_mat_root = None
        self.tab_man_root = None

        if self._is_area_visible("NORMATI"):
            tab_normati = self.main_tabs.add("Commerciali Normati")
            self.tab_normati_root = tab_normati
            subt_norm = ctk.CTkTabview(tab_normati)
            subt_norm.pack(fill="both", expand=True)
            norm_art = subt_norm.add("Articoli")
            norm_cod = subt_norm.add("Codifica")

            def norm_refs_changed():
                self.normati_articles.refresh_reference_data()

            self.normati_articles = NormatiArticlesTab(norm_art, self.service)
            self.normati_articles.pack(fill="both", expand=True)

            self.normati_coding = NormatiCodingTab(norm_cod, self.service, refs_changed_callback=norm_refs_changed)
            self.normati_coding.pack(fill="both", expand=True)

        if self._is_area_visible("COMMERCIALI"):
            tab_comm = self.main_tabs.add("Commerciali")
            self.tab_comm_root = tab_comm
            subt_comm = ctk.CTkTabview(tab_comm)
            subt_comm.pack(fill="both", expand=True)
            comm_forn = subt_comm.add("Fornitori")
            comm_art = subt_comm.add("Articoli")
            comm_cod = subt_comm.add("Codifica")

            def comm_refs_changed():
                self.comm_articles.refresh_reference_data()

            def comm_suppliers_changed():
                self.comm_articles.refresh_suppliers()

            self.suppliers_tab = SuppliersTab(comm_forn, self.service, suppliers_changed_callback=comm_suppliers_changed)
            self.suppliers_tab.pack(fill="both", expand=True)

            self.comm_articles = CommercialArticlesTab(comm_art, self.service)
            self.comm_articles.pack(fill="both", expand=True)

            self.comm_coding = CommercialCodingTab(comm_cod, self.service, refs_changed_callback=comm_refs_changed)
            self.comm_coding.pack(fill="both", expand=True)

        if self._is_area_visible("MATERIALI"):
            tab_mat = self.main_tabs.add("Materiali - Semilavorati")
            self.tab_mat_root = tab_mat
            subt_mat = ctk.CTkTabview(tab_mat)
            subt_mat.pack(fill="both", expand=True)
            tab_mats = subt_mat.add("Materiali")
            tab_tratt = subt_mat.add("Trattamenti termici e superficiali")
            tab_semi = subt_mat.add("Semilavorati")

            self.materials_tab = MaterialsTab(tab_mats, self.service)
            self.materials_tab.pack(fill="both", expand=True)

            self.treatments_tab = TreatmentsTab(tab_tratt, self.service)
            self.treatments_tab.pack(fill="both", expand=True)

            self.semi_tab = SemilavoratiTab(tab_semi, self.service)
            self.semi_tab.pack(fill="both", expand=True)

        if self._is_area_visible("MANUALE"):
            tab_man = self.main_tabs.add("Manuale")
            self.tab_man_root = tab_man
            self.manuale_tab = ManualeTab(tab_man, self.service)
            self.manuale_tab.pack(fill="both", expand=True)

    def on_close(self) -> None:
        try:
            self._cancel_writer_heartbeat()
            if hasattr(self, "db") and not self.db.is_read_only:
                try:
                    self.service.create_periodic_backup("close")
                except Exception:
                    pass
            if hasattr(self, "db"):
                try:
                    self.db.release_writer_lock()
                except Exception:
                    pass
            if hasattr(self, "service"):
                self.service.close()
        finally:
            self.destroy()


def main() -> None:
    App().mainloop()
