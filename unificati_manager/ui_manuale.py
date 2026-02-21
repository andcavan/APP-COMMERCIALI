from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import customtkinter as ctk
from tkinter import messagebox, ttk

from .services import AppService
from .ui_utils import make_treeview_sortable


def _row_str(r: Any, key: str, default: str = "") -> str:
    try:
        v = r[key]
    except Exception:
        v = getattr(r, key, default)
    return "" if v is None else str(v)


class ManualeTab(ctk.CTkFrame):
    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db
        self.entry_id: Optional[int] = None
        self._rows_by_iid: Dict[str, Any] = {}

        self.var_search = ctk.StringVar(value="")
        self.var_version = ctk.StringVar(value="")
        self.var_release_date = ctk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))

        self._build_ui()
        self.refresh_list()
        self.new_entry()

    def _build_ui(self) -> None:
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        outer = ttk.Panedwindow(self, orient="horizontal")
        outer.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        left = ctk.CTkFrame(outer)
        left.grid_rowconfigure(2, weight=1)
        left.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(left, text="Manuale versioni", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        sbar = ctk.CTkFrame(left, fg_color="transparent")
        sbar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        sbar.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(sbar, text="Cerca").grid(row=0, column=0, padx=(0, 6))
        ent_search = ctk.CTkEntry(sbar, textvariable=self.var_search)
        ent_search.grid(row=0, column=1, sticky="ew")
        ent_search.bind("<Return>", lambda _e: self.refresh_list())
        ctk.CTkButton(sbar, text="Aggiorna", width=90, command=self.refresh_list).grid(row=0, column=2, padx=(6, 0))

        lf = ctk.CTkFrame(left)
        lf.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        lf.grid_rowconfigure(0, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        cols = ("VERSIONE", "DATA", "AGG", "AGGIORNAMENTI")
        self.tree = ttk.Treeview(lf, columns=cols, show="headings", height=14)
        cfg = {
            "VERSIONE": (120, "w", False),
            "DATA": (110, "w", False),
            "AGG": (150, "w", False),
            "AGGIORNAMENTI": (520, "w", True),
        }
        for c in cols:
            self.tree.heading(c, text=c)
            w, a, s = cfg[c]
            self.tree.column(c, width=w, anchor=a, stretch=s)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        make_treeview_sortable(self.tree)

        right = ctk.CTkFrame(outer)
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(right, text="Dettaglio versione", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        form = ctk.CTkFrame(right)
        form.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        form.grid_columnconfigure(0, weight=1)
        form.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(form, text="VERSIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(form, textvariable=self.var_version).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        ctk.CTkLabel(form, text="DATA RILASCIO (YYYY-MM-DD)").grid(row=0, column=1, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(form, textvariable=self.var_release_date).grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        ctk.CTkLabel(right, text="Aggiornamenti", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=2, column=0, sticky="w", padx=8, pady=(0, 4)
        )
        self.txt_updates = ctk.CTkTextbox(right, wrap="word")
        self.txt_updates.grid(row=3, column=0, sticky="nsew", padx=8, pady=(0, 8))

        btns = ctk.CTkFrame(right, fg_color="transparent")
        btns.grid(row=4, column=0, sticky="e", padx=8, pady=(0, 8))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_entry).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_entry).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_entry).pack(side="left", padx=4)

        outer.add(left, weight=2)
        outer.add(right, weight=3)

    def refresh_list(self) -> None:
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        rows = self.db.fetch_manual_versions(self.var_search.get())
        self._rows_by_iid = {}
        for r in rows:
            iid = str(r["id"])
            self._rows_by_iid[iid] = r
            updates_full = _row_str(r, "updates").replace("\n", " ").strip()
            updates_preview = updates_full[:140] + ("..." if len(updates_full) > 140 else "")
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    _row_str(r, "version"),
                    _row_str(r, "release_date"),
                    _row_str(r, "updated_at"),
                    updates_preview,
                ),
            )

    def new_entry(self) -> None:
        self.entry_id = None
        self.var_version.set("")
        self.var_release_date.set(datetime.now().strftime("%Y-%m-%d"))
        self.txt_updates.delete("1.0", "end")

    def _on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        row = self._rows_by_iid.get(iid)
        if not row:
            return
        full = self.db.read_manual_version(int(row["id"]))
        self.entry_id = int(full["id"])
        self.var_version.set(_row_str(full, "version"))
        self.var_release_date.set(_row_str(full, "release_date"))
        self.txt_updates.delete("1.0", "end")
        self.txt_updates.insert("1.0", _row_str(full, "updates"))

    def _select_item_row_if_present(self, entry_id: Optional[int]) -> None:
        if entry_id is None:
            return
        iid = str(entry_id)
        if self.tree.exists(iid):
            self.tree.selection_set(iid)
            self.tree.focus(iid)
            self.tree.see(iid)

    def save_entry(self) -> None:
        version = (self.var_version.get() or "").strip()
        release_date = (self.var_release_date.get() or "").strip()
        updates = (self.txt_updates.get("1.0", "end") or "").strip()
        if not version:
            messagebox.showwarning("Manuale", "Compila VERSIONE.")
            return
        if not release_date:
            messagebox.showwarning("Manuale", "Compila DATA RILASCIO.")
            return
        try:
            datetime.strptime(release_date, "%Y-%m-%d")
        except ValueError:
            messagebox.showwarning("Manuale", "Formato data non valido. Usa YYYY-MM-DD.")
            return
        if not updates:
            messagebox.showwarning("Manuale", "Compila AGGIORNAMENTI.")
            return
        try:
            if self.entry_id is None:
                self.entry_id = self.db.create_manual_version(version, release_date, updates)
                messagebox.showinfo("Manuale", "Versione creata.")
            else:
                self.db.update_manual_version(self.entry_id, version, release_date, updates)
                messagebox.showinfo("Manuale", "Versione aggiornata.")
            self.refresh_list()
            self._select_item_row_if_present(self.entry_id)
        except Exception as e:
            messagebox.showerror("Manuale", f"Errore salvataggio: {e}")

    def delete_entry(self) -> None:
        if self.entry_id is None:
            return
        if not messagebox.askyesno("Manuale", "Eliminare la versione selezionata?"):
            return
        try:
            self.db.delete_manual_version(self.entry_id)
            self.new_entry()
            self.refresh_list()
        except Exception as e:
            messagebox.showerror("Manuale", f"Errore eliminazione: {e}")
