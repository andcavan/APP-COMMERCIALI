from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional

import customtkinter as ctk
from tkinter import ttk, messagebox

from .config import APP_NAME
from .services import AppService
from .ui_utils import bind_uppercase, make_treeview_sortable
from .codifica import normalize_mmm, normalize_gggg_normati, is_valid_mmm, is_valid_gggg_normati


class NormatiArticlesTab(ctk.CTkFrame):
    def __init__(self, master, db: AppService) -> None:
        super().__init__(master)
        self.db = db
        self.current_item_id: Optional[int] = None
        self.current_seq: Optional[int] = None
        self._suspend_template_fill: bool = False

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        header = ctk.CTkFrame(self, corner_radius=0)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(header, text="Commerciali Normati — Articoli", font=ctk.CTkFont(size=18, weight="bold")).grid(row=0, column=0, sticky="w", padx=14, pady=10)

        content_paned = ttk.Panedwindow(self, orient="horizontal")
        content_paned.grid(row=1, column=0, sticky="nsew")

        left_wrap = ctk.CTkFrame(content_paned, corner_radius=0, fg_color="transparent")
        right_wrap = ctk.CTkFrame(content_paned, corner_radius=0, fg_color="transparent")
        content_paned.add(left_wrap, weight=2)
        content_paned.add(right_wrap, weight=3)

        left = ctk.CTkFrame(left_wrap, corner_radius=0)
        left.pack(fill="both", expand=True, padx=(14, 7), pady=14)
        left.grid_columnconfigure(0, weight=1)
        left.grid_rowconfigure(3, weight=1)

        self.q_var = ctk.StringVar(value="")
        self.var_only_preferred = ctk.BooleanVar(value=False)
        self.var_filter_cat = ctk.StringVar(value="TUTTE")
        self.var_filter_sub = ctk.StringVar(value="TUTTE")

        search = ctk.CTkEntry(
            left,
            textvariable=self.q_var,
            placeholder_text='Cerca (AND). Es: m5x5 inox oppure "m5x5"...',
        )
        search.grid(row=0, column=0, sticky="ew", padx=14, pady=(14, 8))
        search.bind("<Return>", lambda _e: self.refresh_list())

        filters = ctk.CTkFrame(left, fg_color="transparent")
        filters.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 8))
        filters.grid_columnconfigure((0, 1, 2), weight=1)
        self.om_filter_cat = ctk.CTkOptionMenu(filters, variable=self.var_filter_cat, values=["TUTTE"], command=self.on_list_filter_cat_changed)
        self.om_filter_cat.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.om_filter_sub = ctk.CTkOptionMenu(filters, variable=self.var_filter_sub, values=["TUTTE"], command=lambda _v=None: self.refresh_list())
        self.om_filter_sub.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        ctk.CTkCheckBox(filters, text="Solo preferiti", variable=self.var_only_preferred, command=self.refresh_list).grid(row=0, column=2, sticky="w")

        ctk.CTkButton(left, text="Aggiorna", command=self.refresh_list).grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 8))

        tree_wrap = ctk.CTkFrame(left, corner_radius=0, fg_color="transparent")
        tree_wrap.grid(row=3, column=0, sticky="nsew", padx=14, pady=(0, 14))
        tree_wrap.grid_columnconfigure(0, weight=1)
        tree_wrap.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_wrap, columns=("pref", "code", "cat", "sub", "desc", "upd"), show="headings", selectmode="browse")
        for col, title, w, anch in [
            ("pref", "Pref", 50, "center"),
            ("code", "Codice", 150, "w"),
            ("cat", "Cat", 60, "center"),
            ("sub", "Sub", 70, "center"),
            ("desc", "Descrizione", 360, "w"),
            ("upd", "Agg.", 120, "w"),
        ]:
            self.tree.heading(col, text=title)
            self.tree.column(col, width=w, anchor=anch)
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self.on_select)
        make_treeview_sortable(self.tree)

        right = ctk.CTkScrollableFrame(right_wrap, corner_radius=0, fg_color="transparent")
        right.pack(fill="both", expand=True, padx=(7, 14), pady=14)
        right.grid_columnconfigure(0, weight=1)

        card = ctk.CTkFrame(right, corner_radius=16)
        card.grid(row=0, column=0, sticky="ew", padx=0, pady=(0, 14))
        card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(card, text="Inserimento / Modifica", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", padx=14, pady=(14, 6))

        self.var_cat = ctk.StringVar(value="—")
        self.var_sub = ctk.StringVar(value="—")
        self.var_code = ctk.StringVar(value="—")

        ctk.CTkLabel(card, text="Categoria (3 numeri)").grid(row=1, column=0, sticky="w", padx=14)
        self.om_cat = ctk.CTkOptionMenu(card, variable=self.var_cat, values=["—"], command=self.on_cat_changed)
        self.om_cat.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 10))

        ctk.CTkLabel(card, text="Sotto-categoria (4 numeri)").grid(row=3, column=0, sticky="w", padx=14)
        self.om_sub = ctk.CTkOptionMenu(card, variable=self.var_sub, values=["—"], command=self.on_sub_changed)
        self.om_sub.grid(row=4, column=0, sticky="ew", padx=14, pady=(0, 10))

        ctk.CTkLabel(card, text="Codice articolo (finale)").grid(row=5, column=0, sticky="w", padx=14)
        entry_code = ctk.CTkEntry(card, textvariable=self.var_code)
        entry_code.configure(state="readonly")
        entry_code.grid(row=6, column=0, sticky="ew", padx=14, pady=(0, 10))

        self.var_desc = ctk.StringVar(value="")
        bind_uppercase(self.var_desc)
        ctk.CTkLabel(card, text="Descrizione").grid(row=7, column=0, sticky="w", padx=14)
        self.entry_desc = ctk.CTkEntry(card, textvariable=self.var_desc)
        self.entry_desc.grid(row=8, column=0, sticky="ew", padx=14, pady=(0, 10))

        self.var_preferred = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(card, text="Preferito", variable=self.var_preferred).grid(row=9, column=0, sticky="w", padx=14, pady=(0, 10))

        ctk.CTkLabel(card, text="Note").grid(row=10, column=0, sticky="w", padx=14)
        self.txt_notes = ctk.CTkTextbox(card, height=120, corner_radius=12)
        self.txt_notes.grid(row=11, column=0, sticky="ew", padx=14, pady=(0, 10))

        btns = ctk.CTkFrame(card, fg_color="transparent")
        btns.grid(row=12, column=0, sticky="ew", padx=14, pady=(0, 14))
        btns.grid_columnconfigure((0, 1, 2, 3, 4), weight=1)
        ctk.CTkButton(btns, text="Genera codice", command=self.generate_code).grid(row=0, column=0, sticky="ew", padx=(0, 10))
        ctk.CTkButton(btns, text="Nuovo", command=self.new_item).grid(row=0, column=1, sticky="ew", padx=(0, 10))
        ctk.CTkButton(btns, text="Copia", command=self.copy_item).grid(row=0, column=2, sticky="ew", padx=(0, 10))
        ctk.CTkButton(btns, text="Salva", command=self.save_item).grid(row=0, column=3, sticky="ew", padx=(0, 10))
        ctk.CTkButton(btns, text="Elimina", fg_color="#ef4444", hover_color="#dc2626", command=self.delete_item).grid(row=0, column=4, sticky="ew")

        self._cats: List[sqlite3.Row] = []
        self._subs: List[sqlite3.Row] = []
        self._sub_by_label: Dict[str, sqlite3.Row] = {}
        self._rows_by_iid: Dict[str, sqlite3.Row] = {}
        self._list_filter_cat_by_label: Dict[str, sqlite3.Row] = {}
        self._list_filter_sub_by_label: Dict[str, sqlite3.Row] = {}

        self.refresh_reference_data()
        self.refresh_list()
        self.new_item()

    def refresh_reference_data(self) -> None:
        self._cats = self.db.fetch_categories()
        cat_values = [f"{c['code']} — {c['description']}" for c in self._cats] or ["—"]
        self.om_cat.configure(values=cat_values)
        if self._cats:
            if self.var_cat.get() not in cat_values:
                self.var_cat.set(cat_values[0])
            self.on_cat_changed(self.var_cat.get())
        else:
            self.var_cat.set("—")
            self.om_sub.configure(values=["—"])
            self.var_sub.set("—")
        self.refresh_list_filters()

    def refresh_list_filters(self) -> None:
        current_cat = self.var_filter_cat.get()
        current_sub = self.var_filter_sub.get()

        cat_values = ["TUTTE"]
        self._list_filter_cat_by_label = {}
        for c in self._cats:
            label = f"{c['code']} — {c['description']}"
            cat_values.append(label)
            self._list_filter_cat_by_label[label] = c
        self.om_filter_cat.configure(values=cat_values)
        if current_cat in cat_values:
            self.var_filter_cat.set(current_cat)
        else:
            self.var_filter_cat.set(cat_values[0])

        self._refresh_sub_filter_values(preferred=current_sub)

    def _refresh_sub_filter_values(self, preferred: Optional[str] = None) -> None:
        cat = self._list_filter_cat_by_label.get(self.var_filter_cat.get())
        sub_values = ["TUTTE"]
        self._list_filter_sub_by_label = {}
        if cat is not None:
            for sc in self.db.fetch_subcategories(int(cat["id"])):
                label = f"{sc['code']} — {sc['description']}"
                sub_values.append(label)
                self._list_filter_sub_by_label[label] = sc
        self.om_filter_sub.configure(values=sub_values)
        target = preferred if preferred is not None else self.var_filter_sub.get()
        if target in sub_values:
            self.var_filter_sub.set(target)
        else:
            self.var_filter_sub.set(sub_values[0])

    def on_list_filter_cat_changed(self, _val: str) -> None:
        self._refresh_sub_filter_values()
        self.refresh_list()

    def _get_selected_cat(self) -> Optional[sqlite3.Row]:
        label = (self.var_cat.get() or "").strip()
        if "—" not in label:
            return None
        code = label.split("—", 1)[0].strip()
        return next((c for c in self._cats if c["code"] == code), None)

    def on_cat_changed(self, _val: str) -> None:
        cat = self._get_selected_cat()
        if not cat:
            self._subs = []
            self._sub_by_label = {}
            self.om_sub.configure(values=["—"])
            self.var_sub.set("—")
            return
        self._subs = self.db.fetch_subcategories(int(cat["id"]))
        sub_values = []
        self._sub_by_label = {}
        for sc in self._subs:
            label = f"{sc['code']} — {sc['description']}"
            sub_values.append(label)
            self._sub_by_label[label] = sc
        if not sub_values:
            sub_values = ["—"]
        self.om_sub.configure(values=sub_values)
        if self.var_sub.get() not in sub_values:
            self.var_sub.set(sub_values[0])
        self.on_sub_changed(self.var_sub.get())

    def on_sub_changed(self, _val: str) -> None:
        if self._suspend_template_fill:
            return
        if self.var_desc.get().strip():
            return
        sc = self._sub_by_label.get(self.var_sub.get())
        if not sc:
            return
        tpl = (sc["desc_template"] or "").strip()
        if tpl:
            self.var_desc.set(tpl)

    def refresh_list(self) -> None:
        cat = self._list_filter_cat_by_label.get(self.var_filter_cat.get())
        sc = self._list_filter_sub_by_label.get(self.var_filter_sub.get())
        rows = self.db.search_items(
            self.q_var.get(),
            category_id=int(cat["id"]) if cat is not None else None,
            subcategory_id=int(sc["id"]) if sc is not None else None,
            only_preferred=bool(self.var_only_preferred.get()),
        )
        for i in self.tree.get_children():
            self.tree.delete(i)
        self._rows_by_iid = {}
        for r in rows:
            iid = str(r["id"])
            self._rows_by_iid[iid] = r
            pref = "X" if int(r["preferred"] or 0) else ""
            self.tree.insert("", "end", iid=iid, values=(pref, r["code"], r["cat_code"], r["sub_code"], r["description"], r["updated_at"]))

    def new_item(self) -> None:
        self.current_item_id = None
        self.current_seq = None
        self.var_code.set("—")
        self.var_desc.set("")
        self.var_preferred.set(False)
        self.txt_notes.delete("1.0", "end")
        self.refresh_reference_data()

    def on_select(self, _evt=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        row = self._rows_by_iid.get(sel[0])
        if not row:
            return
        full = self.db.read_item(int(row["id"]))
        self._load_item_to_form(full, as_new=False)

    def _load_item_to_form(self, full: sqlite3.Row, as_new: bool) -> None:
        self._suspend_template_fill = True
        try:
            if as_new:
                self.current_item_id = None
                self.current_seq = None
                self.var_code.set("—")
            else:
                self.current_item_id = int(full["id"])
                self.current_seq = int(full["seq"])
                self.var_code.set(full["code"])

            cat_label = f"{full['cat_code']} — {full['cat_desc']}"
            self.refresh_reference_data()
            if cat_label in self.om_cat.cget("values"):
                self.var_cat.set(cat_label)
                self.on_cat_changed(cat_label)

            sub_label = f"{full['sub_code']} — {full['sub_desc']}"
            if sub_label in self.om_sub.cget("values"):
                self.var_sub.set(sub_label)

            self.var_desc.set(full["description"] or "")
            self.var_preferred.set(bool(int(full["preferred"] or 0)))
            self.txt_notes.delete("1.0", "end")
            self.txt_notes.insert("1.0", full["notes"] or "")
        finally:
            self._suspend_template_fill = False

    def copy_item(self) -> None:
        if not self.current_item_id:
            messagebox.showinfo(APP_NAME, "Seleziona prima un articolo da copiare.")
            return
        full = self.db.read_item(self.current_item_id)
        self._load_item_to_form(full, as_new=True)
        messagebox.showinfo(APP_NAME, "Copia pronta: modifica i dati e premi Salva.\n\nIl codice verrà rigenerato.")

    def generate_code(self) -> None:
        self.on_sub_changed(self.var_sub.get())

        cat = self._get_selected_cat()
        if not cat:
            messagebox.showerror(APP_NAME, "Seleziona una categoria (3 numeri).")
            return
        sc = self._sub_by_label.get(self.var_sub.get())
        if not sc:
            messagebox.showerror(APP_NAME, "Seleziona una sotto-categoria (4 numeri).")
            return
        next_seq = self.db.get_next_seq(int(cat["id"]), int(sc["id"]))
        self.current_seq = next_seq
        self.var_code.set(f"{cat['code']}-{sc['code']}-{next_seq:04d}")

    def _collect_payload(self) -> Dict[str, Any]:
        cat = self._get_selected_cat()
        if not cat:
            raise ValueError("Categoria mancante")
        sc = self._sub_by_label.get(self.var_sub.get())
        if not sc:
            raise ValueError("Sotto-categoria mancante")

        standard_id = int(sc["standard_id"]) if sc["standard_id"] else None

        code = self.var_code.get().strip()
        if code == "—" or not code:
            self.generate_code()
            code = self.var_code.get().strip()

        if self.current_seq is None:
            m = re.search(r"-(\d{4})$", code)
            self.current_seq = int(m.group(1)) if m else 0

        desc = self.var_desc.get().strip()
        if not desc:
            raise ValueError("Descrizione mancante")

        return {
            "code": code,
            "category_id": int(cat["id"]),
            "subcategory_id": int(sc["id"]),
            "standard_id": standard_id,
            "seq": int(self.current_seq),
            "description": desc,
            "notes": self.txt_notes.get("1.0", "end").strip(),
            "preferred": 1 if self.var_preferred.get() else 0,
            "is_active": 1,
        }

    def save_item(self) -> None:
        try:
            payload = self._collect_payload()
            if self.current_item_id:
                self.db.update_item(self.current_item_id, payload)
            else:
                self.current_item_id = self.db.create_item(payload)
            self.refresh_list()
        except sqlite3.IntegrityError as e:
            messagebox.showerror(APP_NAME, f"Codice duplicato o vincolo violato.\n\n{e}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore salvataggio.\n\n{e}")

    def delete_item(self) -> None:
        if not self.current_item_id:
            return
        if not messagebox.askyesno(APP_NAME, "Eliminare definitivamente l'articolo selezionato?"):
            return
        self.db.delete_item(self.current_item_id)
        self.new_item()
        self.refresh_list()





class NormatiCodingTab(ctk.CTkFrame):
    def __init__(self, master, db: AppService, refs_changed_callback) -> None:
        super().__init__(master)
        self.db = db
        self.refs_changed_callback = refs_changed_callback

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.selected_category_id: Optional[int] = None
        self.selected_standard_id: Optional[int] = None
        self.selected_subcategory_id: Optional[int] = None

        # --- Resizable boxes via PanedWindows ---
        paned_v = ttk.Panedwindow(self, orient="vertical")
        paned_v.grid(row=0, column=0, sticky="nsew")

        top_wrap = ctk.CTkFrame(paned_v, corner_radius=0, fg_color="transparent")
        bot_wrap = ctk.CTkFrame(paned_v, corner_radius=0, fg_color="transparent")
        paned_v.add(top_wrap, weight=1)
        paned_v.add(bot_wrap, weight=2)

        paned_h = ttk.Panedwindow(top_wrap, orient="horizontal")
        paned_h.pack(fill="both", expand=True)

        cat_wrap = ctk.CTkFrame(paned_h, corner_radius=0, fg_color="transparent")
        std_wrap = ctk.CTkFrame(paned_h, corner_radius=0, fg_color="transparent")
        paned_h.add(cat_wrap, weight=1)
        paned_h.add(std_wrap, weight=1)

        # Categorie box
        self.box_cat = ctk.CTkFrame(cat_wrap, corner_radius=16)
        self.box_cat.pack(fill="both", expand=True, padx=(14, 7), pady=(14, 7))
        self.box_cat.grid_columnconfigure(0, weight=1)
        self.box_cat.grid_rowconfigure(2, weight=1)
        ctk.CTkLabel(self.box_cat, text="Categorie", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", padx=14, pady=(14, 6))

        self.var_cat_code = ctk.StringVar(value="")
        self.var_cat_desc = ctk.StringVar(value="")
        bind_uppercase(self.var_cat_code)
        bind_uppercase(self.var_cat_desc)

        form = ctk.CTkFrame(self.box_cat, fg_color="transparent")
        form.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 10))
        form.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkLabel(form, text="CODICE (3 numeri)").grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(form, text="DESCRIZIONE").grid(row=0, column=1, sticky="w")
        self.ent_cat_code = ctk.CTkEntry(form, textvariable=self.var_cat_code)
        self.ent_cat_desc = ctk.CTkEntry(form, textvariable=self.var_cat_desc)
        self.ent_cat_code.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        self.ent_cat_desc.grid(row=1, column=1, sticky="ew", padx=(0, 10))
        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=1, column=2, sticky="ew")
        btns.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkButton(btns, text="Nuovo", command=self.cat_new).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns, text="Salva", command=self.cat_save).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns, text="Elimina", fg_color="#ef4444", hover_color="#dc2626", command=self.cat_delete).grid(row=0, column=2, sticky="ew")

        cat_tree_wrap = ctk.CTkFrame(self.box_cat, corner_radius=0, fg_color="transparent")
        cat_tree_wrap.grid(row=2, column=0, sticky="nsew", padx=14, pady=(0, 14))
        cat_tree_wrap.grid_columnconfigure(0, weight=1)
        cat_tree_wrap.grid_rowconfigure(0, weight=1)

        self.tree_cat = ttk.Treeview(cat_tree_wrap, columns=("code", "desc"), show="headings", selectmode="browse")
        self.tree_cat.heading("code", text="CODICE")
        self.tree_cat.heading("desc", text="DESCRIZIONE")
        self.tree_cat.column("code", width=80, anchor="center")
        self.tree_cat.column("desc", width=260, anchor="w")
        self.tree_cat.grid(row=0, column=0, sticky="nsew")
        cat_tree_scroll = ttk.Scrollbar(cat_tree_wrap, orient="vertical", command=self.tree_cat.yview)
        self.tree_cat.configure(yscrollcommand=cat_tree_scroll.set)
        cat_tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree_cat.bind("<<TreeviewSelect>>", self.on_cat_select)
        make_treeview_sortable(self.tree_cat)

        # Norme box
        self.box_std = ctk.CTkFrame(std_wrap, corner_radius=16)
        self.box_std.pack(fill="both", expand=True, padx=(7, 14), pady=(14, 7))
        self.box_std.grid_columnconfigure(0, weight=1)
        self.box_std.grid_rowconfigure(2, weight=1)
        ctk.CTkLabel(self.box_std, text="Norme", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", padx=14, pady=(14, 6))

        self.var_std_code = ctk.StringVar(value="")
        self.var_std_desc = ctk.StringVar(value="")
        bind_uppercase(self.var_std_code)
        bind_uppercase(self.var_std_desc)

        form2 = ctk.CTkFrame(self.box_std, fg_color="transparent")
        form2.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 10))
        form2.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkLabel(form2, text="CODICE").grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(form2, text="DESCRIZIONE").grid(row=0, column=1, sticky="w")
        self.ent_std_code = ctk.CTkEntry(form2, textvariable=self.var_std_code)
        self.ent_std_desc = ctk.CTkEntry(form2, textvariable=self.var_std_desc)
        self.ent_std_code.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        self.ent_std_desc.grid(row=1, column=1, sticky="ew", padx=(0, 10))
        btns2 = ctk.CTkFrame(form2, fg_color="transparent")
        btns2.grid(row=1, column=2, sticky="ew")
        btns2.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkButton(btns2, text="Nuovo", command=self.std_new).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns2, text="Salva", command=self.std_save).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns2, text="Elimina", fg_color="#ef4444", hover_color="#dc2626", command=self.std_delete).grid(row=0, column=2, sticky="ew")

        std_tree_wrap = ctk.CTkFrame(self.box_std, corner_radius=0, fg_color="transparent")
        std_tree_wrap.grid(row=2, column=0, sticky="nsew", padx=14, pady=(0, 14))
        std_tree_wrap.grid_columnconfigure(0, weight=1)
        std_tree_wrap.grid_rowconfigure(0, weight=1)

        self.tree_std = ttk.Treeview(std_tree_wrap, columns=("code", "desc"), show="headings", selectmode="browse")
        self.tree_std.heading("code", text="CODICE")
        self.tree_std.heading("desc", text="DESCRIZIONE")
        self.tree_std.column("code", width=110, anchor="w")
        self.tree_std.column("desc", width=260, anchor="w")
        self.tree_std.grid(row=0, column=0, sticky="nsew")
        std_tree_scroll = ttk.Scrollbar(std_tree_wrap, orient="vertical", command=self.tree_std.yview)
        self.tree_std.configure(yscrollcommand=std_tree_scroll.set)
        std_tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree_std.bind("<<TreeviewSelect>>", self.on_std_select)
        make_treeview_sortable(self.tree_std)

        # Sotto-categorie box (bottom)
        self.box_sub = ctk.CTkFrame(bot_wrap, corner_radius=16)
        self.box_sub.pack(fill="both", expand=True, padx=14, pady=(7, 14))
        self.box_sub.grid_columnconfigure(0, weight=1)
        self.box_sub.grid_rowconfigure(3, weight=1)
        ctk.CTkLabel(self.box_sub, text="Sotto-categorie", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", padx=14, pady=(14, 6))

        self.var_sub_code = ctk.StringVar(value="")
        self.var_sub_desc = ctk.StringVar(value="")
        self.var_sub_std = ctk.StringVar(value="—")
        bind_uppercase(self.var_sub_code)
        bind_uppercase(self.var_sub_desc)
        bind_uppercase(self.var_sub_std)

        form3 = ctk.CTkFrame(self.box_sub, fg_color="transparent")
        form3.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 8))
        form3.grid_columnconfigure((0, 1, 2, 3), weight=1)
        ctk.CTkLabel(form3, text="CODICE (4 numeri)").grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(form3, text="DESCRIZIONE").grid(row=0, column=1, sticky="w")
        ctk.CTkLabel(form3, text="NORMA DI RIFERIMENTO").grid(row=0, column=2, sticky="w")
        self.ent_sub_code = ctk.CTkEntry(form3, textvariable=self.var_sub_code)
        self.ent_sub_desc = ctk.CTkEntry(form3, textvariable=self.var_sub_desc)
        self.om_sub_std = ctk.CTkOptionMenu(form3, variable=self.var_sub_std, values=["—"])
        self.ent_sub_code.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        self.ent_sub_desc.grid(row=1, column=1, sticky="ew", padx=(0, 10))
        self.om_sub_std.grid(row=1, column=2, sticky="ew", padx=(0, 10))
        btns3 = ctk.CTkFrame(form3, fg_color="transparent")
        btns3.grid(row=1, column=3, sticky="ew")
        btns3.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkButton(btns3, text="Nuovo", command=self.sub_new).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns3, text="Salva", command=self.sub_save).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        ctk.CTkButton(btns3, text="Elimina", fg_color="#ef4444", hover_color="#dc2626", command=self.sub_delete).grid(row=0, column=2, sticky="ew")

        tpl_row = ctk.CTkFrame(self.box_sub, fg_color="transparent")
        tpl_row.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 10))
        tpl_row.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(tpl_row, text="TEMPLATE DESCRIZIONE (da importare in Articoli)").grid(row=0, column=0, sticky="w")
        self.txt_sub_tpl = ctk.CTkTextbox(tpl_row, height=70, corner_radius=12)
        self.txt_sub_tpl.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        sub_tree_wrap = ctk.CTkFrame(self.box_sub, corner_radius=0, fg_color="transparent")
        sub_tree_wrap.grid(row=3, column=0, sticky="nsew", padx=14, pady=(0, 14))
        sub_tree_wrap.grid_columnconfigure(0, weight=1)
        sub_tree_wrap.grid_rowconfigure(0, weight=1)

        self.tree_sub = ttk.Treeview(sub_tree_wrap, columns=("code", "desc", "std"), show="headings", selectmode="browse")
        self.tree_sub.heading("code", text="CODICE")
        self.tree_sub.heading("desc", text="DESCRIZIONE")
        self.tree_sub.heading("std", text="NORMA")
        self.tree_sub.column("code", width=90, anchor="center")
        self.tree_sub.column("desc", width=520, anchor="w")
        self.tree_sub.column("std", width=140, anchor="w")
        self.tree_sub.grid(row=0, column=0, sticky="nsew")
        sub_tree_scroll = ttk.Scrollbar(sub_tree_wrap, orient="vertical", command=self.tree_sub.yview)
        self.tree_sub.configure(yscrollcommand=sub_tree_scroll.set)
        sub_tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree_sub.bind("<<TreeviewSelect>>", self.on_sub_select)
        make_treeview_sortable(self.tree_sub)

        self._cats: List[sqlite3.Row] = []
        self._stds: List[sqlite3.Row] = []
        self._subs: List[sqlite3.Row] = []
        self._std_by_code: Dict[str, int] = {}

        self.refresh_all()

    def refresh_all(self) -> None:
        self.refresh_categories()
        if self._cats:
            if not self.tree_cat.selection():
                self.tree_cat.selection_set(str(self._cats[0]["id"]))
            self.on_cat_select()
        else:
            self.selected_category_id = None
            self.refresh_standards()
            self.refresh_subcategories()
            self._rebuild_std_menu()

    def refresh_categories(self) -> None:
        self._cats = self.db.fetch_categories()
        for i in self.tree_cat.get_children():
            self.tree_cat.delete(i)
        for c in self._cats:
            self.tree_cat.insert("", "end", iid=str(c["id"]), values=(c["code"], c["description"]))

    def refresh_standards(self) -> None:
        self._stds = []
        for i in self.tree_std.get_children():
            self.tree_std.delete(i)
        if not self.selected_category_id:
            self._rebuild_std_menu()
            return
        self._stds = self.db.fetch_standards(self.selected_category_id)
        for s in self._stds:
            self.tree_std.insert("", "end", iid=str(s["id"]), values=(s["code"], s["description"]))
        self._rebuild_std_menu()

    def _rebuild_std_menu(self) -> None:
        values = ["—"] + [s["code"] for s in self._stds]
        self._std_by_code = {s["code"]: int(s["id"]) for s in self._stds}
        self.om_sub_std.configure(values=values)
        if self.var_sub_std.get() not in values:
            self.var_sub_std.set("—")

    def refresh_subcategories(self) -> None:
        self._subs = []
        for i in self.tree_sub.get_children():
            self.tree_sub.delete(i)
        if not self.selected_category_id:
            return
        self._subs = self.db.fetch_subcategories(self.selected_category_id)
        for sc in self._subs:
            self.tree_sub.insert("", "end", iid=str(sc["id"]), values=(sc["code"], sc["description"], sc["standard_code"] or ""))

    def on_cat_select(self, _evt=None) -> None:
        sel = self.tree_cat.selection()
        if not sel:
            return
        cid = int(sel[0])
        self.selected_category_id = cid
        row = next((c for c in self._cats if int(c["id"]) == cid), None)
        if row:
            self.var_cat_code.set(row["code"])
            self.var_cat_desc.set(row["description"])
            self.ent_cat_code.configure(state="disabled")
        else:
            self.ent_cat_code.configure(state="normal")

        self.refresh_standards()
        self.refresh_subcategories()
        self.std_new()
        self.sub_new()
        self.refs_changed_callback()

    def on_std_select(self, _evt=None) -> None:
        sel = self.tree_std.selection()
        if not sel:
            return
        sid = int(sel[0])
        self.selected_standard_id = sid
        row = next((s for s in self._stds if int(s["id"]) == sid), None)
        if row:
            self.var_std_code.set(row["code"])
            self.var_std_desc.set(row["description"])
            self.ent_std_code.configure(state="disabled")
        else:
            self.ent_std_code.configure(state="normal")

    def on_sub_select(self, _evt=None) -> None:
        sel = self.tree_sub.selection()
        if not sel:
            return
        scid = int(sel[0])
        self.selected_subcategory_id = scid
        row = next((s for s in self._subs if int(s["id"]) == scid), None)
        if row:
            self.var_sub_code.set(row["code"])
            self.var_sub_desc.set(row["description"])
            self.var_sub_std.set(row["standard_code"] or "—")
            self.ent_sub_code.configure(state="disabled")
            self.txt_sub_tpl.delete("1.0", "end")
            self.txt_sub_tpl.insert("1.0", row["desc_template"] or "")
        else:
            self.ent_sub_code.configure(state="normal")

    def cat_new(self) -> None:
        self.tree_cat.selection_remove(self.tree_cat.selection())
        self.selected_category_id = None
        self.var_cat_code.set("")
        self.var_cat_desc.set("")
        self.ent_cat_code.configure(state="normal")

    def cat_save(self) -> None:
        try:
            code = normalize_mmm(self.var_cat_code.get())
            desc = self.var_cat_desc.get().strip()
            if not desc:
                raise ValueError("Descrizione mancante.")
            if self.selected_category_id:
                self.db.update_category(self.selected_category_id, desc)
            else:
                if not is_valid_mmm(code):
                    raise ValueError("CODICE categoria non valido: servono 3 numeri.")
                self.db.create_category(code, desc)
            self.refresh_categories()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Codice categoria già esistente.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")

    def cat_delete(self) -> None:
        if not self.selected_category_id:
            return
        if not messagebox.askyesno(APP_NAME, "Eliminare la categoria selezionata?\n\nSe è collegata a norme/sotto-categorie/articoli, l'operazione può fallire."):
            return
        try:
            self.db.delete_category(self.selected_category_id)
            self.cat_new()
            self.refresh_all()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Impossibile eliminare: ci sono record collegati.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")

    def std_new(self) -> None:
        self.tree_std.selection_remove(self.tree_std.selection())
        self.selected_standard_id = None
        self.var_std_code.set("")
        self.var_std_desc.set("")
        self.ent_std_code.configure(state="normal")

    def std_save(self) -> None:
        if not self.selected_category_id:
            messagebox.showerror(APP_NAME, "Seleziona una categoria prima di inserire una norma.")
            return
        try:
            code = self.var_std_code.get().strip()
            desc = self.var_std_desc.get().strip()
            if not code:
                raise ValueError("Codice norma mancante.")
            if not desc:
                raise ValueError("Descrizione norma mancante.")
            if self.selected_standard_id:
                self.db.update_standard(self.selected_standard_id, desc)
            else:
                self.db.create_standard(self.selected_category_id, code, desc)
            self.refresh_standards()
            self.refresh_subcategories()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Codice norma già esistente per questa categoria.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")

    def std_delete(self) -> None:
        if not self.selected_standard_id:
            return
        if not messagebox.askyesno(APP_NAME, "Eliminare la norma selezionata?\n\nSe è collegata a sotto-categorie/articoli, l'operazione può fallire."):
            return
        try:
            self.db.delete_standard(self.selected_standard_id)
            self.std_new()
            self.refresh_standards()
            self.refresh_subcategories()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Impossibile eliminare: ci sono record collegati.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")

    def sub_new(self) -> None:
        self.tree_sub.selection_remove(self.tree_sub.selection())
        self.selected_subcategory_id = None
        self.var_sub_code.set("")
        self.var_sub_desc.set("")
        self.var_sub_std.set("—")
        self.ent_sub_code.configure(state="normal")
        self.txt_sub_tpl.delete("1.0", "end")

    def sub_save(self) -> None:
        if not self.selected_category_id:
            messagebox.showerror(APP_NAME, "Seleziona una categoria prima di inserire una sotto-categoria.")
            return
        try:
            code = normalize_gggg_normati(self.var_sub_code.get())
            desc = self.var_sub_desc.get().strip()
            if not desc:
                raise ValueError("Descrizione sotto-categoria mancante.")
            std_code = self.var_sub_std.get().strip()
            standard_id = self._std_by_code.get(std_code) if std_code and std_code != "—" else None
            tpl = self.txt_sub_tpl.get("1.0", "end").strip()

            if self.selected_subcategory_id:
                self.db.update_subcategory(self.selected_subcategory_id, desc, standard_id, tpl)
            else:
                if not is_valid_gggg_normati(code):
                    raise ValueError("CODICE sotto-categoria non valido: servono 4 numeri.")
                self.db.create_subcategory(self.selected_category_id, code, desc, standard_id, tpl)
            self.refresh_subcategories()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Codice sotto-categoria già esistente per questa categoria.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")

    def sub_delete(self) -> None:
        if not self.selected_subcategory_id:
            return
        if not messagebox.askyesno(APP_NAME, "Eliminare la sotto-categoria selezionata?\n\nSe è collegata ad articoli, l'operazione può fallire."):
            return
        try:
            self.db.delete_subcategory(self.selected_subcategory_id)
            self.sub_new()
            self.refresh_subcategories()
            self.refs_changed_callback()
        except sqlite3.IntegrityError:
            messagebox.showerror(APP_NAME, "Impossibile eliminare: ci sono record collegati.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Errore.\n\n{e}")


# ---------------- UI Commerciali (non normati) ----------------



