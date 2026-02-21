from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import customtkinter as ctk
from tkinter import ttk, messagebox

from .services import AppService
from .ui_utils import bind_uppercase, make_treeview_sortable
from .utils import normalize_upper


def _row_str(r: Any, key: str, default: str = "") -> str:
    try:
        v = r[key]
    except Exception:
        v = getattr(r, key, default)
    return "" if v is None else str(v)


class MaterialPropertyBox(ctk.CTkFrame):
    """Gestione proprietà parametriche (CHEM/PHYS/MECH), senza legame a stati."""

    def __init__(self, master, db: AppService, group_code: str, title: str):
        super().__init__(master)
        self.db = db
        self.group_code = normalize_upper(group_code)
        self.title = title
        self.material_id: Optional[int] = None
        self.prop_id: Optional[int] = None

        self.var_name = ctk.StringVar()
        self.var_unit = ctk.StringVar()
        self.var_value = ctk.StringVar()
        self.var_min = ctk.StringVar()
        self.var_max = ctk.StringVar()
        self.var_ord = ctk.StringVar(value="0")
        self.var_notes = ctk.StringVar()

        for v in [self.var_name, self.var_unit, self.var_value, self.var_min, self.var_max, self.var_notes]:
            bind_uppercase(v)

        self._build_ui()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        lbl = ctk.CTkLabel(self, text=self.title, font=ctk.CTkFont(size=14, weight="bold"))
        lbl.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        # Tree
        tree_frame = ctk.CTkFrame(self)
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=6)
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)

        cols = ("NOME", "UNITA", "VALORE", "MIN", "MAX", "ORD")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=7)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120 if c in ("NOME", "VALORE") else 80, anchor="w", stretch=True)
        make_treeview_sortable(self.tree, numeric_cols=["ORD"])
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # Form
        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        for i in range(7):
            form.grid_columnconfigure(i, weight=1)

        def add_label(txt, r, c):
            ctk.CTkLabel(form, text=txt).grid(row=r, column=c, sticky="w", padx=6, pady=(6, 2))

        add_label("NOME", 0, 0)
        self.ent_name = ctk.CTkEntry(form, textvariable=self.var_name)
        self.ent_name.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        add_label("UNITÀ", 0, 1)
        self.ent_unit = ctk.CTkEntry(form, textvariable=self.var_unit)
        self.ent_unit.grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        add_label("VALORE", 0, 2)
        self.ent_value = ctk.CTkEntry(form, textvariable=self.var_value)
        self.ent_value.grid(row=1, column=2, sticky="ew", padx=6, pady=(0, 6))

        add_label("MIN", 0, 3)
        self.ent_min = ctk.CTkEntry(form, textvariable=self.var_min)
        self.ent_min.grid(row=1, column=3, sticky="ew", padx=6, pady=(0, 6))

        add_label("MAX", 0, 4)
        self.ent_max = ctk.CTkEntry(form, textvariable=self.var_max)
        self.ent_max.grid(row=1, column=4, sticky="ew", padx=6, pady=(0, 6))

        add_label("ORD", 0, 5)
        self.ent_ord = ctk.CTkEntry(form, textvariable=self.var_ord)
        self.ent_ord.grid(row=1, column=5, sticky="ew", padx=6, pady=(0, 6))

        add_label("NOTE", 0, 6)
        self.ent_notes = ctk.CTkEntry(form, textvariable=self.var_notes)
        self.ent_notes.grid(row=1, column=6, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=2, column=0, columnspan=7, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_prop).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_prop).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_prop).pack(side="left", padx=4)

    def set_states(self, states: List[Tuple[int, str, str]]) -> None:
        # Proprieta materiale senza legame a stati: no-op.
        return

    def set_material(self, material_id: Optional[int]) -> None:
        self.material_id = material_id
        self.new_prop()
        self.refresh()

    def refresh(self) -> None:
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        if not self.material_id:
            return
        rows = self.db.fetch_material_properties(self.material_id, self.group_code)
        for r in rows:
            self.tree.insert(
                "",
                "end",
                iid=str(r["id"]),
                values=(
                    _row_str(r, "name"),
                    _row_str(r, "unit"),
                    _row_str(r, "value"),
                    _row_str(r, "min_value"),
                    _row_str(r, "max_value"),
                    _row_str(r, "sort_order"),
                ),
            )

    def new_prop(self) -> None:
        self.prop_id = None
        self.var_name.set("")
        self.var_unit.set("")
        self.var_value.set("")
        self.var_min.set("")
        self.var_max.set("")
        self.var_ord.set("0")
        self.var_notes.set("")
        self.ent_name.configure(state="normal")

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        self.prop_id = int(iid)
        vals = self.tree.item(iid, "values")
        if not vals:
            return
        name, unit, value, vmin, vmax, ordv = vals
        self.var_name.set(name)
        self.var_unit.set(unit)
        self.var_value.set(value)
        self.var_min.set(vmin)
        self.var_max.set(vmax)
        self.var_ord.set(ordv)
        # note is not in columns -> reload via service method
        try:
            self.var_notes.set(self.db.read_material_property_notes(self.prop_id))
        except Exception:
            self.var_notes.set("")
        self.ent_name.configure(state="disabled")

    def save_prop(self) -> None:
        if not self.material_id:
            messagebox.showwarning("Materiali", "Seleziona prima un materiale.")
            return
        name = (self.var_name.get() or "").strip()
        if not name:
            messagebox.showwarning("Proprietà", "Inserisci un NOME proprietà.")
            return
        state_code = ""
        try:
            ordv = int((self.var_ord.get() or "0").strip() or "0")
        except Exception:
            ordv = 0

        try:
            if self.prop_id is None:
                self.db.create_material_property(
                    self.material_id,
                    self.group_code,
                    state_code,
                    name,
                    self.var_unit.get(),
                    self.var_value.get(),
                    self.var_min.get(),
                    self.var_max.get(),
                    self.var_notes.get(),
                    ordv,
                )
            else:
                self.db.update_material_property(
                    self.prop_id,
                    state_code,
                    self.var_unit.get(),
                    self.var_value.get(),
                    self.var_min.get(),
                    self.var_max.get(),
                    self.var_notes.get(),
                    ordv,
                )
            self.refresh()
            messagebox.showinfo("Proprietà", "Salvato.")
        except Exception as e:
            messagebox.showerror("Proprietà", f"Errore salvataggio: {e}")

    def delete_prop(self) -> None:
        if self.prop_id is None:
            return
        if not messagebox.askyesno("Proprietà", "Eliminare la proprietà selezionata?"):
            return
        try:
            self.db.delete_material_property(self.prop_id)
            self.new_prop()
            self.refresh()
        except Exception as e:
            messagebox.showerror("Proprietà", f"Errore eliminazione: {e}")


class LinkedSemisBox(ctk.CTkFrame):
    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db
        self.material_id: Optional[int] = None
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self, text="Semilavorati collegati", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        frame = ctk.CTkFrame(self)
        frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_rowconfigure(0, weight=1)

        cols = ("TIPO", "STATO", "DESCRIZIONE", "DIM", "AGG")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", height=7)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=160 if c == "DESCRIZIONE" else 100, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")

    def set_material(self, material_id: Optional[int]) -> None:
        self.material_id = material_id
        self.refresh()

    def refresh(self) -> None:
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        if not self.material_id:
            return
        rows = self.db.fetch_semis_by_material(self.material_id)
        for r in rows:
            self.tree.insert(
                "",
                "end",
                iid=str(r["id"]),
                values=(
                    _row_str(r, "type_desc"),
                    _row_str(r, "state_desc"),
                    _row_str(r, "description"),
                    _row_str(r, "dimensions"),
                    _row_str(r, "updated_at"),
                ),
            )


class MaterialTaxonomyDialog(ctk.CTkToplevel):
    def __init__(self, master, db: AppService, on_changed=None):
        super().__init__(master)
        self.db = db
        self.on_changed = on_changed

        self.family_id: Optional[int] = None
        self.subfamily_id: Optional[int] = None
        self._family_rows: List[Any] = []
        self._sub_rows: List[Any] = []

        self.var_family = ctk.StringVar()
        self.var_subfamily = ctk.StringVar()
        bind_uppercase(self.var_family)
        bind_uppercase(self.var_subfamily)

        self.title("Gestione Famiglie Materiali")
        self.geometry("980x560")
        self.minsize(860, 480)
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._close)

        self._build_ui()
        self.refresh_all()

    def _close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()

    def _notify_changed(self):
        if callable(self.on_changed):
            try:
                self.on_changed()
            except Exception:
                pass

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(self)
        left.grid(row=0, column=0, sticky="nsew", padx=(10, 6), pady=10)
        left.grid_columnconfigure(0, weight=1)
        left.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(left, text="Famiglie", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        lf = ctk.CTkFrame(left)
        lf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        lf.grid_columnconfigure(0, weight=1)
        lf.grid_rowconfigure(0, weight=1)

        self.tree_fam = ttk.Treeview(lf, columns=("FAMIGLIA",), show="headings", height=14)
        self.tree_fam.heading("FAMIGLIA", text="FAMIGLIA")
        self.tree_fam.column("FAMIGLIA", width=300, anchor="w", stretch=True)
        make_treeview_sortable(self.tree_fam)
        self.tree_fam.grid(row=0, column=0, sticky="nsew")
        sb_f = ttk.Scrollbar(lf, orient="vertical", command=self.tree_fam.yview)
        self.tree_fam.configure(yscrollcommand=sb_f.set)
        sb_f.grid(row=0, column=1, sticky="ns")
        self.tree_fam.bind("<<TreeviewSelect>>", self._on_select_family)

        ff = ctk.CTkFrame(left)
        ff.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        ff.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(ff, text="Descrizione famiglia").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(ff, textvariable=self.var_family).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))
        fbtn = ctk.CTkFrame(ff, fg_color="transparent")
        fbtn.grid(row=2, column=0, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(fbtn, text="Nuovo", width=90, command=self.new_family).pack(side="left", padx=4)
        ctk.CTkButton(fbtn, text="Salva", width=90, command=self.save_family).pack(side="left", padx=4)
        ctk.CTkButton(fbtn, text="Elimina", width=90, command=self.delete_family).pack(side="left", padx=4)

        right = ctk.CTkFrame(self)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 10), pady=10)
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(1, weight=1)

        self.lbl_sub_title = ctk.CTkLabel(right, text="Sottofamiglie", font=ctk.CTkFont(size=16, weight="bold"))
        self.lbl_sub_title.grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))

        rf = ctk.CTkFrame(right)
        rf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        rf.grid_columnconfigure(0, weight=1)
        rf.grid_rowconfigure(0, weight=1)

        self.tree_sub = ttk.Treeview(rf, columns=("SOTTOFAMIGLIA",), show="headings", height=14)
        self.tree_sub.heading("SOTTOFAMIGLIA", text="SOTTOFAMIGLIA")
        self.tree_sub.column("SOTTOFAMIGLIA", width=300, anchor="w", stretch=True)
        make_treeview_sortable(self.tree_sub)
        self.tree_sub.grid(row=0, column=0, sticky="nsew")
        sb_s = ttk.Scrollbar(rf, orient="vertical", command=self.tree_sub.yview)
        self.tree_sub.configure(yscrollcommand=sb_s.set)
        sb_s.grid(row=0, column=1, sticky="ns")
        self.tree_sub.bind("<<TreeviewSelect>>", self._on_select_subfamily)

        sf = ctk.CTkFrame(right)
        sf.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        sf.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(sf, text="Descrizione sottofamiglia").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(sf, textvariable=self.var_subfamily).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))
        sbtn = ctk.CTkFrame(sf, fg_color="transparent")
        sbtn.grid(row=2, column=0, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(sbtn, text="Nuovo", width=90, command=self.new_subfamily).pack(side="left", padx=4)
        ctk.CTkButton(sbtn, text="Salva", width=90, command=self.save_subfamily).pack(side="left", padx=4)
        ctk.CTkButton(sbtn, text="Elimina", width=90, command=self.delete_subfamily).pack(side="left", padx=4)

    def refresh_all(self, preserve_family_id: Optional[int] = None, preserve_subfamily_id: Optional[int] = None):
        self._family_rows = list(self.db.fetch_material_families())

        for k in self.tree_fam.get_children(""):
            self.tree_fam.delete(k)
        for r in self._family_rows:
            self.tree_fam.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"),))

        selected_family_id = preserve_family_id
        if selected_family_id is None:
            selected_family_id = self.family_id
        valid_family_ids = {int(r["id"]) for r in self._family_rows}
        if selected_family_id not in valid_family_ids:
            selected_family_id = next(iter(valid_family_ids), None)

        self.family_id = selected_family_id
        if self.family_id is not None:
            self.tree_fam.selection_set(str(self.family_id))
            self.tree_fam.focus(str(self.family_id))
            self.var_family.set(self._family_desc_by_id(self.family_id))
            self.refresh_subfamilies(self.family_id, preserve_subfamily_id)
        else:
            self.var_family.set("")
            self.refresh_subfamilies(None)

    def refresh_subfamilies(self, family_id: Optional[int], preserve_subfamily_id: Optional[int] = None):
        old_subfamily_id = self.subfamily_id
        for k in self.tree_sub.get_children(""):
            self.tree_sub.delete(k)
        self.subfamily_id = None
        self.var_subfamily.set("")

        if family_id is None:
            self._sub_rows = []
            self.lbl_sub_title.configure(text="Sottofamiglie")
            return

        fam_desc = self._family_desc_by_id(family_id)
        self.lbl_sub_title.configure(text=f"Sottofamiglie - {fam_desc}")
        self._sub_rows = list(self.db.fetch_material_subfamilies(int(family_id)))
        for r in self._sub_rows:
            self.tree_sub.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"),))

        selected_sub_id = preserve_subfamily_id if preserve_subfamily_id is not None else old_subfamily_id
        valid_sub_ids = {int(r["id"]) for r in self._sub_rows}
        if selected_sub_id not in valid_sub_ids:
            selected_sub_id = next(iter(valid_sub_ids), None)

        self.subfamily_id = selected_sub_id
        if self.subfamily_id is not None:
            self.tree_sub.selection_set(str(self.subfamily_id))
            self.tree_sub.focus(str(self.subfamily_id))
            self.var_subfamily.set(self._subfamily_desc_by_id(self.subfamily_id))

    def _family_desc_by_id(self, family_id: int) -> str:
        for r in self._family_rows:
            if int(r["id"]) == int(family_id):
                return _row_str(r, "description")
        return ""

    def _subfamily_desc_by_id(self, subfamily_id: int) -> str:
        for r in self._sub_rows:
            if int(r["id"]) == int(subfamily_id):
                return _row_str(r, "description")
        return ""

    def _on_select_family(self, _evt=None):
        sel = self.tree_fam.selection()
        if not sel:
            return
        self.family_id = int(sel[0])
        self.var_family.set(self._family_desc_by_id(self.family_id))
        self.refresh_subfamilies(self.family_id)

    def _on_select_subfamily(self, _evt=None):
        sel = self.tree_sub.selection()
        if not sel:
            return
        self.subfamily_id = int(sel[0])
        self.var_subfamily.set(self._subfamily_desc_by_id(self.subfamily_id))

    def new_family(self):
        self.family_id = None
        self.var_family.set("")
        for k in self.tree_fam.selection():
            self.tree_fam.selection_remove(k)
        self.refresh_subfamilies(None)

    def save_family(self):
        desc = (self.var_family.get() or "").strip()
        if not desc:
            messagebox.showwarning("Materiali", "Inserisci la descrizione famiglia.")
            return
        try:
            if self.family_id is None:
                new_id = self.db.create_material_family(desc)
                self.refresh_all(preserve_family_id=new_id)
            else:
                self.db.update_material_family(self.family_id, desc)
                self.refresh_all(preserve_family_id=self.family_id)
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore salvataggio famiglia: {e}")

    def delete_family(self):
        if self.family_id is None:
            return
        if not messagebox.askyesno("Materiali", "Eliminare la famiglia selezionata?"):
            return
        try:
            self.db.delete_material_family(self.family_id)
            self.refresh_all()
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore eliminazione famiglia: {e}")

    def new_subfamily(self):
        self.subfamily_id = None
        self.var_subfamily.set("")
        for k in self.tree_sub.selection():
            self.tree_sub.selection_remove(k)

    def save_subfamily(self):
        if self.family_id is None:
            messagebox.showwarning("Materiali", "Seleziona prima una famiglia.")
            return
        desc = (self.var_subfamily.get() or "").strip()
        if not desc:
            messagebox.showwarning("Materiali", "Inserisci la descrizione sottofamiglia.")
            return
        try:
            if self.subfamily_id is None:
                new_id = self.db.create_material_subfamily(self.family_id, desc)
                self.refresh_subfamilies(self.family_id, preserve_subfamily_id=new_id)
            else:
                self.db.update_material_subfamily(self.subfamily_id, desc)
                self.refresh_subfamilies(self.family_id, preserve_subfamily_id=self.subfamily_id)
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore salvataggio sottofamiglia: {e}")

    def delete_subfamily(self):
        if self.subfamily_id is None:
            return
        if not messagebox.askyesno("Materiali", "Eliminare la sottofamiglia selezionata?"):
            return
        try:
            self.db.delete_material_subfamily(self.subfamily_id)
            self.refresh_subfamilies(self.family_id)
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore eliminazione sottofamiglia: {e}")


class MaterialDetailsDialog(ctk.CTkToplevel):
    """Dettagli avanzati materiale: proprieta e semilavorati collegati."""

    def __init__(self, master, db: AppService, on_close=None):
        super().__init__(master)
        self.db = db
        self.on_close = on_close
        self.material_id: Optional[int] = None

        self.title("Materiale - Proprieta e collegamenti")
        self.geometry("1200x760")
        self.minsize(980, 620)
        self.transient(master)
        self.protocol("WM_DELETE_WINDOW", self._close)

        self._build_ui()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        paned = ttk.Panedwindow(self, orient="vertical")
        paned.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        self.box_chem = MaterialPropertyBox(paned, self.db, "CHEM", "Proprieta chimiche")
        self.box_phys = MaterialPropertyBox(paned, self.db, "PHYS", "Proprieta fisiche")
        self.box_mech = MaterialPropertyBox(paned, self.db, "MECH", "Proprieta meccaniche")
        self.box_link = LinkedSemisBox(paned, self.db)

        paned.add(self.box_chem, weight=1)
        paned.add(self.box_phys, weight=1)
        paned.add(self.box_mech, weight=1)
        paned.add(self.box_link, weight=1)

    def _close(self):
        if callable(self.on_close):
            try:
                self.on_close()
            except Exception:
                pass
        self.destroy()

    def set_states(self, states: List[Tuple[int, str, str]]):
        self.box_chem.set_states(states)
        self.box_phys.set_states(states)
        self.box_mech.set_states(states)

    def set_material(self, material_id: Optional[int]):
        self.material_id = material_id
        self.box_chem.set_material(material_id)
        self.box_phys.set_material(material_id)
        self.box_mech.set_material(material_id)
        self.box_link.set_material(material_id)


class MaterialsTab(ctk.CTkFrame):
    EMPTY_CHOICE = "-"

    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db

        self.material_id: Optional[int] = None
        self._taxonomy_dialog: Optional[MaterialTaxonomyDialog] = None
        self._families: List[Tuple[int, str]] = []
        self._subfamilies: List[Tuple[int, str]] = []

        self.var_search = ctk.StringVar()
        self.var_family = ctk.StringVar(value=self.EMPTY_CHOICE)
        self.var_desc = ctk.StringVar(value=self.EMPTY_CHOICE)
        self.var_std = ctk.StringVar()
        self.var_notes = ctk.StringVar()

        for v in [self.var_std, self.var_notes]:
            bind_uppercase(v)
        self._build_ui()
        self.refresh_lists()
        self.refresh_materials()
        self.new_material()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        outer = ttk.Panedwindow(self, orient="horizontal")
        outer.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        # Left list
        left = ctk.CTkFrame(outer)
        left.grid_rowconfigure(2, weight=1)
        left.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(left, text="Materiali", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        sbar = ctk.CTkFrame(left, fg_color="transparent")
        sbar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        sbar.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(sbar, text="Cerca").grid(row=0, column=0, padx=(0, 6))
        ctk.CTkEntry(sbar, textvariable=self.var_search).grid(row=0, column=1, sticky="ew")
        ctk.CTkButton(sbar, text="Aggiorna", width=90, command=self.refresh_materials).grid(row=0, column=2, padx=(6, 0))
        ctk.CTkButton(sbar, text="Gestisci famiglie", width=170, command=self._open_taxonomy_dialog).grid(
            row=0, column=3, padx=(6, 0)
        )

        lf = ctk.CTkFrame(left)
        lf.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        lf.grid_columnconfigure(0, weight=1)
        lf.grid_rowconfigure(0, weight=1)

        cols = ("FAMIGLIA", "SOTTOFAMIGLIA/STATO", "CHIMICHE", "FISICHE", "MECCANICHE", "AGG")
        self.tree = ttk.Treeview(lf, columns=cols, show="headings", height=14)
        for c in cols:
            self.tree.heading(c, text=c)
            if c in ("FAMIGLIA", "SOTTOFAMIGLIA/STATO"):
                w = 170
            elif c == "AGG":
                w = 130
            else:
                w = 220
            self.tree.column(c, width=w, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select_material)

        # Right detail
        right = ctk.CTkFrame(outer)
        right.grid_rowconfigure(2, weight=1)
        right.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(right, text="Dettaglio", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        form = ctk.CTkFrame(right)
        form.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        for i in range(3):
            form.grid_columnconfigure(i, weight=1)

        def lab(txt, r, c):
            ctk.CTkLabel(form, text=txt).grid(row=r, column=c, sticky="w", padx=6, pady=(6, 2))

        lab("FAMIGLIA", 0, 0)
        self.opt_family = ctk.CTkOptionMenu(
            form,
            values=[self.EMPTY_CHOICE],
            variable=self.var_family,
            command=self._on_family_changed,
        )
        self.opt_family.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        lab("SOTTOFAMIGLIA / STATO MATERIALE", 0, 1)
        self.opt_desc = ctk.CTkOptionMenu(form, values=[self.EMPTY_CHOICE], variable=self.var_desc)
        self.opt_desc.grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        lab("NORMA", 0, 2)
        ctk.CTkEntry(form, textvariable=self.var_std).grid(row=1, column=2, sticky="ew", padx=6, pady=(0, 6))

        lab("NOTE", 2, 0)
        ctk.CTkEntry(form, textvariable=self.var_notes).grid(row=3, column=0, columnspan=3, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=4, column=0, columnspan=3, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_material).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_material).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_material).pack(side="left", padx=4)

        props = ctk.CTkTabview(right)
        props.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        tab_chem = props.add("Chimiche")
        tab_phys = props.add("Fisiche")
        tab_mech = props.add("Meccaniche")

        self.box_chem = MaterialPropertyBox(tab_chem, self.db, "CHEM", "Proprieta chimiche")
        self.box_chem.pack(fill="both", expand=True)
        self.box_phys = MaterialPropertyBox(tab_phys, self.db, "PHYS", "Proprieta fisiche")
        self.box_phys.pack(fill="both", expand=True)
        self.box_mech = MaterialPropertyBox(tab_mech, self.db, "MECH", "Proprieta meccaniche")
        self.box_mech.pack(fill="both", expand=True)

        outer.add(left, weight=2)
        outer.add(right, weight=3)

    def _open_taxonomy_dialog(self):
        try:
            if self._taxonomy_dialog is not None and self._taxonomy_dialog.winfo_exists():
                self._taxonomy_dialog.focus_set()
                return
        except Exception:
            self._taxonomy_dialog = None
        self._taxonomy_dialog = MaterialTaxonomyDialog(self, self.db, on_changed=self._on_taxonomy_changed)
        self._taxonomy_dialog.focus_set()

    def _on_taxonomy_changed(self):
        self._refresh_material_taxonomy(self.var_family.get(), self.var_desc.get())
        self.refresh_materials()
        self._select_material_row_if_present(self.material_id)

    def _select_material_row_if_present(self, material_id: Optional[int]):
        if material_id is None:
            return
        iid = str(material_id)
        if self.tree.exists(iid):
            self.tree.selection_set(iid)
            self.tree.focus(iid)
            self._on_select_material()

    def refresh_lists(self, selected_family: Optional[str] = None, selected_subfamily: Optional[str] = None):
        # Proprieta materiale senza legame con gli stati semilavorato.
        self._refresh_material_taxonomy(selected_family, selected_subfamily)

    def _refresh_material_taxonomy(self, selected_family: Optional[str] = None, selected_subfamily: Optional[str] = None):
        fam_rows = list(self.db.fetch_material_families())
        self._families = [(int(r["id"]), _row_str(r, "description")) for r in fam_rows]

        family_values = [d for _, d in self._families]
        if not family_values:
            family_values = [self.EMPTY_CHOICE]
        self.opt_family.configure(values=family_values)

        desired_family = (selected_family or self.var_family.get() or "").strip()
        if desired_family not in family_values:
            desired_family = family_values[0]
        self.var_family.set(desired_family)

        self._refresh_subfamilies_for_family(desired_family, selected_subfamily)

    def _refresh_subfamilies_for_family(self, family_desc: str, selected_subfamily: Optional[str] = None):
        family_id = self._family_id_from_desc(family_desc)
        if family_id is None:
            self._subfamilies = []
            self.opt_desc.configure(values=[self.EMPTY_CHOICE])
            self.var_desc.set(self.EMPTY_CHOICE)
            return

        sub_rows = list(self.db.fetch_material_subfamilies(family_id))
        self._subfamilies = [(int(r["id"]), _row_str(r, "description")) for r in sub_rows]
        sub_values = [d for _, d in self._subfamilies]
        if not sub_values:
            sub_values = [self.EMPTY_CHOICE]
        self.opt_desc.configure(values=sub_values)

        desired_sub = (selected_subfamily or self.var_desc.get() or "").strip()
        if desired_sub not in sub_values:
            desired_sub = sub_values[0]
        self.var_desc.set(desired_sub)

    def _family_id_from_desc(self, desc: str) -> Optional[int]:
        for fam_id, fam_desc in self._families:
            if fam_desc == desc:
                return fam_id
        return None

    def _on_family_changed(self, value: str):
        self._refresh_subfamilies_for_family(value)

    def _property_summary(self, material_id: int, group_code: str, max_items: int = 3) -> str:
        rows = self.db.fetch_material_properties(int(material_id), group_code)
        parts: List[str] = []
        for r in rows:
            name = _row_str(r, "name").strip()
            if not name:
                continue
            value = _row_str(r, "value").strip()
            token = name if not value else f"{name}={value}"
            parts.append(token)
            if len(parts) >= max_items:
                break
        if not parts:
            return ""
        out = " | ".join(parts)
        if len(rows) > max_items:
            out += " | ..."
        return out

    def refresh_materials(self):
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        q = (self.var_search.get() or "").strip()
        rows = self.db.search_materials(q)
        for r in rows:
            self.tree.insert(
                "",
                "end",
                iid=str(r["id"]),
                values=(
                    _row_str(r, "family"),
                    _row_str(r, "description"),
                    self._property_summary(int(r["id"]), "CHEM"),
                    self._property_summary(int(r["id"]), "PHYS"),
                    self._property_summary(int(r["id"]), "MECH"),
                    _row_str(r, "updated_at"),
                ),
            )

    def new_material(self):
        self.material_id = None
        self.var_std.set("")
        self.var_notes.set("")
        self._refresh_material_taxonomy()
        self.box_chem.set_material(None)
        self.box_phys.set_material(None)
        self.box_mech.set_material(None)

    def _on_select_material(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.material_id = int(sel[0])
        row = self.db.read_material(self.material_id)
        family = _row_str(row, "family")
        subfamily = _row_str(row, "description")
        self.refresh_lists(selected_family=family, selected_subfamily=subfamily)
        self.var_std.set(_row_str(row, "standard"))
        self.var_notes.set(_row_str(row, "notes"))
        self.box_chem.set_material(self.material_id)
        self.box_phys.set_material(self.material_id)
        self.box_mech.set_material(self.material_id)

    def save_material(self):
        family = (self.var_family.get() or "").strip()
        desc = (self.var_desc.get() or "").strip()
        if not family or family == self.EMPTY_CHOICE or not desc or desc == self.EMPTY_CHOICE:
            messagebox.showwarning("Materiali", "Seleziona FAMIGLIA e SOTTOFAMIGLIA/STATO.")
            return
        try:
            if self.material_id is None:
                # codice interno auto-generato dal DB (non visibile in UI)
                self.material_id = self.db.create_material(None, family, desc, self.var_std.get(), self.var_notes.get())
                messagebox.showinfo("Materiali", "Creato.")
            else:
                self.db.update_material(self.material_id, family, desc, self.var_std.get(), self.var_notes.get())
                messagebox.showinfo("Materiali", "Aggiornato.")
            self.refresh_materials()
            self._select_material_row_if_present(self.material_id)
            self.box_chem.set_material(self.material_id)
            self.box_phys.set_material(self.material_id)
            self.box_mech.set_material(self.material_id)
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore salvataggio: {e}")

    def delete_material(self):
        if self.material_id is None:
            return
        if not messagebox.askyesno("Materiali", "Eliminare il materiale selezionato?"):
            return
        try:
            self.db.delete_material(self.material_id)
            self.new_material()
            self.refresh_materials()
        except Exception as e:
            messagebox.showerror("Materiali", f"Errore eliminazione: {e}")


class _TreatmentBox(ctk.CTkFrame):
    def __init__(self, master, db: AppService, title: str, kind: str):
        super().__init__(master)
        self.db = db
        self.title = title
        self.kind = kind  # "heat" or "surface"
        self.tid: Optional[int] = None

        self.var_desc = ctk.StringVar()
        self.var_std = ctk.StringVar()
        self.var_notes = ctk.StringVar()

        for v in [self.var_desc, self.var_std, self.var_notes]:
            bind_uppercase(v)

        self._build_ui()
        self.refresh()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(self, text=self.title, font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        # list
        lf = ctk.CTkFrame(self)
        lf.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        lf.grid_rowconfigure(0, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        cols = ("DESCRIZIONE", "AGG")
        self.tree = ttk.Treeview(lf, columns=cols, show="headings", height=10)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=280 if c == "DESCRIZIONE" else 140, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # form
        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        form.grid_columnconfigure(0, weight=1)
        form.grid_rowconfigure(3, weight=1)

        ctk.CTkLabel(form, text="DESCRIZIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        self.ent_desc = ctk.CTkEntry(form, textvariable=self.var_desc)
        self.ent_desc.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        ctk.CTkLabel(form, text="NORMA").grid(row=0, column=1, sticky="w", padx=6, pady=(6, 2))
        self.ent_std = ctk.CTkEntry(form, textvariable=self.var_std)
        self.ent_std.grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        form.grid_columnconfigure(0, weight=1)
        form.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(form, text="CARATTERISTICHE").grid(row=2, column=0, sticky="w", padx=6, pady=(6, 2))
        self.txt_char = ctk.CTkTextbox(form, height=90)
        self.txt_char.grid(row=3, column=0, columnspan=2, sticky="nsew", padx=6, pady=(0, 6))

        ctk.CTkLabel(form, text="NOTE").grid(row=4, column=0, sticky="w", padx=6, pady=(6, 2))
        self.ent_notes = ctk.CTkEntry(form, textvariable=self.var_notes)
        self.ent_notes.grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=6, column=0, columnspan=2, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete).pack(side="left", padx=4)

    def refresh(self):
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        rows = self.db.fetch_heat_treatments() if self.kind == "heat" else self.db.fetch_surface_treatments()
        for r in rows:
            self.tree.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"), _row_str(r, "updated_at")))

    def new(self):
        self.tid = None
        self.var_desc.set("")
        self.var_std.set("")
        self.var_notes.set("")
        self.txt_char.delete("1.0", "end")

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.tid = int(sel[0])
        row = self.db.read_heat_treatment(self.tid) if self.kind == "heat" else self.db.read_surface_treatment(self.tid)
        self.var_desc.set(_row_str(row, "description"))
        self.var_std.set(_row_str(row, "standard"))
        self.var_notes.set(_row_str(row, "notes"))
        self.txt_char.delete("1.0", "end")
        self.txt_char.insert("1.0", _row_str(row, "characteristics"))

    def save(self):
        desc = (self.var_desc.get() or "").strip()
        if not desc:
            messagebox.showwarning("Trattamenti", "Compila almeno DESCRIZIONE.")
            return
        chars = self.txt_char.get("1.0", "end").strip()
        try:
            if self.kind == "heat":
                if self.tid is None:
                    # codice interno auto-generato dal DB (non visibile in UI)
                    self.tid = self.db.create_heat_treatment(None, desc, chars, self.var_std.get(), self.var_notes.get())
                else:
                    self.db.update_heat_treatment(self.tid, desc, chars, self.var_std.get(), self.var_notes.get())
            else:
                if self.tid is None:
                    # codice interno auto-generato dal DB (non visibile in UI)
                    self.tid = self.db.create_surface_treatment(None, desc, chars, self.var_std.get(), self.var_notes.get())
                else:
                    self.db.update_surface_treatment(self.tid, desc, chars, self.var_std.get(), self.var_notes.get())
            self.refresh()
            messagebox.showinfo("Trattamenti", "Salvato.")
        except Exception as e:
            messagebox.showerror("Trattamenti", f"Errore salvataggio: {e}")

    def delete(self):
        if self.tid is None:
            return
        if not messagebox.askyesno("Trattamenti", "Eliminare il trattamento selezionato?"):
            return
        try:
            if self.kind == "heat":
                self.db.delete_heat_treatment(self.tid)
            else:
                self.db.delete_surface_treatment(self.tid)
            self.new()
            self.refresh()
        except Exception as e:
            messagebox.showerror("Trattamenti", f"Errore eliminazione: {e}")


class TreatmentsTab(ctk.CTkFrame):
    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        paned = ttk.Panedwindow(self, orient="horizontal")
        paned.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        self.box_heat = _TreatmentBox(paned, db, "Trattamenti termici", "heat")
        self.box_surf = _TreatmentBox(paned, db, "Trattamenti superficiali", "surface")
        paned.add(self.box_heat, weight=1)
        paned.add(self.box_surf, weight=1)


class _SimpleCodeBox(ctk.CTkFrame):
    """Gestione elenco descrizioni (semi_type / semi_state), con codice interno automatico."""

    def __init__(self, master, db: AppService, title: str, kind: str):
        super().__init__(master)
        self.db = db
        self.title = title
        self.kind = kind  # "type" / "state"
        self.item_id: Optional[int] = None

        self.var_desc = ctk.StringVar()
        bind_uppercase(self.var_desc)

        self._build_ui()
        self.refresh()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self, text=self.title, font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        frame = ctk.CTkFrame(self)
        frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        cols = ("DESCRIZIONE",)
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", height=9)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=320, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        form.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(form, text="DESCRIZIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(form, textvariable=self.var_desc).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=2, column=0, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete).pack(side="left", padx=4)

    def refresh(self):
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        rows = self.db.fetch_semi_types() if self.kind == "type" else self.db.fetch_semi_states()
        for r in rows:
            self.tree.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"),))

    def new(self):
        self.item_id = None
        self.var_desc.set("")

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.item_id = int(sel[0])
        vals = self.tree.item(sel[0], "values")
        self.var_desc.set(vals[0] if vals else "")

    def save(self):
        desc = (self.var_desc.get() or "").strip()
        if not desc:
            messagebox.showwarning("Semilavorati", "Compila DESCRIZIONE.")
            return
        try:
            if self.kind == "type":
                if self.item_id is None:
                    # codice interno auto-generato dal DB (non visibile in UI)
                    self.item_id = self.db.create_semi_type(None, desc)
                else:
                    self.db.update_semi_type(self.item_id, desc)
            else:
                if self.item_id is None:
                    # codice interno auto-generato dal DB (non visibile in UI)
                    self.item_id = self.db.create_semi_state(None, desc)
                else:
                    self.db.update_semi_state(self.item_id, desc)
            self.refresh()
            messagebox.showinfo("Semilavorati", "Salvato.")
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore salvataggio: {e}")

    def delete(self):
        if self.item_id is None:
            return
        if not messagebox.askyesno("Semilavorati", "Eliminare la voce selezionata?"):
            return
        try:
            if self.kind == "type":
                self.db.delete_semi_type(self.item_id)
            else:
                self.db.delete_semi_state(self.item_id)
            self.new()
            self.refresh()
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore eliminazione: {e}")


class SemiTypeDialog(ctk.CTkToplevel):
    """Dialog per gestire le famiglie semilavorati (semi_type)."""

    def __init__(self, master, db: AppService, on_changed=None):
        super().__init__(master)
        self.db = db
        self.on_changed = on_changed
        self.type_id: Optional[int] = None
        self._rows: List[Any] = []

        self.var_desc = ctk.StringVar()
        bind_uppercase(self.var_desc)

        self.title("Gestione Famiglie Semilavorati")
        self.geometry("560x520")
        self.minsize(500, 460)
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._close)

        self._build_ui()
        self.refresh()

    def _close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()

    def _notify_changed(self):
        if callable(self.on_changed):
            try:
                self.on_changed()
            except Exception:
                pass

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self, text="Famiglie semilavorati", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=(10, 4)
        )

        lf = ctk.CTkFrame(self)
        lf.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 8))
        lf.grid_rowconfigure(0, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(lf, columns=("DESCRIZIONE",), show="headings", height=12)
        self.tree.heading("DESCRIZIONE", text="DESCRIZIONE")
        self.tree.column("DESCRIZIONE", width=380, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))
        form.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(form, text="DESCRIZIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(form, textvariable=self.var_desc).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=2, column=0, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_type).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_type).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_type).pack(side="left", padx=4)

    def refresh(self):
        self._rows = list(self.db.fetch_semi_types())
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        for r in self._rows:
            self.tree.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"),))

        if self.type_id is not None and self.tree.exists(str(self.type_id)):
            self.tree.selection_set(str(self.type_id))
            self.tree.focus(str(self.type_id))

    def new_type(self):
        self.type_id = None
        self.var_desc.set("")
        for k in self.tree.selection():
            self.tree.selection_remove(k)

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.type_id = int(sel[0])
        vals = self.tree.item(sel[0], "values")
        self.var_desc.set(vals[0] if vals else "")

    def save_type(self):
        desc = (self.var_desc.get() or "").strip()
        if not desc:
            messagebox.showwarning("Semilavorati", "Compila DESCRIZIONE.")
            return
        try:
            if self.type_id is None:
                self.type_id = self.db.create_semi_type(None, desc)
            else:
                self.db.update_semi_type(self.type_id, desc)
            self.refresh()
            self._notify_changed()
            messagebox.showinfo("Semilavorati", "Salvato.")
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore salvataggio: {e}")

    def delete_type(self):
        if self.type_id is None:
            return
        if not messagebox.askyesno("Semilavorati", "Eliminare la famiglia selezionata?"):
            return
        try:
            self.db.delete_semi_type(self.type_id)
            self.new_type()
            self.refresh()
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore eliminazione: {e}")


class SemiStateDialog(ctk.CTkToplevel):
    """Dialog per gestire gli stati semilavorato (semi_state)."""

    def __init__(self, master, db: AppService, on_changed=None):
        super().__init__(master)
        self.db = db
        self.on_changed = on_changed
        self.state_id: Optional[int] = None
        self._rows: List[Any] = []

        self.var_desc = ctk.StringVar()
        bind_uppercase(self.var_desc)

        self.title("Gestione Stati Semilavorato")
        self.geometry("560x520")
        self.minsize(500, 460)
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._close)

        self._build_ui()
        self.refresh()

    def _close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()

    def _notify_changed(self):
        if callable(self.on_changed):
            try:
                self.on_changed()
            except Exception:
                pass

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self, text="Stati semilavorato", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=(10, 4)
        )

        lf = ctk.CTkFrame(self)
        lf.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 8))
        lf.grid_rowconfigure(0, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(lf, columns=("DESCRIZIONE",), show="headings", height=12)
        self.tree.heading("DESCRIZIONE", text="DESCRIZIONE")
        self.tree.column("DESCRIZIONE", width=380, anchor="w", stretch=True)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))
        form.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(form, text="DESCRIZIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        ctk.CTkEntry(form, textvariable=self.var_desc).grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=2, column=0, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_state).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_state).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_state).pack(side="left", padx=4)

    def refresh(self):
        self._rows = list(self.db.fetch_semi_states())
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        for r in self._rows:
            self.tree.insert("", "end", iid=str(r["id"]), values=(_row_str(r, "description"),))

        if self.state_id is not None and self.tree.exists(str(self.state_id)):
            self.tree.selection_set(str(self.state_id))
            self.tree.focus(str(self.state_id))

    def new_state(self):
        self.state_id = None
        self.var_desc.set("")
        for k in self.tree.selection():
            self.tree.selection_remove(k)

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.state_id = int(sel[0])
        vals = self.tree.item(sel[0], "values")
        self.var_desc.set(vals[0] if vals else "")

    def save_state(self):
        desc = (self.var_desc.get() or "").strip()
        if not desc:
            messagebox.showwarning("Semilavorati", "Compila DESCRIZIONE.")
            return
        try:
            if self.state_id is None:
                self.state_id = self.db.create_semi_state(None, desc)
            else:
                self.db.update_semi_state(self.state_id, desc)
            self.refresh()
            self._notify_changed()
            messagebox.showinfo("Semilavorati", "Salvato.")
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore salvataggio: {e}")

    def delete_state(self):
        if self.state_id is None:
            return
        if not messagebox.askyesno("Semilavorati", "Eliminare lo stato selezionato?"):
            return
        try:
            self.db.delete_semi_state(self.state_id)
            self.new_state()
            self.refresh()
            self._notify_changed()
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore eliminazione: {e}")


class SemiTaxonomyDialog(ctk.CTkToplevel):
    """Gestione unificata famiglie/stati semilavorato."""

    def __init__(self, master, db: AppService, on_close=None):
        super().__init__(master)
        self.db = db
        self.on_close = on_close

        self.title("Gestione Famiglie e Stati Semilavorato")
        self.geometry("1220x640")
        self.minsize(980, 520)
        self.transient(master)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._close)

        self._build_ui()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        paned = ttk.Panedwindow(self, orient="horizontal")
        paned.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        self.box_types = _SimpleCodeBox(paned, self.db, "Famiglie semilavorati", "type")
        self.box_states = _SimpleCodeBox(paned, self.db, "Stati semilavorato", "state")
        paned.add(self.box_types, weight=1)
        paned.add(self.box_states, weight=1)

    def _close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        if callable(self.on_close):
            try:
                self.on_close()
            except Exception:
                pass
        self.destroy()


class SemiDimensionsBox(ctk.CTkFrame):
    """Lista dimensionale del semilavorato: dimensione + peso al metro."""

    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db
        self.semi_item_id: Optional[int] = None
        self.dim_id: Optional[int] = None
        self._current_type_desc: str = ""
        self._hint_popup: Optional[ctk.CTkToplevel] = None

        self.var_dimension = ctk.StringVar()
        self.var_weight = ctk.StringVar()
        self.var_preferred = ctk.BooleanVar(value=False)
        bind_uppercase(self.var_dimension)
        bind_uppercase(self.var_weight)

        self._build_ui()
        self._set_enabled(False)

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(self, text="Lista dimensionale", font=ctk.CTkFont(size=15, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        frame = ctk.CTkFrame(self)
        frame.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        cols = ("PREF", "DIMENSIONE", "PESO AUTO")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", height=8)
        self.tree.heading("PREF", text="PREF")
        self.tree.heading("DIMENSIONE", text="DIMENSIONE")
        self.tree.heading("PESO AUTO", text="PESO AUTO")
        self.tree.column("PREF", width=60, anchor="center", stretch=False)
        self.tree.column("DIMENSIONE", width=360, anchor="w", stretch=True)
        self.tree.column("PESO AUTO", width=160, anchor="w", stretch=False)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        form = ctk.CTkFrame(self)
        form.grid(row=2, column=0, sticky="ew", padx=8, pady=(0, 8))
        form.grid_columnconfigure(0, weight=2)
        form.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(form, text="DIMENSIONE").grid(row=0, column=0, sticky="w", padx=6, pady=(6, 2))
        self.ent_dimension = ctk.CTkEntry(form, textvariable=self.var_dimension)
        self.ent_dimension.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        self.lbl_weight = ctk.CTkLabel(form, text="PESO AUTO (KG/M)")
        self.lbl_weight.grid(row=0, column=1, sticky="w", padx=6, pady=(6, 2))
        self.ent_weight = ctk.CTkEntry(form, textvariable=self.var_weight)
        self.ent_weight.grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=2, column=0, columnspan=2, sticky="e", padx=6, pady=(0, 6))
        self.chk_preferred = ctk.CTkCheckBox(btns, text="Preferita", variable=self.var_preferred)
        self.chk_preferred.pack(side="left", padx=(0, 8))
        self.btn_new = ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_dimension)
        self.btn_new.pack(side="left", padx=4)
        self.btn_save = ctk.CTkButton(btns, text="Salva", width=90, command=self.save_dimension)
        self.btn_save.pack(side="left", padx=4)
        self.btn_delete = ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_dimension)
        self.btn_delete.pack(side="left", padx=4)
        self.btn_help = ctk.CTkButton(btns, text="Guida", width=90, command=self._open_hint_popup)
        self.btn_help.pack(side="left", padx=4)

    def _set_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for w in (self.ent_dimension, self.ent_weight):
            w.configure(state=state)
        self.chk_preferred.configure(state=state)
        btn_state = "normal" if enabled else "disabled"
        for b in (self.btn_new, self.btn_save, self.btn_delete):
            b.configure(state=btn_state)

    def set_semi_item(self, semi_item_id: Optional[int]):
        self.semi_item_id = semi_item_id
        self.dim_id = None
        self.var_dimension.set("")
        self.var_weight.set("")
        self.var_preferred.set(False)
        self._current_type_desc = ""
        if self.semi_item_id:
            try:
                row = self.db.read_semi_item(self.semi_item_id)
                self._current_type_desc = _row_str(row, "type_desc")
            except Exception:
                self._current_type_desc = ""
        self._refresh_weight_label()
        self.refresh()
        self._set_enabled(bool(self.semi_item_id))

    def _refresh_weight_label(self):
        t = normalize_upper(self._current_type_desc or "")
        if t == "LAMIERE":
            self.lbl_weight.configure(text="PESO AUTO (KG/M2)")
        else:
            self.lbl_weight.configure(text="PESO AUTO (KG/M)")

    @staticmethod
    def _hint_text(type_desc: str) -> str:
        t = normalize_upper(type_desc or "")
        lines = [
            "Promemoria inserimento dimensioni",
            "",
            "Formati principali:",
            "- TONDI: D20 oppure O20",
            "- ESAGONI: CH24",
            "- PIATTI: 40X10 (kg/m)",
            "- LAMIERE: SP3 oppure 1000X2000X3 (calcolo in kg/m2)",
            "- TUBI: 30X2 (diametro x spessore)",
            "- TUBOLARI: 40X20X2",
            "",
            "Profilati L/U/T (peso automatico):",
            "- L: L40X40X4 oppure L50X30X5",
            "- U: U80X45X6 oppure U80X45X8X6 (TFxTW)",
            "- T: T80X60X8 oppure T80X60X8X6 (TFxTW)",
            "",
            "Nota: TRAVI (IPE/HEA/IPN/UPN...) peso/m manuale per ora.",
        ]
        if t == "PROFILATI":
            lines.extend(["", "Tipo corrente: PROFILATI."])
        if t == "TRAVI":
            lines.extend(["", "Tipo corrente: TRAVI (inserimento peso/m manuale)."])
        if t == "LAMIERE":
            lines.extend(["", "Tipo corrente: LAMIERE (peso automatico in kg/m2)."])
        return "\n".join(lines)

    def _open_hint_popup(self):
        try:
            if self._hint_popup is not None and self._hint_popup.winfo_exists():
                self._hint_popup.focus_set()
                return
        except Exception:
            self._hint_popup = None

        popup = ctk.CTkToplevel(self)
        self._hint_popup = popup
        popup.title("Guida Inserimento Dimensioni")
        popup.geometry("780x460")
        popup.minsize(620, 360)
        popup.transient(self.winfo_toplevel())

        popup.grid_columnconfigure(0, weight=1)
        popup.grid_rowconfigure(1, weight=1)

        title = "Guida formati dimensioni e peso al metro"
        if self._current_type_desc:
            title = f"{title} - Tipo: {self._current_type_desc}"
        ctk.CTkLabel(popup, text=title, font=ctk.CTkFont(size=15, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=12, pady=(10, 6)
        )

        txt = ctk.CTkTextbox(popup, wrap="word")
        txt.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 8))
        txt.insert("1.0", self._hint_text(self._current_type_desc))
        txt.configure(state="disabled")

        btns = ctk.CTkFrame(popup, fg_color="transparent")
        btns.grid(row=2, column=0, sticky="e", padx=12, pady=(0, 12))
        ctk.CTkButton(btns, text="Chiudi", width=100, command=popup.destroy).pack(side="left")

    def refresh(self):
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        if not self.semi_item_id:
            return
        rows = self.db.fetch_semi_dimensions(self.semi_item_id)
        for r in rows:
            pref = "X" if int(r["preferred"] or 0) else ""
            self.tree.insert(
                "",
                "end",
                iid=str(r["id"]),
                values=(pref, _row_str(r, "dimension"), _row_str(r, "weight_per_m")),
            )

    def new_dimension(self):
        self.dim_id = None
        self.var_dimension.set("")
        self.var_weight.set("")
        self.var_preferred.set(False)
        self.ent_dimension.focus_set()

    def _on_select(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.dim_id = int(sel[0])
        vals = self.tree.item(sel[0], "values")
        if not vals:
            return
        self.var_preferred.set(str(vals[0] or "").strip().upper() in {"X", "1", "TRUE", "SI"})
        self.var_dimension.set(vals[1] or "")
        self.var_weight.set(vals[2] or "")

    def save_dimension(self):
        if not self.semi_item_id:
            messagebox.showwarning("Semilavorati", "Salva prima il semilavorato per gestire la lista dimensionale.")
            return
        dimension = (self.var_dimension.get() or "").strip()
        if not dimension:
            messagebox.showwarning("Semilavorati", "Compila DIMENSIONE.")
            return
        manual_weight = (self.var_weight.get() or "").strip()
        calc_weight = self.db.calculate_semi_weight_per_m(self.semi_item_id, dimension)
        if calc_weight is not None:
            weight_value = f"{calc_weight:.3f}"
            self.var_weight.set(weight_value)
        else:
            weight_value = manual_weight
            if not weight_value:
                messagebox.showwarning(
                    "Semilavorati",
                    "Peso/m non calcolabile automaticamente per questa dimensione/tipo. "
                    "Per LAMIERE il calcolo e in kg/m2. Apri 'Guida' per i formati supportati oppure inseriscilo manualmente.",
                )
                return
        try:
            if self.dim_id is None:
                self.dim_id = self.db.create_semi_dimension(
                    self.semi_item_id,
                    dimension,
                    weight_value,
                    preferred=1 if self.var_preferred.get() else 0,
                )
            else:
                self.db.update_semi_dimension(
                    self.dim_id,
                    dimension,
                    weight_value,
                    preferred=1 if self.var_preferred.get() else 0,
                )
            self.refresh()
            if self.dim_id is not None and self.tree.exists(str(self.dim_id)):
                self.tree.selection_set(str(self.dim_id))
                self.tree.focus(str(self.dim_id))
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore salvataggio dimensione: {e}")

    def delete_dimension(self):
        if self.dim_id is None:
            return
        if not messagebox.askyesno("Semilavorati", "Eliminare la dimensione selezionata?"):
            return
        try:
            self.db.delete_semi_dimension(self.dim_id)
            self.new_dimension()
            self.refresh()
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore eliminazione dimensione: {e}")


class SemilavoratiTab(ctk.CTkFrame):
    def __init__(self, master, db: AppService):
        super().__init__(master)
        self.db = db
        self.item_id: Optional[int] = None
        self._copy_from_item_id: Optional[int] = None
        self._taxonomy_dialog: Optional[SemiTaxonomyDialog] = None

        self.var_search = ctk.StringVar()
        self.var_only_preferred_dim = ctk.BooleanVar(value=False)

        self.var_desc = ctk.StringVar()
        self.var_dim = ctk.StringVar()
        self.var_std = ctk.StringVar()
        self.var_notes = ctk.StringVar()

        for v in [self.var_desc, self.var_dim, self.var_std, self.var_notes]:
            bind_uppercase(v)

        # dropdown variables (type/state/material)
        self.var_type = ctk.StringVar()
        self.var_state = ctk.StringVar()
        self.var_mat = ctk.StringVar()  # "—" or family - subfamily

        self._types: List[Tuple[int, str]] = []      # id, description
        self._states: List[Tuple[int, str]] = []     # id, description
        self._materials: List[Tuple[int, str]] = []  # id, material label (family - subfamily)

        self._build_ui()
        self.refresh_ref_lists()
        self.refresh_items()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        outer = ttk.Panedwindow(self, orient="horizontal")
        outer.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        # left list
        left = ctk.CTkFrame(outer)
        left.grid_rowconfigure(2, weight=1)
        left.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(left, text="Semilavorati", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )
        sbar = ctk.CTkFrame(left, fg_color="transparent")
        sbar.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
        sbar.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(sbar, text="Cerca").grid(row=0, column=0, padx=(0, 6))
        ctk.CTkEntry(sbar, textvariable=self.var_search).grid(row=0, column=1, sticky="ew")
        ctk.CTkButton(sbar, text="Aggiorna", width=90, command=self.refresh_items).grid(row=0, column=2, padx=(6, 0))
        ctk.CTkButton(sbar, text="Rif.", width=60, command=self.refresh_ref_lists).grid(row=0, column=3, padx=(6, 0))
        ctk.CTkButton(sbar, text="Gestisci famiglie", width=170, command=self._open_taxonomy_dialog).grid(
            row=0, column=4, padx=(6, 0)
        )
        ctk.CTkCheckBox(
            sbar,
            text="Solo pref. dim",
            variable=self.var_only_preferred_dim,
            command=self.refresh_items,
        ).grid(row=0, column=5, sticky="w", padx=(10, 0))

        lf = ctk.CTkFrame(left)
        lf.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        lf.grid_rowconfigure(0, weight=1)
        lf.grid_columnconfigure(0, weight=1)

        cols = ("PREF", "FAMIGLIA", "STATO", "MATERIALE", "DESCRIZIONE", "DIM", "AGG")
        self.tree = ttk.Treeview(lf, columns=cols, show="headings", height=14)
        col_cfg = {
            "PREF": (55, "center", False),
            "FAMIGLIA": (130, "w", True),
            "STATO": (130, "w", True),
            "MATERIALE": (180, "w", True),
            "DESCRIZIONE": (260, "w", True),
            "DIM": (140, "w", True),
            "AGG": (140, "w", True),
        }
        for c in cols:
            self.tree.heading(c, text=c)
            width, anchor, stretch = col_cfg.get(c, (140, "w", True))
            self.tree.column(c, width=width, anchor=anchor, stretch=stretch)
        make_treeview_sortable(self.tree)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.bind("<<TreeviewSelect>>", self._on_select_item)

        # right form
        right = ctk.CTkFrame(outer)
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(right, text="Dettaglio", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )

        form = ctk.CTkFrame(right)
        form.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        for i in range(4):
            form.grid_columnconfigure(i, weight=1)

        def lab(t, r, c):
            ctk.CTkLabel(form, text=t).grid(row=r, column=c, sticky="w", padx=6, pady=(6, 2))

        lab("FAMIGLIA SEMILAVORATO", 0, 0)
        self.opt_type = ctk.CTkOptionMenu(form, values=["—"], variable=self.var_type)
        self.opt_type.grid(row=1, column=0, sticky="ew", padx=6, pady=(0, 6))

        lab("STATO / SOTTOFAMIGLIA", 0, 1)
        self.opt_state = ctk.CTkOptionMenu(form, values=["—"], variable=self.var_state)
        self.opt_state.grid(row=1, column=1, sticky="ew", padx=6, pady=(0, 6))

        lab("MATERIALE", 0, 2)
        self.opt_mat = ctk.CTkOptionMenu(form, values=["—"], variable=self.var_mat)
        self.opt_mat.grid(row=1, column=2, sticky="ew", padx=6, pady=(0, 6))

        lab("NORMA", 0, 3)
        ctk.CTkEntry(form, textvariable=self.var_std).grid(row=1, column=3, sticky="ew", padx=6, pady=(0, 6))

        lab("DESCRIZIONE", 2, 0)
        ctk.CTkEntry(form, textvariable=self.var_desc).grid(row=3, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 6))

        lab("DIMENSIONI", 2, 2)
        ctk.CTkEntry(form, textvariable=self.var_dim).grid(row=3, column=2, columnspan=2, sticky="ew", padx=6, pady=(0, 6))

        lab("NOTE", 4, 0)
        ctk.CTkEntry(form, textvariable=self.var_notes).grid(row=5, column=0, columnspan=4, sticky="ew", padx=6, pady=(0, 6))

        btns = ctk.CTkFrame(form, fg_color="transparent")
        btns.grid(row=6, column=0, columnspan=4, sticky="e", padx=6, pady=(0, 6))
        ctk.CTkButton(btns, text="Nuovo", width=90, command=self.new_item).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Copia", width=90, command=self.copy_item).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Salva", width=90, command=self.save_item).pack(side="left", padx=4)
        ctk.CTkButton(btns, text="Elimina", width=90, command=self.delete_item).pack(side="left", padx=4)

        self.box_dims = SemiDimensionsBox(right, self.db)
        self.box_dims.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))

        outer.add(left, weight=2)
        outer.add(right, weight=2)

    def _on_taxonomy_dialog_closed(self):
        self._taxonomy_dialog = None
        self.refresh_ref_lists()
        self.refresh_items()

    def _open_taxonomy_dialog(self):
        try:
            if self._taxonomy_dialog is not None and self._taxonomy_dialog.winfo_exists():
                self._taxonomy_dialog.focus_set()
                return
        except Exception:
            self._taxonomy_dialog = None
        self._taxonomy_dialog = SemiTaxonomyDialog(self, self.db, on_close=self._on_taxonomy_dialog_closed)
        self._taxonomy_dialog.focus_set()

    def refresh_ref_lists(self):
        self._types = [(int(r["id"]), str(r["description"])) for r in self.db.fetch_semi_types()]
        self._states = [(int(r["id"]), str(r["description"])) for r in self.db.fetch_semi_states()]
        mats = self.db.search_materials("")
        self._materials = []
        base_counts: Dict[str, int] = {}
        for r in mats:
            mid = int(r["id"])
            base_label = f"{_row_str(r, 'family')} - {_row_str(r, 'description')}".strip(" -")
            idx = base_counts.get(base_label, 0) + 1
            base_counts[base_label] = idx
            label = base_label if idx == 1 else f"{base_label} ({idx})"
            self._materials.append((mid, label))

        type_vals = [t[1] for t in self._types] or ["—"]
        state_vals = [s[1] for s in self._states] or ["—"]
        mat_vals = ["—"] + [m[1] for m in self._materials]

        self.opt_type.configure(values=type_vals)
        self.opt_state.configure(values=state_vals)
        self.opt_mat.configure(values=mat_vals)

        if self.var_type.get() not in type_vals:
            self.var_type.set(type_vals[0])
        if self.var_state.get() not in state_vals:
            self.var_state.set(state_vals[0])
        if self.var_mat.get() not in mat_vals:
            self.var_mat.set("—")

    def refresh_items(self):
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        q = (self.var_search.get() or "").strip()
        rows = self.db.search_semi_items(
            q,
            only_preferred_dimension=bool(self.var_only_preferred_dim.get()),
        )
        for r in rows:
            pref = "X" if int(r["has_preferred_dimension"] or 0) else ""
            self.tree.insert(
                "",
                "end",
                iid=str(r["id"]),
                values=(
                    pref,
                    _row_str(r, "type_desc"),
                    _row_str(r, "state_desc"),
                    _row_str(r, "mat_label"),
                    _row_str(r, "description"),
                    _row_str(r, "dim_display") or _row_str(r, "dimensions"),
                    _row_str(r, "updated_at"),
                ),
            )

    def new_item(self):
        self.item_id = None
        self._copy_from_item_id = None
        self.var_desc.set("")
        self.var_dim.set("")
        self.var_std.set("")
        self.var_notes.set("")
        self.box_dims.set_semi_item(None)
        # keep dropdowns as-is

    def copy_item(self):
        if self.item_id is None:
            return
        self._copy_from_item_id = self.item_id
        # just reset ID, keep current fields
        self.item_id = None
        self.box_dims.set_semi_item(None)
        messagebox.showinfo(
            "Semilavorati",
            "Dati copiati: salva per creare un nuovo semilavorato (verra copiata anche la lista dimensionale).",
        )

    def _on_select_item(self, _evt=None):
        sel = self.tree.selection()
        if not sel:
            return
        self.item_id = int(sel[0])
        self._copy_from_item_id = None
        row = self.db.read_semi_item(self.item_id)
        self.var_type.set(_row_str(row, "type_desc"))
        self.var_state.set(_row_str(row, "state_desc"))
        self.var_mat.set(self._mat_label_from_id(row["material_id"]))
        self.var_desc.set(_row_str(row, "description"))
        self.var_dim.set(_row_str(row, "dimensions"))
        self.var_std.set(_row_str(row, "standard"))
        self.var_notes.set(_row_str(row, "notes"))
        self.box_dims.set_semi_item(self.item_id)

    def _select_item_row_if_present(self, item_id: Optional[int]):
        if item_id is None:
            return
        iid = str(item_id)
        if self.tree.exists(iid):
            self.tree.selection_set(iid)
            self.tree.focus(iid)
            self.tree.see(iid)

    def _type_id_from_desc(self, desc: str) -> Optional[int]:
        for tid, d in self._types:
            if d == desc:
                return tid
        return None

    def _state_id_from_desc(self, desc: str) -> Optional[int]:
        for sid, d in self._states:
            if d == desc:
                return sid
        return None

    def _mat_id_from_label(self, label: str) -> Optional[int]:
        for mid, l in self._materials:
            if l == label:
                return mid
        return None

    def _mat_label_from_id(self, material_id: Optional[int]) -> str:
        if not material_id:
            return "—"
        for mid, l in self._materials:
            if mid == int(material_id):
                return l
        return "—"

    def save_item(self):
        t_desc = (self.var_type.get() or "").strip()
        s_desc = (self.var_state.get() or "").strip()
        desc = (self.var_desc.get() or "").strip()
        if not t_desc or not s_desc or not desc:
            messagebox.showwarning("Semilavorati", "Compila almeno FAMIGLIA, STATO e DESCRIZIONE.")
            return
        type_id = self._type_id_from_desc(t_desc)
        state_id = self._state_id_from_desc(s_desc)
        if not type_id or not state_id:
            messagebox.showwarning("Semilavorati", "Famiglie/Stati non validi. Premi 'Rif.' e riprova.")
            return
        mat_label = (self.var_mat.get() or "").strip()
        material_id = None if mat_label == "—" else self._mat_id_from_label(mat_label)

        payload = {
            "type_id": type_id,
            "state_id": state_id,
            "material_id": material_id,
            "description": desc,
            "dimensions": self.var_dim.get(),
            "standard": self.var_std.get(),
            "notes": self.var_notes.get(),
            "is_active": 1,
        }
        try:
            if self.item_id is None:
                self.item_id = self.db.create_semi_item(payload)
                if self._copy_from_item_id is not None:
                    self.db.clone_semi_dimensions(self._copy_from_item_id, self.item_id)
                self._copy_from_item_id = None
                messagebox.showinfo("Semilavorati", "Creato.")
            else:
                self.db.update_semi_item(self.item_id, payload)
                messagebox.showinfo("Semilavorati", "Aggiornato.")
            self.refresh_items()
            self._select_item_row_if_present(self.item_id)
            self.box_dims.set_semi_item(self.item_id)
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore salvataggio: {e}")

    def delete_item(self):
        if self.item_id is None:
            return
        if not messagebox.askyesno("Semilavorati", "Eliminare il semilavorato selezionato?"):
            return
        try:
            self.db.delete_semi_item(self.item_id)
            self.new_item()
            self.refresh_items()
            self.box_dims.set_semi_item(None)
        except Exception as e:
            messagebox.showerror("Semilavorati", f"Errore eliminazione: {e}")



