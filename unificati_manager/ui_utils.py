from __future__ import annotations

import re
from typing import Iterable, Optional

import customtkinter as ctk
from tkinter import ttk


def bind_uppercase(var: ctk.StringVar) -> None:
    """Forza il contenuto in MAIUSCOLO (senza loop di callback)."""
    state = {"busy": False}

    def _on_change(*_):
        if state["busy"]:
            return
        state["busy"] = True
        try:
            v = (var.get() or "").upper()
            if v != var.get():
                var.set(v)
        finally:
            state["busy"] = False

    var.trace_add("write", _on_change)


def make_treeview_sortable(tree: ttk.Treeview, numeric_cols: Optional[Iterable[str]] = None) -> None:
    numeric_cols = set(numeric_cols or [])

    def _convert(v: str, col: str):
        v = (v or "").strip()
        if col in numeric_cols:
            try:
                return float(v.replace(",", "."))
            except Exception:
                return v.lower()
        if re.fullmatch(r"-?\d+([.,]\d+)?", v):
            try:
                return float(v.replace(",", "."))
            except Exception:
                pass
        return v.lower()

    def _sort(col: str, reverse: bool):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        data.sort(key=lambda t: _convert(t[0], col), reverse=reverse)
        for idx, (_, k) in enumerate(data):
            tree.move(k, "", idx)
        tree.heading(col, command=lambda: _sort(col, not reverse))

    for col in tree["columns"]:
        tree.heading(col, command=lambda c=col: _sort(c, False))
