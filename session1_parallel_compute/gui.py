"""
SaleFlow — Session 1 Parallel Compute Console
GUI for MIT 261 Predict Future Sales project.

Uses only Python's standard Tkinter GUI library.
Place/run this file inside session1_parallel_compute/.
"""

from __future__ import annotations

import csv
import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

BASE = Path(__file__).resolve().parent
RESULTS = BASE / "results"

STAGES = [
    ("1. Profile files", "pandas", "profile_files.py",
     "Profile the four files, verify keys/FKs, and check eligibility."),
    ("2. Load and join", "pandas", "load_and_join.py",
     "Join sales, shops, items, and item categories; reconcile row count."),
    ("3. Partition strategy", "pandas", "partition_strategy.py",
     "Evaluate candidate keys and justify shop_id."),
    ("4. Sequential baseline", "pandas", "sequential_baseline.py",
     "Create the pandas reference result."),
    ("5. Parallel compute", "Spark", "parallel_compute.py",
     "Run bounded PySpark aggregation and validate against baseline."),
    ("6. Benchmark", "Spark", "benchmark.py",
     "Compare 2, 4, and 8 Spark partitions."),
    ("7. Partition balance", "Spark", "partition_analysis.py",
     "Measure physical partition balance and skew."),
]

ARTIFACTS = [
    ("file_profile.json", "profile_files.py"),
    ("working_dataset.parquet", "load_and_join.py"),
    ("partition_strategy.json", "partition_strategy.py"),
    ("baseline_result.csv", "sequential_baseline.py"),
    ("shop_revenue.parquet", "parallel_compute.py"),
    ("validation_report.json", "parallel_compute.py"),
    ("session1_benchmark.csv", "benchmark.py"),
    ("partition_sizes.csv", "partition_analysis.py"),
]


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {} if default is None else default


def read_csv_rows(path: Path):
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def human_size(n):
    if n is None:
        return "—"
    n = float(n)
    units = ["B", "KB", "MB", "GB"]
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024



class SaleFlowApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SaleFlow — Session 1 Parallel Compute Console")
        self.geometry("1500x900")
        self.minsize(1180, 720)

        # Modern light theme
        self.bg = "#f7fafc"
        self.surface = "#ffffff"
        self.teal = "#0f766e"
        self.teal_dark = "#0b5f59"
        self.teal_soft = "#e8f7f5"
        self.blue_soft = "#edf6ff"
        self.purple_soft = "#f4f0ff"
        self.green_soft = "#eef9f1"
        self.green = "#198754"
        self.orange_soft = "#fff7e7"
        self.orange = "#b7791f"
        self.gray = "#64748b"
        self.text = "#0f172a"
        self.border = "#dbe4ee"
        self.muted = "#8a99aa"

        self.configure(bg=self.bg)
        self.log_queue = queue.Queue()
        self.running = False
        self.stage_status = {s[0]: "not run" for s in STAGES}

        self._styles()
        self._build_header()
        self._build_tabs()
        self._build_pipeline_tab()
        self._build_files_tab()
        self._build_join_tab()
        self._build_baseline_tab()
        self._build_correctness_tab()
        self._build_partition_tab()
        self._build_console_tab()

        self.refresh_from_results()
        self.after(120, self._drain_log_queue)

    def _styles(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(
            "TNotebook",
            background=self.bg,
            borderwidth=0,
            tabmargins=(18, 0, 18, 0)
        )
        style.configure(
            "TNotebook.Tab",
            padding=(15, 10),
            font=("Segoe UI", 9),
            background=self.bg,
            foreground="#475569",
            borderwidth=0
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", self.bg)],
            foreground=[("selected", self.teal)]
        )

        style.configure(
            "Treeview",
            font=("Segoe UI", 9),
            rowheight=28,
            background=self.surface,
            fieldbackground=self.surface,
            foreground=self.text,
            borderwidth=0
        )
        style.configure(
            "Treeview.Heading",
            font=("Segoe UI", 9, "bold"),
            background=self.teal,
            foreground="white",
            relief="flat",
            padding=(6, 6)
        )
        style.map(
            "Treeview.Heading",
            background=[("active", self.teal_dark)]
        )

    def _build_header(self):
        header = tk.Frame(self, bg=self.surface, height=88)
        header.pack(fill="x")
        header.pack_propagate(False)

        left = tk.Frame(header, bg=self.surface)
        left.pack(side="left", fill="both", expand=True, padx=(24, 10), pady=12)

        tk.Label(
            left,
            text="SaleFlow — Session 1 Parallel Compute Console",
            font=("Segoe UI", 18, "bold"),
            fg=self.text,
            bg=self.surface
        ).pack(anchor="w")

        tk.Label(
            left,
            text="MIT 261 Parallel and Distributed Systems  •  Predict Future Sales  •  "
                 "partition key shop_id  •  bounded parallelism 4  •  settings (2, 4, 8)",
            font=("Segoe UI", 9),
            fg=self.gray,
            bg=self.surface
        ).pack(anchor="w", pady=(5, 0))

        badge = tk.Frame(
            header,
            bg="#f0fdf4",
            highlightbackground="#bbf7d0",
            highlightthickness=1
        )
        badge.pack(side="right", padx=24, pady=24)

        tk.Label(
            badge,
            text="●",
            font=("Segoe UI", 9, "bold"),
            fg="#16a34a",
            bg="#f0fdf4"
        ).pack(side="left", padx=(10, 4), pady=6)

        tk.Label(
            badge,
            text="All systems nominal",
            font=("Segoe UI", 9),
            fg="#166534",
            bg="#f0fdf4"
        ).pack(side="left", padx=(0, 10), pady=6)

        tk.Frame(self, bg=self.border, height=1).pack(fill="x")

    def _build_tabs(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True)

        self.tab_pipeline = tk.Frame(self.notebook, bg=self.bg)
        self.tab_files = tk.Frame(self.notebook, bg=self.bg)
        self.tab_join = tk.Frame(self.notebook, bg=self.bg)
        self.tab_baseline = tk.Frame(self.notebook, bg=self.bg)
        self.tab_correct = tk.Frame(self.notebook, bg=self.bg)
        self.tab_partition = tk.Frame(self.notebook, bg=self.bg)
        self.tab_console = tk.Frame(self.notebook, bg=self.bg)

        self.notebook.add(self.tab_pipeline, text="Pipeline")
        self.notebook.add(self.tab_files, text="Files & eligibility")
        self.notebook.add(self.tab_join, text="Join & partition key")
        self.notebook.add(self.tab_baseline, text="Baseline vs parallel")
        self.notebook.add(self.tab_correct, text="Correctness & output")
        self.notebook.add(self.tab_partition, text="Partition balance")
        self.notebook.add(self.tab_console, text="Console")

    def _card(self, parent, title, subtitle, card_bg, accent):
        f = tk.Frame(
            parent,
            bg=card_bg,
            highlightbackground=self.border,
            highlightthickness=1
        )

        inner = tk.Frame(f, bg=card_bg)
        inner.pack(fill="both", expand=True, padx=18, pady=14)

        dot = tk.Label(
            inner,
            text="●",
            font=("Segoe UI", 16, "bold"),
            fg=accent,
            bg=card_bg
        )
        dot.pack(side="left", padx=(0, 14))

        textwrap_frame = tk.Frame(inner, bg=card_bg)
        textwrap_frame.pack(side="left", fill="both", expand=True)

        value = tk.Label(
            textwrap_frame,
            text=title,
            font=("Segoe UI", 18, "bold"),
            bg=card_bg,
            fg=self.text
        )
        value.pack(anchor="w")

        tk.Label(
            textwrap_frame,
            text=subtitle,
            font=("Segoe UI", 8),
            bg=card_bg,
            fg=self.gray
        ).pack(anchor="w", pady=(4, 0))

        return f, value

    def _modern_button(self, parent, text, command, primary=False, warm=False):
        if primary:
            bg = self.teal
            fg = "white"
            active = self.teal_dark
        elif warm:
            bg = "#fff4e8"
            fg = "#9a5c1b"
            active = "#ffe7cf"
        else:
            bg = self.surface
            fg = "#334155"
            active = "#f1f5f9"

        return tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg=fg,
            activebackground=active,
            activeforeground=fg,
            relief="flat",
            bd=0,
            padx=13,
            pady=7,
            font=("Segoe UI", 8),
            cursor="hand2",
            highlightbackground=self.border,
            highlightthickness=1
        )

    def _build_pipeline_tab(self):
        outer = tk.Frame(self.tab_pipeline, bg=self.bg)
        outer.pack(fill="both", expand=True, padx=20, pady=14)

        cards = tk.Frame(outer, bg=self.bg)
        cards.pack(fill="x", pady=(0, 12))

        c1, self.card_rows = self._card(cards, "—", "rows through the join", self.blue_soft, "#2780d9")
        c2, self.card_groups = self._card(cards, "—", "groups in the result", self.teal_soft, "#159c97")
        c3, self.card_fast = self._card(cards, "—", "fastest condition measured", self.purple_soft, "#7567d8")
        c4, self.card_pass = self._card(cards, "—", "parallel vs baseline", self.green_soft, self.green)

        for c in (c1, c2, c3, c4):
            c.pack(side="left", fill="x", expand=True, padx=5)

        action_box = tk.Frame(
            outer,
            bg=self.surface,
            highlightbackground=self.border,
            highlightthickness=1
        )
        action_box.pack(fill="x", pady=(0, 10))

        controls = tk.Frame(action_box, bg=self.surface)
        controls.pack(fill="x", padx=12, pady=10)

        tk.Label(
            controls,
            text="Run a stage",
            font=("Segoe UI", 9, "bold"),
            bg=self.surface,
            fg=self.text
        ).pack(side="left", padx=(0, 10))

        for label, _, script, _ in STAGES:
            short = label.split(". ", 1)[1]
            btn = self._modern_button(
                controls,
                short,
                command=lambda s=script: self.run_stage(s),
                warm=("Parallel" in short or "Benchmark" in short or "Partition balance" in short)
            )
            btn.pack(side="left", padx=3)

        tk.Frame(controls, bg=self.border, width=1, height=28).pack(side="left", padx=10)

        self._modern_button(
            controls,
            "▶  Run everything",
            self.run_everything,
            primary=True
        ).pack(side="left", padx=3)

        self._modern_button(
            controls,
            "↻  Refresh from results",
            self.refresh_from_results
        ).pack(side="left", padx=3)

        stage_frame = tk.Frame(
            outer,
            bg=self.surface,
            highlightbackground=self.border,
            highlightthickness=1
        )
        stage_frame.pack(fill="x", pady=(0, 8))

        cols = ("stage", "engine", "status", "headline")
        self.stage_tree = ttk.Treeview(
            stage_frame,
            columns=cols,
            show="headings",
            height=7
        )

        self.stage_tree.heading("stage", text="Stage")
        self.stage_tree.heading("engine", text="Engine")
        self.stage_tree.heading("status", text="Status")
        self.stage_tree.heading("headline", text="Headline result")

        self.stage_tree.column("stage", width=240, anchor="w")
        self.stage_tree.column("engine", width=100, anchor="center")
        self.stage_tree.column("status", width=110, anchor="center")
        self.stage_tree.column("headline", width=790, anchor="w")
        self.stage_tree.pack(fill="x")

        self.stage_tree.tag_configure("passed", background="#eefaf1")
        self.stage_tree.tag_configure("notrun", background=self.surface)
        self.stage_tree.tag_configure("running", background="#fff9df")
        self.stage_tree.tag_configure("failed", background="#fff0f0")

        note = tk.Label(
            outer,
            text="ⓘ  Stages 5 to 7 use Spark. The GUI launches your existing project scripts and "
                 "then refreshes the generated result files. Spark startup can make the first "
                 "parallel stage take longer.",
            bg=self.orange_soft,
            fg=self.orange,
            anchor="w",
            justify="left",
            font=("Segoe UI", 8),
            padx=10,
            pady=8,
            highlightbackground="#f3d7a3",
            highlightthickness=1
        )
        note.pack(fill="x", pady=(0, 10))

        tk.Label(
            outer,
            text="Artifacts in results/",
            bg=self.bg,
            fg=self.text,
            font=("Segoe UI", 10, "bold")
        ).pack(anchor="w", pady=(0, 5))

        aframe = tk.Frame(
            outer,
            bg=self.surface,
            highlightbackground=self.border,
            highlightthickness=1
        )
        aframe.pack(fill="both", expand=True)

        acols = ("artifact", "writer", "status", "size", "modified")
        self.artifact_tree = ttk.Treeview(
            aframe,
            columns=acols,
            show="headings",
            height=8
        )

        for c, t in zip(
            acols,
            ["Artifact", "Written by", "Status", "Size", "Last written"]
        ):
            self.artifact_tree.heading(c, text=t)

        self.artifact_tree.column("artifact", width=285)
        self.artifact_tree.column("writer", width=210)
        self.artifact_tree.column("status", width=110, anchor="center")
        self.artifact_tree.column("size", width=100, anchor="e")
        self.artifact_tree.column("modified", width=215)

        self.artifact_tree.pack(fill="both", expand=True)
        self.artifact_tree.tag_configure("written", background="#f4fbf6")
        self.artifact_tree.tag_configure("missing", background=self.surface)

    def _make_text_tab(self, parent, title):
        wrap = tk.Frame(parent, bg=self.bg)
        wrap.pack(fill="both", expand=True, padx=20, pady=16)

        titlebar = tk.Frame(wrap, bg=self.bg)
        titlebar.pack(fill="x", pady=(0, 10))

        tk.Label(
            titlebar,
            text=title,
            bg=self.bg,
            fg=self.text,
            font=("Segoe UI", 16, "bold")
        ).pack(anchor="w")

        card = tk.Frame(
            wrap,
            bg=self.surface,
            highlightbackground=self.border,
            highlightthickness=1
        )
        card.pack(fill="both", expand=True)

        text = tk.Text(
            card,
            wrap="word",
            font=("Consolas", 10),
            bg=self.surface,
            fg="#243044",
            relief="flat",
            borderwidth=0,
            padx=14,
            pady=12
        )
        text.pack(fill="both", expand=True)
        text.configure(state="disabled")
        return text

    def _build_files_tab(self):
        self.files_text = self._make_text_tab(
            self.tab_files,
            "Dataset files and eligibility"
        )

    def _build_join_tab(self):
        self.join_text = self._make_text_tab(
            self.tab_join,
            "Join path and partition-key decision"
        )

    def _build_baseline_tab(self):
        self.baseline_text = self._make_text_tab(
            self.tab_baseline,
            "Sequential baseline vs bounded PySpark"
        )

    def _build_correctness_tab(self):
        self.correct_text = self._make_text_tab(
            self.tab_correct,
            "Correctness validation and Session 1 output"
        )

    def _build_partition_tab(self):
        self.partition_text = self._make_text_tab(
            self.tab_partition,
            "Partition balance and skew"
        )

    def _build_console_tab(self):
        wrap = tk.Frame(self.tab_console, bg=self.bg)
        wrap.pack(fill="both", expand=True, padx=20, pady=16)

        top = tk.Frame(wrap, bg=self.bg)
        top.pack(fill="x")

        tk.Label(
            top,
            text="Execution console",
            bg=self.bg,
            fg=self.text,
            font=("Segoe UI", 16, "bold")
        ).pack(side="left")

        self._modern_button(
            top,
            "Render diagrams",
            lambda: self.run_stage("render_diagrams.py")
        ).pack(side="right", padx=4)

        self._modern_button(
            top,
            "Clear",
            lambda: self.console.delete("1.0", "end")
        ).pack(side="right", padx=4)

        console_card = tk.Frame(
            wrap,
            bg="#111827",
            highlightbackground="#243044",
            highlightthickness=1
        )
        console_card.pack(fill="both", expand=True, pady=(10, 0))

        self.console = tk.Text(
            console_card,
            wrap="word",
            font=("Consolas", 9),
            bg="#111827",
            fg="#e5e7eb",
            insertbackground="white",
            relief="flat",
            borderwidth=0,
            padx=12,
            pady=12
        )
        self.console.pack(fill="both", expand=True)

    def _set_text(self, widget, value):
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", value)
        widget.configure(state="disabled")

    def refresh_from_results(self):
        profile = read_json(RESULTS / "file_profile.json")
        strategy = read_json(RESULTS / "partition_strategy.json")
        validation = read_json(RESULTS / "validation_report.json")
        bench = read_csv_rows(RESULTS / "session1_benchmark.csv")
        parts = read_csv_rows(RESULTS / "partition_sizes.csv")

        rows = (
            strategy.get("join", {}).get("rows_after")
            or validation.get("join", {}).get("rows_after")
            or profile.get("profiles", {}).get("sales", {}).get("rows")
        )
        groups = validation.get("validation", {}).get("parallel_groups")
        correct = validation.get("validation", {}).get("correct")

        fastest = None
        best_time = None
        for r in bench:
            p = str(r.get("parallelism_partitions", ""))
            if p.isdigit():
                try:
                    t = float(r.get("execution_time_s", ""))
                except Exception:
                    continue
                if best_time is None or t < best_time:
                    best_time, fastest = t, p

        self.card_rows.config(text=f"{int(rows):,}" if rows else "—")
        self.card_groups.config(text=f"{int(groups):,}" if groups is not None else "—")
        self.card_fast.config(text=f"{fastest}p" if fastest else "—")
        self.card_pass.config(
            text="PASSED" if correct is True else ("FAILED" if correct is False else "—"),
            fg=self.green if correct is not False else "#b42318"
        )

        self._refresh_stage_tree(profile, strategy, validation, bench, parts)
        self._refresh_artifacts()
        self._refresh_detail_tabs(profile, strategy, validation, bench, parts)

    def _refresh_stage_tree(self, profile, strategy, validation, bench, parts):
        for i in self.stage_tree.get_children():
            self.stage_tree.delete(i)

        profile_ok = bool(profile)
        join_ok = (RESULTS / "working_dataset.parquet").exists()
        strategy_ok = bool(strategy)
        baseline_ok = (RESULTS / "baseline_result.csv").exists()
        parallel_ok = bool(validation)
        bench_ok = bool(bench)
        part_ok = bool(parts)

        sales_rows = profile.get("profiles", {}).get("sales", {}).get("rows", 0)
        eligible = profile.get("eligibility", {})
        eligible_count = sum(
            1 for v in eligible.values()
            if isinstance(v, dict) and v.get("met") is True
        )

        j = strategy.get("join", {})
        pred = strategy.get("prediction", {})
        val = validation.get("validation", {})

        fastest = "—"
        if bench:
            candidates = []
            for r in bench:
                p = str(r.get("parallelism_partitions", ""))
                if p.isdigit():
                    try:
                        candidates.append((float(r["execution_time_s"]), p))
                    except Exception:
                        pass
            if candidates:
                t, p = min(candidates)
                fastest = f"best {p}p at {t:.4f}s"

        max_ratio = None
        if parts:
            ratios = []
            for r in parts:
                try:
                    ratios.append(float(r.get("vs_even", "")))
                except Exception:
                    pass
            if ratios:
                max_ratio = max(ratios)

        status_rows = [
            (
                STAGES[0][0], "pandas", profile_ok,
                f"4 files profiled · {eligible_count}/4 eligibility conditions met · {sales_rows:,} event rows"
                if profile_ok else STAGES[0][3]
            ),
            (
                STAGES[1][0], "pandas", join_ok,
                f"{j.get('rows_before', 0):,} rows → {j.get('rows_after', 0):,} rows · delta {j.get('delta', 0)}"
                if j else STAGES[1][3]
            ),
            (
                STAGES[2][0], "pandas", strategy_ok,
                f"chosen shop_id · {pred.get('distinct', '—')} groups · skew {pred.get('skew_ratio', '—')}:1"
                if strategy_ok else STAGES[2][3]
            ),
            (
                STAGES[3][0], "pandas", baseline_ok,
                "pandas reference result written to baseline_result.csv"
                if baseline_ok else STAGES[3][3]
            ),
            (
                STAGES[4][0], "Spark", parallel_ok,
                f"{val.get('parallel_groups', '—')} groups · validation {'PASS' if val.get('correct') else 'FAIL'}"
                if parallel_ok else STAGES[4][3]
            ),
            (
                STAGES[5][0], "Spark", bench_ok,
                fastest if bench_ok else STAGES[5][3]
            ),
            (
                STAGES[6][0], "Spark", part_ok,
                f"physical partition counts measured · max/even {max_ratio:.3f}x"
                if max_ratio is not None else STAGES[6][3]
            ),
        ]

        for idx, (label, engine, ok, actual_headline) in enumerate(status_rows):
            current = self.stage_status.get(label, "")

            # Headline behavior:
            # - not run  -> show the stage's default description
            # - running  -> show a clear running message
            # - passed   -> show the real result generated by the current run
            # - failed   -> tell the user to check the Console tab
            default_headline = STAGES[idx][3]
            script_name = STAGES[idx][2]

            if current == "running":
                status, tag = "running", "running"
                headline = f"Running {script_name}..."
            elif current == "failed":
                status, tag = "failed", "failed"
                headline = "Stage failed — check the Console tab for details."
            elif current == "passed":
                status, tag = "passed", "passed"
                headline = actual_headline
            else:
                status, tag = "not run", "notrun"
                headline = default_headline

            self.stage_tree.insert(
                "",
                "end",
                values=(label, engine, status, headline),
                tags=(tag,)
            )

    def _refresh_artifacts(self):
        for i in self.artifact_tree.get_children():
            self.artifact_tree.delete(i)

        for name, writer in ARTIFACTS:
            path = RESULTS / name
            if path.exists():
                stat = path.stat()
                modified = __import__("datetime").datetime.fromtimestamp(
                    stat.st_mtime
                ).strftime("%Y-%m-%d %H:%M:%S")
                values = (
                    name, writer, "written",
                    human_size(stat.st_size), modified
                )
                tag = "written"
            else:
                values = (name, writer, "missing", "—", "—")
                tag = "missing"

            self.artifact_tree.insert(
                "", "end", values=values, tags=(tag,)
            )

    def _refresh_detail_tabs(self, profile, strategy, validation, bench, parts):
        lines = []
        profiles = profile.get("profiles", {})
        if profiles:
            for key, p in profiles.items():
                lines.append(
                    f"{p.get('file')}  [{p.get('role')}]\n"
                    f"  Rows: {p.get('rows', 0):,}    Columns: {p.get('columns')}\n"
                    f"  Columns: {', '.join(p.get('column_names', []))}\n"
                    f"  Nulls: {sum(p.get('null_counts', {}).values())}\n"
                )
            lines.append("Eligibility")
            for k, v in profile.get("eligibility", {}).items():
                lines.append(f"  {k}: {'PASS' if v.get('met') else 'FAIL'}")

            integ = profile.get("integrity", {}).get("orphan_counts", {})
            lines.append("\nForeign-key orphan counts")
            for k, v in integ.items():
                lines.append(f"  {k}: {v}")
        else:
            lines = ["No profiling result yet. Click “Profile files” or “Run everything”."]

        self._set_text(self.files_text, "\n".join(lines))

        lines = []
        if strategy:
            j = strategy.get("join", {})
            p = strategy.get("prediction", {})
            lines += [
                f"Chosen partition key: {strategy.get('chosen_key')}",
                f"Owned by: {strategy.get('owned_by')}",
                f"Join survival: {strategy.get('join_survival', {}).get('verdict')}",
                "",
                "Join path:",
                f"  {j.get('join_path')}",
                f"Rows before: {j.get('rows_before', 0):,}",
                f"Rows after : {j.get('rows_after', 0):,}",
                f"Delta      : {j.get('delta')}",
                "",
                "Predicted records per shop_id:",
                f"  distinct = {p.get('distinct')}",
                f"  min      = {p.get('min'):,}",
                f"  median   = {p.get('median'):,}",
                f"  max      = {p.get('max'):,}",
                f"  skew     = {p.get('skew_ratio')}:1",
                "",
                "Workload:",
                f"  {strategy.get('workload')}",
            ]
        else:
            lines = ["No partition_strategy.json yet."]

        self._set_text(self.join_text, "\n".join(lines))

        lines = []
        if bench:
            lines.append(
                f"{'Run':28} {'Partitions':14} {'Time(s)':12} "
                f"{'Groups':8} {'Correct':8} {'Speedup'}"
            )
            lines.append("-" * 90)

            for r in bench:
                lines.append(
                    f"{r.get('run','')[:28]:28} "
                    f"{r.get('parallelism_partitions','')[:14]:14} "
                    f"{r.get('execution_time_s',''):12} "
                    f"{r.get('groups',''):8} "
                    f"{r.get('correct',''):8} "
                    f"{r.get('speedup_vs_baseline','')}"
                )

            lines.append(
                "\nNote: on this workload, pandas may be faster because only 60 final "
                "groups are produced and Spark has scheduling/startup overhead."
            )
        else:
            lines = ["No session1_benchmark.csv yet."]

        self._set_text(self.baseline_text, "\n".join(lines))

        lines = []
        if validation:
            j = validation.get("join", {})
            v = validation.get("validation", {})

            lines += [
                f"Configured parallelism: {validation.get('parallelism')}",
                f"Parallel execution time: {validation.get('parallel_seconds')} s",
                "",
                "Join reconciliation",
                f"  Before: {j.get('rows_before', 0):,}",
                f"  After : {j.get('rows_after', 0):,}",
                f"  Delta : {j.get('delta')}",
                f"  BroadcastHashJoin occurrences: {j.get('broadcast_hash_joins')}",
                f"  SortMergeJoin occurrences    : {j.get('sort_merge_joins')}",
                "",
                "Baseline vs parallel",
                f"  Parallel groups: {v.get('parallel_groups')}",
                f"  Baseline groups: {v.get('baseline_groups')}",
                f"  Max txn-count difference    : {v.get('max_txn_count_difference')}",
                f"  Max revenue-total difference: {v.get('max_revenue_total_difference')}",
                f"  Max revenue-mean difference : {v.get('max_revenue_mean_difference')}",
                f"  Allowed total tolerance     : {v.get('revenue_total_allowed_tolerance')}",
                "",
                f"RESULT: {'PASSED' if v.get('correct') else 'FAILED'}",
                "",
                "Output: results/shop_revenue.parquet",
            ]
        else:
            lines = ["No validation_report.json yet."]

        self._set_text(self.correct_text, "\n".join(lines))

        lines = []
        if parts:
            lines.append(
                f"{'Level':18} {'Setting':8} {'Identifier':15} "
                f"{'Count':12} {'Even':12} {'vs even'}"
            )
            lines.append("-" * 84)

            for r in parts:
                lines.append(
                    f"{r.get('level','')[:18]:18} "
                    f"{r.get('setting',''):8} "
                    f"{r.get('identifier','')[:15]:15} "
                    f"{r.get('record_count',''):12} "
                    f"{r.get('even_share',''):12} "
                    f"{r.get('vs_even','')}"
                )
        else:
            lines = ["No partition_sizes.csv yet."]

        self._set_text(self.partition_text, "\n".join(lines))

    def _script_to_label(self, script):
        for label, _, s, _ in STAGES:
            if s == script:
                return label
        return script

    def run_stage(self, script):
        if self.running:
            messagebox.showinfo(
                "SaleFlow",
                "A stage is already running."
            )
            return

        path = BASE / script
        if not path.exists():
            messagebox.showerror(
                "Missing script",
                f"{script} was not found in:\n{BASE}"
            )
            return

        label = self._script_to_label(script)
        if label in self.stage_status:
            self.stage_status[label] = "running"
            self.refresh_from_results()

        self.running = True
        self.notebook.select(self.tab_console)
        self._log(f"\n>>> RUNNING: {script}\n")

        def worker():
            rc = self._execute_script(script)
            self.log_queue.put(("stage_done", script, rc))

        threading.Thread(target=worker, daemon=True).start()

    def run_everything(self):
        if self.running:
            messagebox.showinfo(
                "SaleFlow",
                "The pipeline is already running."
            )
            return

        self.running = True
        self.notebook.select(self.tab_console)
        self._log("\n>>> RUN EVERYTHING\n")

        def worker():
            ok = True

            for label, _, script, _ in STAGES:
                self.stage_status[label] = "running"
                self.log_queue.put(("refresh",))
                self.log_queue.put(
                    (
                        "text",
                        f"\n{'='*70}\n{label} — {script}\n{'='*70}\n"
                    )
                )

                rc = self._execute_script(script)
                self.stage_status[label] = "passed" if rc == 0 else "failed"
                self.log_queue.put(("refresh",))

                if rc != 0:
                    ok = False
                    self.log_queue.put(
                        (
                            "text",
                            f"\nPipeline stopped because {script} returned exit code {rc}.\n"
                        )
                    )
                    break

            self.log_queue.put(("all_done", ok))

        threading.Thread(target=worker, daemon=True).start()

    def _execute_script(self, script):
        try:
            proc = subprocess.Popen(
                [sys.executable, "-u", str(BASE / script)],
                cwd=str(BASE),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                errors="replace",
                env=os.environ.copy(),
            )

            assert proc.stdout is not None

            for line in proc.stdout:
                self.log_queue.put(("text", line))

            return proc.wait()

        except Exception as exc:
            self.log_queue.put(
                ("text", f"\nERROR: {exc}\n")
            )
            return 1

    def _log(self, text):
        self.console.insert("end", text)
        self.console.see("end")

    def _drain_log_queue(self):
        try:
            while True:
                item = self.log_queue.get_nowait()
                kind = item[0]

                if kind == "text":
                    self._log(item[1])

                elif kind == "refresh":
                    self.refresh_from_results()

                elif kind == "stage_done":
                    _, script, rc = item
                    label = self._script_to_label(script)

                    if label in self.stage_status:
                        self.stage_status[label] = "passed" if rc == 0 else "failed"

                    self.running = False
                    self.refresh_from_results()
                    self._log(
                        f"\n>>> FINISHED: {script} (exit code {rc})\n"
                    )

                elif kind == "all_done":
                    self.running = False
                    self.refresh_from_results()
                    self._log(
                        "\n>>> PIPELINE COMPLETE\n"
                        if item[1]
                        else "\n>>> PIPELINE STOPPED WITH AN ERROR\n"
                    )

        except queue.Empty:
            pass

        self.after(120, self._drain_log_queue)


if __name__ == "__main__":
    app = SaleFlowApp()
    app.mainloop()
