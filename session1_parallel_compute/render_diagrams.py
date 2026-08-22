"""
render_diagrams.py - Session 1 diagram generator.

Creates:
    docs/entity-model-session1.png
    architecture/architecture-session1.png

Requires Graphviz command-line tool ("dot") to be installed.
Run:
    python render_diagrams.py
"""

import shutil
import subprocess
import sys
from pathlib import Path
import config as cfg

NAVY = "#1F3864"
BLUE = "#2E74B5"
LIGHT = "#D9EAF7"
AMBER = "#C55A11"
GREY = "#767171"

ENTITY = f"""
digraph EntityModel {{
  rankdir=LR;
  bgcolor="white";
  splines=polyline;
  nodesep=0.7;
  ranksep=0.9;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>Predict Future Sales — Session 1 Entity Model</b><br/>
  <font point-size="11">multiplicity shown at both ends · monetization metric = revenue</font><br/>>;

  node [shape=plaintext fontname="Helvetica"];
  edge [color="{BLUE}" fontname="Helvetica" fontsize=10 penwidth=1.6];

  Shop [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Shop</b><br/><font point-size="9">Entity · shops.csv</font></td></tr>
      <tr><td align="left"><b>shop_id : Integer «PK» «partitionKey»</b></td></tr>
      <tr><td align="left">shop_name : String</td></tr>
    </table>>];

  Sale [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Sale</b><br/><font point-size="9">Event · sales_train.csv</font></td></tr>
      <tr><td align="left">sale_id : Integer «surrogatePK»</td></tr>
      <tr><td align="left">date : Date <b>«eventTime»</b></td></tr>
      <tr><td align="left">date_block_num : Integer</td></tr>
      <tr><td align="left">shop_id : Integer «FK»</td></tr>
      <tr><td align="left">item_id : Integer «FK»</td></tr>
      <tr><td align="left">item_price : Double</td></tr>
      <tr><td align="left">item_cnt_day : Double</td></tr>
      <tr><td align="left" bgcolor="#FFF2CC"><b>revenue : Double «metricField»</b></td></tr>
    </table>>];

  Item [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT}"><b>Item</b><br/><font point-size="9">Entity · items.csv</font></td></tr>
      <tr><td align="left"><b>item_id : Integer «PK»</b></td></tr>
      <tr><td align="left">item_name : String</td></tr>
      <tr><td align="left">item_category_id : Integer «FK»</td></tr>
    </table>>];

  Category [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="#F2F2F2"><b>ItemCategory</b><br/><font point-size="9">Lookup · item_categories.csv</font></td></tr>
      <tr><td align="left"><b>item_category_id : Integer «PK»</b></td></tr>
      <tr><td align="left">item_category_name : String</td></tr>
    </table>>];

  Shop -> Sale [dir=both arrowtail=none arrowhead=none xlabel="1           0..*\\nshop_id"];
  Item -> Sale [dir=both arrowtail=none arrowhead=none xlabel="1           0..*\\nitem_id"];
  Category -> Item [dir=both arrowtail=none arrowhead=none xlabel="1           0..*\\nitem_category_id"];
}}
"""

ARCHITECTURE = f"""
digraph Architecture {{
  rankdir=LR;
  bgcolor="white";
  splines=ortho;
  nodesep=0.4;
  ranksep=0.8;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>Predict Future Sales — Session 1 Ingestion and Parallel-Compute Layer</b><br/>
  <font point-size="11">PySpark local mode · bounded parallelism = {cfg.CHOSEN_PARTITIONS}</font><br/>>;

  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=10 penwidth=1.3];
  edge [color="{BLUE}" penwidth=1.4 fontname="Helvetica" fontsize=9];

  sales [label="sales_train.csv\\nEvent · ~2.9M rows" fillcolor="{LIGHT}" color="{BLUE}"];
  shops [label="shops.csv\\nEntity" fillcolor="{LIGHT}" color="{BLUE}"];
  items [label="items.csv\\nEntity" fillcolor="{LIGHT}" color="{BLUE}"];
  cats [label="item_categories.csv\\nLookup" fillcolor="#F2F2F2" color="{GREY}"];

  profile [label="profile_files.py\\neligibility + inventory" fillcolor="#FFF2CC" color="{AMBER}"];
  join [label="load_and_join.py\\nvalidated joins\\nderive revenue" fillcolor="#FFF2CC" color="{AMBER}"];
  repart [label="repartition({cfg.CHOSEN_PARTITIONS}, 'shop_id')\\nbounded parallelism" fillcolor="#E2EFDA" color="#548235"];
  agg [label="parallel_compute.py\\ngroupBy(shop_id)\\ncount · sum · avg" fillcolor="#E2EFDA" color="#548235"];
  base [label="sequential_baseline.py\\npandas reference" fillcolor="#FCE4D6" color="{AMBER}"];
  valid [label="correctness validation\\ncounts exact · floats Δ < 1e-6" fillcolor="#FCE4D6" color="{AMBER}"];

  output [label="shop_revenue.parquet\\nshop_id · txn_count\\nrevenue_total · revenue_mean" fillcolor="{LIGHT}" color="{NAVY}" penwidth=2.2];
  bench [label="session1_benchmark.csv" fillcolor="{LIGHT}" color="{NAVY}"];

  sales -> profile;
  shops -> profile;
  items -> profile;
  cats -> profile;
  profile -> join;
  join -> repart;
  repart -> agg;
  join -> base;
  agg -> valid;
  base -> valid;
  valid -> output;
  agg -> bench;
}}
"""

def render(dot_text: str, output: Path) -> None:
    dot = shutil.which("dot")
    dot_file = output.with_suffix(".dot")
    dot_file.write_text(dot_text, encoding="utf-8")

    if not dot:
        raise RuntimeError(
            "Graphviz 'dot' was not found. The .dot source was created at "
            f"{dot_file}. Install Graphviz, then run this script again."
        )

    subprocess.run(
        [dot, "-Tpng", str(dot_file), "-o", str(output)],
        check=True,
    )

def main() -> int:
    cfg.DOCS_DIR.mkdir(parents=True, exist_ok=True)
    cfg.ARCH_DIR.mkdir(parents=True, exist_ok=True)

    entity_out = cfg.DOCS_DIR / "entity-model-session1.png"
    arch_out = cfg.ARCH_DIR / "architecture-session1.png"

    try:
        render(ENTITY, entity_out)
        render(ARCHITECTURE, arch_out)
    except Exception as exc:
        print(f"Diagram rendering failed: {exc}")
        return 1

    print(f"Wrote {entity_out}")
    print(f"Wrote {arch_out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
