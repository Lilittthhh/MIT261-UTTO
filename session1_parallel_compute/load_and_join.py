"""
load_and_join.py - Session 1, Part 4.

Assembles the working dataset and proves the join neither duplicates nor loses
an Event row.

Join path (all inner, all many-to-one):
    sales
      |> shops on shop_id
      |> items on item_id
      |> item_categories on item_category_id

The monetization metric is derived as:
    revenue = item_price * item_cnt_day

Run:
    python load_and_join.py
"""

import sys
import time
import numpy as np
import pandas as pd
import config as cfg

def load_frames() -> dict[str, pd.DataFrame]:
    return {name: pd.read_csv(cfg.path_for(name)) for name in cfg.FILES}

def build_working_dataset(
    frames: dict[str, pd.DataFrame] | None = None,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict]:

    if frames is None:
        frames = load_frames()

    sales = frames["sales"].copy()
    shops = frames["shops"]
    items = frames["items"]
    cats = frames["item_categories"]

    # Raw sales_train.csv has no natural row ID; create a reproducible surrogate.
    sales.insert(0, "sale_id", np.arange(len(sales), dtype=np.int64))
    sales[cfg.EVENT_TIME_FIELD] = pd.to_datetime(
        sales[cfg.EVENT_TIME_FIELD], format="%d.%m.%Y"
    )
    sales[cfg.METRIC_FIELD] = sales["item_price"] * sales["item_cnt_day"]

    rows_before = len(sales)

    start = time.perf_counter()
    working = (
        sales
        .merge(
            shops,
            on=cfg.SHOP_KEY,
            how="inner",
            validate="many_to_one",
        )
        .merge(
            items,
            on=cfg.ITEM_KEY,
            how="inner",
            validate="many_to_one",
        )
        .merge(
            cats,
            on=cfg.CATEGORY_KEY,
            how="inner",
            validate="many_to_one",
        )
    )
    join_seconds = time.perf_counter() - start
    rows_after = len(working)

    report = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "delta": rows_after - rows_before,
        "columns_after": len(working.columns),
        "join_seconds": round(join_seconds, 4),
        "join_path": (
            "sales |> shops on shop_id |> items on item_id "
            "|> item_categories on item_category_id"
        ),
        "derived_metric": "revenue = item_price * item_cnt_day",
    }

    if verbose:
        cfg.banner("JOIN PATH RECONCILIATION")
        print(f" join path        : {report['join_path']}")
        print(f" rows before join : {rows_before:,}")
        print(f" rows after join  : {rows_after:,}")
        print(f" difference       : {report['delta']}")
        print(f" columns after    : {report['columns_after']}")
        print(f" join time        : {report['join_seconds']} s")

    assert rows_after == rows_before, (
        f"Join changed row count: {rows_before} -> {rows_after}. "
        "Check parent-key uniqueness and unmatched foreign keys."
    )

    return working, report

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - LOAD AND JOIN")

    frames = load_frames()
    for name, df in frames.items():
        print(f" loaded {name:<18} {len(df):>9,} rows")

    working, _ = build_working_dataset(frames)

    print("\n working dataset columns:")
    for col in working.columns:
        print(f" - {col}")

    working.to_parquet(cfg.OUT_JOINED, index=False)
    print(f"\nWrote {cfg.OUT_JOINED} ({len(working):,} rows)")

    counts = working[cfg.PARTITION_KEY].value_counts()
    cfg.banner(f"PARTITION KEY PREVIEW - {cfg.PARTITION_KEY}")
    print(f" distinct values : {counts.size}")
    print(
        f" records per key : min={counts.min():,} "
        f"median={int(counts.median()):,} max={counts.max():,}"
    )
    print(f" skew ratio      : {counts.max() / counts.min():.2f} : 1")
    print(f"\n heaviest 5:\n{counts.head(5).to_string()}")
    print(f"\n lightest 5:\n{counts.tail(5).to_string()}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
