"""
partition_strategy.py - Session 1, Part 5.

Evaluates every joined column as a possible partition key and records evidence
for the selected key.

Chosen key: shop_id

Run:
    python partition_strategy.py
"""

import json
import sys
import pandas as pd
import config as cfg
from load_and_join import build_working_dataset, load_frames

NEVER_PARTITION = {
    "sale_id",
    cfg.METRIC_FIELD,
    "item_price",
    "item_cnt_day",
    cfg.EVENT_TIME_FIELD,
    "shop_name",
    "item_name",
    "item_category_name",
}

MIN_DISTINCT = 2
MAX_DISTINCT_RATIO = 0.5

def owning_file(column: str, frames: dict[str, pd.DataFrame]) -> str:
    owners = [name for name, df in frames.items() if column in df.columns]
    if not owners:
        if column == "sale_id":
            return "derived from sales row order"
        if column == cfg.METRIC_FIELD:
            return "derived in join"
        return "derived in join"
    return " + ".join(owners)

def evaluate(working: pd.DataFrame, frames: dict[str, pd.DataFrame]) -> list[dict]:
    rows = len(working)
    candidates = []

    for col in working.columns:
        counts = working[col].value_counts(dropna=False)
        distinct = int(counts.size)
        rejects = []

        if col in NEVER_PARTITION:
            rejects.append("identifier/measure/time/free-text, not a grouping key")
        if distinct < MIN_DISTINCT:
            rejects.append(f"only {distinct} distinct value")
        if distinct > rows * MAX_DISTINCT_RATIO:
            rejects.append(f"{distinct:,} distinct values is near-unique")

        skew = round(counts.max() / counts.min(), 2) if distinct else 0.0
        if not rejects and skew == 1.0:
            rejects.append("perfectly uniform - no skew to analyse")

        candidates.append({
            "column": col,
            "source_file": owning_file(col, frames),
            "distinct": distinct,
            "min": int(counts.min()) if distinct else 0,
            "median": int(counts.median()) if distinct else 0,
            "max": int(counts.max()) if distinct else 0,
            "skew_ratio": skew,
            "viable": not rejects,
            "rejected_because": "; ".join(rejects),
        })

    return candidates

def survives_join(
    key: str,
    frames: dict[str, pd.DataFrame],
    working: pd.DataFrame,
) -> dict:
    present = key in working.columns
    origin = owning_file(key, frames)
    nulls = int(working[key].isna().sum()) if present else None
    return {
        "key": key,
        "present_after_join": present,
        "entered_from": origin,
        "nulls_after_join": nulls,
        "verdict": (
            "survives the join intact"
            if present and nulls == 0
            else "DOES NOT survive the join"
        ),
    }

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - PARTITIONING STRATEGY")

    frames = load_frames()
    working, join_report = build_working_dataset(frames, verbose=False)
    print(f" joined dataset: {len(working):,} rows x {len(working.columns)} columns")

    cfg.banner("CANDIDATE PARTITION KEYS")
    candidates = evaluate(working, frames)

    header = (
        f" {'column':<24}{'distinct':>10}{'min':>10}"
        f"{'median':>10}{'max':>10}{'skew':>8} verdict"
    )
    print(header)
    print(" " + "-" * (len(header) + 25))

    for c in sorted(candidates, key=lambda x: (not x["viable"], -x["skew_ratio"])):
        verdict = "VIABLE" if c["viable"] else c["rejected_because"]
        print(
            f" {c['column']:<24}{c['distinct']:>10,}{c['min']:>10,}"
            f"{c['median']:>10,}{c['max']:>10,}{c['skew_ratio']:>8.2f} "
            f"{verdict}"
        )

    viable = [c for c in candidates if c["viable"]]
    print(f"\n {len(viable)} viable candidate(s) of {len(candidates)} columns")

    chosen = next(c for c in candidates if c["column"] == cfg.PARTITION_KEY)
    survival = survives_join(cfg.PARTITION_KEY, frames, working)

    cfg.banner(f"CHOSEN KEY: {cfg.PARTITION_KEY}")
    print(f" entity/file that owns key : {survival['entered_from']}")
    print(f" distinct key values       : {chosen['distinct']:,}")
    print(f" survives join             : {survival['verdict']}")
    print(f" nulls after join          : {survival['nulls_after_join']}")
    print(
        f" predicted records/key     : min={chosen['min']:,} "
        f"median={chosen['median']:,} max={chosen['max']:,}"
    )
    print(f" predicted skew ratio      : {chosen['skew_ratio']} : 1")

    alt = next(c for c in candidates if c["column"] == cfg.ALTERNATIVE_KEY)
    cfg.banner(f"ALTERNATIVE KEY: {cfg.ALTERNATIVE_KEY}")
    print(f" distinct values : {alt['distinct']:,}")
    print(
        f" records per key : min={alt['min']:,} "
        f"median={alt['median']:,} max={alt['max']:,}"
    )
    print(f" skew ratio      : {alt['skew_ratio']} : 1")
    print(f" verdict         : {'VIABLE' if alt['viable'] else alt['rejected_because']}")
    print(
        " shop_id is retained because revenue per shop is a clear monetization "
        "question and gives a simple, explainable business grouping."
    )

    cfg.banner("WORKLOAD DEFINITION")
    workload = (
        f"Compute transaction count, total {cfg.METRIC_FIELD}, and mean "
        f"{cfg.METRIC_FIELD} per {cfg.PARTITION_KEY} across the joined "
        f"{len(working):,}-row dataset."
    )
    print(f" {workload}")

    schema = {
        cfg.PARTITION_KEY: "int64",
        "txn_count": "int64",
        "revenue_total": "double",
        "revenue_mean": "double",
    }
    print("\n expected output schema:")
    for col, dtype in schema.items():
        print(f" {col:<16} {dtype}")

    print("\n independence check:")
    print(" each shop's aggregate can be computed independently and combined later.")

    out = cfg.RESULTS_DIR / "partition_strategy.json"
    out.write_text(json.dumps({
        "chosen_key": cfg.PARTITION_KEY,
        "owned_by": survival["entered_from"],
        "join_survival": survival,
        "prediction": {
            "distinct": chosen["distinct"],
            "min": chosen["min"],
            "median": chosen["median"],
            "max": chosen["max"],
            "skew_ratio": chosen["skew_ratio"],
        },
        "alternative": alt,
        "all_candidates": candidates,
        "workload": workload,
        "output_schema": schema,
        "join": join_report,
    }, indent=2), encoding="utf-8")

    print(f"\nWrote {out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
