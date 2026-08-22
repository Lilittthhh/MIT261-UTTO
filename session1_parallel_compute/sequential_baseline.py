"""
sequential_baseline.py - Session 1, Part 6.

Computes the reference result with pandas, single-threaded and in-process.
The same join and the same aggregation are used by the parallel version.

Run:
    python sequential_baseline.py
"""

import statistics
import sys
import time
import pandas as pd
import config as cfg
from load_and_join import build_working_dataset


def compute(working: pd.DataFrame) -> pd.DataFrame:
    """Transaction count, total revenue, and mean revenue per shop."""
    result = (
        working
        .groupby(cfg.PARTITION_KEY)[cfg.METRIC_FIELD]
        .agg(["count", "sum", "mean"])
        .reset_index()
    )

    result.columns = [
        cfg.PARTITION_KEY,
        "txn_count",
        "revenue_total",
        "revenue_mean",
    ]

    return result


def run_baseline(
    working: pd.DataFrame | None = None,
    repeats: int = cfg.BASELINE_REPEATS,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict]:

    if working is None:
        working, _ = build_working_dataset(verbose=False)

    if repeats < 1:
        raise ValueError("repeats must be at least 1")

    times = []
    result: pd.DataFrame | None = None

    for _ in range(repeats):
        start = time.perf_counter()
        result = compute(working)
        times.append(time.perf_counter() - start)

    assert result is not None

    report = {
        "runs": [round(t, 4) for t in times],
        "median_seconds": round(statistics.median(times), 4),
        "mean_seconds": round(statistics.fmean(times), 4),
        "groups": int(len(result)),
        "repeats": repeats,
    }

    if verbose:
        cfg.banner("SEQUENTIAL BASELINE")

        for i, t in enumerate(times, 1):
            print(f" run {i}: {t:.4f} s")

        print(f"\n median : {report['median_seconds']:.4f} s")
        print(f" mean   : {report['mean_seconds']:.4f} s")
        print(f" groups : {report['groups']}")

    return result, report


def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - SEQUENTIAL BASELINE")

    working, _ = build_working_dataset()
    result, _ = run_baseline(working)

    ranked = result.sort_values("revenue_total", ascending=False)

    print("\n top 10 shops by revenue:")
    print(ranked.head(10).to_string(index=False))

    total = result["revenue_total"].sum()
    print(f"\n total revenue across {len(result)} shops: {total:,.2f}")

    result.sort_values(cfg.PARTITION_KEY).to_csv(cfg.OUT_BASELINE, index=False)
    print(f"\nWrote {cfg.OUT_BASELINE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
