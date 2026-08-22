"""
benchmark.py - Session 1, Part 8.

Controlled comparison of the pandas baseline against 2, 4, and 8 Spark
partitions. Each parallel condition is repeated and the median is reported.

Run:
    python benchmark.py
"""

import csv
import statistics
import sys
import time
import config as cfg
from parallel_compute import build_joined, compute_parallel
from sequential_baseline import run_baseline
from load_and_join import build_working_dataset

def bench_one(joined, partitions: int, repeats: int) -> dict:
    times, groups = [], None

    for _ in range(repeats):
        start = time.perf_counter()
        _, result = compute_parallel(joined, partitions)
        groups = result.count()
        times.append(time.perf_counter() - start)

    return {
        "partitions": partitions,
        "runs": [round(t, 4) for t in times],
        "median": round(statistics.median(times), 4),
        "min": round(min(times), 4),
        "max": round(max(times), 4),
        "groups": groups,
    }

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - BENCHMARK")

    working, join_report = build_working_dataset(verbose=False)
    baseline_result, baseline_report = run_baseline(working, verbose=False)

    print(f" join time (pandas) : {join_report['join_seconds']} s")
    print(
        f" baseline median    : {baseline_report['median_seconds']} s "
        f"over {baseline_report['repeats']} runs"
    )
    print(f" baseline groups    : {baseline_report['groups']}")

    spark = cfg.build_spark()
    rows = []

    try:
        joined, spark_join = build_joined(spark, verbose=False)

        print(f" spark joined rows  : {joined.count():,}")
        print(f" BroadcastHashJoin  : {spark_join['broadcast_hash_joins']}")
        print(f" SortMergeJoin      : {spark_join['sort_merge_joins']}")

        cfg.banner("BOUNDED PARALLELISM CONDITIONS")

        results = []
        for partitions in cfg.PARTITION_SETTINGS:
            r = bench_one(joined, partitions, cfg.BENCHMARK_REPEATS)
            results.append(r)

            runs = ", ".join(f"{t:.4f}" for t in r["runs"])
            print(
                f" partitions={partitions:>2} | "
                f"median={r['median']:.4f} s | "
                f"groups={r['groups']} | runs=[{runs}]"
            )

        group_counts = {r["groups"] for r in results}
        group_counts.add(baseline_report["groups"])
        assert len(group_counts) == 1, (
            f"Conditions disagree on group count: {group_counts}"
        )

        rows.append({
            "run": "Sequential baseline",
            "parallelism_partitions": "1 / non-parallel",
            "execution_time_s": f"{baseline_report['median_seconds']:.4f}",
            "groups": baseline_report["groups"],
            "correct": "Yes",
            "speedup_vs_baseline": "1.00",
            "observation": "pandas, in-process, no Spark scheduling",
        })

        best = min(results, key=lambda r: r["median"])

        for r in results:
            speedup = (
                baseline_report["median_seconds"] / r["median"]
                if r["median"] > 0 else 0
            )
            rows.append({
                "run": f"Parallel ({r['partitions']} partitions)",
                "parallelism_partitions": r["partitions"],
                "execution_time_s": f"{r['median']:.4f}",
                "groups": r["groups"],
                "correct": "Yes",
                "speedup_vs_baseline": f"{speedup:.4f}",
                "observation": (
                    "fastest parallel setting"
                    if r is best
                    else "slower than best parallel setting"
                ),
            })

        cfg.banner("INTERPRETATION")
        print(
            f" best parallel setting : {best['partitions']} partitions "
            f"at {best['median']:.4f} s"
        )

        ratio = best["median"] / baseline_report["median_seconds"]
        if ratio > 1:
            print(f" Spark is {ratio:.2f}x SLOWER than the pandas baseline.")
            print(
                " This is a valid result if JVM scheduling/serialization "
                "overhead exceeds the useful work."
            )
        else:
            print(f" Spark is {1 / ratio:.2f}x faster than the baseline.")

        if spark_join["sort_merge_joins"] == 0:
            print(
                " All dimension joins were broadcast, so observed differences "
                "are mainly repartition/aggregation and scheduling cost."
            )

        with open(cfg.OUT_BENCHMARK, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nWrote {cfg.OUT_BENCHMARK}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
