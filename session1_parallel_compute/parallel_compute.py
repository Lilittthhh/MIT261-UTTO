"""
parallel_compute.py - Session 1, Parts 7 and 9.

PySpark local-mode implementation plus correctness validation.

Design:
- shops, items, and item_categories are broadcast because they are small.
- repartition(n, shop_id) uses an explicit bounded partition count.
- output is checked against the pandas baseline within numeric tolerance.

Run:
    python parallel_compute.py
"""

import json
import sys
import time
import pandas as pd
import config as cfg

def load_spark_frames(spark):
    """Read each input file into Spark DataFrames using explicit schemas."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        LongType,
        DoubleType,
    )

    sales_schema = StructType([
        StructField("date", StringType(), True),
        StructField("date_block_num", LongType(), True),
        StructField("shop_id", LongType(), True),
        StructField("item_id", LongType(), True),
        StructField("item_price", DoubleType(), True),
        StructField("item_cnt_day", DoubleType(), True),
    ])

    shops_schema = StructType([
        StructField("shop_name", StringType(), True),
        StructField("shop_id", LongType(), True),
    ])

    items_schema = StructType([
        StructField("item_name", StringType(), True),
        StructField("item_id", LongType(), True),
        StructField("item_category_id", LongType(), True),
    ])

    categories_schema = StructType([
        StructField("item_category_name", StringType(), True),
        StructField("item_category_id", LongType(), True),
    ])

    def read(name, schema):
        return (
            spark.read
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", True)
            .schema(schema)
            .csv(str(cfg.path_for(name)))
        )

    sales = (
        read("sales", sales_schema)
        .withColumn(
            cfg.EVENT_TIME_FIELD,
            F.to_date(F.col(cfg.EVENT_TIME_FIELD), "dd.MM.yyyy")
        )
        .withColumn(
            cfg.METRIC_FIELD,
            F.col("item_price") * F.col("item_cnt_day")
        )
    )

    shops = read("shops", shops_schema)
    items = read("items", items_schema)
    cats = read("item_categories", categories_schema)

    return sales, shops, items, cats

def build_joined(spark, verbose: bool = True):
    """Join all files in Spark, broadcasting the small parent tables."""
    from pyspark.sql import functions as F

    sales, shops, items, cats = load_spark_frames(spark)

    if verbose:
        cfg.banner("SPARK CSV PARSE CHECK")
        print(f" sales shop_id nulls        : {sales.filter(F.col('shop_id').isNull()).count()}")
        print(f" sales item_id nulls        : {sales.filter(F.col('item_id').isNull()).count()}")
        print(f" shops shop_id nulls        : {shops.filter(F.col('shop_id').isNull()).count()}")
        print(f" items item_id nulls        : {items.filter(F.col('item_id').isNull()).count()}")
        print(
            f" items category_id nulls    : "
            f"{items.filter(F.col('item_category_id').isNull()).count()}"
        )

    rows_before = sales.count()

    start = time.perf_counter()
    joined = (
        sales
        .join(F.broadcast(shops), on=cfg.SHOP_KEY, how="inner")
        .join(F.broadcast(items), on=cfg.ITEM_KEY, how="inner")
        .join(F.broadcast(cats), on=cfg.CATEGORY_KEY, how="inner")
        .cache()
    )
    rows_after = joined.count()  # materialise cache and join
    join_seconds = time.perf_counter() - start

    assert rows_after == rows_before, (
        f"Spark join changed row count: {rows_before} -> {rows_after}"
    )

    plan = joined._jdf.queryExecution().executedPlan().toString()
    report = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "delta": rows_after - rows_before,
        "join_seconds": round(join_seconds, 4),
        "broadcast_hash_joins": plan.count("BroadcastHashJoin"),
        "sort_merge_joins": plan.count("SortMergeJoin"),
    }

    if verbose:
        cfg.banner("SPARK JOIN")
        print(f" rows before        : {rows_before:,}")
        print(f" rows after         : {rows_after:,}")
        print(f" difference         : {report['delta']}")
        print(f" join/cache time    : {report['join_seconds']} s")
        print(f" BroadcastHashJoin  : {report['broadcast_hash_joins']}")
        print(f" SortMergeJoin      : {report['sort_merge_joins']}")

    return joined, report

def compute_parallel(joined, partitions: int):
    """Repartition on shop_id and calculate the Session 1 workload."""
    from pyspark.sql import functions as F

    partitioned = joined.repartition(partitions, cfg.PARTITION_KEY)

    result = (
        partitioned
        .groupBy(cfg.PARTITION_KEY)
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum(cfg.METRIC_FIELD).alias("revenue_total"),
            F.avg(cfg.METRIC_FIELD).alias("revenue_mean"),
        )
    )
    return partitioned, result

def validate(result) -> dict:
    """Compare Spark result against baseline_result.csv."""
    if not cfg.OUT_BASELINE.exists():
        print(" baseline file not found; computing it now")
        from sequential_baseline import run_baseline
        baseline, _ = run_baseline(verbose=False)
    else:
        baseline = pd.read_csv(cfg.OUT_BASELINE)

    parallel = result.toPandas()

    parallel = parallel.sort_values(cfg.PARTITION_KEY).reset_index(drop=True)
    baseline = baseline.sort_values(cfg.PARTITION_KEY).reset_index(drop=True)

    assert len(parallel) == len(baseline), (
        f"Group count mismatch: parallel={len(parallel)}, baseline={len(baseline)}"
    )

    comparison = parallel.merge(
        baseline,
        on=cfg.PARTITION_KEY,
        suffixes=("_par", "_base"),
        validate="one_to_one",
    )

    count_diff = int(
        (comparison["txn_count_par"] - comparison["txn_count_base"]).abs().max()
    )
    total_diff = float(
        (comparison["revenue_total_par"] - comparison["revenue_total_base"])
        .abs().max()
    )
    mean_diff = float(
        (comparison["revenue_mean_par"] - comparison["revenue_mean_base"])
        .abs().max()
    )

    assert count_diff == 0, f"Transaction-count difference: {count_diff}"

    # Spark and pandas can differ by a tiny floating-point residual because
    # they may sum values in a different order.
    max_baseline_total = float(comparison["revenue_total_base"].abs().max())
    total_allowed = max(cfg.TOLERANCE, max_baseline_total * 1e-12)

    assert total_diff <= total_allowed, (
        f"Revenue-total difference: {total_diff} "
        f"(allowed: {total_allowed})"
    )
    assert mean_diff <= cfg.TOLERANCE, f"Revenue-mean difference: {mean_diff}"

    return {
        "parallel_groups": len(parallel),
        "baseline_groups": len(baseline),
        "max_txn_count_difference": count_diff,
        "max_revenue_total_difference": total_diff,
        "max_revenue_mean_difference": mean_diff,
        "tolerance": cfg.TOLERANCE,
        "revenue_total_allowed_tolerance": total_allowed,
        "correct": True,
    }

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - PARALLEL COMPUTE")

    spark = cfg.build_spark()
    try:
        joined, join_report = build_joined(spark)

        cfg.banner(f"BOUNDED PARALLELISM - {cfg.CHOSEN_PARTITIONS} PARTITIONS")
        start = time.perf_counter()
        partitioned, result = compute_parallel(joined, cfg.CHOSEN_PARTITIONS)
        groups = result.count()
        elapsed = time.perf_counter() - start

        print(f" configured partitions : {partitioned.rdd.getNumPartitions()}")
        print(f" result groups          : {groups}")
        print(f" parallel time          : {elapsed:.4f} s")

        cfg.banner("PHYSICAL PLAN")
        result.explain("formatted")

        cfg.banner("CORRECTNESS VALIDATION")
        validation = validate(result)
        print(f" parallel groups : {validation['parallel_groups']}")
        print(f" baseline groups : {validation['baseline_groups']}")
        print(
            " max |txn_count_par - txn_count_base| = "
            f"{validation['max_txn_count_difference']}"
        )
        print(
            " max |revenue_total_par - revenue_total_base| = "
            f"{validation['max_revenue_total_difference']:.8g}"
        )
        print(
            " max |revenue_mean_par - revenue_mean_base| = "
            f"{validation['max_revenue_mean_difference']:.8g}"
        )
        print(
            " revenue-total allowed tolerance           = "
            f"{validation['revenue_total_allowed_tolerance']:.8g}"
        )
        print(" Correctness check passed.")

        # Save the final aggregated output with pandas/pyarrow so Windows
        # does not require Hadoop winutils.exe just to persist the Parquet file.
        final_pd = result.orderBy(cfg.PARTITION_KEY).toPandas()
        final_pd.to_parquet(cfg.OUT_FINAL, index=False)

        cfg.OUT_VALIDATION.write_text(json.dumps({
            "parallelism": cfg.CHOSEN_PARTITIONS,
            "parallel_seconds": round(elapsed, 4),
            "join": join_report,
            "validation": validation,
        }, indent=2), encoding="utf-8")

        print(f"\nWrote {cfg.OUT_FINAL}")
        print(f"Wrote {cfg.OUT_VALIDATION}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
