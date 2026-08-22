"""
partition_analysis.py - Session 1, Part 10.

Measures skew at:
1. Key level   - records per shop_id.
2. Spark level - records per physical partition.

Run:
    python partition_analysis.py
"""

import csv
import sys
import config as cfg
from parallel_compute import build_joined
from load_and_join import build_working_dataset

def key_level(working) -> dict:
    counts = working[cfg.PARTITION_KEY].value_counts()
    return {
        "distinct_keys": int(counts.size),
        "min": int(counts.min()),
        "median": int(counts.median()),
        "max": int(counts.max()),
        "skew_ratio": round(counts.max() / counts.min(), 2),
        "heaviest": counts.head(5).to_dict(),
        "lightest": counts.tail(5).to_dict(),
        "counts": counts,
    }

def partition_level(joined, partitions: int) -> dict:
    sizes = (
        joined
        .repartition(partitions, cfg.PARTITION_KEY)
        .rdd
        .glom()
        .map(len)
        .collect()
    )

    total = sum(sizes)
    even = total / partitions
    nonzero = [s for s in sizes if s > 0]

    return {
        "partitions": partitions,
        "sizes": sizes,
        "min": min(sizes),
        "max": max(sizes),
        "even_share": round(even, 1),
        "skew_ratio": (
            round(max(nonzero) / min(nonzero), 2)
            if len(nonzero) > 1 else 1.0
        ),
        "worst_vs_even": round(max(sizes) / even, 2),
    }

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - PARTITION ANALYSIS")

    working, _ = build_working_dataset(verbose=False)

    kl = key_level(working)
    cfg.banner(f"KEY LEVEL - {cfg.PARTITION_KEY}")
    print(f" distinct keys : {kl['distinct_keys']}")
    print(
        f" records/key   : min={kl['min']:,} "
        f"median={kl['median']:,} max={kl['max']:,}"
    )
    print(f" skew ratio    : {kl['skew_ratio']} : 1")

    print("\n heaviest 5:")
    for k, v in kl["heaviest"].items():
        print(f" {str(k):<20} {v:>9,}")

    print(" lightest 5:")
    for k, v in kl["lightest"].items():
        print(f" {str(k):<20} {v:>9,}")

    alt = working[cfg.ALTERNATIVE_KEY].value_counts()
    print(
        f"\n alternative key '{cfg.ALTERNATIVE_KEY}': "
        f"{alt.size:,} distinct, "
        f"min={alt.min():,} median={int(alt.median()):,} max={alt.max():,} "
        f"-> ratio {alt.max() / alt.min():.2f}:1"
    )

    spark = cfg.build_spark()
    rows = []

    try:
        joined, _ = build_joined(spark, verbose=False)

        cfg.banner("PARTITION LEVEL")
        header = f" {'n':>3} {'even':>12} {'min':>12} {'max':>12} {'ratio':>8}"
        print(header)
        print(" " + "-" * (len(header) - 2))

        pls = []
        for partitions in cfg.PARTITION_SETTINGS:
            pl = partition_level(joined, partitions)
            pls.append(pl)

            print(
                f" {pl['partitions']:>3} {pl['even_share']:>12,.1f} "
                f"{pl['min']:>12,} {pl['max']:>12,} "
                f"{pl['skew_ratio']:>8.2f}"
            )

            for idx, size in enumerate(pl["sizes"]):
                rows.append({
                    "level": "spark_partition",
                    "setting": pl["partitions"],
                    "identifier": f"partition_{idx}",
                    "record_count": size,
                    "even_share": pl["even_share"],
                    "vs_even": round(size / pl["even_share"], 3),
                })

        cfg.banner("INTERPRETATION")
        print(
            f" Key-level skew is fixed at {kl['skew_ratio']}:1 because "
            "it is a property of shop transaction volume."
        )

        first, last = pls[0], pls[-1]
        if last["skew_ratio"] > first["skew_ratio"]:
            print(
                f" Physical partition skew worsens from "
                f"{first['skew_ratio']}:1 at {first['partitions']} partitions "
                f"to {last['skew_ratio']}:1 at {last['partitions']}."
            )
        else:
            print(
                f" Physical partition skew changes from "
                f"{first['skew_ratio']}:1 at {first['partitions']} partitions "
                f"to {last['skew_ratio']}:1 at {last['partitions']}."
            )

        with open(cfg.OUT_PARTITIONS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nWrote {cfg.OUT_PARTITIONS}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
