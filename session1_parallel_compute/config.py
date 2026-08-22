"""
Shared configuration for MIT 261 Session 1 - Predict Future Sales parallel compute.

Every script imports from this file so dataset paths, keys, metric field,
benchmark settings, and output paths are defined exactly once.

Dataset: Predict Future Sales
Course : MIT 261 - Parallel and Distributed Systems
Session: 1 - Foundations in In-Memory Cluster Compute
"""

from pathlib import Path

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
SESSION_DIR = Path(__file__).resolve().parent
REPO_ROOT = SESSION_DIR

# The instructor's guide uses "Datasets". If it is absent, "data" is accepted
# so the scripts also work with the folder structure already used in VS Code.
DATA_DIR = SESSION_DIR / "Datasets"
if not DATA_DIR.exists():
    DATA_DIR = SESSION_DIR / "data"
if not DATA_DIR.exists():
    # Also support data/ beside session1_parallel_compute/
    sibling_data = SESSION_DIR.parent / "data"
    if sibling_data.exists():
        DATA_DIR = sibling_data

RESULTS_DIR = SESSION_DIR / "results"
DOCS_DIR = REPO_ROOT / "docs"
ARCH_DIR = REPO_ROOT / "architecture"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)
ARCH_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# Input files
# --------------------------------------------------------------------------
# Event  - transactional/time-stamped records
# Entity - master/dimension data
# Lookup - small reference table; does NOT count toward the 3-file minimum
FILES = {
    "sales": {"file": "sales_train.csv", "role": "Event"},
    "shops": {"file": "shops.csv", "role": "Entity"},
    "items": {"file": "items.csv", "role": "Entity"},
    "item_categories": {"file": "item_categories.csv", "role": "Lookup"},
}

def path_for(name: str) -> Path:
    """Absolute path of one input file."""
    return DATA_DIR / FILES[name]["file"]

# --------------------------------------------------------------------------
# Workload definition
# --------------------------------------------------------------------------
# Monetization metric is derived during the join:
# revenue = item_price * item_cnt_day
PARTITION_KEY = "shop_id"
METRIC_FIELD = "revenue"
EVENT_TIME_FIELD = "date"

SHOP_KEY = "shop_id"
ITEM_KEY = "item_id"
CATEGORY_KEY = "item_category_id"

# An alternative natural key used for comparison in partition_strategy.py.
ALTERNATIVE_KEY = "item_id"

# --------------------------------------------------------------------------
# Benchmark settings
# --------------------------------------------------------------------------
PARTITION_SETTINGS = (2, 4, 8)
BENCHMARK_REPEATS = 3
BASELINE_REPEATS = 5
CHOSEN_PARTITIONS = 4
TOLERANCE = 1e-6

# --------------------------------------------------------------------------
# Spark settings
# --------------------------------------------------------------------------
SPARK_APP_NAME = "MIT261-Session1-Predict-Future-Sales"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "3g"
SPARK_SHUFFLE_PARTITIONS = "8"

# These tables are tiny relative to sales_train.csv and are safe to broadcast.
BROADCAST_FILES = ("shops", "items", "item_categories")

# --------------------------------------------------------------------------
# Output artifacts
# --------------------------------------------------------------------------
OUT_PROFILE = RESULTS_DIR / "file_profile.json"
OUT_JOINED = RESULTS_DIR / "working_dataset.parquet"
OUT_BASELINE = RESULTS_DIR / "baseline_result.csv"
OUT_BENCHMARK = RESULTS_DIR / "session1_benchmark.csv"
OUT_PARTITIONS = RESULTS_DIR / "partition_sizes.csv"
OUT_FINAL = RESULTS_DIR / "shop_revenue.parquet"
OUT_VALIDATION = RESULTS_DIR / "validation_report.json"

def require_files() -> None:
    missing = [str(path_for(name)) for name in FILES if not path_for(name).exists()]
    if missing:
        raise FileNotFoundError(
            "Missing dataset files:\n  - " + "\n  - ".join(missing)
            + f"\n\nCurrent DATA_DIR: {DATA_DIR}"
        )

def build_spark():
    """Create the SparkSession used by every parallel script."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def banner(title: str) -> None:
    """Consistent console heading."""
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
