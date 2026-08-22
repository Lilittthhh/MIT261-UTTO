# MIT261-UTTO — Session 1 Parallel Compute

## Project Title
**SaleFlow: Retail Sales and Revenue Intelligence Using the Predict Future Sales Dataset**

## Course
MIT 261 – Parallel and Distributed Systems  
Session 1 – Foundations in In-Memory Cluster Compute

## Dataset
**Predict Future Sales** — Kaggle competition dataset

Source: https://www.kaggle.com/c/competitive-data-science-predict-future-sales

### Files used
- `sales_train.csv` — Event / transactional data
- `shops.csv` — Entity data
- `items.csv` — Entity data
- `item_categories.csv` — Lookup data

The main transactional file contains **2,935,849 sales records**.

## Relational Model
The working dataset uses these joins:

1. `sales_train.csv` → `shops.csv` on `shop_id`
2. `sales_train.csv` → `items.csv` on `item_id`
3. `items.csv` → `item_categories.csv` on `item_category_id`

Relationships:
- Shop `1 → many` Sales
- Item `1 → many` Sales
- ItemCategory `1 → many` Items

The join was validated with no row duplication or loss:
- Rows before join: **2,935,849**
- Rows after join: **2,935,849**
- Orphan foreign-key records: **0**

## Partitioning Strategy
- **Partition key:** `shop_id`
- **Event-time field:** `date`
- **Metric:** `revenue`

Revenue is derived as:

```text
revenue = item_price × item_cnt_day
```

The workload computes, per shop:
- Transaction count
- Total revenue
- Mean revenue

## Technologies
- Python 3.11
- pandas 2.3.3
- PySpark 4.2.0
- pyarrow 25.0.1
- Graphviz
- Java / Spark local mode
- Windows 64-bit

## Machine Used for Benchmarking
- CPU: 11th Gen Intel Core i5-1135G7
- 4 cores / 8 threads
- RAM: 12 GB
- OS: Windows 64-bit

## Project Structure

```text
MIT261-UTTO/
└── session1_parallel_compute/
    ├── architecture/
    │   ├── architecture-session1.dot
    │   └── architecture-session1.png
    ├── datasets/
    │   ├── sales_train.csv
    │   ├── shops.csv
    │   ├── items.csv
    │   └── item_categories.csv
    ├── docs/
    │   ├── entity-model-session1.dot
    │   └── entity-model-session1.png
    ├── results/
    │   ├── baseline_result.csv
    │   ├── file_profile.json
    │   ├── partition_sizes.csv
    │   ├── partition_strategy.json
    │   ├── session1_benchmark.csv
    │   ├── shop_revenue.parquet
    │   ├── validation_report.json
    │   └── working_dataset.parquet
    ├── benchmark.py
    ├── config.py
    ├── load_and_join.py
    ├── parallel_compute.py
    ├── partition_analysis.py
    ├── partition_strategy.py
    ├── profile_files.py
    ├── render_diagrams.py
    ├── sequential_baseline.py
    └── RUN_ORDER.txt
```

## Setup

### 1. Open the project folder

```powershell
cd C:\Users\ureha\OneDrive\Desktop\MIT\MIT261-UTTO\session1_parallel_compute
```

### 2. Activate the virtual environment

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script execution for the current session:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### 3. Install required Python packages

```powershell
pip install pandas pyspark pyarrow
```

Graphviz must also be installed and available through the Windows `PATH`.

Verify Graphviz with:

```powershell
dot -V
```

## Execution Order
Run the scripts from the `session1_parallel_compute` directory.

### 1. Profile the dataset

```powershell
python profile_files.py
```

### 2. Load and join the dataset

```powershell
python load_and_join.py
```

This validates the relational joins, creates a surrogate `sale_id`, parses `date`, derives `revenue`, reconciles row counts, and writes `results/working_dataset.parquet`.

### 3. Analyse the partition strategy

```powershell
python partition_strategy.py
```

The selected partition key is `shop_id`.

### 4. Run the sequential baseline

```powershell
python sequential_baseline.py
```

### 5. Run the parallel Spark implementation

```powershell
python parallel_compute.py
```

The configured final run uses **4 partitions**.

### 6. Run the benchmark

```powershell
python benchmark.py
```

Benchmark settings are 2, 4, and 8 Spark partitions plus the sequential baseline.

### 7. Analyse partition balance and skew

```powershell
python partition_analysis.py
```

### 8. Generate diagrams

```powershell
python render_diagrams.py
```

Produces:
- `docs/entity-model-session1.png`
- `architecture/architecture-session1.png`

## Benchmark Results

| Run | Partitions | Median Time (s) | Groups | Correct |
|---|---:|---:|---:|---|
| Sequential baseline | 1 / non-parallel | 0.0790 | 60 | Yes |
| Parallel A | 2 | 0.8378 | 60 | Yes |
| Parallel B | 4 | 0.7852 | 60 | Yes |
| Parallel C | 8 | 0.7147 | 60 | Yes |

The fastest tested Spark configuration was **8 partitions at 0.7147 seconds**.

The pandas baseline remained faster for this workload because the final aggregation has only 60 groups and Spark adds JVM, scheduling, serialization, repartitioning, and coordination overhead.

## Correctness Validation
The Spark aggregate is compared against the pandas baseline after sorting by `shop_id`.

Validation results:
- Parallel groups: **60**
- Baseline groups: **60**
- Maximum transaction-count difference: **0**
- Maximum revenue-total difference: **1.296401e-06**
- Maximum revenue-mean difference: **2.2055247e-11**
- Allowed total-revenue tolerance: **0.00023521702**
- Result: **PASS**

Transaction counts are checked exactly. Floating-point revenue values use a small scale-aware tolerance because Spark and pandas may sum floating-point values in different orders.

## Partition Balance and Skew
The selected key has **60 distinct `shop_id` values**.

Observed shop-level counts:
- Minimum: **306**
- Median: **42,037**
- Maximum: **235,636**
- Key-level skew ratio: **770.05:1**
- Largest shop key: **shop_id 31**

For the configured 4-partition run:
- Partition 0: 1,273,996 rows
- Partition 1: 318,666 rows
- Partition 2: 751,967 rows
- Partition 3: 591,220 rows

Possible future mitigation includes salting heavy shop keys, using a composite shop/time key, or range partitioning.

## Join Strategy
Spark uses broadcast joins for:
- `shops.csv`
- `items.csv`
- `item_categories.csv`

These files are small relative to the 2.9-million-row sales file, so broadcasting avoids large dimension-side shuffle joins.

## Notes for Windows
Spark may display warnings related to `HADOOP_HOME`, `winutils.exe`, or the native Hadoop library. These warnings did not prevent computation from succeeding.

Because the Spark Parquet writer encountered a Windows Hadoop-related issue, the final small aggregate is serialized with pandas/pyarrow after Spark completes the distributed computation.

## Session 1 Outputs
Important generated artifacts include:
- `results/working_dataset.parquet`
- `results/baseline_result.csv`
- `results/shop_revenue.parquet`
- `results/session1_benchmark.csv`
- `results/validation_report.json`
- `results/partition_sizes.csv`
- `docs/entity-model-session1.png`
- `architecture/architecture-session1.png`

## Multi-Session Continuity
- **Session 2:** Replay sales events chronologically using `date` as event time.
- **Session 3:** Expose Shop, Item, ItemCategory, and Sales/Revenue as service/API boundaries.
- **Session 4:** Compute daily/monthly revenue and transaction windows by `shop_id`.
- **Session 5:** Containerize the Spark processing scripts and runtime environment.
- **Session 6:** Provision the execution environment and automate profiling, validation, and benchmark regression checks.

## Repository
GitHub repository:

https://github.com/Lilittthhh/MIT261-UTTO

## AI-Use Disclosure
ChatGPT (OpenAI) was used to assist with interpretation of the activity/code guide, troubleshooting Python/PySpark/Windows environment issues, refining validation logic, and preparing documentation based on measured program outputs.

The student executed the scripts and benchmark runs, reviewed the outputs, and verified the joins, computation, validation, partition analysis, and diagrams.
