"""
profile_files.py - Session 1, Parts 2 and 3.

Profiles every input file, detects candidate primary keys by uniqueness,
verifies foreign-key integrity, and evaluates the four dataset eligibility
conditions from the activity sheet.

Run:
    python profile_files.py
"""

import json
import sys
import pandas as pd
import config as cfg

def profile_one(name: str) -> tuple[pd.DataFrame, dict]:
    path = cfg.path_for(name)
    df = pd.read_csv(path)

    candidate_pks = [
        c for c in df.columns
        if df[c].notna().all() and df[c].is_unique
    ]
    constant_cols = [c for c in df.columns if df[c].nunique(dropna=True) <= 1]

    prof = {
        "file": path.name,
        "role": cfg.FILES[name]["role"],
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": df.columns.tolist(),
        "size_kb": round(path.stat().st_size / 1024, 1),
        "candidate_primary_keys": candidate_pks,
        "constant_columns": constant_cols,
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
        "distinct_counts": {c: int(df[c].nunique(dropna=True)) for c in df.columns},
    }

    if name == "sales":
        prof["primary_key_note"] = (
            "sales_train.csv has no natural single-column primary key. "
            "A surrogate sale_id is generated from source row order in load_and_join.py."
        )
        prof["negative_item_price_rows"] = int((df["item_price"] < 0).sum())
        prof["negative_item_cnt_day_rows"] = int((df["item_cnt_day"] < 0).sum())

    return df, prof

def check_integrity(frames: dict[str, pd.DataFrame]) -> dict:
    sales = frames["sales"]
    shops = frames["shops"]
    items = frames["items"]
    cats = frames["item_categories"]

    checks = {
        "sales.shop_id -> shops.shop_id":
            bool(sales[cfg.SHOP_KEY].isin(set(shops[cfg.SHOP_KEY])).all()),
        "sales.item_id -> items.item_id":
            bool(sales[cfg.ITEM_KEY].isin(set(items[cfg.ITEM_KEY])).all()),
        "items.item_category_id -> item_categories.item_category_id":
            bool(items[cfg.CATEGORY_KEY].isin(set(cats[cfg.CATEGORY_KEY])).all()),
    }

    orphans = {
        "sales_without_shop":
            int((~sales[cfg.SHOP_KEY].isin(set(shops[cfg.SHOP_KEY]))).sum()),
        "sales_without_item":
            int((~sales[cfg.ITEM_KEY].isin(set(items[cfg.ITEM_KEY]))).sum()),
        "items_without_category":
            int((~items[cfg.CATEGORY_KEY].isin(set(cats[cfg.CATEGORY_KEY]))).sum()),
    }
    return {"foreign_keys_resolve": checks, "orphan_counts": orphans}

def check_eligibility(frames: dict[str, pd.DataFrame], prof: dict) -> dict:
    qualifying = [n for n, p in prof.items() if p["role"] in ("Event", "Entity")]

    sales = frames["sales"]
    one_to_many = []

    # SHOP 1..* SALES
    if not sales[cfg.SHOP_KEY].is_unique:
        per_parent = sales[cfg.SHOP_KEY].value_counts()
        one_to_many.append({
            "parent": "shops",
            "child": "sales",
            "key": cfg.SHOP_KEY,
            "children_min": int(per_parent.min()),
            "children_median": int(per_parent.median()),
            "children_max": int(per_parent.max()),
        })

    # ITEM 1..* SALES
    if not sales[cfg.ITEM_KEY].is_unique:
        per_parent = sales[cfg.ITEM_KEY].value_counts()
        one_to_many.append({
            "parent": "items",
            "child": "sales",
            "key": cfg.ITEM_KEY,
            "children_min": int(per_parent.min()),
            "children_median": int(per_parent.median()),
            "children_max": int(per_parent.max()),
        })

    ts = pd.to_datetime(sales[cfg.EVENT_TIME_FIELD], format="%d.%m.%Y")
    event_rows = sum(p["rows"] for p in prof.values() if p["role"] == "Event")

    return {
        "condition_1_three_related_files": {
            "met": len(qualifying) >= 3,
            "qualifying_files": qualifying,
            "lookup_files_excluded":
                [n for n, p in prof.items() if p["role"] == "Lookup"],
        },
        "condition_2_one_to_many": {
            "met": len(one_to_many) >= 1,
            "associations": one_to_many,
        },
        "condition_3_timestamp": {
            "met": bool(ts.notna().all()),
            "field": f"sales.{cfg.EVENT_TIME_FIELD}",
            "min": str(ts.min()),
            "max": str(ts.max()),
            "span_days": int((ts.max() - ts.min()).days),
        },
        "condition_4_volume": {
            "met": event_rows >= 50_000,
            "event_rows": event_rows,
        },
    }

def main() -> int:
    cfg.require_files()
    cfg.banner("SESSION 1 - FILE PROFILING")

    frames, prof = {}, {}
    for name in cfg.FILES:
        df, p = profile_one(name)
        frames[name], prof[name] = df, p

        print(
            f"\nFILE: {p['file']:<26} role={p['role']:<7} "
            f"rows={p['rows']:>9,} cols={p['columns']} "
            f"size={p['size_kb']:,.1f} KB"
        )
        print(f" columns: {', '.join(p['column_names'])}")
        print(f" candidate primary keys: {p['candidate_primary_keys']}")
        if p["constant_columns"]:
            print(f" WARNING constant columns: {p['constant_columns']}")
        if name == "sales":
            print(f" NOTE: {p['primary_key_note']}")
            print(
                f" negative item_price rows={p['negative_item_price_rows']:,}; "
                f"negative item_cnt_day rows={p['negative_item_cnt_day_rows']:,}"
            )

    cfg.banner("REFERENTIAL INTEGRITY")
    integrity = check_integrity(frames)
    for label, ok in integrity["foreign_keys_resolve"].items():
        print(f" {'PASS' if ok else 'FAIL'} {label}")
    print(f" orphan records: {integrity['orphan_counts']}")

    cfg.banner("DATASET ELIGIBILITY")
    elig = check_eligibility(frames, prof)
    for key, result in elig.items():
        print(f" {'MET' if result['met'] else 'NOT MET':<7} {key}")

    for assoc in elig["condition_2_one_to_many"]["associations"]:
        print(
            f" {assoc['parent']} 1..* {assoc['child']} on {assoc['key']}: "
            f"min={assoc['children_min']:,} "
            f"median={assoc['children_median']:,} "
            f"max={assoc['children_max']:,}"
        )

    report = {"profiles": prof, "integrity": integrity, "eligibility": elig}
    cfg.OUT_PROFILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {cfg.OUT_PROFILE}")

    all_met = all(v["met"] for v in elig.values())
    all_fk = all(integrity["foreign_keys_resolve"].values())
    if not (all_met and all_fk):
        print("\nDATASET NOT ELIGIBLE - investigate the failed condition.")
        return 1

    print("\nAll eligibility conditions met. Proceed to load_and_join.py")
    return 0

if __name__ == "__main__":
    sys.exit(main())
