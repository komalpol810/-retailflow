"""
Silver-layer data quality checks for RetailFlow.

Three kinds of checks, reported together:
  1. Great Expectations suites - single-table rules (schema, nulls,
     uniqueness, validity, business rules).
  2. Volume check - row count vs a known-good baseline (hand-rolled).
  3. Referential integrity - child keys exist in parent tables
     (hand-rolled, because it spans two tables).

run_all() returns a list of result dicts so the pipeline gate
(next milestone) can reuse it. Run directly with:
    python -m quality.silver_suite
"""

import sys

import great_expectations as gx
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

gxe = gx.expectations

SILVER_DIR = "lakehouse/silver"

# ---------------------------------------------------------------
# Rules, written as plain data. One entry per Silver table.
# ---------------------------------------------------------------
TABLES = {
    "customers": dict(
        types={
            "customer_id": "StringType",
            "customer_unique_id": "StringType",
            "customer_zip_code_prefix": "IntegerType",
            "customer_state": "StringType",
        },
        not_null=["customer_id", "customer_unique_id"],
        unique=["customer_id"],
    ),
    "orders": dict(
        types={
            "order_id": "StringType",
            "customer_id": "StringType",
            "order_status": "StringType",
            "order_purchase_timestamp": "TimestampType",
            "order_delivered_customer_date": "TimestampType",
        },
        not_null=["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
        unique=["order_id"],
        in_set={
            "order_status": [
                "delivered", "shipped", "canceled", "unavailable",
                "invoiced", "processing", "created", "approved",
            ]
        },
        # (later_column, earlier_column): later >= earlier
        a_ge_b=[("order_delivered_customer_date", "order_purchase_timestamp")],
    ),
    "order_items": dict(
        types={
            "order_id": "StringType",
            "order_item_id": "IntegerType",
            "product_id": "StringType",
            "seller_id": "StringType",
            "price": "DoubleType",
            "freight_value": "DoubleType",
        },
        not_null=["order_id", "order_item_id", "product_id", "seller_id", "price"],
        unique_together=[["order_id", "order_item_id"]],
        positive=["price"],
        non_negative=["freight_value"],
    ),
    "order_payments": dict(
        types={
            "order_id": "StringType",
            "payment_sequential": "IntegerType",
            "payment_type": "StringType",
            "payment_value": "DoubleType",
        },
        not_null=["order_id", "payment_sequential", "payment_value"],
        unique_together=[["order_id", "payment_sequential"]],
        non_negative=["payment_value", "payment_installments"],
        in_set={
            "payment_type": ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
        },
    ),
    "order_reviews": dict(
        types={
            "review_id": "StringType",
            "order_id": "StringType",
            "review_score": "IntegerType",
        },
        not_null=["review_id", "order_id", "review_score"],
        unique_together=[["review_id", "order_id"]],
        between={"review_score": (1, 5)},
    ),
    "products": dict(
        types={"product_id": "StringType"},
        not_null=["product_id"],
        unique=["product_id"],
        non_negative=["product_weight_g"],
    ),
    "sellers": dict(
        types={"seller_id": "StringType", "seller_state": "StringType"},
        not_null=["seller_id"],
        unique=["seller_id"],
    ),
    "category_translation": dict(
        types={"product_category_name": "StringType"},
        not_null=["product_category_name"],
        unique=["product_category_name"],
    ),
    "geolocation": dict(
        types={
            "geolocation_zip_code_prefix": "IntegerType",
            "geolocation_lat": "DoubleType",
            "geolocation_lng": "DoubleType",
        },
        not_null=["geolocation_zip_code_prefix"],
    ),
}

# Known-good row counts from the clean local run.
BASELINES = {
    "customers": 99441,
    "orders": 99441,
    "order_items": 112650,
    "order_payments": 103886,
    "order_reviews": 99224,
    "products": 32951,
    "sellers": 3095,
    "category_translation": 71,
    "geolocation": 1000163,
}
VOLUME_TOLERANCE = 0.20  # allow +/- 20%

# (child_table, child_column, parent_table, parent_column)
REFERENCES = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
]


def get_spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("silver_quality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_expectations(spec):
    """Turn one table's plain-data rules into GE expectation objects."""
    exps = []
    for col, typ in spec.get("types", {}).items():
        exps.append(gxe.ExpectColumnToExist(column=col))
        exps.append(gxe.ExpectColumnValuesToBeOfType(column=col, type_=typ))
    for col in spec.get("not_null", []):
        exps.append(gxe.ExpectColumnValuesToNotBeNull(column=col))
    for col in spec.get("unique", []):
        exps.append(gxe.ExpectColumnValuesToBeUnique(column=col))
    for cols in spec.get("unique_together", []):
        exps.append(gxe.ExpectCompoundColumnsToBeUnique(column_list=cols))
    for col in spec.get("positive", []):
        exps.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, strict_min=True))
    for col in spec.get("non_negative", []):
        exps.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0))
    for col, (lo, hi) in spec.get("between", {}).items():
        exps.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=lo, max_value=hi))
    for col, allowed in spec.get("in_set", {}).items():
        exps.append(gxe.ExpectColumnValuesToBeInSet(column=col, value_set=allowed))
    for later, earlier in spec.get("a_ge_b", []):
        exps.append(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=later, column_B=earlier, or_equal=True
            )
        )
    return exps


def dimension_of(expectation_type):
    """Map a GE expectation type to a data quality dimension."""
    if "pair" in expectation_type:
        return "consistency"
    if "exist" in expectation_type or "of_type" in expectation_type:
        return "schema"
    if "null" in expectation_type:
        return "completeness"
    if "unique" in expectation_type:
        return "uniqueness"
    return "validity"


def describe(kwargs):
    if "column_A" in kwargs:
        return f"{kwargs['column_A']} >= {kwargs['column_B']}"
    if "column_list" in kwargs:
        return "(" + ", ".join(kwargs["column_list"]) + ")"
    return kwargs.get("column", "")


def run_ge_suite(source, table, df, spec):
    asset = source.add_dataframe_asset(f"{table}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    suite = gx.ExpectationSuite(
        name=f"silver_{table}", expectations=build_expectations(spec)
    )
    validation = batch.validate(suite)

    out = []
    for r in validation.to_json_dict()["results"]:
        cfg = r["expectation_config"]
        etype = cfg["type"]
        res = r.get("result") or {}
        detail = ""
        if not r["success"]:
            detail = (
                f"{res['unexpected_count']} bad rows"
                if "unexpected_count" in res
                else "could not be evaluated or failed"
            )
        out.append({
            "table": table,
            "dimension": dimension_of(etype),
            "check": f"{etype.replace('expect_', '')} {describe(cfg['kwargs'])}",
            "passed": bool(r["success"]),
            "detail": detail,
        })
    return out


def check_volume(table, df):
    base = BASELINES[table]
    lo = int(base * (1 - VOLUME_TOLERANCE))
    hi = int(base * (1 + VOLUME_TOLERANCE))
    n = df.count()
    return {
        "table": table,
        "dimension": "timeliness/volume",
        "check": "row count within baseline range",
        "passed": lo <= n <= hi,
        "detail": f"{n} rows, allowed {lo} to {hi}",
    }


def check_referential(frames):
    out = []
    for child, ccol, parent, pcol in REFERENCES:
        children = frames[child].select(ccol).where(f"{ccol} IS NOT NULL").distinct()
        parents = frames[parent].select(pcol).distinct()
        orphans = children.join(
            parents, children[ccol] == parents[pcol], "left_anti"
        ).count()
        out.append({
            "table": child,
            "dimension": "consistency",
            "check": f"{child}.{ccol} exists in {parent}.{pcol}",
            "passed": orphans == 0,
            "detail": f"{orphans} orphan keys" if orphans else "",
        })
    return out


def run_all(spark, silver_dir=SILVER_DIR):
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark("silver")
    frames = {
        name: spark.read.format("delta").load(f"{silver_dir}/{name}")
        for name in TABLES
    }
    results = []
    for table, spec in TABLES.items():
        results += run_ge_suite(source, table, frames[table], spec)
        results.append(check_volume(table, frames[table]))
    results += check_referential(frames)
    return results


def print_report(results):
    print("\n" + "=" * 70)
    print("SILVER DATA QUALITY REPORT")
    print("=" * 70)
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        line = f"{status}  [{r['dimension']}] {r['table']}: {r['check']}"
        if r["detail"]:
            line += f"  -> {r['detail']}"
        print(line)

    failed = [r for r in results if not r["passed"]]
    print("\n" + "-" * 70)
    print(f"Total checks: {len(results)}   Passed: {len(results) - len(failed)}   Failed: {len(failed)}")
    for dim in sorted({r["dimension"] for r in results}):
        subset = [r for r in results if r["dimension"] == dim]
        ok = sum(1 for r in subset if r["passed"])
        print(f"  {dim}: {ok}/{len(subset)} passed")
    print("RESULT:", "FAILED" if failed else "ALL CHECKS PASSED")


def main():
    spark = get_spark()
    results = run_all(spark)
    print_report(results)
    spark.stop()
    return 1 if any(not r["passed"] for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
