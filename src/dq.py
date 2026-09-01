"""Data quality gate.

Every transaction passes through here before any fraud rule sees it. The
reason is practical: a null amount or a duplicated txn_id will happily
produce a fraud alert, and an analyst then spends an afternoon chasing a
transaction that never existed. Bad data is quarantined, not scored.

Checks are declared in config/dq_rules.yaml, so adding a check is a config
change rather than a code change.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

# Each check returns a Column that is True when the row FAILS the check.
CHECK_BUILDERS = {
    "not_null": lambda col, params: F.col(col).isNull(),
    "positive": lambda col, params: F.col(col) <= 0,
    "max_value": lambda col, params: F.col(col) > float(params["value"]),
    "allowed_values": lambda col, params: ~F.col(col).isin(params["values"])
    & F.col(col).isNotNull(),
    "regex": lambda col, params: ~F.col(col).rlike(params["pattern"])
    & F.col(col).isNotNull(),
}


def apply_dq(df: DataFrame, dq_config: dict) -> tuple[DataFrame, DataFrame]:
    """Split a DataFrame into (clean, quarantined).

    Quarantined rows keep a dq_failures array naming every check they
    broke, so the quarantine table is diagnosable on its own without
    re-running the job.
    """
    failure_cols = []

    for rule in dq_config.get("checks", []):
        check_type = rule["type"]
        column = rule["column"]
        params = rule.get("params", {})
        label = rule.get("name", f"{column}_{check_type}")

        builder = CHECK_BUILDERS.get(check_type)
        if builder is None:
            raise ValueError(f"Unknown DQ check type: {check_type}")

        failed = builder(column, params)
        failure_cols.append(F.when(failed, F.lit(label)))

    # Duplicate detection needs a window, so it is handled separately from
    # the row-level checks above.
    key = dq_config.get("unique_key")
    if key:
        dupe_window = Window.partitionBy(key).orderBy(F.col("txn_ts").asc_nulls_last())
        df = df.withColumn("_dupe_rank", F.row_number().over(dupe_window))
        failure_cols.append(F.when(F.col("_dupe_rank") > 1, F.lit(f"duplicate_{key}")))

    if failure_cols:
        df = df.withColumn("dq_failures", F.array_compact(F.array(*failure_cols)))
    else:
        df = df.withColumn("dq_failures", F.array().cast("array<string>"))

    df = df.withColumn("dq_passed", F.size("dq_failures") == 0)

    clean = df.filter(F.col("dq_passed")).drop("_dupe_rank", "dq_passed")
    quarantined = df.filter(~F.col("dq_passed")).drop("_dupe_rank", "dq_passed")

    return clean, quarantined


def dq_summary(quarantined: DataFrame) -> DataFrame:
    """Failure counts by check name - the thing you actually put on a dashboard."""
    return (
        quarantined.select(F.explode("dq_failures").alias("check"))
        .groupBy("check")
        .count()
        .orderBy(F.col("count").desc())
    )
