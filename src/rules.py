"""Fraud rules engine.

Each rule is a small function that takes the transaction DataFrame plus its
own parameters and returns a boolean Column: True when the rule fires. The
engine then unions the triggered rule names into an array, sums the
weights into a risk score, and bands the score.

Two design decisions worth naming, since interviewers ask about both:

* Thresholds live in config/rules.yaml, not in the code. Fraud thresholds
  get retuned constantly as patterns shift, and a threshold change should
  not require a deployment.
* Rules are additive columns on one pass over the data rather than one
  filter per rule. Twelve separate filters means twelve scans; this way
  the windows are computed once and every rule reads from them.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# Shared windows. Defined once, reused by several rules.
# --------------------------------------------------------------------------


def _with_derived_columns(df: DataFrame) -> DataFrame:
    """Attach the lag/rolling columns that the rules below depend on."""
    epoch = F.col("txn_ts").cast("long")
    by_account = Window.partitionBy("account_id").orderBy(epoch)

    # Rolling one-hour count, expressed in seconds via rangeBetween.
    hour_window = by_account.rangeBetween(-3600, 0)
    day_window = by_account.rangeBetween(-86400, 0)

    return (
        df.withColumn("_epoch", epoch)
        .withColumn("txns_last_hour", F.count("*").over(hour_window))
        .withColumn("amount_last_day", F.sum("amount").over(day_window))
        .withColumn("prev_country", F.lag("country").over(by_account))
        .withColumn("prev_epoch", F.lag(epoch).over(by_account))
        .withColumn(
            "hours_since_prev",
            (F.col("_epoch") - F.col("prev_epoch")) / 3600.0,
        )
        .withColumn(
            "beneficiary_seq",
            F.row_number().over(
                Window.partitionBy("account_id", "beneficiary_id").orderBy(epoch)
            ),
        )
        .withColumn("txn_hour", F.hour("txn_ts"))
    )


# --------------------------------------------------------------------------
# Individual rules. Each returns a boolean Column.
# --------------------------------------------------------------------------


def high_value(params: dict) -> Column:
    """Single transaction above an absolute ceiling."""
    return F.col("amount") >= float(params["amount_threshold"])


def velocity(params: dict) -> Column:
    """More transactions in a rolling hour than a genuine customer produces."""
    return F.col("txns_last_hour") >= int(params["max_txns_per_hour"])


def structuring(params: dict) -> Column:
    """Deposits parked just below the reporting threshold.

    Structuring is deliberate: someone who wants to move 60k without
    filing a CTR breaks it into seven deposits of 9.5k. The tell is the
    amount sitting in a narrow band under the threshold, repeatedly.
    """
    threshold = float(params["reporting_threshold"])
    band_floor = threshold * float(params["band_pct"])
    in_band = (F.col("amount") >= band_floor) & (F.col("amount") < threshold)
    return in_band & (F.col("amount_last_day") >= threshold)


def impossible_travel(params: dict) -> Column:
    """Two card-present transactions too far apart to be the same person."""
    return (
        F.col("prev_country").isNotNull()
        & (F.col("country") != F.col("prev_country"))
        & (F.col("hours_since_prev") <= float(params["max_hours_between"]))
    )


def dormant_reactivation(params: dict) -> Column:
    """A long-silent account suddenly moving real money."""
    return (
        F.col("hours_since_prev") >= float(params["dormant_days"]) * 24
    ) & (F.col("amount") >= float(params["amount_threshold"]))


def new_beneficiary_high_value(params: dict) -> Column:
    """First payment to a payee, and it is a large one."""
    return (F.col("beneficiary_seq") == 1) & (
        F.col("amount") >= float(params["amount_threshold"])
    )


def odd_hour(params: dict) -> Column:
    """Meaningful money moving in the small hours."""
    start = int(params["start_hour"])
    end = int(params["end_hour"])
    return (
        (F.col("txn_hour") >= start)
        & (F.col("txn_hour") < end)
        & (F.col("amount") >= float(params["amount_threshold"]))
    )


def high_risk_corridor(params: dict) -> Column:
    """Channel and category combinations that carry elevated base risk."""
    return F.col("merchant_category").isin(params["categories"]) & (
        F.col("amount") >= float(params["amount_threshold"])
    )


RULE_REGISTRY = {
    "high_value": high_value,
    "velocity": velocity,
    "structuring": structuring,
    "impossible_travel": impossible_travel,
    "dormant_reactivation": dormant_reactivation,
    "new_beneficiary_high_value": new_beneficiary_high_value,
    "odd_hour": odd_hour,
    "high_risk_corridor": high_risk_corridor,
}


# --------------------------------------------------------------------------
# Engine
# --------------------------------------------------------------------------


def score(df: DataFrame, rules_config: dict) -> DataFrame:
    """Apply every enabled rule and attach triggered_rules / risk_score / risk_band."""
    df = _with_derived_columns(df)

    triggered = []
    weights = []

    for rule in rules_config.get("rules", []):
        if not rule.get("enabled", True):
            continue

        name = rule["name"]
        builder = RULE_REGISTRY.get(rule["type"])
        if builder is None:
            raise ValueError(f"Unknown rule type: {rule['type']}")

        fires = builder(rule.get("params", {}))
        weight = float(rule.get("weight", 10))

        # Keep the per-rule flag as a column so an analyst can filter on a
        # single rule without re-parsing the triggered_rules array.
        flag_col = f"rule_{name}"
        df = df.withColumn(flag_col, F.coalesce(fires, F.lit(False)))

        triggered.append(F.when(F.col(flag_col), F.lit(name)))
        weights.append(F.when(F.col(flag_col), F.lit(weight)).otherwise(F.lit(0.0)))

    if not triggered:
        raise ValueError("No enabled rules found in configuration")

    bands = rules_config.get(
        "risk_bands", {"critical": 70, "high": 45, "medium": 20}
    )

    return (
        df.withColumn("triggered_rules", F.array_compact(F.array(*triggered)))
        .withColumn("risk_score", sum(weights))
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") >= bands["critical"], F.lit("CRITICAL"))
            .when(F.col("risk_score") >= bands["high"], F.lit("HIGH"))
            .when(F.col("risk_score") >= bands["medium"], F.lit("MEDIUM"))
            .when(F.col("risk_score") > 0, F.lit("LOW"))
            .otherwise(F.lit("NONE")),
        )
    )


def alerts_only(scored: DataFrame, min_band: str = "MEDIUM") -> DataFrame:
    """Filter scored transactions down to the ones worth an analyst's time.

    Alert volume is the whole game in fraud monitoring. A rule set that
    flags 8% of traffic is not a control, it is a queue nobody works.
    """
    order = ["NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"]
    keep = order[order.index(min_band) :]
    return scored.filter(F.col("risk_band").isin(keep))
