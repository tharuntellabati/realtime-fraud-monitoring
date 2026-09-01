"""Batch pipeline: landing -> DQ gate -> rules engine -> alerts.

Run it with:

    python -m src.pipeline --landing data/landing --out data/out

Everything is local Parquet so the project runs on a laptop with no cloud
account. The same code runs on Databricks by pointing --landing and --out
at ADLS or S3 paths.
"""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F

from src import recon
from src.dq import apply_dq, dq_summary
from src.rules import alerts_only, score
from src.schema import TXN_SCHEMA


def build_spark(app_name: str = "txn-fraud-engine") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def read_landing(spark: SparkSession, landing: str) -> DataFrame:
    """Read the raw feed against a declared schema.

    PERMISSIVE mode plus _corrupt_record means a malformed line becomes a
    quarantine candidate rather than an exception that kills the run at
    2am. Unparseable rows are still rows you have to account for.
    """
    return spark.read.schema(TXN_SCHEMA).option("mode", "PERMISSIVE").json(landing)


def write_outputs(
    out_dir: str,
    alerts: DataFrame,
    quarantined: DataFrame,
    summary: DataFrame,
) -> None:
    base = Path(out_dir)
    base.mkdir(parents=True, exist_ok=True)

    alerts.write.mode("overwrite").parquet(str(base / "alerts"))
    quarantined.write.mode("overwrite").parquet(str(base / "quarantine"))
    summary.write.mode("overwrite").parquet(str(base / "dq_summary"))


def run(landing: str, out_dir: str, dq_config: str, rules_config: str) -> dict:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    run_id = str(uuid.uuid4())[:8]
    dq_cfg = load_config(dq_config)
    rules_cfg = load_config(rules_config)

    source = read_landing(spark, landing).cache()
    clean, quarantined = apply_dq(source, dq_cfg)
    clean, quarantined = clean.cache(), quarantined.cache()

    scored = score(clean, rules_cfg)
    alerts = alerts_only(scored, rules_cfg.get("min_alert_band", "MEDIUM")).withColumn(
        "run_id", F.lit(run_id)
    )

    recon_result = recon.reconcile(source, clean, quarantined)
    summary = dq_summary(quarantined)

    write_outputs(out_dir, alerts, quarantined, summary)

    # ---- console report ------------------------------------------------
    print(recon.format_report(recon_result))

    print("\n  DATA QUALITY FAILURES BY CHECK")
    print("  " + "-" * 40)
    summary.show(truncate=False)

    print("\n  ALERTS BY RISK BAND")
    print("  " + "-" * 40)
    alerts.groupBy("risk_band").count().orderBy(F.col("count").desc()).show()

    print("\n  ALERTS BY RULE")
    print("  " + "-" * 40)
    (
        alerts.select(F.explode("triggered_rules").alias("rule"))
        .groupBy("rule")
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    )

    print("\n  TOP 10 ALERTS BY RISK SCORE")
    print("  " + "-" * 40)
    (
        alerts.select(
            "txn_id", "account_id", "amount", "channel", "country",
            "risk_score", "risk_band", "triggered_rules",
        )
        .orderBy(F.col("risk_score").desc())
        .show(10, truncate=False)
    )

    metrics = {
        "run_id": run_id,
        "reconciliation": recon_result,
        "alert_count": alerts.count(),
        "alert_rate_pct": round(
            100.0 * alerts.count() / max(recon_result["clean_count"], 1), 2
        ),
    }

    with (Path(out_dir) / "run_metrics.json").open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)

    print(f"\n  run_id={run_id}  alerts={metrics['alert_count']}  "
          f"alert_rate={metrics['alert_rate_pct']}%  "
          f"recon={recon_result['status']}\n")

    spark.stop()
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Transaction fraud monitoring pipeline")
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--out", default="data/out")
    parser.add_argument("--dq-config", default="config/dq_rules.yaml")
    parser.add_argument("--rules-config", default="config/rules.yaml")
    args = parser.parse_args()

    run(args.landing, args.out, args.dq_config, args.rules_config)


if __name__ == "__main__":
    main()
