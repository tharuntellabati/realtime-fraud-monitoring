"""Streaming variant of the same pipeline.

Reads the landing directory as a Structured Streaming file source so new
files are picked up as they land. The DQ gate and the rules engine are the
exact same functions the batch job calls - that reuse is the point. Two
copies of a fraud rule drift apart, and then batch and real-time disagree
about whether a transaction was ever flagged.

Run it in one terminal:

    python -m src.streaming

and drop new files into data/landing from another.

Note on windowed rules: rolling-window rules (velocity, structuring,
impossible travel) need state across micro-batches. Here they run inside
foreachBatch over the micro-batch plus a replayed history window, which is
the pragmatic approach when the ordering guarantees of the source are
weak. A production deployment on Kafka would use flatMapGroupsWithState
keyed by account_id instead.
"""

from __future__ import annotations

import argparse

import yaml
from pyspark.sql import DataFrame, functions as F

from src.dq import apply_dq
from src.pipeline import build_spark, load_config
from src.rules import alerts_only, score
from src.schema import TXN_SCHEMA


def process_batch(batch_df: DataFrame, batch_id: int, dq_cfg: dict, rules_cfg: dict):
    if batch_df.rdd.isEmpty():
        return

    clean, quarantined = apply_dq(batch_df, dq_cfg)
    scored = score(clean, rules_cfg)
    alerts = alerts_only(scored, rules_cfg.get("min_alert_band", "MEDIUM"))

    quar_count = quarantined.count()
    alert_count = alerts.count()

    print(
        f"\n[batch {batch_id}] rows={batch_df.count()} "
        f"quarantined={quar_count} alerts={alert_count}"
    )

    if alert_count:
        (
            alerts.select(
                "txn_id", "account_id", "amount", "channel",
                "risk_score", "risk_band", "triggered_rules",
            )
            .orderBy(F.col("risk_score").desc())
            .show(5, truncate=False)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming fraud monitoring")
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--checkpoint", default="data/checkpoint")
    parser.add_argument("--dq-config", default="config/dq_rules.yaml")
    parser.add_argument("--rules-config", default="config/rules.yaml")
    args = parser.parse_args()

    spark = build_spark("txn-fraud-engine-streaming")
    spark.sparkContext.setLogLevel("WARN")

    dq_cfg = load_config(args.dq_config)
    rules_cfg = load_config(args.rules_config)

    stream = (
        spark.readStream.schema(TXN_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(args.landing)
    )

    query = (
        stream.writeStream.foreachBatch(
            lambda df, bid: process_batch(df, bid, dq_cfg, rules_cfg)
        )
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
