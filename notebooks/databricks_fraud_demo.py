# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Transaction Fraud Monitoring Engine
# MAGIC
# MAGIC Runs the pipeline end to end on Databricks against a synthetic banking feed.
# MAGIC
# MAGIC This notebook does not reimplement anything. It imports the same
# MAGIC `src.dq`, `src.rules` and `src.recon` modules the batch job and the
# MAGIC streaming job use, so what runs here is the engine itself:
# MAGIC
# MAGIC 1. Generate a synthetic feed with fraud patterns and data defects planted on purpose
# MAGIC 2. Pass every record through the data quality gate
# MAGIC 3. Score the clean records against eight windowed fraud rules
# MAGIC 4. Reconcile source against clean plus quarantined, on counts and on amounts
# MAGIC
# MAGIC **Setup:** clone this repository into the workspace as a Git folder
# MAGIC (`Workspace` &rarr; `Create` &rarr; `Git folder`), open this notebook from
# MAGIC inside it, and attach any cluster or serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Put the repository on the import path

# COMMAND ----------

import os
import sys

# Walk up from the notebook until the directory containing src/ is found, so
# this works whether the notebook is opened from notebooks/ or from the root.
candidate = os.getcwd()
for _ in range(4):
    if os.path.isdir(os.path.join(candidate, "src")):
        break
    candidate = os.path.dirname(candidate)
else:
    raise RuntimeError(
        "Could not locate the src/ package. Open this notebook from inside the "
        "cloned repository, not from a standalone workspace folder."
    )

REPO_ROOT = candidate
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

print(f"repo root      {REPO_ROOT}")
print(f"contents       {sorted(os.listdir(REPO_ROOT))}")

# COMMAND ----------

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from src.dq import apply_dq, dq_summary
from src.generator import generate
from src.recon import format_report, reconcile
from src.rules import alerts_only, score
from src.schema import TXN_SCHEMA

print("engine modules imported from the repository")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load the rule configuration
# MAGIC
# MAGIC Thresholds and weights live in YAML, not in code. Retuning a threshold
# MAGIC after a false-positive review is a change to this file, not a redeploy.

# COMMAND ----------

with open(os.path.join(REPO_ROOT, "config", "rules.yaml"), encoding="utf-8") as fh:
    rules_cfg = yaml.safe_load(fh)

with open(os.path.join(REPO_ROOT, "config", "dq_rules.yaml"), encoding="utf-8") as fh:
    dq_cfg = yaml.safe_load(fh)

print(f"{len(rules_cfg['rules'])} fraud rules loaded")
for rule in rules_cfg["rules"]:
    state = "enabled " if rule.get("enabled", True) else "disabled"
    print(f"  {state}  weight {rule['weight']:>3}  {rule['name']}")

print(f"\n{len(dq_cfg['checks'])} data quality checks + duplicate detection on "
      f"{dq_cfg['unique_key']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate the synthetic feed
# MAGIC
# MAGIC The generator plants fraud patterns the rules are supposed to catch and
# MAGIC data defects the DQ gate is supposed to quarantine. Both are deliberate,
# MAGIC so every run can assert the controls actually fired.

# COMMAND ----------

from datetime import datetime

rows = generate(n_accounts=60, days=3, seed=42)
print(f"{len(rows)} transactions generated")


def _to_row(record: dict) -> dict:
    """Parse the ISO timestamp the generator emits into a real datetime."""
    parsed = dict(record)
    if parsed.get("txn_ts"):
        parsed["txn_ts"] = datetime.strptime(parsed["txn_ts"], "%Y-%m-%dT%H:%M:%S")
    return parsed


# TXN_SCHEMA declares the required fields non-nullable, which is correct when
# reading JSON: Spark does not enforce it there, so a malformed row arrives as
# nulls and the DQ gate quarantines it. createDataFrame does enforce it and
# would reject the planted defects before they ever reach the gate, so build a
# permissive copy of the same schema for this in-memory load.
PERMISSIVE_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in TXN_SCHEMA.fields]
)

source = spark.createDataFrame([_to_row(r) for r in rows], schema=PERMISSIVE_SCHEMA)
source.cache()

print(f"{source.count()} rows in the source DataFrame")
display(source.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data quality gate
# MAGIC
# MAGIC A null amount or a duplicated transaction id will happily produce a fraud
# MAGIC alert, and an analyst then spends an afternoon chasing a transaction that
# MAGIC never existed. Bad rows are quarantined with the failing check names
# MAGIC attached, never silently dropped.

# COMMAND ----------

clean, quarantined = apply_dq(source, dq_cfg)
clean.cache()
quarantined.cache()

print(f"clean         {clean.count():>6}")
print(f"quarantined   {quarantined.count():>6}")

# COMMAND ----------

# MAGIC %md
# MAGIC Every quarantined row keeps a `dq_failures` array naming each check it
# MAGIC broke, so the quarantine table is diagnosable on its own.

# COMMAND ----------

display(
    quarantined.select(
        "txn_id", "account_id", "amount", "currency", "channel", "dq_failures"
    )
)

# COMMAND ----------

display(dq_summary(quarantined))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Score the clean records
# MAGIC
# MAGIC Eight rules are computed as additive boolean columns over shared windows
# MAGIC in a single pass, rather than one filter per rule. Scores sum across
# MAGIC triggered rules and band into LOW / MEDIUM / HIGH / CRITICAL.

# COMMAND ----------

scored = score(clean, rules_cfg)
alerts = alerts_only(scored, rules_cfg.get("min_alert_band", "MEDIUM"))
alerts.cache()

alert_count = alerts.count()
clean_count = clean.count()
print(f"alerts        {alert_count:>6}")
print(f"alert rate    {100.0 * alert_count / max(clean_count, 1):>6.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top alerts by risk score
# MAGIC
# MAGIC Stacking is the point. A dormant account making a large wire to a
# MAGIC brand-new payee at an odd hour trips several rules at once and lands well
# MAGIC above any single rule's weight.

# COMMAND ----------

display(
    alerts.select(
        "txn_id",
        "account_id",
        "amount",
        "channel",
        "country",
        "merchant_category",
        "risk_score",
        "risk_band",
        "triggered_rules",
    ).orderBy(F.col("risk_score").desc())
)

# COMMAND ----------

display(alerts.groupBy("risk_band").count().orderBy(F.col("count").desc()))

# COMMAND ----------

display(
    alerts.select(F.explode("triggered_rules").alias("rule"))
    .groupBy("rule")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reconciliation
# MAGIC
# MAGIC Source rows must equal clean plus quarantined, and source amount must
# MAGIC equal clean plus quarantined amount. A join that silently drops rows, a
# MAGIC filter with a null-handling bug or a partition that failed to write all
# MAGIC show up here as a break, and none of them show up in a row count you
# MAGIC never took.

# COMMAND ----------

result = reconcile(source, clean, quarantined)
print(format_report(result))

assert result["status"] == "PASS", f"Reconciliation failed: {result}"
print("\n  reconciliation asserted PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run summary

# COMMAND ----------

summary = spark.createDataFrame(
    [
        ("source rows", str(result["source_count"])),
        ("clean rows", str(result["clean_count"])),
        ("quarantined rows", str(result["quarantined_count"])),
        ("count break", str(result["count_break"])),
        ("source amount", f"{result['source_amount']:,.2f}"),
        ("clean + quarantined", f"{result['split_amount']:,.2f}"),
        ("amount break", f"{result['amount_break']:,.2f}"),
        ("reconciliation", result["status"]),
        ("alerts raised", str(alert_count)),
        ("alert rate", f"{100.0 * alert_count / max(clean_count, 1):.2f}%"),
    ],
    schema="measure string, value string",
)

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: write the outputs as Delta
# MAGIC
# MAGIC The batch job writes Parquet to a local path. On Databricks the same
# MAGIC frames land as managed Delta tables, which is the only change needed to
# MAGIC move this from a laptop to a lakehouse.

# COMMAND ----------

# Set to True to persist. Requires a catalog and schema you can write to.
WRITE_OUTPUTS = False
CATALOG = "workspace"
SCHEMA = "default"

if WRITE_OUTPUTS:
    for name, frame in [
        ("fraud_alerts", alerts),
        ("fraud_quarantine", quarantined),
        ("fraud_dq_summary", dq_summary(quarantined)),
    ]:
        target = f"{CATALOG}.{SCHEMA}.{name}"
        frame.write.format("delta").mode("overwrite").saveAsTable(target)
        print(f"wrote {target}")
else:
    print("WRITE_OUTPUTS is False, nothing persisted")
