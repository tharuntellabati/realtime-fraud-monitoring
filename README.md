# Real-Time Transaction Fraud Monitoring Engine

[![CI](https://github.com/tharuntellabati/realtime-fraud-monitoring/actions/workflows/ci.yml/badge.svg)](https://github.com/tharuntellabati/realtime-fraud-monitoring/actions/workflows/ci.yml)

A config-driven fraud detection pipeline for banking transaction data, built on PySpark. Transactions pass through a data quality gate, get scored against eight windowed fraud rules, and land as ranked alerts — with source-to-target reconciliation proving no record was silently lost along the way.

Runs on a laptop against a synthetic feed. The same code runs on Databricks by changing two paths.

---

## Why this exists

Most fraud detection demos score transactions and stop there. Two things break that in production:

**Bad data produces confident alerts.** A null amount or a duplicated transaction id will happily generate a fraud flag, and an analyst spends an afternoon investigating a transaction that never existed. So every record passes a DQ gate first, and failures are quarantined with the failing check names attached — never dropped, never scored.

**Alert volume is the whole game.** A rule set that flags 8% of traffic isn't a control, it's a queue nobody works. This one runs at **2.2%** on the synthetic feed, and the test suite includes an explicit false-positive guard asserting that ordinary activity scores zero.

---

## Architecture

```
data/landing/*.json
        │
        ▼
┌──────────────────┐   declared schema, PERMISSIVE mode
│   Ingest         │   malformed rows become quarantine
└────────┬─────────┘   candidates, not exceptions
         ▼
┌──────────────────┐   9 checks from config/dq_rules.yaml
│   DQ Gate        │──────────────► quarantine/
└────────┬─────────┘                (with dq_failures array)
         ▼ clean
┌──────────────────┐   8 rules from config/rules.yaml
│  Rules Engine    │   weighted score → risk band
└────────┬─────────┘
         ▼
┌──────────────────┐   alerts/ + dq_summary/ + run_metrics.json
│  Reconciliation  │   source == clean + quarantined
└──────────────────┘   counts AND amounts must tie
```

---

## Detection rules

| Rule | Weight | What it catches |
|---|---|---|
| `structuring` | 40 | Deposits parked just under the 10k reporting threshold while the daily total clears it anyway |
| `impossible_travel` | 35 | Same account transacting in two countries closer together than a person could travel |
| `velocity_burst` | 30 | Card testing / account takeover — a burst of authorisations in a rolling hour |
| `dormant_reactivation` | 30 | Long-silent account suddenly moving funds; a signature of both ATO and mule activation |
| `high_value_txn` | 25 | Single transaction above the manual-review ceiling |
| `new_beneficiary_high_value` | 20 | First payment to a payee, and it's a large one |
| `high_risk_corridor` | 15 | Elevated-risk merchant categories above a floor amount |
| `odd_hour_activity` | 10 | Deliberately low weight — odd hours alone mean little, the value is what it contributes when it stacks |

Scores sum across triggered rules and band into `LOW` / `MEDIUM` / `HIGH` / `CRITICAL`. Stacking is the point: a dormant account making a large wire to a brand-new payee at 3am trips four rules and lands at 90, well above any single rule's weight.

**Thresholds live in `config/rules.yaml`, not in code.** Fraud thresholds get retuned constantly as patterns shift, and a threshold change after a false-positive review should be a pull request against a config file, not a redeploy of the engine.

---

## Results from a run

The generator plants known fraud patterns and known data defects, so every run can assert the controls actually fired. A rule that has never been shown to catch anything is not a control, it's a comment.

```
==========================================================
  RECONCILIATION
==========================================================
  source rows                   719
  clean rows                    713
  quarantined rows                6
  count break                     0

  source amount          360,683.91
  clean + quarantined    360,683.91
  amount break                 0.00

  status                       PASS
==========================================================

  ALERTS BY RISK BAND          ALERTS BY RULE
  CRITICAL   3                 impossible_travel            6
  HIGH       2                 structuring                  6
  MEDIUM    11                 velocity_burst               5
                               high_risk_corridor           3
  alert_rate = 2.24%           dormant_reactivation         1
```

Top alert, all four rules stacking on one transaction:

```
ACC9001  $27,500.00  WIRE  AE   score 90  CRITICAL
  [high_value_txn, dormant_reactivation,
   new_beneficiary_high_value, high_risk_corridor]
```

---

## Data quality checks

Nine row-level checks plus duplicate detection run before any rule sees a transaction, declared in `config/dq_rules.yaml`:

`not_null` on the four required fields · `positive` amounts · `max_value` ceiling (anything above 10M on a retail feed is an upstream defect, not a transaction) · `allowed_values` for currency and channel · `regex` on country codes · duplicate detection on `txn_id`

Quarantined rows keep a `dq_failures` array naming every check they broke, so the quarantine table is diagnosable on its own without re-running the job.

---

## Running it

Requires **Python 3.9-3.11** and Java 8u371+, 11, or 17 (PySpark needs a JVM).

PySpark 3.5.1 does not support Python 3.12 — the Spark Python workers crash on startup with no traceback. Use 3.11 or older.

```bash
git clone https://github.com/tharuntellabati/realtime-fraud-monitoring.git
cd realtime-fraud-monitoring
pip install -r requirements.txt

python -m src.generator          # writes ~720 synthetic transactions
python -m src.pipeline           # DQ gate → rules → recon → alerts
pytest -q                        # 7 tests
```

Outputs land in `data/out/` as Parquet: `alerts/`, `quarantine/`, `dq_summary/`, plus `run_metrics.json`.

**Streaming mode.** `python -m src.streaming` reads the landing directory as a Structured Streaming file source and processes new files as they arrive. It calls the same `apply_dq` and `score` functions the batch job uses — that reuse is deliberate, since two copies of a fraud rule drift apart and then batch and real-time disagree about whether a transaction was ever flagged.

**On Windows**, three things bite, in this order:

1. **`JAVA_HOME` must point at the JDK root, not `\bin`.** If it ends in `\bin`, Spark looks
   for `%JAVA_HOME%\bin\java.exe` and reports `The system cannot find the path specified`.
2. **`winutils.exe` and `hadoop.dll`** must sit in `%HADOOP_HOME%\bin`. Without them even
   listing the landing directory fails with `UnsatisfiedLinkError: NativeIO$Windows.access0`.
3. **No spaces in the path to your Python interpreter.** Spark's worker launcher does not
   quote it, so a venv under `C:\Users\...\My Folder\` produces `Missing Python executable`.
   Point `PYSPARK_PYTHON` at a space-free path.

If that's a hassle, skip it: CI runs the full pipeline on Ubuntu on every push, and the
project imports directly into Databricks Community Edition (free) — put the `src/`
files in a repo folder and run `pipeline.run()` from a notebook.

---

## Design notes

**One pass, not one filter per rule.** Rules are additive boolean columns computed over shared windows rather than eight separate filters. Eight filters means eight scans; this way the lag and rolling-window columns are computed once and every rule reads from them.

**Rolling windows via `rangeBetween` on epoch seconds.** `Window.partitionBy("account_id").orderBy(epoch).rangeBetween(-3600, 0)` gives a true rolling hour rather than a fixed tumbling bucket — which matters, because a burst straddling a bucket boundary is exactly the one you'd miss.

**Reconciliation is not optional.** A join that silently drops rows, a filter with a null-handling bug, a partition that failed to write — all show up as a break here, and none show up in a row count you never took.

**Streaming state.** The windowed rules run inside `foreachBatch` over the micro-batch, which is the pragmatic approach when source ordering guarantees are weak. A production Kafka deployment would use `flatMapGroupsWithState` keyed by `account_id` instead. That tradeoff is called out in `src/streaming.py` rather than papered over.

---

## Project layout

```
config/
  rules.yaml          8 fraud rules — thresholds, weights, enable flags
  dq_rules.yaml       9 data quality checks
src/
  schema.py           declared transaction schema
  generator.py        synthetic feed with planted fraud + planted defects
  dq.py               config-driven DQ gate, clean/quarantine split
  rules.py            windowed rule engine, scoring, banding
  recon.py            source-to-target reconciliation
  pipeline.py         batch orchestration
  streaming.py        Structured Streaming variant
tests/
  test_rules.py       7 tests: each pattern caught + false-positive guard
```

---

## Stack

PySpark 3.5 · Spark SQL window functions · Structured Streaming · PyYAML · pytest · Parquet
