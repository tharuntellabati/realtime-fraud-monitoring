"""Tests that the controls actually fire.

These are the tests that matter for a detection system. Asserting that a
function returns a DataFrame proves nothing; asserting that a known
structuring pattern produces a structuring alert proves the control works.
Each test builds the minimum data needed to trip exactly one rule.

Run with:  pytest -q
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import yaml
from pyspark.sql import Row, SparkSession

from src.dq import apply_dq
from src.rules import score

BASE_TS = datetime(2026, 3, 1, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def rules_cfg():
    with open("config/rules.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture(scope="session")
def dq_cfg():
    with open("config/dq_rules.yaml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def txn(txn_id, account, ts, amount, **kw):
    return Row(
        txn_id=txn_id,
        account_id=account,
        customer_id="C1",
        beneficiary_id=kw.get("beneficiary_id", "B1"),
        txn_ts=ts,
        amount=float(amount),
        currency=kw.get("currency", "USD"),
        channel=kw.get("channel", "POS"),
        country=kw.get("country", "US"),
        merchant_category=kw.get("merchant_category", "grocery"),
    )


def triggered_for(scored, txn_id):
    row = scored.filter(scored.txn_id == txn_id).first()
    return set(row["triggered_rules"]), row["risk_score"]


def test_velocity_burst_is_flagged(spark, rules_cfg):
    rows = [
        txn(f"T{i}", "ACC1", BASE_TS + timedelta(minutes=i * 2), 120)
        for i in range(10)
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    rules, _ = triggered_for(scored, "T9")
    assert "velocity_burst" in rules


def test_structuring_is_flagged(spark, rules_cfg):
    rows = [
        txn(f"S{i}", "ACC2", BASE_TS + timedelta(hours=i * 2), 9400, channel="BRANCH")
        for i in range(4)
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    rules, _ = triggered_for(scored, "S3")
    assert "structuring" in rules


def test_impossible_travel_is_flagged(spark, rules_cfg):
    rows = [
        txn("G1", "ACC3", BASE_TS, 200, country="US"),
        txn("G2", "ACC3", BASE_TS + timedelta(minutes=30), 900, country="SG"),
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    rules, _ = triggered_for(scored, "G2")
    assert "impossible_travel" in rules


def test_dormant_reactivation_is_flagged(spark, rules_cfg):
    rows = [
        txn("D1", "ACC4", BASE_TS - timedelta(days=200), 30),
        txn("D2", "ACC4", BASE_TS, 26000, channel="WIRE"),
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    rules, _ = triggered_for(scored, "D2")
    assert "dormant_reactivation" in rules


def test_clean_transaction_produces_no_alert(spark, rules_cfg):
    """The false-positive guard. Ordinary activity must score zero."""
    rows = [
        txn(f"N{i}", "ACC5", BASE_TS + timedelta(days=i), 45)
        for i in range(3)
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    assert scored.filter(scored.risk_band != "NONE").count() == 0


def test_stacked_rules_escalate_the_band(spark, rules_cfg):
    """Two rules firing together should outrank either one alone."""
    rows = [
        txn("X1", "ACC6", BASE_TS - timedelta(days=200), 20),
        txn(
            "X2", "ACC6", BASE_TS.replace(hour=3), 30000,
            channel="WIRE", beneficiary_id="B_NEW",
        ),
    ]
    scored = score(spark.createDataFrame(rows), rules_cfg)
    rules, risk = triggered_for(scored, "X2")
    assert len(rules) >= 3
    assert risk >= rules_cfg["risk_bands"]["high"]


def test_dq_quarantines_bad_rows_without_dropping_them(spark, dq_cfg):
    rows = [
        txn("Q1", "ACC7", BASE_TS, 100),
        txn("Q2", "ACC7", BASE_TS, -50),          # negative amount
        txn("Q3", "ACC7", BASE_TS, 100, currency="XXX"),  # bad currency
        txn("Q1", "ACC7", BASE_TS, 100),          # duplicate txn_id
    ]
    df = spark.createDataFrame(rows)
    clean, quarantined = apply_dq(df, dq_cfg)

    assert clean.count() + quarantined.count() == df.count()
    assert quarantined.count() == 3
    assert clean.count() == 1
