"""Explicit schema for the incoming transaction feed.

Schema is declared rather than inferred so that a malformed upstream file
fails loudly at read time instead of silently changing column types
between runs.
"""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TXN_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("beneficiary_id", StringType(), nullable=True),
        StructField("txn_ts", TimestampType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
    ]
)

# Columns carried through every layer of the pipeline unchanged.
CORE_COLUMNS = [f.name for f in TXN_SCHEMA.fields]

ALERT_COLUMNS = CORE_COLUMNS + [
    "triggered_rules",
    "risk_score",
    "risk_band",
    "run_id",
]
