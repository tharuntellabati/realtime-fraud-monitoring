"""Generates a synthetic transaction feed.

The generator deliberately plants two kinds of things in the data:

1. Fraud patterns the rules engine is supposed to catch (velocity bursts,
   structuring, impossible travel, dormant-account reactivation).
2. Data defects the DQ gate is supposed to quarantine (nulls in required
   fields, negative amounts, duplicate transaction ids, unparseable dates).

Because both are planted on purpose, every run can assert that the
controls actually fired - which is the point of the project. A rule that
has never been shown to catch anything is not a control, it is a comment.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

CHANNELS = ["ATM", "POS", "ONLINE", "WIRE", "MOBILE", "BRANCH"]
COUNTRIES = ["US", "US", "US", "US", "GB", "IN", "SG", "AE", "NG"]
MCC = ["grocery", "fuel", "electronics", "travel", "gambling", "crypto", "utilities"]


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


# Each account transacts from a home country. Without this, random country
# assignment makes every consecutive pair look like cross-border activity
# and the impossible-travel rule fires on a quarter of the feed - which
# would say more about the generator than about the rule.
HOME_COUNTRY: dict[str, str] = {}


def _home_country(account_id: str) -> str:
    if account_id not in HOME_COUNTRY:
        HOME_COUNTRY[account_id] = random.choice(COUNTRIES)
    return HOME_COUNTRY[account_id]


def _base_txn(account_id: str, ts: datetime, **overrides) -> dict:
    txn = {
        "txn_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": f"CUST{account_id[-4:]}",
        "beneficiary_id": f"BEN{random.randint(1000, 1400)}",
        "txn_ts": _iso(ts),
        "amount": round(random.uniform(5, 800), 2),
        "currency": "USD",
        "channel": random.choice(CHANNELS),
        "country": _home_country(account_id),
        "merchant_category": random.choice(MCC),
    }
    txn.update(overrides)
    return txn


def generate(n_accounts: int = 60, days: int = 3, seed: int = 42) -> list[dict]:
    random.seed(seed)
    start = datetime.now().replace(microsecond=0) - timedelta(days=days)
    accounts = [f"ACC{1000 + i}" for i in range(n_accounts)]
    rows: list[dict] = []

    # --- normal background activity -------------------------------------
    for acct in accounts:
        for _ in range(random.randint(4, 18)):
            offset = timedelta(
                days=random.randint(0, days),
                hours=random.randint(8, 21),
                minutes=random.randint(0, 59),
            )
            rows.append(_base_txn(acct, start + offset))

    # --- planted fraud pattern: velocity burst ---------------------------
    # 12 card-not-present hits on one account inside ten minutes.
    burst_acct = accounts[3]
    burst_start = start + timedelta(days=1, hours=14)
    for i in range(12):
        rows.append(
            _base_txn(
                burst_acct,
                burst_start + timedelta(seconds=45 * i),
                amount=round(random.uniform(60, 220), 2),
                channel="ONLINE",
                country="US",
            )
        )

    # --- planted fraud pattern: structuring ------------------------------
    # Six cash deposits parked just under the 10k reporting threshold.
    struct_acct = accounts[7]
    struct_start = start + timedelta(days=2, hours=9)
    for i in range(6):
        rows.append(
            _base_txn(
                struct_acct,
                struct_start + timedelta(hours=2 * i),
                amount=round(random.uniform(9100, 9850), 2),
                channel="BRANCH",
                country="US",
            )
        )

    # --- planted fraud pattern: impossible travel ------------------------
    # Same account transacting on two continents 40 minutes apart.
    geo_acct = accounts[11]
    HOME_COUNTRY[geo_acct] = "US"
    geo_ts = start + timedelta(days=2, hours=16)
    rows.append(_base_txn(geo_acct, geo_ts, country="US", channel="POS", amount=310.00))
    rows.append(
        _base_txn(
            geo_acct,
            geo_ts + timedelta(minutes=40),
            country="SG",
            channel="POS",
            amount=1450.00,
        )
    )

    # --- planted fraud pattern: dormant reactivation ---------------------
    # One old transaction, then silence, then a large wire out.
    dormant_acct = "ACC9001"
    rows.append(
        _base_txn(
            dormant_acct,
            start - timedelta(days=200),
            amount=42.10,
            channel="POS",
        )
    )
    rows.append(
        _base_txn(
            dormant_acct,
            start + timedelta(days=2, hours=3),
            amount=27500.00,
            channel="WIRE",
            country="AE",
            beneficiary_id="BEN9999",
        )
    )

    # --- planted data defects (for the DQ gate) --------------------------
    defect_ts = start + timedelta(days=1, hours=11)
    rows.append(_base_txn(accounts[20], defect_ts, amount=-500.00))  # negative
    null_key = _base_txn(accounts[21], defect_ts)
    null_key["account_id"] = None  # null key
    rows.append(null_key)
    rows.append(_base_txn(accounts[22], defect_ts, amount=None))  # null amount
    rows.append(_base_txn(accounts[23], defect_ts, currency="XXX"))  # bad currency
    rows.append(_base_txn(accounts[24], defect_ts, channel="TELEPATHY"))  # bad channel

    dupe = _base_txn(accounts[25], defect_ts, amount=99.99)
    rows.append(dupe)
    rows.append(dict(dupe))  # exact duplicate txn_id

    random.shuffle(rows)
    return rows


def write_feed(rows: list[dict], out_dir: str, files: int = 3) -> None:
    """Write the feed as newline-delimited JSON, split across several files.

    Splitting matters: it is the difference between a demo that reads one
    tidy file and a job that has to handle a landing directory the way a
    real feed arrives.
    """
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    for old in path.glob("*.json"):
        old.unlink()

    chunk = max(1, len(rows) // files)
    for i in range(files):
        slice_ = rows[i * chunk : (i + 1) * chunk] if i < files - 1 else rows[i * chunk :]
        if not slice_:
            continue
        target = path / f"txns_part{i:02d}.json"
        with target.open("w", encoding="utf-8") as fh:
            for row in slice_:
                fh.write(json.dumps(row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic transaction feed")
    parser.add_argument("--out", default="data/landing", help="output directory")
    parser.add_argument("--accounts", type=int, default=60)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rows = generate(args.accounts, args.days, args.seed)
    write_feed(rows, args.out)
    print(f"Wrote {len(rows)} transactions to {args.out}/")


if __name__ == "__main__":
    main()
