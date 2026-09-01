"""Source-to-target reconciliation.

The pipeline splits the feed into clean and quarantined, then scores the
clean side. Reconciliation asserts the obvious invariant that gets broken
more often than anyone expects:

    source rows == clean rows + quarantined rows
    source amount == clean amount + quarantined amount

A join that silently drops rows, a filter with a null-handling bug, a
partition that failed to write - all of them show up here as a break, and
none of them show up in a row count you never took.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def reconcile(source: DataFrame, clean: DataFrame, quarantined: DataFrame) -> dict:
    src_count = source.count()
    clean_count = clean.count()
    quar_count = quarantined.count()

    def total_amount(df: DataFrame) -> float:
        value = df.select(F.coalesce(F.sum("amount"), F.lit(0.0))).first()[0]
        return round(float(value), 2)

    src_amt = total_amount(source)
    split_amt = round(total_amount(clean) + total_amount(quarantined), 2)

    count_break = src_count - (clean_count + quar_count)
    # Tolerance covers float summation drift, nothing more. A real break
    # will be orders of magnitude larger than a cent.
    amount_break = round(src_amt - split_amt, 2)

    return {
        "source_count": src_count,
        "clean_count": clean_count,
        "quarantined_count": quar_count,
        "count_break": count_break,
        "source_amount": src_amt,
        "split_amount": split_amt,
        "amount_break": amount_break,
        "status": "PASS" if count_break == 0 and abs(amount_break) < 0.01 else "FAIL",
    }


def format_report(result: dict) -> str:
    lines = [
        "",
        "=" * 58,
        "  RECONCILIATION",
        "=" * 58,
        f"  source rows          {result['source_count']:>12,}",
        f"  clean rows           {result['clean_count']:>12,}",
        f"  quarantined rows     {result['quarantined_count']:>12,}",
        f"  count break          {result['count_break']:>12,}",
        "",
        f"  source amount        {result['source_amount']:>12,.2f}",
        f"  clean + quarantined  {result['split_amount']:>12,.2f}",
        f"  amount break         {result['amount_break']:>12,.2f}",
        "",
        f"  status               {result['status']:>12}",
        "=" * 58,
    ]
    return "\n".join(lines)
