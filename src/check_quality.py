"""Reconcile DuckDB state with exported bronze, silver, gold, and quarantine Parquet."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parquet_rows(connection: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    if not files:
        return 0
    return connection.execute(
        "SELECT count(*) FROM read_parquet(?)", [[str(path) for path in files]]
    ).fetchone()[0]


def assert_export_matches(
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    export_query: str,
    source_query: str,
    layer: str,
) -> None:
    if not files:
        return
    paths = [str(path) for path in files]
    for left, right in ((export_query, source_query), (source_query, export_query)):
        difference = connection.execute(
            f"SELECT 1 FROM ({left} EXCEPT ALL {right}) LIMIT 1", [paths]
        ).fetchone()
        if difference:
            raise RuntimeError(f"{layer} Parquet differs from DuckDB state")


def check_quality(output_dir: Path) -> dict[str, int]:
    connection = duckdb.connect(str(output_dir / "lakehouse.duckdb"), read_only=True)
    try:
        delivered, valid, duplicates, quarantined, pending = connection.execute("""
            SELECT coalesce(sum(delivered_count), 0), coalesce(sum(valid_count), 0),
                   coalesce(sum(duplicate_count), 0), coalesce(sum(quarantined_count), 0),
                   count(*) FILTER (WHERE NOT exported)
            FROM meta.files
        """).fetchone()
        dirty = connection.execute("SELECT count(*) FROM meta.dirty_partitions").fetchone()[0]
        deliveries = connection.execute("SELECT count(*) FROM core.deliveries").fetchone()[0]
        events = connection.execute("SELECT count(*) FROM core.events").fetchone()[0]
        orders = connection.execute("SELECT count(*) FROM core.orders").fetchone()[0]
        quarantine_rows = connection.execute("SELECT count(*) FROM core.quarantine").fetchone()[0]
        bronze_files = sorted((output_dir / "bronze").glob("batch_id=*/events.parquet"))
        silver_files = sorted((output_dir / "silver").glob("order_date=*/orders.parquet"))
        gold_files = sorted((output_dir / "gold").glob("order_date=*/daily_orders.parquet"))
        quarantine_files = sorted((output_dir / "quarantine").glob("batch_id=*/invalid.parquet"))
        bronze = parquet_rows(connection, bronze_files)
        silver = parquet_rows(connection, silver_files)
        quarantine_export = parquet_rows(connection, quarantine_files)
        gold_orders = 0
        if gold_files:
            gold_columns = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [[str(path) for path in gold_files]],
            ).fetchall()
            gold_types = {column[0]: column[1] for column in gold_columns}
            if gold_types.get("paid_revenue_cents") != "BIGINT":
                raise RuntimeError("gold paid_revenue_cents must be BIGINT")
            gold_orders = connection.execute(
                "SELECT coalesce(sum(orders_total), 0) FROM read_parquet(?)",
                [[str(path) for path in gold_files]],
            ).fetchone()[0]
        if pending or dirty:
            raise RuntimeError(f"unfinished exports: {pending} files and {dirty} date partitions")
        if (delivered, valid, quarantined) != (valid + quarantined, deliveries, quarantine_rows):
            raise RuntimeError("manifest counts differ from DuckDB delivery or quarantine rows")
        if events != valid - duplicates:
            raise RuntimeError("unique event count differs from valid deliveries minus duplicates")
        if (bronze, silver, quarantine_export, gold_orders) != (valid, orders, quarantined, orders):
            raise RuntimeError("Parquet row counts differ from DuckDB state")
        assert_export_matches(
            connection,
            bronze_files,
            "SELECT file_name, line_number, event_id, order_id, event_version, "
            "customer_id, order_date, updated_at, status, amount_cents, currency "
            "FROM read_parquet(?)",
            "SELECT d.file_name, d.line_number, e.event_id, e.order_id, e.event_version, "
            "e.customer_id, e.order_date, e.updated_at, e.status, e.amount_cents, e.currency "
            "FROM core.deliveries d JOIN core.events e USING (event_id)",
            "bronze",
        )
        assert_export_matches(
            connection,
            quarantine_files,
            "SELECT file_name, line_number, raw_line, reason FROM read_parquet(?)",
            "SELECT file_name, line_number, raw_line, reason FROM core.quarantine",
            "quarantine",
        )
        if silver_files:
            source_rows = connection.execute("""
                SELECT order_id, event_id::VARCHAR, event_version, order_date,
                       status, amount_cents, currency
                FROM core.orders ORDER BY order_id
            """).fetchall()
            export_rows = connection.execute("""
                SELECT order_id, event_id::VARCHAR, event_version, order_date,
                       status, amount_cents, currency
                FROM read_parquet(?) ORDER BY order_id
            """, [[str(path) for path in silver_files]]).fetchall()
            if source_rows != export_rows:
                raise RuntimeError("silver Parquet differs from current order state")
            expected_gold = connection.execute("""
                SELECT order_date, currency, count(*),
                       count(*) FILTER (WHERE status = 'placed'),
                       count(*) FILTER (WHERE status = 'paid'),
                       count(*) FILTER (WHERE status = 'cancelled'),
                       count(*) FILTER (WHERE status = 'refunded'),
                       cast(coalesce(sum(amount_cents) FILTER (WHERE status = 'paid'), 0) AS BIGINT)
                FROM core.orders GROUP BY order_date, currency ORDER BY order_date, currency
            """).fetchall()
            observed_gold = connection.execute("""
                SELECT order_date, currency, orders_total, placed_orders, paid_orders,
                       cancelled_orders, refunded_orders, paid_revenue_cents
                FROM read_parquet(?) ORDER BY order_date, currency
            """, [[str(path) for path in gold_files]]).fetchall()
            if expected_gold != observed_gold:
                raise RuntimeError("gold Parquet metrics differ from current order state")
        return {
            "delivered": delivered,
            "unique_events": events,
            "orders": orders,
            "duplicates": duplicates,
            "quarantined": quarantined,
            "date_partitions": len(silver_files),
        }
    finally:
        connection.close()


def check_default_fixture(output_dir: Path, summary: dict[str, int]) -> None:
    expected = {
        "delivered": 200,
        "unique_events": 190,
        "orders": 100,
        "duplicates": 10,
        "quarantined": 0,
        "date_partitions": 29,
    }
    if summary != expected:
        raise RuntimeError(f"default fixture counts changed: {summary} != {expected}")
    connection = duckdb.connect(str(output_dir / "lakehouse.duckdb"), read_only=True)
    try:
        statuses = dict(connection.execute(
            "SELECT status, count(*) FROM core.orders GROUP BY status"
        ).fetchall())
        if statuses != {"paid": 40, "placed": 30, "refunded": 20, "cancelled": 10}:
            raise RuntimeError(f"default fixture latest statuses changed: {statuses}")
        currency_totals = connection.execute("""
            SELECT currency, sum(paid_orders), sum(paid_revenue_cents)
            FROM read_parquet(?) GROUP BY currency ORDER BY currency
        """, [str(output_dir / "gold/order_date=*/daily_orders.parquet")]).fetchall()
        if currency_totals != [("EUR", 23, 578748), ("USD", 17, 479572)]:
            raise RuntimeError(f"default fixture revenue changed: {currency_totals}")
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("warehouse"))
    parser.add_argument("--expect-default", action="store_true")
    args = parser.parse_args()
    summary = check_quality(args.output_dir)
    if args.expect_default:
        check_default_fixture(args.output_dir, summary)
    print(summary)


if __name__ == "__main__":
    main()
