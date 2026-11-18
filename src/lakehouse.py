"""Incrementally ingest JSONL order changes and export Parquet lakehouse layers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from jsonschema import Draft202012Validator, FormatChecker


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "contracts/order_event_v1.schema.json"


def initialize(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA IF NOT EXISTS meta")
    connection.execute("CREATE SCHEMA IF NOT EXISTS core")
    connection.execute("""
        CREATE TABLE IF NOT EXISTS meta.files (
            file_name VARCHAR PRIMARY KEY,
            sha256 VARCHAR NOT NULL,
            delivered_count INTEGER NOT NULL,
            valid_count INTEGER NOT NULL,
            duplicate_count INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL,
            exported BOOLEAN NOT NULL DEFAULT false
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS meta.dirty_partitions (
            order_date DATE PRIMARY KEY
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS core.events (
            event_id UUID PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            event_version INTEGER NOT NULL,
            customer_id VARCHAR NOT NULL,
            order_date DATE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            status VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency VARCHAR NOT NULL,
            canonical_json VARCHAR NOT NULL,
            UNIQUE (order_id, event_version)
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS core.deliveries (
            file_name VARCHAR NOT NULL,
            line_number INTEGER NOT NULL,
            event_id UUID NOT NULL,
            PRIMARY KEY (file_name, line_number)
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS core.quarantine (
            file_name VARCHAR NOT NULL,
            line_number INTEGER NOT NULL,
            raw_line VARCHAR NOT NULL,
            reason VARCHAR NOT NULL,
            PRIMARY KEY (file_name, line_number)
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS core.orders (
            order_id VARCHAR PRIMARY KEY,
            event_id UUID NOT NULL,
            event_version INTEGER NOT NULL,
            customer_id VARCHAR NOT NULL,
            order_date DATE NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            status VARCHAR NOT NULL,
            amount_cents BIGINT NOT NULL,
            currency VARCHAR NOT NULL
        )
    """)


def load_validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def classification(error) -> str:
    if error.validator in {"additionalProperties", "required"}:
        return "schema_drift"
    if error.validator == "const" and list(error.path) == ["schema_version"]:
        return "schema_drift"
    return "invalid_value"


def parse_event(raw_line: str, validator: Draft202012Validator) -> tuple[dict | None, str | None]:
    try:
        event = json.loads(raw_line)
    except json.JSONDecodeError:
        return None, "invalid_json"
    errors = sorted(validator.iter_errors(event), key=lambda item: (str(item.path), item.message))
    if errors:
        return None, classification(errors[0])
    return event, None


def process_file(connection: duckdb.DuckDBPyConnection, path: Path, validator: Draft202012Validator) -> str:
    file_name = path.name
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.jsonl", file_name):
        raise ValueError(f"unsupported batch filename: {file_name}")
    content = path.read_bytes()
    checksum = hashlib.sha256(content).hexdigest()
    previous = connection.execute("SELECT sha256 FROM meta.files WHERE file_name = ?", [file_name]).fetchone()
    if previous:
        if previous[0] != checksum:
            raise ValueError(f"processed file changed: {file_name}")
        return "skipped"
    lines = content.decode("utf-8", errors="replace").splitlines()
    delivered = valid = duplicates = quarantined = 0
    connection.execute("BEGIN TRANSACTION")
    try:
        for line_number, raw_line in enumerate(lines, 1):
            delivered += 1
            event, reason = parse_event(raw_line, validator)
            if reason:
                connection.execute(
                    "INSERT INTO core.quarantine VALUES (?, ?, ?, ?)",
                    [file_name, line_number, raw_line, reason],
                )
                quarantined += 1
                continue
            canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
            event_id = event["event_id"]
            order_id = event["order_id"]
            version = event["event_version"]
            existing = connection.execute(
                "SELECT canonical_json FROM core.events WHERE event_id = ?", [event_id]
            ).fetchone()
            if existing:
                if existing[0] != canonical:
                    raise ValueError(f"conflicting content for event_id {event_id}")
                duplicates += 1
            else:
                version_owner = connection.execute(
                    "SELECT event_id FROM core.events WHERE order_id = ? AND event_version = ?",
                    [order_id, version],
                ).fetchone()
                if version_owner:
                    raise ValueError(f"conflicting version {version} for order {order_id}")
                order_day = date.fromisoformat(event["order_date"])
                updated = datetime.fromisoformat(event["updated_at"].replace("Z", "+00:00"))
                updated = updated.astimezone(timezone.utc)
                current = connection.execute(
                    "SELECT event_version, customer_id, order_date, amount_cents, currency "
                    "FROM core.orders WHERE order_id = ?", [order_id],
                ).fetchone()
                identity = (event["customer_id"], order_day, event["amount_cents"], event["currency"])
                if current and current[1:] != identity:
                    raise ValueError(f"stable order attributes changed for {order_id}")
                values = [
                    event_id, order_id, version, event["customer_id"], order_day,
                    updated, event["status"], event["amount_cents"], event["currency"],
                ]
                connection.execute(
                    "INSERT INTO core.events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    values + [canonical],
                )
                if not current:
                    connection.execute(
                        "INSERT INTO core.orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [order_id, event_id, version, event["customer_id"], order_day,
                         updated, event["status"], event["amount_cents"], event["currency"]],
                    )
                    connection.execute("INSERT OR IGNORE INTO meta.dirty_partitions VALUES (?)", [order_day])
                elif version > current[0]:
                    connection.execute(
                        "UPDATE core.orders SET event_id = ?, event_version = ?, updated_at = ?, "
                        "status = ? WHERE order_id = ?",
                        [event_id, version, updated, event["status"], order_id],
                    )
                    connection.execute("INSERT OR IGNORE INTO meta.dirty_partitions VALUES (?)", [order_day])
            connection.execute(
                "INSERT INTO core.deliveries VALUES (?, ?, ?)",
                [file_name, line_number, event_id],
            )
            valid += 1
        connection.execute(
            "INSERT INTO meta.files VALUES (?, ?, ?, ?, ?, ?, false)",
            [file_name, checksum, delivered, valid, duplicates, quarantined],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return f"processed {delivered} lines ({valid} valid, {duplicates} duplicates, {quarantined} quarantined)"


def copy_parquet(connection: duckdb.DuckDBPyConnection, query: str, parameters: list, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.stem}-{uuid.uuid4().hex}.parquet")
    try:
        connection.execute(
            f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            parameters + [str(temporary)],
        )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def repair_exports(connection: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    pending_files = connection.execute(
        "SELECT file_name, quarantined_count FROM meta.files WHERE NOT exported ORDER BY file_name"
    ).fetchall()
    for file_name, quarantined_count in pending_files:
        batch_id = Path(file_name).stem
        copy_parquet(
            connection,
            "SELECT d.file_name, d.line_number, e.event_id, e.order_id, e.event_version, "
            "e.customer_id, e.order_date, e.updated_at, e.status, e.amount_cents, e.currency "
            "FROM core.deliveries d JOIN core.events e USING (event_id) "
            "WHERE d.file_name = ? ORDER BY d.line_number",
            [file_name],
            output_dir / "bronze" / f"batch_id={batch_id}" / "events.parquet",
        )
        if quarantined_count:
            copy_parquet(
                connection,
                "SELECT file_name, line_number, raw_line, reason FROM core.quarantine "
                "WHERE file_name = ? ORDER BY line_number",
                [file_name],
                output_dir / "quarantine" / f"batch_id={batch_id}" / "invalid.parquet",
            )
        connection.execute("UPDATE meta.files SET exported = true WHERE file_name = ?", [file_name])

    dirty_dates = connection.execute(
        "SELECT order_date FROM meta.dirty_partitions ORDER BY order_date"
    ).fetchall()
    for (order_day,) in dirty_dates:
        partition = f"order_date={order_day.isoformat()}"
        copy_parquet(
            connection,
            "SELECT order_id, event_id, event_version, customer_id, order_date, "
            "updated_at, status, amount_cents, currency "
            "FROM core.orders WHERE order_date = ? ORDER BY order_id",
            [order_day],
            output_dir / "silver" / partition / "orders.parquet",
        )
        copy_parquet(
            connection,
            "SELECT order_date, currency, count(*) AS orders_total, "
            "count(*) FILTER (WHERE status = 'placed') AS placed_orders, "
            "count(*) FILTER (WHERE status = 'paid') AS paid_orders, "
            "count(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders, "
            "count(*) FILTER (WHERE status = 'refunded') AS refunded_orders, "
            "cast(coalesce(sum(amount_cents) FILTER (WHERE status = 'paid'), 0) AS BIGINT) "
            "AS paid_revenue_cents "
            "FROM core.orders WHERE order_date = ? GROUP BY order_date, currency ORDER BY currency",
            [order_day],
            output_dir / "gold" / partition / "daily_orders.parquet",
        )
        connection.execute("DELETE FROM meta.dirty_partitions WHERE order_date = ?", [order_day])


def run(raw_dir: Path, output_dir: Path) -> list[tuple[str, str]]:
    files = sorted(raw_dir.glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"no JSONL batches in {raw_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(output_dir / "lakehouse.duckdb"))
    try:
        initialize(connection)
        validator = load_validator()
        repair_exports(connection, output_dir)
        results = []
        for path in files:
            outcome = process_file(connection, path, validator)
            repair_exports(connection, output_dir)
            results.append((path.name, outcome))
        return results
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("warehouse"))
    args = parser.parse_args()
    for filename, outcome in run(args.raw_dir, args.output_dir):
        print(f"{filename}: {outcome}")


if __name__ == "__main__":
    main()
