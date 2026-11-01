"""Stream, parse, validate, and optionally load NASA Common Log Format access logs."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, String, Table, Text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "sample" / "nasa_access_sample.log"
DEFAULT_OUTPUT = ROOT / "output"

# NASA-HTTP records use the widely seen Common Log Format layout:
# host ident user [timestamp] "request" status bytes
LOG_PATTERN = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<request>[^"]*)"\s+(?P<status>\S+)\s+(?P<bytes>\S+)\s*$'
)
REQUEST_PATTERN = re.compile(
    r'^(?P<method>\S+)\s+(?P<target>\S+)(?:\s+(?P<protocol>\S+))?$'
)

CLEAN_COLUMNS = [
    "record_id", "source_id", "source_line", "timestamp", "host",
    "http_method", "url_path", "protocol", "status_code", "bytes_sent",
]
REJECT_COLUMNS = ["source_id", "source_line", "raw_line", "rejection_reason"]

metadata = MetaData()
web_requests = Table(
    "web_requests",
    metadata,
    Column("record_id", String(200), primary_key=True),
    Column("source_id", String(100), nullable=False),
    Column("source_line", BigInteger, nullable=False),
    Column("event_timestamp", DateTime(timezone=True), nullable=False),
    Column("host", Text, nullable=False),
    Column("http_method", String(32), nullable=False),
    Column("url_path", Text, nullable=False),
    Column("protocol", String(32), nullable=True),
    Column("status_code", Integer, nullable=False),
    Column("bytes_sent", BigInteger, nullable=True),
)


def parse_raw_line(line: str, source_id: str, line_number: int) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Parse the log structure. Return either a parsed record or a rejection."""
    raw_line = line.rstrip("\r\n")
    match = LOG_PATTERN.match(raw_line)
    if not match:
        return None, {
            "source_id": source_id,
            "source_line": line_number,
            "raw_line": raw_line,
            "rejection_reason": "malformed_log_line",
        }

    fields = match.groupdict()
    request_match = REQUEST_PATTERN.match(fields["request"])
    if not request_match:
        return None, {
            "source_id": source_id,
            "source_line": line_number,
            "raw_line": raw_line,
            "rejection_reason": "invalid_request_line",
        }

    request = request_match.groupdict()
    target = request["target"]
    # The NASA log normally stores a path, but preserve a full target if present.
    # Strip query parameters for resource-level analysis so IDs/tokens do not explode cardinality.
    url_path = target.split("?", 1)[0] or "/"

    return {
        "source_id": source_id,
        "source_line": line_number,
        "record_id": f"{source_id}:{line_number}",
        "host": fields["host"],
        "timestamp_raw": fields["timestamp"],
        "http_method": request["method"].upper(),
        "url_path": url_path,
        "protocol": request.get("protocol") or None,
        "status_raw": fields["status"],
        "bytes_raw": fields["bytes"],
        "raw_line": raw_line,
    }, None


def transform_parsed_records(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use Pandas to type/validate a batch; return cleaned and rejected DataFrames."""
    if not records:
        return pd.DataFrame(columns=CLEAN_COLUMNS), pd.DataFrame(columns=REJECT_COLUMNS)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(
        df["timestamp_raw"],
        format="%d/%b/%Y:%H:%M:%S %z",
        errors="coerce",
        utc=True,
    )
    df["status_code"] = pd.to_numeric(df["status_raw"], errors="coerce")
    bytes_raw = df["bytes_raw"].astype("string").str.strip()
    df["bytes_sent"] = pd.to_numeric(bytes_raw.mask(bytes_raw.eq("-")), errors="coerce")

    clean_records: list[dict[str, Any]] = []
    reject_records: list[dict[str, Any]] = []

    for row in df.to_dict(orient="records"):
        reasons: list[str] = []
        if pd.isna(row["timestamp"]):
            reasons.append("invalid_timestamp")

        status = row["status_code"]
        if pd.isna(status) or not float(status).is_integer() or not 100 <= int(status) <= 599:
            reasons.append("invalid_status_code")

        bytes_original = str(row["bytes_raw"]).strip()
        bytes_value = row["bytes_sent"]
        if bytes_original != "-":
            if pd.isna(bytes_value) or not float(bytes_value).is_integer() or int(bytes_value) < 0:
                reasons.append("invalid_bytes_sent")

        if not str(row["host"]).strip():
            reasons.append("missing_host")
        if not str(row["http_method"]).strip():
            reasons.append("missing_http_method")
        if not str(row["url_path"]).strip():
            reasons.append("missing_url_path")

        if reasons:
            reject_records.append({
                "source_id": row["source_id"],
                "source_line": int(row["source_line"]),
                "raw_line": row["raw_line"],
                "rejection_reason": ";".join(reasons),
            })
            continue

        bytes_out = None if bytes_original == "-" else int(bytes_value)
        clean_records.append({
            "record_id": row["record_id"],
            "source_id": row["source_id"],
            "source_line": int(row["source_line"]),
            "timestamp": row["timestamp"].to_pydatetime(),
            "host": str(row["host"]),
            "http_method": str(row["http_method"]),
            "url_path": str(row["url_path"]),
            "protocol": row["protocol"],
            "status_code": int(status),
            "bytes_sent": bytes_out,
        })

    clean_df = pd.DataFrame(clean_records, columns=CLEAN_COLUMNS)
    if not clean_df.empty:
        # Preserve integer byte counts even when some rows have SQL NULL values.
        clean_df["bytes_sent"] = pd.array(clean_df["bytes_sent"], dtype="Int64")
    return clean_df, pd.DataFrame(reject_records, columns=REJECT_COLUMNS)


def iter_line_batches(input_path: Path, chunk_size: int, limit: int | None) -> Iterable[list[tuple[int, str]]]:
    """Read text logs incrementally to avoid loading a multi-million-line file at once."""
    batch: list[tuple[int, str]] = []
    with input_path.open("r", encoding="latin-1", errors="replace") as handle:
        for line_number, line in enumerate(handle, start=1):
            if limit is not None and line_number > limit:
                break
            batch.append((line_number, line))
            if len(batch) >= chunk_size:
                yield batch
                batch = []
    if batch:
        yield batch


def prepare_batch(lines: list[tuple[int, str]], source_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    parsed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for line_number, line in lines:
        record, reject = parse_raw_line(line, source_id, line_number)
        if record is not None:
            parsed.append(record)
        if reject is not None:
            rejected.append(reject)

    cleaned_df, validation_rejects = transform_parsed_records(parsed)
    if not validation_rejects.empty:
        rejected.extend(validation_rejects.to_dict(orient="records"))
    rejected_df = pd.DataFrame(rejected, columns=REJECT_COLUMNS)
    return cleaned_df, rejected_df


def prepare_output_files(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    cleaned_path = output_dir / "cleaned_nasa_logs.csv"
    rejected_path = output_dir / "rejected_nasa_logs.csv"
    pd.DataFrame(columns=CLEAN_COLUMNS).to_csv(cleaned_path, index=False)
    pd.DataFrame(columns=REJECT_COLUMNS).to_csv(rejected_path, index=False)
    return cleaned_path, rejected_path


def load_to_postgres(engine: Engine, cleaned: pd.DataFrame) -> int:
    """Insert a batch; record IDs are source ID + source line for repeatable reruns."""
    if cleaned.empty:
        return 0
    records = cleaned.rename(columns={"timestamp": "event_timestamp"}).to_dict(orient="records")
    for record in records:
        value = record.get("bytes_sent")
        record["bytes_sent"] = None if pd.isna(value) else int(value)
        record["source_line"] = int(record["source_line"])
        record["status_code"] = int(record["status_code"])
    statement = pg_insert(web_requests).values(records).on_conflict_do_nothing(
        index_elements=["record_id"]
    )
    with engine.begin() as connection:
        result = connection.execute(statement)
    return max(result.rowcount or 0, 0)


def run_pipeline(
    input_path: Path,
    output_dir: Path,
    source_id: str,
    dry_run: bool = False,
    database_url: str = "",
    chunk_size: int = 5000,
    limit: int | None = None,
) -> dict[str, int]:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input log not found: {input_path}\n"
            "Use the bundled sample at data/sample/nasa_access_sample.log, or download the Kaggle dataset."
        )
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    cleaned_path, rejected_path = prepare_output_files(output_dir)
    raw_count = clean_count = reject_count = inserted_count = 0
    engine: Engine | None = None

    if not dry_run:
        if not database_url:
            raise ValueError("DATABASE_URL is empty. Copy .env.example to .env and set your PostgreSQL URL, or use --dry-run.")
        engine = create_engine(database_url, pool_pre_ping=True)
        schema_path = ROOT / "sql" / "schema.sql"
        statements = [s.strip() for s in schema_path.read_text(encoding="utf-8").split(";") if s.strip()]
        try:
            with engine.begin() as connection:
                for statement in statements:
                    connection.exec_driver_sql(statement)

            for lines in iter_line_batches(input_path, chunk_size, limit):
                raw_count += len(lines)
                cleaned, rejected = prepare_batch(lines, source_id)
                cleaned.to_csv(cleaned_path, mode="a", index=False, header=False)
                rejected.to_csv(rejected_path, mode="a", index=False, header=False)
                clean_count += len(cleaned)
                reject_count += len(rejected)
                inserted_count += load_to_postgres(engine, cleaned)
                print(f"Processed {raw_count:,} lines | accepted {clean_count:,} | rejected {reject_count:,}", end="\r", flush=True)
        finally:
            engine.dispose()
    else:
        for lines in iter_line_batches(input_path, chunk_size, limit):
            raw_count += len(lines)
            cleaned, rejected = prepare_batch(lines, source_id)
            cleaned.to_csv(cleaned_path, mode="a", index=False, header=False)
            rejected.to_csv(rejected_path, mode="a", index=False, header=False)
            clean_count += len(cleaned)
            reject_count += len(rejected)
            print(f"Processed {raw_count:,} lines | accepted {clean_count:,} | rejected {reject_count:,}", end="\r", flush=True)

    if raw_count:
        print()
    print("\nLogLens NASA ETL summary")
    print("------------------------")
    print(f"Source:            {input_path}")
    print(f"Source ID:         {source_id}")
    print(f"Lines processed:   {raw_count:,}")
    print(f"Rows accepted:     {clean_count:,}")
    print(f"Rows rejected:     {reject_count:,}")
    print(f"Clean output:      {cleaned_path}")
    print(f"Rejected output:   {rejected_path}")
    if dry_run:
        print("Database:          skipped (dry run)")
    else:
        print(f"Rows newly loaded: {inserted_count:,}")
        print(f"Already present:   {clean_count - inserted_count:,}")
    return {"processed": raw_count, "accepted": clean_count, "rejected": reject_count, "inserted": inserted_count}


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="ETL for NASA HTTP access logs in Common Log Format.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to a raw .log/.txt access log")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT, help="Directory for cleaned/rejected CSV outputs")
    parser.add_argument("--source-id", default="nasa_sample", help="Stable identifier for this source file, used to make reruns idempotent")
    parser.add_argument("--dry-run", action="store_true", help="Parse and transform only; do not connect to PostgreSQL")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""), help="SQLAlchemy PostgreSQL connection URL")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Number of raw lines processed in each batch")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of source lines to process")
    args = parser.parse_args()

    try:
        run_pipeline(
            input_path=args.input.resolve(),
            output_dir=args.output_dir.resolve(),
            source_id=args.source_id,
            dry_run=args.dry_run,
            database_url=args.database_url,
            chunk_size=args.chunk_size,
            limit=args.limit,
        )
    except Exception as exc:
        parser.exit(1, f"ERROR: {exc}\n")


if __name__ == "__main__":
    main()
