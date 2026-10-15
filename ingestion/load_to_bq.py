#!/usr/bin/env python3
"""
Load generated batches into BigQuery (dataset `raw_saas`).

Two modes, both idempotent:

  --backfill-through 2026-08-31   Rebuild the append-only raw tables from every batch up to and
                                  including that date (WRITE_TRUNCATE), and load that day's
                                  customers dump.
  --day 2026-09-01                Load one daily batch. Deletes any rows already loaded for that
                                  _batch_date first, so re-running a day never duplicates it.
                                  Replaces raw_saas.customers with that day's dump.

Raw tables are STRING columns plus `_batch_date` (DATE, the partition column). Typing and
cleaning happen in dbt staging, as in project 1.

  raw_saas.subscription_events   append-only, partitioned by _batch_date
  raw_saas.invoices              append-only
  raw_saas.payment_attempts      append-only
  raw_saas.product_events        append-only
  raw_saas.customers             current state, replaced on every load (the dbt snapshot source)

Examples:
  python load_to_bq.py --data generated/data --project my-gcp-project --backfill-through 2026-08-31
  python load_to_bq.py --data generated/data --project my-gcp-project --day 2026-09-01
  python load_to_bq.py --data generated/data --backfill-through 2026-08-31 --dry-run   # no BigQuery needed
"""
from __future__ import annotations

import argparse
import csv
import tempfile
from datetime import date
from pathlib import Path

APPEND_TABLES = ["subscription_events", "invoices", "payment_attempts", "product_events"]
SNAPSHOT_TABLE = "customers"
DATASET = "raw_saas"
LOCATION = "asia-northeast3"   # same region as project 1; change if your dataset lives elsewhere


def batch_dirs(data: Path, through: date | None = None, only: date | None = None) -> list[Path]:
    dirs = []
    for p in sorted(data.glob("batch_date=*")):
        d = date.fromisoformat(p.name.split("=", 1)[1])
        if only is not None and d != only:
            continue
        if through is not None and d > through:
            continue
        dirs.append(p)
    return dirs


def header(path: Path) -> list[str]:
    with open(path, newline="") as f:
        return next(csv.reader(f))


def concat(dirs: list[Path], table: str, dest: Path) -> tuple[int, list[str]]:
    """Concatenate one table's CSVs into a single file with one header. Returns (rows, columns)."""
    cols, rows = None, 0
    with open(dest, "w", newline="") as out:
        for d in dirs:
            src = d / f"{table}.csv"
            with open(src, newline="") as f:
                lines = f.readlines()
            if cols is None:
                cols = lines[0].rstrip("\n").split(",")
                out.write(lines[0])
            elif lines[0].rstrip("\n").split(",") != cols:
                raise SystemExit(f"header mismatch in {src}")
            out.writelines(lines[1:])
            rows += len(lines) - 1
    return rows, cols or []


def schema_for(cols: list[str], bigquery):
    return [
        bigquery.SchemaField(c, "DATE" if c == "_batch_date" else "STRING") for c in cols
    ]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="path to generated/data")
    ap.add_argument("--project", help="GCP project id (defaults to the gcloud default)")
    ap.add_argument("--backfill-through", help="YYYY-MM-DD")
    ap.add_argument("--day", help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true", help="count rows, touch nothing")
    a = ap.parse_args()
    if bool(a.backfill_through) == bool(a.day):
        raise SystemExit("pass exactly one of --backfill-through or --day")

    data = Path(a.data)
    if a.backfill_through:
        through = date.fromisoformat(a.backfill_through)
        dirs = batch_dirs(data, through=through)
        customers_dir = dirs[-1:]
    else:
        day = date.fromisoformat(a.day)
        dirs = batch_dirs(data, only=day)
        customers_dir = dirs
    if not dirs:
        raise SystemExit("no batches found for that date range")

    bigquery = None
    client = None
    if not a.dry_run:
        from google.cloud import bigquery as bq  # imported late so --dry-run needs no cloud libraries
        bigquery = bq
        client = bq.Client(project=a.project)
        ds = bq.Dataset(f"{client.project}.{DATASET}")
        ds.location = LOCATION
        client.create_dataset(ds, exists_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        for table in APPEND_TABLES + [SNAPSHOT_TABLE]:
            use = customers_dir if table == SNAPSHOT_TABLE else dirs
            dest = Path(tmp) / f"{table}.csv"
            n, cols = concat(use, table, dest)
            print(f"{table:22s} {len(use):4d} batches {n:9,d} rows")
            if a.dry_run:
                continue
            ref = f"{client.project}.{DATASET}.{table}"
            replace = table == SNAPSHOT_TABLE or bool(a.backfill_through)
            if not replace:
                # Idempotent daily load: remove this batch if it was loaded before.
                try:
                    client.query(f"delete from `{ref}` where _batch_date = @d", job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("d", "DATE", day)])).result()
                except Exception as e:  # table does not exist yet on the very first run
                    if "Not found" not in str(e):
                        raise
            cfg = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                schema=schema_for(cols, bigquery),
                write_disposition="WRITE_TRUNCATE" if replace else "WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(field="_batch_date") if table != SNAPSHOT_TABLE else None,
            )
            with open(dest, "rb") as f:
                client.load_table_from_file(f, ref, job_config=cfg).result()
            print(f"   -> loaded {ref}")


if __name__ == "__main__":
    main()