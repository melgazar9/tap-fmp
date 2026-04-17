"""Pre-flight schema audit for the snake_case converter fix.

Run BEFORE deploying the helpers.py converter change to production. Connects
read-only to Postgres, compares actual table columns against the canonical
column names the fixed tap will produce, and emits:

  - A human-readable report of orphan columns per table
  - Migration SQL (DROP / RENAME / UPDATE) for admin review

The script never executes DDL. It writes SQL to a file for you to inspect.

Usage
-----
    # 1. Generate the singer catalog (ground truth for post-fix column names)
    meltano invoke tap-fmp --discover > /tmp/tap-fmp-catalog.json

    # 2. Run the audit against read-only Postgres
    export READ_ONLY_POSTGRES_HOST=... READ_ONLY_POSTGRES_PORT=5432
    export READ_ONLY_POSTGRES_USER=... READ_ONLY_POSTGRES_PASSWORD=...
    export READ_ONLY_POSTGRES_DATABASE=financial_elt

    python scripts/schema_migration_audit.py \\
        --catalog /tmp/tap-fmp-catalog.json \\
        --schema tap_fmp_production \\
        --out-dir ./migration-output

The --out-dir will contain: report.md, migration.sql, postgres_snapshot.json.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql


def connect_readonly() -> psycopg2.extensions.connection:
    required = [
        "READ_ONLY_POSTGRES_HOST",
        "READ_ONLY_POSTGRES_PORT",
        "READ_ONLY_POSTGRES_USER",
        "READ_ONLY_POSTGRES_PASSWORD",
        "READ_ONLY_POSTGRES_DATABASE",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)}")

    conn = psycopg2.connect(
        host=os.environ["READ_ONLY_POSTGRES_HOST"],
        port=os.environ["READ_ONLY_POSTGRES_PORT"],
        user=os.environ["READ_ONLY_POSTGRES_USER"],
        password=os.environ["READ_ONLY_POSTGRES_PASSWORD"],
        dbname=os.environ["READ_ONLY_POSTGRES_DATABASE"],
    )
    conn.set_session(readonly=True)
    return conn


def snapshot_postgres(conn, schema: str) -> dict[str, list[dict[str, Any]]]:
    """Return {table_name: [{column_name, data_type, is_nullable}]} for schema."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
            """,
            (schema,),
        )
        out: dict[str, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            out.setdefault(row["table_name"], []).append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"],
                }
            )
    return out


def count_non_null(conn, schema: str, table: str, column: str) -> int:
    """Run COUNT(*) FILTER (WHERE col IS NOT NULL). Safely quote identifiers."""
    query = sql.SQL(
        "SELECT COUNT(*) FILTER (WHERE {col} IS NOT NULL) FROM {schema}.{table}"
    ).format(
        col=sql.Identifier(column),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        (n,) = cur.fetchone()
    return n


def load_catalog_streams(path: Path) -> dict[str, set[str]]:
    """Return {stream_name: {column_name, ...}} from singer catalog JSON."""
    data = json.loads(path.read_text())
    out: dict[str, set[str]] = {}
    for stream in data.get("streams", []):
        name = stream.get("stream") or stream.get("tap_stream_id")
        if not name:
            continue
        props = stream.get("schema", {}).get("properties", {})
        out[name] = set(props.keys())
    return out


# Columns that target-postgres adds automatically and should not be flagged
# as orphans just because they don't appear in the tap schema.
_SDC_PREFIXES = ("_sdc_",)
_METADATA_COLS = {
    "_sdc_extracted_at",
    "_sdc_received_at",
    "_sdc_batched_at",
    "_sdc_deleted_at",
    "_sdc_sequence",
    "_sdc_table_version",
    "_sdc_sync_started_at",
}


def is_metadata_column(col: str) -> bool:
    return col in _METADATA_COLS or any(col.startswith(p) for p in _SDC_PREFIXES)


def audit(
    pg_snapshot: dict[str, list[dict[str, Any]]],
    catalog: dict[str, set[str]],
    conn,
    schema: str,
) -> list[dict[str, Any]]:
    """Diff per table. Return list of findings."""
    findings: list[dict[str, Any]] = []
    for table, columns in pg_snapshot.items():
        expected = catalog.get(table)
        pg_cols = {c["column_name"] for c in columns}

        if expected is None:
            findings.append(
                {
                    "table": table,
                    "status": "no_catalog_entry",
                    "message": f"Table exists in Postgres but stream '{table}' not found in catalog.",
                    "pg_columns": sorted(pg_cols),
                }
            )
            continue

        orphans = sorted(c for c in pg_cols - expected if not is_metadata_column(c))
        missing = sorted(expected - pg_cols)

        orphan_details = []
        for col in orphans:
            n = count_non_null(conn, schema, table, col)
            orphan_details.append({"column": col, "non_null_count": n})

        findings.append(
            {
                "table": table,
                "status": "ok" if not orphans and not missing else "drift",
                "orphans": orphan_details,
                "missing_in_pg": missing,
            }
        )

    catalog_only = sorted(set(catalog) - set(pg_snapshot))
    if catalog_only:
        findings.append(
            {
                "table": "__catalog_only_streams__",
                "status": "info",
                "streams_without_tables": catalog_only,
            }
        )
    return findings


def render_report(findings: list[dict[str, Any]]) -> str:
    lines = ["# Schema migration audit", ""]
    ok_count = sum(1 for f in findings if f.get("status") == "ok")
    drift_count = sum(1 for f in findings if f.get("status") == "drift")
    lines.append(f"- Clean tables: **{ok_count}**")
    lines.append(f"- Tables with drift: **{drift_count}**")
    lines.append("")

    for f in findings:
        if f.get("status") == "ok":
            continue
        lines.append(f"## `{f['table']}`")
        lines.append("")
        if f.get("status") == "no_catalog_entry":
            lines.append(f"- {f['message']}")
            lines.append("")
            continue
        if f.get("status") == "info":
            lines.append(
                "Streams defined in catalog but no Postgres table yet "
                "(will be created on next run):"
            )
            for s in f["streams_without_tables"]:
                lines.append(f"  - `{s}`")
            lines.append("")
            continue

        orphans = f.get("orphans", [])
        empty = [o for o in orphans if o["non_null_count"] == 0]
        populated = [o for o in orphans if o["non_null_count"] > 0]

        if empty:
            lines.append("### Safe to drop (0 non-null rows)")
            for o in empty:
                lines.append(f"  - `{o['column']}`")
            lines.append("")
        if populated:
            lines.append("### Needs migration (has data)")
            for o in populated:
                lines.append(
                    f"  - `{o['column']}` — {o['non_null_count']:,} non-null rows"
                )
            lines.append("")
        if f.get("missing_in_pg"):
            lines.append(
                "### Expected columns not yet in Postgres (target-postgres "
                "will auto-create on next run)"
            )
            for c in f["missing_in_pg"]:
                lines.append(f"  - `{c}`")
            lines.append("")
    return "\n".join(lines)


def render_sql(findings: list[dict[str, Any]], schema: str) -> str:
    def q(ident: str) -> str:
        # Quote-safe identifier for embedded SQL. Embeds any double-quote by doubling.
        return '"' + ident.replace('"', '""') + '"'

    lines = [
        "-- Schema migration SQL (review before running)",
        "-- Generated by scripts/schema_migration_audit.py",
        "-- Apply in a transaction with an admin user, NOT read-only.",
        "",
        "BEGIN;",
        "",
    ]
    for f in findings:
        if f.get("status") != "drift":
            continue
        table = f["table"]
        orphans = f.get("orphans", [])
        empty = [o["column"] for o in orphans if o["non_null_count"] == 0]
        populated = [o for o in orphans if o["non_null_count"] > 0]

        if empty:
            lines.append(f"-- {table}: drop empty orphan columns")
            for col in empty:
                lines.append(
                    f"ALTER TABLE {q(schema)}.{q(table)} DROP COLUMN {q(col)};"
                )
            lines.append("")
        if populated:
            lines.append(
                f"-- {table}: populated orphans — REVIEW MANUALLY before running"
            )
            lines.append("-- For each column below, pick ONE path and uncomment:")
            lines.append("--   Path A (canonical column does not yet exist):")
            lines.append("--     ALTER TABLE ... RENAME COLUMN <old> TO <canonical>;")
            lines.append("--   Path B (canonical column exists, may have data):")
            lines.append(
                "--     UPDATE ... SET <canonical> = <old> WHERE <canonical> IS NULL;"
            )
            lines.append("--     ALTER TABLE ... DROP COLUMN <old>;")
            for o in populated:
                col = o["column"]
                n = o["non_null_count"]
                lines.append(f"-- {col} ({n:,} non-null rows)")
                lines.append(
                    f"-- ALTER TABLE {q(schema)}.{q(table)} "
                    f"RENAME COLUMN {q(col)} TO {q('<canonical>')};"
                )
                lines.append(
                    f"-- UPDATE {q(schema)}.{q(table)} SET {q('<canonical>')} = {q(col)} "
                    f"WHERE {q('<canonical>')} IS NULL;"
                )
                lines.append(
                    f"-- ALTER TABLE {q(schema)}.{q(table)} DROP COLUMN {q(col)};"
                )
            lines.append("")
    lines.append("-- ROLLBACK;  -- uncomment to abort")
    lines.append("COMMIT;")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--catalog",
        required=True,
        type=Path,
        help="Path to singer catalog JSON (from `meltano invoke tap-fmp --discover`)",
    )
    ap.add_argument(
        "--schema", required=True, help="Postgres schema name (e.g. tap_fmp_production)"
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        type=Path,
        help="Directory to write report.md, migration.sql, postgres_snapshot.json",
    )
    args = ap.parse_args()

    if not args.catalog.exists():
        sys.exit(f"Catalog not found: {args.catalog}")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading catalog from {args.catalog}...")
    catalog = load_catalog_streams(args.catalog)
    print(f"  {len(catalog)} streams in catalog")

    print("Connecting to Postgres (read-only)...")
    conn = connect_readonly()
    try:
        print(f"Snapshotting schema '{args.schema}'...")
        snapshot = snapshot_postgres(conn, args.schema)
        print(f"  {len(snapshot)} tables")

        snapshot_path = args.out_dir / "postgres_snapshot.json"
        snapshot_path.write_text(json.dumps(snapshot, indent=2))
        print(f"  wrote {snapshot_path}")

        print(
            "Auditing (this counts non-null values per orphan column; may take a minute)..."
        )
        findings = audit(snapshot, catalog, conn, args.schema)
    finally:
        conn.close()

    report_path = args.out_dir / "report.md"
    report_path.write_text(render_report(findings))
    print(f"  wrote {report_path}")

    sql_path = args.out_dir / "migration.sql"
    sql_path.write_text(render_sql(findings, args.schema))
    print(f"  wrote {sql_path}")

    drift = sum(1 for f in findings if f.get("status") == "drift")
    if drift:
        print(
            f"\nDrift detected on {drift} table(s). Review {report_path} before running {sql_path}."
        )
    else:
        print("\nNo drift. Postgres and catalog agree.")


if __name__ == "__main__":
    main()
