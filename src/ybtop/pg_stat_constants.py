"""Optional YugabyteDB DocDB columns on pg_stat_statements (newer releases)."""

from __future__ import annotations

# Presence is gated on docdb_seeks; when present, all are selected together.
PG_STAT_DOCDB_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "docdb_seeks",
    "docdb_nexts",
    "docdb_prevs",
    "docdb_read_rpcs",
    "docdb_write_rpcs",
    "catalog_wait_time",
    "docdb_read_operations",
    "docdb_write_operations",
    "docdb_rows_scanned",
    "docdb_rows_returned",
    "docdb_wait_time",
    "conflict_retries",
    "read_restart_retries",
    "total_retries",
    "docdb_obsolete_rows_scanned",
    "docdb_read_time",
    "docdb_write_time",
)
