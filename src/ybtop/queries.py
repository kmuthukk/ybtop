from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import psycopg

from ybtop.capabilities import Capabilities
from ybtop.db import fetch_all
from ybtop.pg_stat_constants import PG_STAT_DOCDB_OPTIONAL_COLUMNS


def _pg_stat_time_select(caps: Capabilities) -> str:
    if caps.pg_stat_use_exec_time:
        return """s.total_exec_time::float8 AS total_exec_time,
        s.mean_exec_time::float8 AS mean_exec_time"""
    return """s.total_time::float8 AS total_exec_time,
        s.mean_time::float8 AS mean_exec_time"""


def _pg_stat_order_by_time(caps: Capabilities) -> str:
    return "s.total_exec_time DESC" if caps.pg_stat_use_exec_time else "s.total_time DESC"


def _ash_from_clause(caps: Capabilities) -> str:
    if caps.yb_ash_range_function:
        return "FROM yb_active_session_history(%(t1)s::timestamptz, %(t2)s::timestamptz) ash"
    return (
        "FROM yb_active_session_history ash\n"
        "        WHERE ash.sample_time >= %(t1)s::timestamptz AND ash.sample_time < %(t2)s::timestamptz"
    )


def _pg_stat_rows_and_docdb(caps: Capabilities) -> str:
    parts = ["s.rows::float8 AS rows"]
    if caps.pg_stat_docdb_metrics:
        for c in PG_STAT_DOCDB_OPTIONAL_COLUMNS:
            parts.append(f"s.{c}::float8 AS {c}")
    return ",\n        ".join(parts)


def pg_stat_statements_raw(conn: psycopg.Connection, caps: Capabilities) -> list[dict[str, Any]]:
    time_cols = _pg_stat_time_select(caps)
    extra = _pg_stat_rows_and_docdb(caps)
    sql = f"""
    SELECT
        s.queryid::text AS queryid,
        s.query,
        s.calls::bigint AS calls,
        {time_cols},
        {extra},
        NULLIF(BTRIM(db.datname::text), '') AS dbname
    FROM pg_stat_statements s
    LEFT JOIN pg_database db ON db.oid = s.dbid
    """
    return fetch_all(conn, sql)


def pg_stat_statements_top(conn: psycopg.Connection, limit: int, caps: Capabilities) -> list[dict[str, Any]]:
    time_cols = _pg_stat_time_select(caps)
    extra = _pg_stat_rows_and_docdb(caps)
    order_by = _pg_stat_order_by_time(caps)
    sql = f"""
    SELECT
        s.queryid::text AS queryid,
        s.query::text AS query,
        s.calls::bigint AS calls,
        {time_cols},
        {extra},
        NULLIF(BTRIM(db.datname::text), '') AS dbname
    FROM pg_stat_statements s
    LEFT JOIN pg_database db ON db.oid = s.dbid
    ORDER BY {order_by}
    LIMIT %(limit)s;
    """
    return fetch_all(conn, sql, {"limit": limit})


def ash_aggregated(
    conn: psycopg.Connection,
    ash_start: datetime,
    ash_end: datetime,
    caps: Capabilities,
    outer_limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    lim_clause = ""
    params: dict[str, Any] = {"t1": ash_start, "t2": ash_end}
    if outer_limit is not None:
        lim_clause = "\n    LIMIT %(outer_limit)s"
        params["outer_limit"] = int(outer_limit)
    ash_from = _ash_from_clause(caps)
    sql = f"""
    SELECT
        s.query_id::text AS query_id,
        s.wait_event_component,
        LEFT(s.wait_event::text, 48) AS wait_event,
        s.wait_event_type,
        s.wait_event_aux,
        s.ysql_dbid,
        s.samples,
        NULLIF(BTRIM(COALESCE(lt.namespace_name::text, d.datname::text)), '') AS namespace_name,
        NULLIF(BTRIM(lt.table_name::text), '') AS object_name,
        lt.table_id::text AS table_id
    FROM (
        SELECT
            query_id,
            wait_event_component,
            wait_event,
            wait_event_type,
            wait_event_aux,
            ysql_dbid,
            COUNT(*)::bigint AS samples
        {ash_from}
        GROUP BY
            query_id,
            wait_event_component,
            wait_event,
            wait_event_type,
            wait_event_aux,
            ysql_dbid
    ) s
    LEFT JOIN pg_database d ON d.oid = s.ysql_dbid
    LEFT JOIN LATERAL (
        SELECT
            lt1.namespace_name::text AS namespace_name,
            lt1.table_name::text AS table_name,
            lt1.table_id::text AS table_id
        FROM yb_local_tablets lt1
        WHERE s.wait_event_aux IS NOT NULL
          AND s.wait_event_aux = SUBSTRING(lt1.tablet_id::text, 1, 15)
        LIMIT 1
    ) lt ON TRUE
    ORDER BY s.samples DESC{lim_clause};
    """
    return fetch_all(conn, sql, params)


def yb_local_tablets_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    sql = """
    SELECT
        tablet_id::text AS tablet_id,
        table_type::text AS table_type,
        table_id::text AS table_id,
        namespace_name::text AS namespace_name,
        table_name::text AS table_name,
        partition_key_start::text AS partition_key_start,
        partition_key_end::text AS partition_key_end,
        state::text AS state
    FROM yb_local_tablets
    WHERE state != 'TABLET_DATA_TOMBSTONED';
    """
    return fetch_all(conn, sql)
