from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, Optional

from ybtop import queries as Q
from ybtop.capabilities import detect_capabilities
from ybtop.db import connect
from ybtop.merge import merge_ash_groups, merge_pg_stat_statements
from ybtop.topology import YsqlNode, discover_ysql_nodes, dsn_for_node


def _fan_out_map(
    seed_dsn: str,
    nodes: list[YsqlNode],
    work: Callable[[Any, YsqlNode], list[dict[str, Any]]],
) -> tuple[list[list[dict[str, Any]]], Optional[str]]:
    """Run work(conn, node) for each node; collect per-node result lists."""
    errors: list[str] = []
    chunks: list[list[dict[str, Any]]] = []
    for node in nodes:
        dsn = dsn_for_node(seed_dsn, node)
        try:
            with connect(dsn) as conn:
                chunks.append(work(conn, node))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{node.host}:{node.port}: {exc}")
    if errors and not chunks:
        return [], "; ".join(errors)
    err = "; ".join(errors) if errors else None
    return chunks, err


def collect_cluster_statements(
    seed_dsn: str, limit: int
) -> tuple[Optional[list[dict[str, Any]]], Optional[str]]:
    try:
        nodes = discover_ysql_nodes(seed_dsn)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    caps = detect_capabilities(seed_dsn)

    def work(conn: Any, _node: YsqlNode) -> list[dict[str, Any]]:
        return Q.pg_stat_statements_raw(conn, caps)

    chunks, err = _fan_out_map(seed_dsn, nodes, work)
    if not chunks:
        return None, err or "no data"
    merged = merge_pg_stat_statements(
        chunks,
        include_docdb_per_call=False,
        include_rows_total=False,
    )[:limit]
    return merged, err


def collect_cluster_ash(
    seed_dsn: str,
    ash_start: datetime,
    ash_end: datetime,
    limit: int,
) -> tuple[Optional[list[dict[str, Any]]], Optional[str]]:
    try:
        nodes = discover_ysql_nodes(seed_dsn)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    caps = detect_capabilities(seed_dsn)

    def work(conn: Any, _node: YsqlNode) -> list[dict[str, Any]]:
        return Q.ash_aggregated(conn, ash_start, ash_end, caps, outer_limit=None)

    chunks, err = _fan_out_map(seed_dsn, nodes, work)
    if not chunks:
        return None, err or "no data"
    merged = merge_ash_groups(chunks, include_namespace_objname=False)[:limit]
    return merged, err


def collect_cluster_local_tablets(
    seed_dsn: str,
    limit: int,
) -> tuple[Optional[list[dict[str, Any]]], Optional[str]]:
    try:
        nodes = discover_ysql_nodes(seed_dsn)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

    rows_out: list[dict[str, Any]] = []
    errors: list[str] = []

    for node in nodes:
        dsn = dsn_for_node(seed_dsn, node)
        try:
            with connect(dsn) as conn:
                for r in Q.yb_local_tablets_rows(conn):
                    rr = dict(r)
                    rr["node_host"] = node.host
                    rr["node_port"] = node.port
                    rows_out.append(rr)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{node.host}:{node.port}: {exc}")

    rows_out.sort(key=lambda r: (r.get("namespace_name") or "", r.get("table_name") or "", r.get("tablet_id") or ""))
    if limit and limit > 0:
        rows_out = rows_out[:limit]
    err = "; ".join(errors) if errors else None
    if not rows_out and err:
        return None, err
    return rows_out, err


def reset_pg_stat_statements_cluster(seed_dsn: str) -> list[dict[str, Any]]:
    """Call pg_stat_statements_reset() on each YSQL node discovered from the seed connection."""
    reset_sql = "SELECT pg_stat_statements_reset();"
    out: list[dict[str, Any]] = []
    try:
        nodes = discover_ysql_nodes(seed_dsn)
    except Exception as exc:  # noqa: BLE001
        return [
            {
                "node": "(yb_servers discovery)",
                "status": "failed",
                "detail": str(exc),
            }
        ]
    for node in nodes:
        label = f"{node.host}:{node.port}"
        dsn = dsn_for_node(seed_dsn, node)
        try:
            with connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(reset_sql)
            out.append({"node": label, "status": "ok", "detail": ""})
        except Exception as exc:  # noqa: BLE001
            out.append({"node": label, "status": "failed", "detail": str(exc)})
    return out
