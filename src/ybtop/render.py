from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, Tuple, Union

import psycopg.conninfo
from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def keyed_table(
    title: str,
    rows: list[dict[str, str]],
    column_keys: list[str],
) -> Table:
    """Render fixed-column table (column order taken from *column_keys*)."""
    table = Table(
        title=title,
        box=box.SIMPLE_HEAD,
        show_lines=False,
        header_style="bold",
        expand=True,
    )
    if not rows and not column_keys:
        table.add_column("(no rows)")
        table.add_row("—")
        return table
    for k in column_keys:
        kl = k.lower()
        w = 52 if any(x in kl for x in ("query", "location", "event", "wait")) else 12
        if "host" in kl or (("node" in kl) and ("session" not in kl)):
            w = 28
        table.add_column(k, overflow="ellipsis", max_width=w)
    if not rows:
        table.add_row(*(["—"] + [""] * (len(column_keys) - 1) if len(column_keys) else ["—"]))
        return table
    for r in rows:
        table.add_row(*[str(r.get(c, "")) for c in column_keys])
    return table


def table_from_rows(title: str, rows: list[dict[str, Any]]) -> Table:
    table = Table(
        title=title,
        box=box.SIMPLE_HEAD,
        show_lines=False,
        header_style="bold",
        expand=True,
    )
    if not rows:
        table.add_column("(no rows)")
        table.add_row("—")
        return table
    for key in rows[0]:
        table.add_column(str(key), overflow="ellipsis", max_width=48)
    for row in rows:
        table.add_row(*["" if v is None else str(v) for v in row.values()])
    return table


def dashboard_panels(
    *,
    statements: Tuple[Optional[list[dict[str, Any]]], Optional[str]],
    ash: Tuple[Optional[list[dict[str, Any]]], Optional[str]],
) -> Group:
    panels: list[Panel] = []

    def add(name: str, rows_err: Tuple[Optional[list[dict[str, Any]]], Optional[str]]) -> None:
        rows, err = rows_err
        if err and not rows:
            body: Union[Table, Text] = Text(err, style="red")
            subtitle = "error"
        elif err and rows:
            body = table_from_rows("", rows or [])
            subtitle = f"{len(rows or [])} rows · partial: {err}"
        else:
            body = table_from_rows("", rows or [])
            subtitle = f"{len(rows or [])} rows"
        panels.append(Panel(body, title=name, subtitle=subtitle, border_style="cyan"))

    add("pg_stat_statements", statements)
    add("yb_active_session_history", ash)
    return Group(*panels)


def format_seed_line(seed_dsn: str) -> str:
    """One-line description of the YSQL seed endpoint for the Rich watch strip."""
    try:
        d = psycopg.conninfo.conninfo_to_dict(seed_dsn)
    except Exception:  # noqa: BLE001
        return "Seed: (unparseable DSN)"
    host = d.get("host") or "?"
    port = d.get("port") or "5433"
    return f"Seed: {host}:{port}"


def _ash_interval_seconds_utc(doc: dict[str, Any]) -> float:
    """Window length in seconds (ash_window end − start), same as the JSON snapshot; min 0."""
    w = doc.get("ash_window") or {}
    s, e = w.get("start_utc"), w.get("end_utc")
    if s is None or e is None:
        return 0.0
    try:
        s1, s2 = str(s).strip(), str(e).strip()
        if s1.endswith("Z"):
            s1 = s1[:-1] + "+00:00"
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        t1 = datetime.fromisoformat(s1)
        t2 = datetime.fromisoformat(s2)
        if t1.tzinfo is None:
            t1 = t1.replace(tzinfo=timezone.utc)
        if t2.tzinfo is None:
            t2 = t2.replace(tzinfo=timezone.utc)
        d = (t2 - t1).total_seconds()
        return float(d) if d > 0 else 0.0
    except (TypeError, ValueError, OSError):
        return 0.0


def crz_ash_summary_rows(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """
    One row per cloud:region:zone from node_topology, with:
    - nodes: count of YSQL nodes in that placement
    - active sessions/sec: sum(samples) in window / ash interval seconds
    - load %: 100 * row samples / all rows' total samples
    """
    interval_sec = _ash_interval_seconds_utc(doc)
    rate_denom = interval_sec if interval_sec > 0 else 0.0

    topo: dict[str, Any] = doc.get("node_topology") or {}
    ash_pn: dict[str, list[dict[str, Any]]] = (
        (doc.get("yb_active_session_history") or {}).get("per_node") or {}
    )
    acc: dict[tuple[str, str, str], dict[str, Any]] = {}
    for nid, t in topo.items():
        cloud = (t or {}).get("cloud") or ""
        region = (t or {}).get("region") or ""
        zone = (t or {}).get("zone") or ""
        k = (cloud, region, zone)
        if k not in acc:
            acc[k] = {
                "cloud": cloud,
                "region": region,
                "zone": zone,
                "nodes": 0,
                "samples": 0,
            }
        acc[k]["nodes"] += 1
        for r in ash_pn.get(nid) or []:
            acc[k]["samples"] += int(r.get("samples") or 0)

    raw = list(acc.values())
    total_samples = sum(int(x["samples"]) for x in raw)
    if rate_denom > 0:
        raw.sort(
            key=lambda x: (
                x["samples"] / rate_denom,
                str(x["cloud"]),
                str(x["region"]),
                str(x["zone"]),
            ),
            reverse=True,
        )
    else:
        raw.sort(
            key=lambda x: (str(x["cloud"]), str(x["region"]), str(x["zone"])),
        )
    out: list[dict[str, Any]] = []
    for r in raw:
        s = int(r["samples"])
        if rate_denom > 0:
            rate = round(s / rate_denom, 4)
        else:
            rate = 0.0
        load_pct = (100.0 * s / total_samples) if total_samples > 0 else 0.0
        out.append(
            {
                "cloud": r["cloud"],
                "region": r["region"],
                "zone": r["zone"],
                "nodes": r["nodes"],
                "active sessions/sec": rate,
                "load %": f"{load_pct:.2f}%",
            }
        )
    return out


def _format_ash_sessions_per_sec(n: float) -> str:
    """Match web `formatAshSessionsPerSec` style (variable precision)."""
    if n == 0:
        return "0"
    if n >= 100:
        return f"{n:.2f}"
    if n >= 10:
        return f"{n:.3f}"
    if n >= 1:
        return f"{n:.4f}"
    return f"{n:.5f}"


def live_top5_nodes_by_active_session_sec(doc: dict[str, Any]) -> Union[Table, Text]:
    """
    Top 5 nodes by sum(ASH samples) / ash_window length (same rate as the cloud/region/zone panel).
    """
    topo: dict[str, Any] = doc.get("node_topology") or {}
    if not topo:
        return Text(
            "Top 5 — nodes (by active sessions/sec): (no node_topology in snapshot)",
            style="dim",
        )

    ash_pn: dict[str, list[dict[str, Any]]] = (
        (doc.get("yb_active_session_history") or {}).get("per_node") or {}
    )
    interval_sec = _ash_interval_seconds_utc(doc)
    rate_denom = interval_sec if interval_sec > 0 else 0.0

    combined: list[dict[str, Any]] = []
    for nid, t in topo.items():
        rows = ash_pn.get(nid) or []
        samples = sum(int(r.get("samples") or 0) for r in rows)
        host = (t or {}).get("host")
        port = (t or {}).get("port")
        host_s = (str(host).strip() if host is not None and str(host).strip() else None)
        if host_s and port is not None and str(port).strip() != "":
            hp = f"{host_s}:{port}"
        elif host_s:
            hp = host_s
        else:
            hp = str(nid)
        rate = (float(samples) / float(rate_denom)) if rate_denom > 0 else 0.0
        combined.append(
            {
                "host:port": hp,
                "cloud": (t or {}).get("cloud") or "",
                "region": (t or {}).get("region") or "",
                "zone": (t or {}).get("zone") or "",
                "samples": samples,
                "active sessions/sec": rate,
            }
        )

    total_samples = sum(int(c["samples"]) for c in combined)
    for c in combined:
        sm = int(c["samples"])
        lp = (100.0 * sm / total_samples) if total_samples > 0 else 0.0
        c["load %"] = f"{lp:.2f}%"

    # DESC by Active Sessions/Sec (numeric), then by host:port for a stable order.
    combined.sort(
        key=lambda r: (float(r["active sessions/sec"]), r["host:port"], str(r.get("cloud"))),
        reverse=True,
    )
    top5 = combined[:5]

    # Same info order as the cloud/region/zone panel (placement, then node address, then rates).
    col_keys = [
        "cloud",
        "region",
        "zone",
        "host:port",
        "active sessions/sec",
        "load %",
    ]
    rows_out: list[dict[str, str]] = []
    for r in top5:
        rate = float(r["active sessions/sec"])
        rows_out.append(
            {
                "cloud": str(r["cloud"]),
                "region": str(r["region"]),
                "zone": str(r["zone"]),
                "host:port": str(r["host:port"]),
                "active sessions/sec": _format_ash_sessions_per_sec(rate),
                "load %": str(r["load %"]),
            }
        )

    return keyed_table("Top 5 — nodes (by active sessions/sec)", rows_out, col_keys)
