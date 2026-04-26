from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.text import Text

from ybtop import __version__
from ybtop import collect
from ybtop.config import (
    DEFAULT_ASH_WINDOW_MINUTES,
    DEFAULT_REFRESH_INTERVAL_SEC,
    DEFAULT_SERVE_HOST,
    DEFAULT_SERVE_PORT,
    DEFAULT_SNAPSHOT_OUTPUT_DIR,
    DEFAULT_SNAPSHOT_RETENTION_HOURS,
    DEFAULT_YSQL_DBNAME,
    DEFAULT_YSQL_PORT,
    DEFAULT_YSQL_USER,
    SNAPSHOT_ASH_PER_NODE,
    SNAPSHOT_STATEMENTS_PER_NODE,
    Settings,
    load_dsn_from_env_or_none,
    resolve_ash_range,
    resolve_seed_dsn,
)
from ybtop.render import crz_ash_summary_rows, format_seed_line, table_from_rows
from ybtop.snapshot_write import (
    build_snapshot_document,
    gc_snapshots_and_manifest,
    write_snapshot_and_update_manifest,
)


def _parse_ts(raw: str) -> datetime:
    """Parse ISO-8601 timestamps; assume UTC if no offset is given."""
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def run_watch(settings: Settings) -> None:
    console = Console()
    out_dir = Path(settings.snapshot_output_dir)
    iteration = 0
    last_checkpoint: Optional[str] = None
    with Live(console=console, refresh_per_second=4) as live:
        while True:
            iteration += 1
            started = time.monotonic()
            doc: Any = None
            try:
                ash_start, ash_end = resolve_ash_range(settings)
                doc = build_snapshot_document(
                    seed_dsn=settings.seed_dsn,
                    ash_start=ash_start,
                    ash_end=ash_end,
                    statements_per_node=settings.snapshot_statements_per_node,
                    ash_per_node=settings.snapshot_ash_per_node,
                )
                snap_path = write_snapshot_and_update_manifest(output_dir=out_dir, document=doc)
                last_checkpoint = snap_path.name
                gc_snapshots_and_manifest(
                    output_dir=out_dir,
                    retention_hours=settings.snapshot_retention_hours,
                )
            except Exception as exc:  # noqa: BLE001
                doc = None
                console.print(f"[yellow]snapshot write failed:[/yellow] {exc}")
            utc_now = datetime.now(timezone.utc)
            iter_line = Text(f"Iteration {iteration}")
            ck_line = Text(
                f"Checkpoint: {last_checkpoint or '—'}",
                style="dim",
            )
            utc_line = Text(utc_now.strftime("UTC %Y-%m-%d %H:%M:%S"), style="bold")
            seed_line = Text(format_seed_line(settings.seed_dsn), style="dim")
            parts: list[Any] = [iter_line, ck_line, utc_line, seed_line]
            if doc is not None and isinstance(doc, dict):
                crz = crz_ash_summary_rows(doc)
                if crz:
                    parts.append(
                        table_from_rows(
                            "cloud · region · zone  (nodes, active sessions/s, load %)",
                            crz,
                        )
                    )
                else:
                    parts.append(
                        Text("Placement / ASH summary: (no rows)", style="dim"),
                    )
            else:
                parts.append(
                    Text(
                        "Placement / ASH summary: (unavailable; snapshot not written this tick)",
                        style="dim",
                    ),
                )
            live.update(Group(*parts))
            elapsed = time.monotonic() - started
            sleep_for = max(0.1, settings.refresh_interval - elapsed)
            time.sleep(sleep_for)


def run_reset_pg_stat_statements(settings: Settings) -> None:
    console = Console()
    rows = collect.reset_pg_stat_statements_cluster(settings.seed_dsn)
    console.print(
        table_from_rows(
            "pg_stat_statements_reset() per node",
            rows,
        )
    )
    failed = [r for r in rows if r.get("status") != "ok"]
    if failed:
        raise SystemExit(1)


def _connection_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("connection (any one YSQL node in the universe)")
    g.add_argument("--dsn", help="Libpq URL for one node (overrides env).")
    g.add_argument("--host", help="Seed node host/IP (alternative to --dsn).")
    g.add_argument(
        "--port",
        type=int,
        default=DEFAULT_YSQL_PORT,
        help="YSQL port when using --host.",
    )
    g.add_argument("--user", default=DEFAULT_YSQL_USER, help="User when using --host.")
    g.add_argument(
        "--password",
        default=None,
        help="Password when using --host (default: YBTOP_PASSWORD env if set).",
    )
    g.add_argument("--dbname", default=DEFAULT_YSQL_DBNAME, help="Database when using --host.")


def _ash_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("ASH time range")
    g.add_argument(
        "--ash-window-minutes",
        type=int,
        default=DEFAULT_ASH_WINDOW_MINUTES,
        metavar="MINS",
        help=(
            "ASH rolling window length in minutes when --ash-start/--ash-end are omitted "
            "(each watch refresh ends at UTC now)."
        ),
    )
    g.add_argument(
        "--ash-start",
        metavar="ISO8601",
        help="Window start (timestamptz). If set, --ash-end is required.",
    )
    g.add_argument(
        "--ash-end",
        metavar="ISO8601",
        help="Window end (timestamptz). If set, --ash-start is required.",
    )


def build_parser() -> argparse.ArgumentParser:
    fmt = argparse.ArgumentDefaultsHelpFormatter
    p = argparse.ArgumentParser(
        prog="ybtop",
        formatter_class=fmt,
        description=(
            "YugabyteDB observability: connect to one YSQL node, discover the rest via yb_servers(), "
            "merge per-node stats, write snapshot JSON + manifest on watch, and serve a browser UI."
        ),
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser(
        "watch",
        help=(
            "Live multi-panel dashboard; writes ybtop.out.*.json and ybtop.manifest.json each tick; "
            "starts the browser viewer (HTTP) by default."
        ),
        formatter_class=fmt,
    )
    _connection_args(w)
    _ash_args(w)
    w.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_REFRESH_INTERVAL_SEC,
        metavar="SEC",
        help="Seconds between dashboard refresh and snapshot write.",
    )
    w.add_argument(
        "--output-dir",
        default=DEFAULT_SNAPSHOT_OUTPUT_DIR,
        help="Directory for ybtop.out.*.json and ybtop.manifest.json.",
    )
    w.add_argument(
        "--snapshot-retention-hours",
        type=float,
        default=DEFAULT_SNAPSHOT_RETENTION_HOURS,
        metavar="HOURS",
        help="Delete snapshot files older than this (manifest pruned accordingly). 0 disables GC.",
    )
    w.add_argument(
        "--snapshot-statements-per-node",
        type=int,
        default=SNAPSHOT_STATEMENTS_PER_NODE,
        metavar="N",
        help="Top N statements per node stored in each snapshot file.",
    )
    w.add_argument(
        "--snapshot-ash-per-node",
        type=int,
        default=SNAPSHOT_ASH_PER_NODE,
        metavar="N",
        help="Top N ASH groups per node stored in each snapshot file.",
    )
    v = w.add_argument_group("viewer (HTTP; same as ybtop serve)")
    v.add_argument(
        "--no-serve",
        action="store_true",
        help="Do not start the browser viewer; only the terminal dashboard and snapshot files.",
    )
    v.add_argument(
        "--serve-bind",
        default=DEFAULT_SERVE_HOST,
        help="HTTP listen address for the embedded viewer (not YSQL; see connection group for --port).",
    )
    v.add_argument(
        "--serve-port",
        type=int,
        default=DEFAULT_SERVE_PORT,
        help="HTTP listen port for the embedded viewer.",
    )

    reset_p = sub.add_parser(
        "reset_pg_stat_statements",
        help="Run SELECT pg_stat_statements_reset() on every YSQL node (via yb_servers()).",
        formatter_class=fmt,
        epilog=(
            "Requires permission to reset statement statistics on each node (typically the "
            "yugabyte superuser). Clears counters only; the pg_stat_statements extension stays loaded."
        ),
    )
    _connection_args(reset_p)

    serve_p = sub.add_parser(
        "serve",
        help="HTTP server for the static viewer (reads snapshot dir; does not modify manifest).",
        formatter_class=fmt,
    )
    serve_p.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing ybtop.manifest.json and ybtop.out.*.json (same as watch --output-dir).",
    )
    serve_p.add_argument(
        "--bind",
        default=DEFAULT_SERVE_HOST,
        help="Listen address for HTTP.",
    )
    serve_p.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVE_PORT,
        help="Listen port for HTTP.",
    )
    return p


def _settings_from_args(args: argparse.Namespace) -> Settings:
    env_dsn = load_dsn_from_env_or_none()
    password = args.password if args.password is not None else os.environ.get("YBTOP_PASSWORD")
    if args.dsn:
        seed = args.dsn
    elif args.host:
        seed = resolve_seed_dsn(
            dsn=None,
            host=args.host,
            port=int(args.port),
            user=args.user,
            password=password,
            dbname=args.dbname,
        )
    elif env_dsn:
        seed = env_dsn
    else:
        raise SystemExit("Provide --dsn, or --host, or set YBTOP_DSN / DATABASE_URL.")
    ash_start_raw = getattr(args, "ash_start", None)
    ash_end_raw = getattr(args, "ash_end", None)
    ash_start = _parse_ts(ash_start_raw) if ash_start_raw else None
    ash_end = _parse_ts(ash_end_raw) if ash_end_raw else None
    if (ash_start is None) ^ (ash_end is None):
        raise SystemExit("Provide both --ash-start and --ash-end, or neither.")
    return Settings(
        seed_dsn=seed,
        refresh_interval=float(getattr(args, "interval", DEFAULT_REFRESH_INTERVAL_SEC)),
        ash_window_minutes=int(getattr(args, "ash_window_minutes", DEFAULT_ASH_WINDOW_MINUTES)),
        ash_start=ash_start,
        ash_end=ash_end,
        snapshot_output_dir=str(getattr(args, "output_dir", DEFAULT_SNAPSHOT_OUTPUT_DIR)),
        snapshot_retention_hours=float(
            getattr(args, "snapshot_retention_hours", DEFAULT_SNAPSHOT_RETENTION_HOURS)
        ),
        snapshot_statements_per_node=int(
            getattr(args, "snapshot_statements_per_node", SNAPSHOT_STATEMENTS_PER_NODE)
        ),
        snapshot_ash_per_node=int(getattr(args, "snapshot_ash_per_node", SNAPSHOT_ASH_PER_NODE)),
    )


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        from ybtop.serve import run_serve

        run_serve(data_dir=args.data_dir, host=args.bind, port=args.port)
        return

    settings = _settings_from_args(args)

    if args.command == "watch":
        if not args.no_serve:
            from ybtop.serve import start_serve_background

            start_serve_background(
                data_dir=settings.snapshot_output_dir,
                host=args.serve_bind,
                port=int(args.serve_port),
            )
        try:
            run_watch(settings)
        except KeyboardInterrupt:
            sys.exit(0)
    elif args.command == "reset_pg_stat_statements":
        run_reset_pg_stat_statements(settings)
    else:
        raise SystemExit("Unknown command")
