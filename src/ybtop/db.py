from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Optional, Union

import psycopg
from psycopg.rows import dict_row


def _connect_with_hint(dsn: str) -> psycopg.Connection:
    try:
        return psycopg.connect(dsn, row_factory=dict_row)
    except psycopg.OperationalError as exc:
        err = str(exc).lower()
        if "no password supplied" in err or "password not supplied" in err:
            raise psycopg.OperationalError(
                f"{exc}\n"
                "Hint: this server requires a password. Use --password, set YBTOP_PASSWORD, "
                "or put the password in the DSN, e.g. postgresql://yugabyte:YOURPASS@host:5433/yugabyte"
            ) from exc
        raise


@contextmanager
def connect(dsn: str) -> Iterator[psycopg.Connection]:
    conn = _connect_with_hint(dsn)
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(
    conn: psycopg.Connection,
    sql: str,
    params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return list(cur.fetchall())
