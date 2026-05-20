"""DuckDB client — in-memory, read-only, no persistent .duckdb files.

CSV reading strategy
--------------------
Two challenges exist in the dataset:

1. Chainlink files contain very large integers (global_round_id ~2^63) that
   confuse DuckDB's type sniffer → use strict_mode=false.

2. Transitional CSV files have an old 16-column header but new 54-column data
   rows (regeneration in progress) → DuckDB cannot reliably detect the header.
   Fix: read column names from the first line of the file with Python's csv
   module, which is always correct regardless of column count mismatches.

All timestamp comparisons use TRY_CAST(col AS TIMESTAMPTZ) because
strict_mode=false causes DuckDB to infer timestamp columns as VARCHAR.
"""

from __future__ import annotations

import csv as csv_mod
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

# null_padding=true: tolerate rows with more or fewer fields than the header.
# strict_mode=false: allow large integers (Chainlink global_round_id).
# ignore_errors=true: skip duplicate header rows embedded in data (seen in eth_usdc_uniswap_v3_005.csv at line ~1.5M).
_CSV_OPTS = "delim=',', header=true, max_line_size=102400, strict_mode=false, null_padding=true, ignore_errors=true"


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _src(path: str) -> str:
    return f"read_csv('{path}', {_CSV_OPTS})"


def _ts_cast(col: str) -> str:
    """Wrap a column name in a TIMESTAMPTZ cast for reliable comparison."""
    return f'TRY_CAST("{col}" AS TIMESTAMPTZ)'


def describe_csv(path: Path) -> list[dict[str, str]]:
    """Return column names from the CSV header line.

    Uses Python's csv module instead of DuckDB DESCRIBE to handle files where
    the header column count differs from the data column count (transitional
    format during regeneration).  Type is always reported as VARCHAR because
    we only need names for canonical mapping.
    """
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv_mod.reader(f)
        header = next(reader)
    return [{"name": col.strip(), "type": "VARCHAR"} for col in header if col.strip()]


def query_csv(
    path: Path,
    columns: list[str],
    start: datetime | None,
    end: datetime | None,
    limit: int,
    timestamp_col: str = "timestamp",
) -> list[dict[str, Any]]:
    """Return rows from a CSV within an optional time range."""
    safe_cols = ", ".join(f'"{c}"' for c in columns)
    params: list[Any] = []
    where_clauses: list[str] = []
    ts = _ts_cast(timestamp_col)
    if start is not None:
        where_clauses.append(f"{ts} >= ?")
        params.append(start)
    if end is not None:
        where_clauses.append(f"{ts} <= ?")
        params.append(end)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    src = _src(str(path))
    sql = f'SELECT {safe_cols} FROM {src} {where} ORDER BY {ts} LIMIT ?'
    params.append(limit)
    con = _conn()
    rows = con.execute(sql, params).fetchall()
    return [dict(zip(columns, r)) for r in rows]


def latest_at_or_before(
    path: Path,
    timestamp: datetime,
    columns: list[str],
    timestamp_col: str = "timestamp",
) -> dict[str, Any] | None:
    """Return the most recent row at or before the given timestamp."""
    safe_cols = ", ".join(f'"{c}"' for c in columns)
    ts = _ts_cast(timestamp_col)
    src = _src(str(path))
    sql = (
        f'SELECT {safe_cols} FROM {src} '
        f'WHERE {ts} <= ? '
        f'ORDER BY {ts} DESC LIMIT 1'
    )
    con = _conn()
    rows = con.execute(sql, [timestamp]).fetchall()
    if not rows:
        return None
    return dict(zip(columns, rows[0]))


def range_query(
    path: Path,
    start: datetime,
    end: datetime,
    columns: list[str],
    limit: int,
    timestamp_col: str = "timestamp",
) -> list[dict[str, Any]]:
    """Return rows where start <= timestamp < end (right-exclusive).

    The right-exclusive convention prevents a swap exactly on a window boundary
    from being attributed to two consecutive windows simultaneously.
    """
    safe_cols = ", ".join(f'"{c}"' for c in columns)
    ts = _ts_cast(timestamp_col)
    src = _src(str(path))
    sql = (
        f'SELECT {safe_cols} FROM {src} '
        f'WHERE {ts} >= ? AND {ts} < ? '
        f'ORDER BY {ts} LIMIT ?'
    )
    con = _conn()
    rows = con.execute(sql, [start, end, limit]).fetchall()
    return [dict(zip(columns, r)) for r in rows]


def count_rows_in_range(
    path: Path,
    start: datetime,
    end: datetime,
    timestamp_col: str = "timestamp",
) -> int:
    """Count rows where start <= timestamp <= end (both inclusive).

    Used for activity checks (b_ref(T) semantics: observations up to and
    including T are considered current). Not used for VWMP windows.
    """
    ts = _ts_cast(timestamp_col)
    src = _src(str(path))
    sql = f'SELECT COUNT(*) FROM {src} WHERE {ts} BETWEEN ? AND ?'
    con = _conn()
    return con.execute(sql, [start, end]).fetchone()[0]  # type: ignore[index]


def sum_column_in_range(
    path: Path,
    col: str,
    start: datetime,
    end: datetime,
    timestamp_col: str = "timestamp",
) -> float | None:
    """Return SUM(col) for rows in [start, end], or None when no rows match.

    Uses TRY_CAST so non-numeric values are silently treated as NULL rather
    than raising an error (consistent with strict_mode=false elsewhere).
    """
    ts = _ts_cast(timestamp_col)
    src = _src(str(path))
    sql = f'SELECT SUM(TRY_CAST("{col}" AS DOUBLE)) FROM {src} WHERE {ts} BETWEEN ? AND ?'
    con = _conn()
    result = con.execute(sql, [start, end]).fetchone()[0]  # type: ignore[index]
    return float(result) if result is not None else None
