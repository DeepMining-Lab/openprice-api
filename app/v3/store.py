"""Read-only access to the V3 Parquet store (one shared DuckDB instance).

* a single in-process DuckDB database, one cursor per thread (cursors share the
  database instance, its Parquet metadata cache and buffer pool);
* one view per dataset over its immutable segments;
* every lookup is an indexed range query on the sorted ``ts`` column (row-group
  min/max pruning), never a file scan;
* ties on the same timestamp resolve to the FIRST row of the CSV (lowest ``rn``),
  exactly like V1/V2 which returned the first row met by the CSV scan.

The manifest written by ``app.v3.sync`` is polled; when its version changes the
views are rebuilt and dependent caches are invalidated through ``store.version``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from app import csv_adapter
from app.config import AppConfig
from app.v3 import sync as sync_mod
from app.v3.sync import CANON_TO_PQ

# Look-back ladder for as-of lookups: cheap bounded scans first, unbounded last.
_LOOKBACKS = ("1 day", "30 days", None)


@dataclass
class DatasetInfo:
    rel: str
    csv_name: str
    raw_columns: list[str]
    tvl_unit: str
    files: list[str]
    n_rows: int
    min_ts: datetime | None
    max_ts: datetime | None
    view: str
    # csv_adapter-compatible schema whose ``mapping`` values are Parquet column names,
    # so V1 pure helpers (compute_s_liq, ...) can be reused unchanged.
    schema: csv_adapter.SchemaInfo

    def has(self, canonical: str) -> bool:
        return canonical in self.schema.mapping

    def col(self, canonical: str) -> str | None:
        return self.schema.mapping.get(canonical)


def _parse_ts(v: str | None) -> datetime | None:
    return datetime.fromisoformat(v) if v else None


class Store:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.root: Path = cfg.v3.parquet_path
        self._lock = threading.RLock()
        self._local = threading.local()
        self._datasets: dict[str, DatasetInfo] = {}
        self._quote_cache: dict[str, str | None] = {}
        self._manifest_mtime = 0.0
        self._last_poll = 0.0
        self.version: str | None = None
        self._con = duckdb.connect()
        self._con.execute(
            f"SET threads={max(1, cfg.v3.duckdb_threads)}; SET TimeZone='UTC'; "
            "SET parquet_metadata_cache=true; SET memory_limit='4GB'"
        )
        self.reload(force=True)

    # ------------------------------------------------------------------ manifest
    def reload(self, force: bool = False) -> bool:
        """(Re)load the manifest and rebuild the views. Returns True when it changed."""
        mp = sync_mod.manifest_path(self.root)
        if not mp.exists():
            return False
        mtime = mp.stat().st_mtime
        if not force and mtime == self._manifest_mtime:
            return False
        manifest = sync_mod.load_manifest(self.root)
        if not force and manifest.get("version") == self.version:
            self._manifest_mtime = mtime
            return False
        with self._lock:
            datasets: dict[str, DatasetInfo] = {}
            for i, (rel, d) in enumerate(sorted(manifest["datasets"].items())):
                ddir = sync_mod.dataset_dir(self.root, rel)
                files = [str(ddir / s["file"]) for s in d["segments"]]
                view = f"ds_{i}"
                if files:
                    self._con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet({files!r})")
                pq_map = {c: CANON_TO_PQ[c] for c in d["mapping"] if c in CANON_TO_PQ}
                schema = csv_adapter.SchemaInfo(
                    path=Path(rel), raw_columns=d["raw_columns"], mapping=pq_map, tvl_unit=d["tvl_unit"]
                )
                datasets[rel] = DatasetInfo(
                    rel=rel, csv_name=d["csv_name"], raw_columns=d["raw_columns"], tvl_unit=d["tvl_unit"],
                    files=files, n_rows=d["n_rows"], min_ts=_parse_ts(d["min_ts"]), max_ts=_parse_ts(d["max_ts"]),
                    view=view, schema=schema,
                )
            self._datasets = datasets
            self._quote_cache = {}
            self._manifest_mtime = mtime
            self.version = manifest.get("version")
        return True

    def maybe_reload(self) -> bool:
        """Cheap poll (one stat every ``manifest_poll_seconds``) used on the request path."""
        now = time.monotonic()
        if now - self._last_poll < self.cfg.v3.manifest_poll_seconds:
            return False
        self._last_poll = now
        return self.reload()

    # ------------------------------------------------------------------- helpers
    def _cursor(self) -> duckdb.DuckDBPyConnection:
        cur = getattr(self._local, "cur", None)
        if cur is None:
            cur = self._local.cur = self._con.cursor()
        return cur

    def dataset(self, rel: str) -> DatasetInfo | None:
        d = self._datasets.get(rel)
        return d

    def datasets(self) -> dict[str, DatasetInfo]:
        return self._datasets

    # ------------------------------------------------------------------- queries
    def as_of(self, rel: str, t: datetime, cols: list[str]) -> dict[str, Any] | None:
        """Latest row with ``ts <= t`` (first CSV row among equal timestamps)."""
        d = self._datasets.get(rel)
        if d is None or not d.files or d.min_ts is None or t < d.min_ts:
            return None
        select = ", ".join(dict.fromkeys(["ts", *cols]))
        cur = self._cursor()
        for lb in _LOOKBACKS:
            if lb is None:
                sql = f"SELECT {select} FROM {d.view} WHERE ts <= ? ORDER BY ts DESC, rn ASC LIMIT 1"
                args: list[Any] = [t]
            else:
                sql = (f"SELECT {select} FROM {d.view} WHERE ts <= ? AND ts > ?::TIMESTAMPTZ - INTERVAL '{lb}' "
                       "ORDER BY ts DESC, rn ASC LIMIT 1")
                args = [t, t]
            row = cur.execute(sql, args).fetchone()
            if row is not None:
                names = [c.split(" AS ")[-1] for c in select.split(", ")]
                return dict(zip(names, row))
        return None

    def window(self, rel: str, start: datetime, end: datetime, cols: list[str], limit: int | None = None) -> list[dict[str, Any]]:
        """Rows with ``start <= ts < end`` in CSV order, optionally truncated to ``limit`` (legacy)."""
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return []
        select = ", ".join(dict.fromkeys(["ts", *cols]))
        sql = f"SELECT {select} FROM {d.view} WHERE ts >= ? AND ts < ? ORDER BY ts, rn"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._cursor().execute(sql, [start, end]).fetchall()
        names = select.split(", ")
        return [dict(zip(names, r)) for r in rows]

    def sum_between(self, rel: str, col: str, start: datetime, end: datetime) -> float | None:
        """SUM(col) for ``start <= ts <= end`` (both inclusive); None when no non-null value."""
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return None
        r = self._cursor().execute(
            f"SELECT sum({col}) FROM {d.view} WHERE ts >= ? AND ts <= ?", [start, end]
        ).fetchone()[0]
        return float(r) if r is not None else None

    def price_stats(
        self, rel: str, col: str, start: datetime, end: datetime, limit: int | None = None
    ) -> tuple[int, float | None, float | None]:
        """(n, median, MAD) of ``col`` over ``start <= ts < end``, computed inside DuckDB.

        Reproduces the V1/V2 definition exactly: median = sorted[n // 2] (upper median
        for even n), MAD = sorted(|p - median|)[n // 2]. With ``limit`` the window is first
        cut to its ``limit`` oldest rows (legacy V1/V2 truncation), then NULLs are dropped.
        """
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return 0, None, None
        w = f"SELECT {col} AS p FROM {d.view} WHERE ts >= ? AND ts < ?"
        if limit is not None:
            w += f" ORDER BY ts, rn LIMIT {int(limit)}"
        sql = f"""
            WITH w AS ({w}),
            f AS (SELECT p FROM w WHERE p IS NOT NULL),
            n AS (SELECT count(*) AS n FROM f),
            m AS (SELECT p AS med FROM (SELECT p, row_number() OVER (ORDER BY p) AS r FROM f)
                  WHERE r = (SELECT n // 2 + 1 FROM n)),
            dv AS (SELECT abs(f.p - m.med) AS dd FROM f, m),
            mad AS (SELECT dd AS mad FROM (SELECT dd, row_number() OVER (ORDER BY dd) AS r FROM dv)
                    WHERE r = (SELECT n // 2 + 1 FROM n))
            SELECT (SELECT n FROM n), (SELECT med FROM m), (SELECT mad FROM mad)
        """
        n, med, mad = self._cursor().execute(sql, [start, end]).fetchone()
        return int(n), (float(med) if med is not None else None), (float(mad) if mad is not None else None)

    def prices(self, rel: str, col: str, start: datetime, end: datetime, limit: int | None = None) -> list[float]:
        """Non-null values of ``col`` over ``start <= ts < end`` in CSV order (limit applied before NULL filter)."""
        rows = self.window(rel, start, end, [col], limit)
        return [float(r[col]) for r in rows if r[col] is not None]

    def quote_symbol(self, rel: str) -> str | None:
        """Quote-token symbol of a pool (constant per file): latest non-null value."""
        if rel in self._quote_cache:
            return self._quote_cache[rel]
        d = self._datasets.get(rel)
        val: str | None = None
        if d is not None and d.files:
            row = self._cursor().execute(
                f"SELECT quote_symbol FROM {d.view} ORDER BY ts DESC, rn ASC LIMIT 1"
            ).fetchone()
            val = row[0] if row and row[0] is not None else None
        self._quote_cache[rel] = val
        return val


_store: Store | None = None
_store_lock = threading.Lock()


def get_store(cfg: AppConfig | None = None) -> Store:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                from app.config import get_config
                _store = Store(cfg or get_config())
    return _store


def reset_store() -> None:
    global _store
    _store = None
