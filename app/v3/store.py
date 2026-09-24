"""Read-only access to the V3 Parquet store (one shared DuckDB instance).

* a single in-process DuckDB database, one cursor per thread (cursors share the
  database instance, its Parquet metadata cache and buffer pool);
* one view per dataset over its immutable segments;
* every lookup is an indexed range query on the sorted ``ts`` column (row-group
  min/max pruning), never a file scan;
* ties on the same timestamp resolve to the first swap of the block, by on-chain order
  ``(block_number, log_index)``; in legacy mode to the first row of the CSV (lowest ``rn``),
  exactly like V1/V2 which returned the first row met by the CSV scan (on the current data
  both rules pick the same row);
* Chainlink feeds (unless legacy mode or ``v3.chainlink_active_phase_only: false``): only the
  rounds of the aggregator phase that the proxy served at T are read, the latest round first
  among rounds sharing a timestamp (see ``app.v3.chainlink_phases``).

The manifest written by ``app.v3.sync`` is polled; when its version changes the
views are rebuilt and dependent caches are invalidated through ``store.version``.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from app import csv_adapter
from app.config import AppConfig
from app.v3 import sync as sync_mod
from app.v3.chainlink_phases import PhaseTable
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
    columns: frozenset[str] = frozenset()
    file_version: str | None = None
    chainlink: dict[str, Any] = field(default_factory=dict)   # proxy, switches, verified_ts, status
    phases: PhaseTable | None = None                          # None: no switch table for this feed

    def has(self, canonical: str) -> bool:
        return canonical in self.schema.mapping

    def col(self, canonical: str) -> str | None:
        return self.schema.mapping.get(canonical)

    @property
    def has_phases(self) -> bool:
        """A Chainlink feed whose rounds carry their aggregator phase."""
        return bool(self.chainlink.get("proxy")) and "phase" in self.columns


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
        self._coverage: dict[str, datetime] = {}
        self._manifest_mtime = 0.0
        self._last_poll = 0.0
        self.version: str | None = None
        self.generated_at: str | None = None
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
            # Same data; only the Chainlink phase check (verified_ts, status) can have moved.
            with self._lock:
                for rel, d in manifest["datasets"].items():
                    if rel in self._datasets and d.get("chainlink"):
                        self._datasets[rel].chainlink = d["chainlink"]
                        self._datasets[rel].phases = _phase_table(d["chainlink"])
                self._manifest_mtime = mtime
            return False
        with self._lock:
            datasets: dict[str, DatasetInfo] = {}
            for i, (rel, d) in enumerate(sorted(manifest["datasets"].items())):
                ddir = sync_mod.dataset_dir(self.root, rel)
                files = [str(ddir / s["file"]) for s in d["segments"]]
                view = f"ds_{i}"
                columns: frozenset[str] = frozenset()
                if files:
                    self._con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet({files!r})")
                    columns = frozenset(r[0] for r in self._con.execute(f"DESCRIBE {view}").fetchall())
                pq_map = {c: CANON_TO_PQ[c] for c in d["mapping"] if c in CANON_TO_PQ}
                schema = csv_adapter.SchemaInfo(
                    path=Path(rel), raw_columns=d["raw_columns"], mapping=pq_map, tvl_unit=d["tvl_unit"]
                )
                chainlink = d.get("chainlink") or {}
                datasets[rel] = DatasetInfo(
                    rel=rel, csv_name=d["csv_name"], raw_columns=d["raw_columns"], tvl_unit=d["tvl_unit"],
                    files=files, n_rows=d["n_rows"], min_ts=_parse_ts(d["min_ts"]), max_ts=_parse_ts(d["max_ts"]),
                    view=view, schema=schema, columns=columns, file_version=d.get("file_version"),
                    chainlink=chainlink, phases=_phase_table(chainlink),
                )
            self._datasets = datasets
            self._coverage = {}
            for d in datasets.values():
                folder = d.rel.split("/")[0]
                if d.max_ts is not None and (folder not in self._coverage or d.max_ts > self._coverage[folder]):
                    self._coverage[folder] = d.max_ts
            self._quote_cache = {}
            self._manifest_mtime = mtime
            self.version = manifest.get("version")
            self.generated_at = manifest.get("generated_at")
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

    def coverage(self, rel: str) -> datetime | None:
        """Latest synced observation of the dataset's folder (one extraction container per folder).

        A lower bound of the time up to which the extractor has scanned the chain: each asset folder
        holds its Chainlink feed (1 h heartbeat; 24 h for the stablecoin peg feeds), whereas a quiet
        pool's own last row can be days old even when the extraction is up to date.
        """
        return self._coverage.get(rel.split("/")[0])

    @property
    def legacy(self) -> bool:
        return self.cfg.v3.legacy_truncation

    def _tie_order(self, d: DatasetInfo) -> str:
        """Order of rows sharing a timestamp: first swap of the block (on-chain order), or CSV order in legacy mode."""
        if self.legacy or not {"block_number", "log_index"} <= d.columns:
            return "rn ASC"
        return "block_number ASC NULLS LAST, log_index ASC NULLS LAST, rn ASC"

    # ------------------------------------------------------------ Chainlink phases
    def phase_filtering(self, rel: str) -> bool:
        """True when the reads of ``rel`` are restricted to the aggregator phase active at T."""
        d = self._datasets.get(rel)
        return (d is not None and d.phases is not None and d.has_phases
                and self.cfg.v3.chainlink_active_phase_only and not self.legacy)

    def active_phase(self, rel: str, t: datetime) -> int | None:
        """Phase the proxy of ``rel`` served at ``t`` (0 before deployment); None when reads are not filtered."""
        if not self.phase_filtering(rel):
            return None
        return self._datasets[rel].phases.active_phase(t)

    def phase_status(self, rel: str, t: datetime) -> str:
        """``off`` (no filtering: not a feed with phases, legacy mode or disabled), ``verified`` (the switch table
        was checked on-chain after ``t``), ``unverified`` (checked before ``t``) or ``no_table`` (feed with phases
        but no switch table: rounds of every phase are read)."""
        d = self._datasets.get(rel)
        if d is None or not d.has_phases or not self.cfg.v3.chainlink_active_phase_only or self.legacy:
            return "off"
        if d.phases is None:
            return "no_table"
        return "verified" if d.phases.verified is not None and t <= d.phases.verified else "unverified"

    def _served(self, d: DatasetInfo, served: bool) -> str:
        return f" AND {d.phases.served_sql()}" if served and self.phase_filtering(d.rel) else ""

    # ------------------------------------------------------------------- queries
    def as_of(self, rel: str, t: datetime, cols: list[str], phase: int | None = None) -> dict[str, Any] | None:
        """Latest row with ``ts <= t``: the first swap of the block among equal timestamps (CSV order in legacy
        mode). With ``phase`` (Chainlink): only that aggregator phase, the latest round first."""
        d = self._datasets.get(rel)
        if d is None or not d.files or d.min_ts is None or t < d.min_ts:
            return None
        select = ", ".join(dict.fromkeys(["ts", *cols]))
        where, args_extra, order = "", [], f"ts DESC, {self._tie_order(d)}"
        if phase is not None:
            where, args_extra, order = " AND phase = ?", [phase], "ts DESC, agg_round DESC NULLS LAST, rn ASC"
        cur = self._cursor()
        for lb in _LOOKBACKS:
            if lb is None:
                sql = f"SELECT {select} FROM {d.view} WHERE ts <= ?{where} ORDER BY {order} LIMIT 1"
                args: list[Any] = [t, *args_extra]
            else:
                sql = (f"SELECT {select} FROM {d.view} WHERE ts <= ? AND ts > ?::TIMESTAMPTZ - INTERVAL '{lb}'{where} "
                       f"ORDER BY {order} LIMIT 1")
                args = [t, t, *args_extra]
            row = cur.execute(sql, args).fetchone()
            if row is not None:
                names = [c.split(" AS ")[-1] for c in select.split(", ")]
                return dict(zip(names, row))
        return None

    def count_at(self, rel: str, t: datetime, phase: int | None = None) -> int:
        """Rows sharing the timestamp ``t`` (in ``phase`` when given)."""
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return 0
        extra, args = (" AND phase = ?", [t, phase]) if phase is not None else ("", [t])
        return int(self._cursor().execute(f"SELECT count(*) FROM {d.view} WHERE ts = ?{extra}", args).fetchone()[0])

    def _window_order(self, d: DatasetInfo) -> str:
        if self.legacy:
            return "ts, rn"
        keys = ["ts"] + [c for c in ("block_number", "log_index", "agg_round") if c in d.columns] + ["rn"]
        return ", ".join(keys)

    def window(self, rel: str, start: datetime, end: datetime, cols: list[str], limit: int | None = None,
               served: bool = False) -> list[dict[str, Any]]:
        """Rows with ``start <= ts < end`` in time order (CSV order within a timestamp in legacy mode), optionally
        truncated to ``limit`` (legacy). ``served``: Chainlink rounds of the phase the proxy served at their time."""
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return []
        select = ", ".join(dict.fromkeys(["ts", *cols]))
        sql = f"SELECT {select} FROM {d.view} WHERE ts >= ? AND ts < ?{self._served(d, served)} ORDER BY {self._window_order(d)}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._cursor().execute(sql, [start, end]).fetchall()
        names = select.split(", ")
        return [dict(zip(names, r)) for r in rows]

    def distinct_ts(self, rel: str, start: datetime, end: datetime, limit: int) -> list[datetime]:
        """Distinct timestamps with ``start <= ts < end``, ascending, at most ``limit``."""
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return []
        rows = self._cursor().execute(
            f"SELECT DISTINCT ts FROM {d.view} WHERE ts >= ? AND ts < ? ORDER BY ts LIMIT {int(limit)}", [start, end]
        ).fetchall()
        return [r[0] for r in rows]

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
        self, rel: str, col: str, start: datetime, end: datetime, limit: int | None = None, served: bool = False,
    ) -> tuple[int, float | None, float | None]:
        """(n, median, MAD) of ``col`` over ``start <= ts < end``, computed inside DuckDB.

        Reproduces the V1/V2 definition exactly: median = sorted[n // 2] (upper median
        for even n), MAD = sorted(|p - median|)[n // 2]. With ``limit`` the window is first
        cut to its ``limit`` oldest rows (legacy V1/V2 truncation), then NULLs are dropped.
        ``served``: Chainlink rounds of the phase the proxy served at their time only.
        """
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return 0, None, None
        w = f"SELECT {col} AS p FROM {d.view} WHERE ts >= ? AND ts < ?{self._served(d, served)}"
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

    def prices(self, rel: str, col: str, start: datetime, end: datetime, limit: int | None = None,
               served: bool = False) -> list[float]:
        """Non-null values of ``col`` over ``start <= ts < end`` in time order (limit applied before NULL filter)."""
        rows = self.window(rel, start, end, [col], limit, served=served)
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


def _phase_table(chainlink: dict[str, Any]) -> PhaseTable | None:
    return PhaseTable(chainlink) if chainlink.get("switches") else None


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
