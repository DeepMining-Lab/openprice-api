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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from app import csv_adapter
from app.config import AppConfig
from app.v3 import sync as sync_mod
from app.v3.chainlink_phases import PhaseTable
from app.v3.sync import CANON_TO_PQ

# Look-back ladder for as-of lookups: cheap bounded scans first, unbounded last. A dataset of at most
# _SMALL_DATASET_ROWS rows (about one row group) is read with the unbounded query directly: it costs the same as
# one bounded step, and a quiet pool would otherwise take all three.
_LOOKBACKS = ("1 day", "30 days", None)
_SMALL_DATASET_ROWS = 50_000


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
    extraction_head: datetime | None = None                   # chain time the last extraction of the file reached

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


@dataclass
class Probe:
    """What the viability checks of one candidate need, read in a single query (see ``Store.probe``)."""
    row: dict[str, Any] | None   # as-of row at T (``Store.as_of``)
    n_same_ts: int | None        # rows sharing the as-of row's timestamp; None when not counted
    n_24h: int                   # rows with T - 24 h <= ts <= T
    vol_24h: float | None        # SUM(volume) over the same rows; None when there is no non-null value


@dataclass
class WindowStats:
    """MAD filter + VWMP of one window, as ``price_service._filter_mad_outliers`` / ``_compute_vwmp`` compute them."""
    n_raw: int             # rows in the window
    n_valid: int           # rows with a usable price
    kept: int              # prices the VWMP is computed on (0 only when n_valid is 0)
    excluded: int          # prices flagged by the MAD filter
    vwmp: float | None
    mad_fallback: bool = False  # every price was flagged: all are used (V1/V2 rule)


# A 24 h volume is summed as exact decimals (10 decimal places; every stored volume is below 1e8 USD), so the result
# does not depend on the order of the additions: a range computed in bulk (app.v3.batch) gets the same bits as a
# point. Legacy mode keeps the float sum of V1/V2.
VOLUME_DECIMAL = "TRY_CAST({col} AS DECIMAL(38, 10))"


# Beyond this relative distance between a cumulative volume and half the total, the summation order cannot change
# which price the VWMP picks (rounding error <= n * 1.1e-16 of the total absolute volume).
_VWMP_AMBIGUITY = 1e-9


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
            # Same data; only the Chainlink phase check (verified_ts, status) and the extraction heads can have moved.
            with self._lock:
                for rel, d in manifest["datasets"].items():
                    if rel in self._datasets:
                        self._datasets[rel].extraction_head = _parse_ts(d.get("extraction_head_utc"))
                        if d.get("chainlink"):
                            self._datasets[rel].chainlink = d["chainlink"]
                            self._datasets[rel].phases = _phase_table(d["chainlink"])
                self._coverage = _coverage(self._datasets)
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
                    extraction_head=_parse_ts(d.get("extraction_head_utc")),
                )
            self._datasets = datasets
            self._coverage = _coverage(datasets)
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
        """Chain time up to which the dataset's folder has been extracted and synced (one extraction container per
        folder): the latest extraction head of its files (``extraction_head_utc`` of the manifest), or its latest
        observation when no extraction head is recorded. A quiet pool or a peg feed (24 h heartbeat) can have its last
        event hours before the extraction ran; the head says the extractor saw nothing newer up to that time."""
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
        return self._as_of_ladder(d, t, list(dict.fromkeys(["ts", *cols])), self._lookbacks(d, _LOOKBACKS), phase)

    @staticmethod
    def _lookbacks(d: DatasetInfo, ladder: tuple) -> tuple:
        return (None,) if d.n_rows <= _SMALL_DATASET_ROWS else ladder

    def _as_of_ladder(self, d: DatasetInfo, t: datetime, select: list[str], lookbacks: tuple,
                      phase: int | None = None) -> dict[str, Any] | None:
        where, args_extra, order = "", [], f"ts DESC, {self._tie_order(d)}"
        if phase is not None:
            where, args_extra, order = " AND phase = ?", [phase], "ts DESC, agg_round DESC NULLS LAST, rn ASC"
        cols = ", ".join(select)
        cur = self._cursor()
        for lb in lookbacks:
            if lb is None:
                sql = f"SELECT {cols} FROM {d.view} WHERE ts <= ?{where} ORDER BY {order} LIMIT 1"
                args: list[Any] = [t, *args_extra]
            else:
                sql = (f"SELECT {cols} FROM {d.view} WHERE ts <= ? AND ts > ?::TIMESTAMPTZ - INTERVAL '{lb}'{where} "
                       f"ORDER BY {order} LIMIT 1")
                args = [t, t, *args_extra]
            row = cur.execute(sql, args).fetchone()
            if row is not None:
                return dict(zip(select, row))
        return None

    def probe(self, rel: str, t: datetime, cols: list[str], vol_col: str | None) -> Probe:
        """The as-of row at ``t`` (same rule as ``as_of``), the number of rows sharing its timestamp, and the rows and
        SUM(``vol_col``) over ``[t - 24 h, t]`` (both inclusive, like ``sum_between``; an exact decimal sum outside
        legacy mode, see ``VOLUME_DECIMAL``), from one scan of the last 24 hours. When that window is empty the as-of
        row comes from the longer look-backs of ``as_of``."""
        d = self._datasets.get(rel)
        if d is None or not d.files or d.min_ts is None or t < d.min_ts:
            return Probe(None, None, 0, None)
        select = list(dict.fromkeys(["ts", *cols]))
        tie = self._tie_order(d)
        keys = [k.split()[0] for k in tie.split(", ")]
        needed = ", ".join(dict.fromkeys([*select, *keys, *([vol_col] if vol_col else [])]))
        if not vol_col:
            vol = "CAST(NULL AS DOUBLE)"
        elif self.legacy:
            vol = f"sum({vol_col})"
        else:
            vol = f"CAST(sum({VOLUME_DECIMAL.format(col=vol_col)}) AS DOUBLE)"
        sql = f"""
            WITH w AS MATERIALIZED (SELECT {needed} FROM {d.view} WHERE ts >= ? AND ts <= ?),
            top AS (SELECT {', '.join(select)} FROM w ORDER BY ts DESC, {tie} LIMIT 1),
            agg AS (SELECT count(*) AS n24, {vol} AS vol24, max(ts) AS mx FROM w)
            SELECT agg.n24, agg.vol24, (SELECT count(*) FROM w WHERE ts = agg.mx), top.*
            FROM agg LEFT JOIN top ON TRUE
        """
        n24, vol24, n_same, *values = self._cursor().execute(sql, [t - timedelta(hours=24), t]).fetchone()
        vol24 = float(vol24) if vol24 is not None else None
        if not n24:
            return Probe(self._as_of_ladder(d, t, select, self._lookbacks(d, _LOOKBACKS[1:])), None, 0, vol24)
        return Probe(dict(zip(select, values)), int(n_same), int(n24), vol24)

    def window_vwmp(self, rel: str, price_col: str, vol_col: str | None, start: datetime, end: datetime,
                    sigma: float, limit: int | None = None, inverse: bool = False) -> WindowStats | None:
        """MAD filter and VWMP of ``start <= ts < end`` inside DuckDB, without materialising the swaps in Python.

        Same definitions as ``price_service``: rows without a price are dropped (``inverse``: price = 1 / col, rows
        with 0 dropped too), a missing volume counts 1.0, the MAD filter is skipped under 3 prices or when MAD = 0 and
        keeps ``0.6745 * |p - median| / MAD <= sigma``, and the VWMP is the first price, in price order and then
        window order, whose cumulative volume reaches half the total (the upper median of the kept prices when the
        total is <= 0; the highest price when no cumulative volume reaches half). With ``limit`` the window is first
        cut to its oldest ``limit`` rows (legacy). Returns None when a cumulative volume is so close to half the total
        that the order of the float additions could matter: the caller then computes it in Python.
        """
        d = self._datasets.get(rel)
        if d is None or not d.files:
            return WindowStats(0, 0, 0, 0, None)
        order = self._window_order(d)
        vol = vol_col if vol_col else "CAST(NULL AS DOUBLE)"
        src = f"SELECT {', '.join(dict.fromkeys([*order.split(', '), price_col]))}, {vol} AS v0 FROM {d.view} " \
              f"WHERE ts >= ? AND ts < ?"
        if limit is not None:
            src += f" ORDER BY ts, rn LIMIT {int(limit)}"
        valid = f"{price_col} IS NOT NULL" + (f" AND {price_col} <> 0" if inverse else "")
        price = f"1.0::DOUBLE / {price_col}" if inverse else price_col
        sql = f"""
            WITH w0 AS MATERIALIZED (SELECT *, row_number() OVER (ORDER BY {order}) AS i FROM ({src})),
            f AS MATERIALIZED (SELECT i, {price} AS p, coalesce(v0, 1.0::DOUBLE) AS v FROM w0 WHERE {valid}),
            n AS (SELECT count(*) AS n FROM f),
            med AS (SELECT p AS med FROM (SELECT p, row_number() OVER (ORDER BY p) AS r FROM f)
                    WHERE r = (SELECT n // 2 + 1 FROM n)),
            dv AS MATERIALIZED (SELECT i, p, v, abs(p - (SELECT med FROM med)) AS dd FROM f),
            mad AS (SELECT dd AS mad FROM (SELECT dd, row_number() OVER (ORDER BY dd) AS r FROM dv)
                    WHERE r = (SELECT n // 2 + 1 FROM n)),
            k AS MATERIALIZED (SELECT i, p, v FROM dv WHERE (SELECT n FROM n) < 3 OR (SELECT mad FROM mad) = 0
                               OR 0.6745::DOUBLE * dd / (SELECT mad FROM mad) <= ?::DOUBLE),
            tot AS (SELECT count(*) AS nk, sum(v) AS total, sum(abs(v)) AS scale FROM k),
            c AS MATERIALIZED (SELECT p, i, sum(v) OVER (ORDER BY p, i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                               AS cum FROM k)
            SELECT (SELECT count(*) FROM w0), (SELECT n FROM n), tot.nk, tot.total, tot.scale,
                   (SELECT p FROM c WHERE cum >= tot.total / 2 ORDER BY p, i LIMIT 1),
                   (SELECT min(abs(cum - tot.total / 2)) FROM c),
                   (SELECT p FROM (SELECT p, row_number() OVER (ORDER BY p, i) AS r FROM k) WHERE r = tot.nk // 2 + 1),
                   (SELECT max(p) FROM k), (SELECT any_value(p) FROM k)
            FROM tot
        """
        n_raw, n_valid, kept, total, scale, crossing, gap, upper_median, highest, only = \
            self._cursor().execute(sql, [start, end, sigma]).fetchone()
        n_raw, n_valid, kept = int(n_raw), int(n_valid), int(kept)
        if n_valid == 0:
            return WindowStats(n_raw, 0, 0, 0, None)
        if kept == 0:
            return None  # never happens (the median itself is kept); the Python path handles it like V1/V2
        if kept == 1:
            return WindowStats(n_raw, n_valid, 1, n_valid - 1, float(only))
        tol = _VWMP_AMBIGUITY * float(scale)
        if scale == 0:
            return WindowStats(n_raw, n_valid, kept, n_valid - kept, float(upper_median))  # all volumes 0: total is 0
        if abs(float(total)) <= tol or (gap is not None and float(gap) <= tol):
            return None
        if total <= 0:
            return WindowStats(n_raw, n_valid, kept, n_valid - kept, float(upper_median))
        return WindowStats(n_raw, n_valid, kept, n_valid - kept, float(crossing if crossing is not None else highest))

    def tx_hash_of(self, rel: str, ts: datetime, rn: int) -> str | None:
        """Transaction hash of the row ``rn`` (read only for the row that answers; see ``engine._row_cols``)."""
        d = self._datasets.get(rel)
        if d is None or not d.files or "tx_hash" not in d.columns:
            return None
        row = self._cursor().execute(f"SELECT tx_hash FROM {d.view} WHERE ts = ? AND rn = ?", [ts, rn]).fetchone()
        return row[0] if row else None

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


def _coverage(datasets: dict[str, DatasetInfo]) -> dict[str, datetime]:
    out: dict[str, datetime] = {}
    for d in datasets.values():
        folder = d.rel.split("/")[0]
        for t in (d.max_ts, d.extraction_head):
            if t is not None and (folder not in out or t > out[folder]):
                out[folder] = t
    return out


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
