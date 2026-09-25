"""Set-based lookups for range requests (API V3).

A range request asks the engine the same questions at N timestamps: the as-of row of each candidate file with the
rows sharing its timestamp and its 24 h volume (``Store.probe``), and the as-of rows of the ETH/USD reference, of
Chainlink and of the peg feeds (``Store.as_of``). ``BatchStore`` answers each of these questions for all N timestamps
with one query per (file, question), an ASOF JOIN of the N timestamps against the file, instead of N queries. It
delegates everything else (VWMP windows, S_stat, events) to the ``Store`` it wraps, and falls back to the ``Store``
for any timestamp its bulk query cannot answer (a quiet pool whose as-of row is older than the bulk range).

The engine and the service run unchanged on top of it, so every point of a range follows exactly the rules of
``/prices/{asset}/at``. The answers are the same rows under the same tie rules, and the 24 h volume is an exact
decimal sum in both paths (``Store.probe``), so no number depends on the path. Not used in legacy mode.
"""

from __future__ import annotations

import bisect
import threading
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

from app.v3.store import _SMALL_DATASET_ROWS, VOLUME_DECIMAL, Probe, Store

_DAY = timedelta(days=1)
_QUIET_LOOKBACK = timedelta(days=30)   # as-of rows of pools without a swap in the 24 h before a point
_WINDOW_MARGIN = timedelta(hours=12)   # widest VWMP half-window (day granularity; R1 steps are narrower)
_MAX_PREFETCH_ROWS = 1_000_000         # above this expected size, windows are read one by one
_MAX_STATS_ROWS = 5_000                # S_stat windows expected larger than this stay in DuckDB (a sort per point)
_MAX_ROWS_PER_POINT = 2_000            # a bulk read of more rows than this per timestamp costs more than one query
                                       # per point (e.g. daily points on the ETH/USDC pool: ~10 000 swaps apart)
_MISSING = object()  # the bulk query has no answer for this timestamp: ask the Store


class BatchStore:
    """A ``Store`` that answers ``as_of`` and ``probe`` at a known set of timestamps in bulk."""

    def __init__(self, store: Store, stamps: Iterable[datetime]):
        self._store = store
        self._points = sorted(set(stamps))
        self._known = set(self._points)
        self._tables: dict[tuple, dict[datetime, Any]] = {}
        self._locks: dict[tuple, threading.Lock] = {}
        self._guard = threading.Lock()
        self.bulk_queries = 0

    def __getattr__(self, name: str) -> Any:  # everything not answered in bulk
        return getattr(self._store, name)

    # ------------------------------------------------------------------ engine API
    def as_of(self, rel: str, t: datetime, cols: list[str], phase: int | None = None) -> dict[str, Any] | None:
        if t in self._known:
            hit = self._table(("as_of", rel, tuple(cols), phase),
                              lambda: self._bulk_as_of(rel, cols, phase) or {}).get(t, _MISSING)
            if hit is not _MISSING:
                return dict(hit) if hit is not None else None
        return self._store.as_of(rel, t, cols, phase)

    def probe(self, rel: str, t: datetime, cols: list[str], vol_col: str | None) -> Probe:
        if t in self._known:
            hit = self._table(("probe", rel, tuple(cols), vol_col), lambda: self._bulk_probe(rel, cols, vol_col)).get(t, _MISSING)
            if hit is not _MISSING:
                row, n_same, n24, vol24 = hit
                return Probe(dict(row) if row is not None else None, n_same, n24, vol24)
        return self._store.probe(rel, t, cols, vol_col)

    def window(self, rel: str, start: datetime, end: datetime, cols: list[str], limit: int | None = None,
               served: bool = False) -> list[dict[str, Any]]:
        """The rows of ``[start, end)`` sliced from one read of the whole range (same rows, same order as
        ``Store.window``); a window outside it, a truncated or phase-filtered read goes to the Store."""
        if limit is None and not served:
            rows = self._table(("window", rel, tuple(cols)), lambda: self._prefetch(rel, cols))
            if rows is not None and rows["lo"] <= start and end <= rows["hi"]:
                i, j = bisect.bisect_left(rows["ts"], start), bisect.bisect_left(rows["ts"], end)
                return [dict(r) for r in rows["rows"][i:j]]
        return self._store.window(rel, start, end, cols, limit, served=served)

    def price_stats(self, rel: str, col: str, start: datetime, end: datetime, limit: int | None = None,
                    served: bool = False) -> tuple[int, float | None, float | None]:
        """``Store.price_stats`` (n, upper median, MAD of ``col`` over ``[start, end)``) computed in Python from one
        read of the whole range, for windows small enough to sort per point; others go to the Store."""
        d = self._store.dataset(rel)
        window = self._store.cfg.confidence_v2.s_stat.window_seconds
        if limit is None and d is not None and d.min_ts is not None and d.max_ts is not None:
            span = (d.max_ts - d.min_ts).total_seconds()
            if span > 0 and d.n_rows * window / span <= _MAX_STATS_ROWS:
                data = self._table(("stats", rel, col, served), lambda: self._prefetch_stats(rel, col, served, window))
                if data is not None and data["lo"] <= start and end <= data["hi"]:
                    i, j = bisect.bisect_left(data["ts"], start), bisect.bisect_left(data["ts"], end)
                    prices = sorted(p for p in data["px"][i:j] if p is not None)
                    n = len(prices)
                    if n == 0:
                        return 0, None, None
                    med = prices[n // 2]
                    mad = sorted(abs(p - med) for p in prices)[n // 2]
                    return n, float(med), float(mad)
        return self._store.price_stats(rel, col, start, end, limit, served=served)

    # ------------------------------------------------------------------- internals
    def _worth_it(self, d, lo: datetime, hi: datetime, points: list[datetime]) -> bool:
        """True when the rows of ``[lo, hi]`` expected from the file's average density are few enough per timestamp
        for one bulk read to beat one query per point."""
        span = (d.max_ts - d.min_ts).total_seconds() if d.max_ts and d.min_ts else 0.0
        expected = d.n_rows * (hi - lo).total_seconds() / span if span > 0 else d.n_rows
        return expected <= _MAX_ROWS_PER_POINT * max(1, len(points))

    def _table(self, key: tuple, build: Callable[[], dict[datetime, Any]]) -> dict[datetime, Any]:
        table = self._tables.get(key)
        if table is not None:
            return table
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:  # one thread builds, the others wait for it
            if key not in self._tables:
                self._tables[key] = build()
                self.bulk_queries += 1
            return self._tables[key]

    def _bulk_as_of(self, rel: str, cols: list[str], phase: int | None, points: list[datetime] | None = None,
                    lookback: timedelta | None = _DAY) -> dict[datetime, Any] | None:
        """As-of row at every timestamp (``Store.as_of`` semantics), found among the rows of ``[first - lookback,
        last]`` (the whole file when ``lookback`` is None). A timestamp without a row in that range is left out.
        None: not read in bulk (too many rows per timestamp); every timestamp is left to the Store."""
        st, d = self._store, self._store.dataset(rel)
        points = points if points is not None else self._points
        if d is None or not d.files or d.min_ts is None:
            return {t: None for t in points}
        select = list(dict.fromkeys(["ts", *cols]))
        if phase is not None:
            where, args, tie = " AND phase = ?", [phase], "agg_round DESC NULLS LAST, rn ASC"
        else:
            where, args, tie = "", [], st._tie_order(d)
        lo, hi = (points[0] - lookback if lookback is not None else d.min_ts), points[-1]
        if not self._worth_it(d, lo, hi, points):
            return None
        sql = f"""
            WITH pts AS (SELECT unnest(?::TIMESTAMPTZ[]) AS t),
            d AS (SELECT DISTINCT ts FROM {d.view} WHERE ts >= ? AND ts <= ?{where}),
            a AS (SELECT p.t, d.ts AS ats FROM pts p ASOF LEFT JOIN d ON p.t >= d.ts),
            r AS (SELECT {', '.join(select)}, row_number() OVER (PARTITION BY ts ORDER BY {tie}) AS k__
                  FROM {d.view} WHERE ts >= ? AND ts <= ?{where} AND ts IN (SELECT ats FROM a))
            SELECT a.t, r.* EXCLUDE (k__) FROM a LEFT JOIN r ON r.ts = a.ats AND r.k__ = 1
        """
        rows = st._cursor().execute(sql, [points, lo, hi, *args, lo, hi, *args]).fetchall()
        out: dict[datetime, Any] = {}
        for t, *values in rows:
            if values[0] is not None:  # otherwise nothing in the bulk range: the Store's look-back ladder answers
                out[t] = dict(zip(select, values))
        return out

    def _bulk_probe(self, rel: str, cols: list[str], vol_col: str | None) -> dict[datetime, Any]:
        """``Store.probe`` at every timestamp: counts and exact decimal volume sums of ``[t - 24 h, t]`` from cumulative
        sums per timestamp, and the as-of row with the rows sharing its timestamp. A timestamp without any row in its
        24 h is left to the Store (its as-of row comes from the longer look-backs)."""
        st, d = self._store, self._store.dataset(rel)
        if d is None or not d.files or d.min_ts is None:
            return {t: (None, None, 0, None) for t in self._points}
        select = list(dict.fromkeys(["ts", *cols]))
        tie = st._tie_order(d)
        keys = [k.split()[0] for k in tie.split(", ")]
        needed = ", ".join(dict.fromkeys([*select, *keys, *([vol_col] if vol_col else [])]))
        dec = VOLUME_DECIMAL.format(col=vol_col) if vol_col else "CAST(NULL AS DECIMAL(38, 10))"
        vol, count_vol = f"sum({dec})", f"count({dec})"
        lo, hi = self._points[0] - timedelta(hours=24), self._points[-1]
        if not self._worth_it(d, lo, hi, self._points):
            return {}  # every timestamp asks the Store
        sql = f"""
            WITH pts AS (SELECT unnest(?::TIMESTAMPTZ[]) AS t),
            raw AS MATERIALIZED (SELECT {needed} FROM {d.view} WHERE ts >= ? AND ts <= ?),
            agg AS (SELECT ts, count(*) AS c, {count_vol} AS cv, {vol} AS s FROM raw GROUP BY ts),
            cum AS (SELECT ts, c, sum(c) OVER w AS cc, sum(cv) OVER w AS ccv, sum(coalesce(s, 0)) OVER w AS cs
                    FROM agg WINDOW w AS (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)),
            up AS (SELECT p.t, cum.ts AS ats, cum.c, cum.cc, cum.ccv, cum.cs FROM pts p ASOF LEFT JOIN cum ON p.t >= cum.ts),
            dn AS (SELECT p.t, cum.cc, cum.ccv, cum.cs FROM pts p ASOF LEFT JOIN cum
                   ON p.t - INTERVAL 24 HOURS > cum.ts),
            top AS (SELECT {', '.join(select)}, row_number() OVER (PARTITION BY ts ORDER BY {tie}) AS k__
                    FROM raw WHERE ts IN (SELECT ats FROM up))
            SELECT up.t, coalesce(up.cc, 0) - coalesce(dn.cc, 0), coalesce(up.ccv, 0) - coalesce(dn.ccv, 0),
                   CAST(coalesce(up.cs, 0) - coalesce(dn.cs, 0) AS DOUBLE), up.c, top.* EXCLUDE (k__)
            FROM up LEFT JOIN dn ON dn.t = up.t LEFT JOIN top ON top.ts = up.ats AND top.k__ = 1
        """
        rows = st._cursor().execute(sql, [self._points, lo, hi]).fetchall()
        out: dict[datetime, Any] = {}
        quiet: list[datetime] = []
        for t, n24, n_vol, vol24, n_same, *values in rows:
            if not n24:
                quiet.append(t)  # no row in [t - 24 h, t]: the as-of row comes from a longer look-back
                continue
            out[t] = (dict(zip(select, values)), int(n_same), int(n24), float(vol24) if n_vol else None)
        # Store.probe then reads the as-of row with the 30-day and unbounded look-backs (n_same unknown, no volume):
        # the same two steps in bulk (a small file is read whole at once, like Store._lookbacks does).
        steps = [None] if d.n_rows <= _SMALL_DATASET_ROWS else [_QUIET_LOOKBACK]
        if d.n_rows <= _MAX_PREFETCH_ROWS and steps[0] is not None:
            steps.append(None)
        for back in steps:
            if not quiet:
                break
            older = self._bulk_as_of(rel, cols, None, sorted(quiet), back)
            if older is None:
                break  # not read in bulk: the remaining timestamps ask the Store
            for t in quiet:
                if t in older:
                    out[t] = (older[t], None, 0, None)
                elif back is None:
                    out[t] = (None, None, 0, None)  # nothing at or before t in the whole file
            quiet = [t for t in quiet if t not in out]
        return out

    def _prefetch_stats(self, rel: str, col: str, served: bool, window: float) -> dict[str, Any] | None:
        """``col`` over ``[first - S_stat window, last)`` in time order (phase-served Chainlink rounds only if
        ``served``), for ``price_stats``."""
        lo, hi = self._points[0] - timedelta(seconds=window), self._points[-1]
        rows = self._store.window(rel, lo, hi, [col], served=served)
        return {"lo": lo, "hi": hi, "ts": [r["ts"] for r in rows], "px": [r[col] for r in rows]}

    def _prefetch(self, rel: str, cols: list[str]) -> dict[str, Any] | None:
        """Every row of ``[first - 12 h, last + 12 h)`` in ``Store.window`` order, or None when it would be too many."""
        st, d = self._store, self._store.dataset(rel)
        if d is None or not d.files or d.min_ts is None or d.max_ts is None:
            return None
        lo, hi = self._points[0] - _WINDOW_MARGIN, self._points[-1] + _WINDOW_MARGIN
        span = (d.max_ts - d.min_ts).total_seconds()
        if span > 0 and d.n_rows * (hi - lo).total_seconds() / span > _MAX_PREFETCH_ROWS:
            return None
        rows = st.window(rel, lo, hi, cols)
        return {"lo": lo, "hi": hi, "rows": rows, "ts": [r["ts"] for r in rows]}
