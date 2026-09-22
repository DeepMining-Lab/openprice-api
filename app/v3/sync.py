"""CSV -> canonical Parquet store for API V3.

The CSV files are READ-ONLY inputs (they are appended to daily by the extraction
containers). This module derives a query-friendly copy under ``v3.parquet_root``:

* one directory per dataset, made of immutable, time-sorted *segments*;
* every segment is sorted by ``(ts, rn)`` where ``rn`` is the original CSV row
  number, so the CSV scan order (which V1/V2 relied on for same-timestamp ties)
  is reproduced by construction;
* swap events are de-duplicated on ``(tx_hash, log_index)`` keeping the first row
  (the CSV itself is never modified);
* the sync is incremental: only the bytes appended since the last run are read,
  after verifying a fingerprint of what was already consumed. Any mismatch
  (rewritten header, truncated or rewritten file) triggers a full rebuild of that
  dataset only;
* a partial last line (writer in the middle of an append) is never consumed;
* ``manifest.json`` is swapped atomically; the API reloads it when it changes.

Usage:  python -m app.v3.sync [--rebuild] [--only SUBSTR] [--dry-run]
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from app import csv_adapter, registry
from app.config import AppConfig, get_config

SCHEMA_VERSION = 1
FINGERPRINT_BYTES = 65536
ROW_GROUP_SIZE = 50_000
_BLOCK_TS_CANDIDATES = ("block_timestamp_utc", "block_timestamp", "block_time")
_QUOTE_SYMBOL_COLUMNS = ("quote_token_symbol", "quote_asset", "quote_currency")

# Same reader options as V1/V2 (duckdb_client._CSV_OPTS) + all_varchar: values are
# cast explicitly below, so a malformed cell becomes NULL instead of dropping the row.
_CSV_OPTS = (
    "delim=',', header=true, max_line_size=102400, strict_mode=false, "
    "null_padding=true, ignore_errors=true, all_varchar=true"
)

# canonical (csv_adapter) name -> Parquet column name
CANON_TO_PQ: dict[str, str] = {
    "timestamp": "ts",
    "price_usd": "px_usd",
    "price_token_eth": "px_token_eth",
    "price_inverse_eth": "px_inv_eth",
    "volume_usd": "vol_usd",
    "volume_token": "vol_token",
    "tvl_usd": "tvl",
    "slippage": "slip",
    "block_number": "block_number",
}


def _q(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# Paths / manifest
# ---------------------------------------------------------------------------

def dataset_dir(root: Path, rel: str) -> Path:
    return root / rel.replace("/", "__").removesuffix(".csv")


def manifest_path(root: Path) -> Path:
    return root / "manifest.json"


def load_manifest(root: Path) -> dict[str, Any]:
    p = manifest_path(root)
    if not p.exists():
        return {"schema_version": SCHEMA_VERSION, "version": None, "datasets": {}}
    return json.loads(p.read_text())


def _version_of(datasets: dict[str, Any]) -> str:
    stable = {k: (v["segments"], v["csv_offset"], v["n_rows"]) for k, v in sorted(datasets.items())}
    return hashlib.sha1(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:16]


def _write_manifest(root: Path, datasets: dict[str, Any]) -> dict[str, Any]:
    m = {
        "schema_version": SCHEMA_VERSION,
        "version": _version_of(datasets),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "datasets": datasets,
    }
    tmp = manifest_path(root).with_suffix(".json.tmp")
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True))
    os.replace(tmp, manifest_path(root))
    return m


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _sha1(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def _read_header_line(path: Path) -> bytes:
    with path.open("rb") as f:
        return f.readline()


def _fingerprint(path: Path, end: int) -> str:
    """sha1 of the last FINGERPRINT_BYTES bytes before ``end`` (detects rewrites)."""
    start = max(0, end - FINGERPRINT_BYTES)
    with path.open("rb") as f:
        f.seek(start)
        return _sha1(f.read(end - start))


def _last_newline_end(path: Path, size: int) -> int:
    """Offset just after the last '\\n' located before ``size`` (0 if none)."""
    with path.open("rb") as f:
        pos = size
        while pos > 0:
            step = min(1 << 16, pos)
            f.seek(pos - step)
            chunk = f.read(step)
            i = chunk.rfind(b"\n")
            if i != -1:
                return pos - step + i + 1
            pos -= step
    return 0


def _canonical_select(schema: csv_adapter.SchemaInfo, rn_base: int) -> str:
    m, raw = schema.mapping, schema.raw_columns

    def col(canon: str, cast: str) -> str:
        c = m.get(canon)
        return f"TRY_CAST({_q(c)} AS {cast})" if c else f"CAST(NULL AS {cast})"

    def rawcol(names: tuple[str, ...], cast: str) -> str:
        c = next((n for n in names if n in raw), None)
        return f"TRY_CAST({_q(c)} AS {cast})" if c else f"CAST(NULL AS {cast})"

    qs = next((n for n in _QUOTE_SYMBOL_COLUMNS if n in raw), None)
    exprs = {
        "ts": col("timestamp", "TIMESTAMPTZ"),
        "rn": f"CAST({rn_base} + row_number() OVER () AS BIGINT)",
        "px_usd": col("price_usd", "DOUBLE"),
        "px_token_eth": col("price_token_eth", "DOUBLE"),
        "px_inv_eth": col("price_inverse_eth", "DOUBLE"),
        "vol_usd": col("volume_usd", "DOUBLE"),
        "vol_token": col("volume_token", "DOUBLE"),
        "tvl": col("tvl_usd", "DOUBLE"),
        "slip": col("slippage", "DOUBLE"),
        "block_number": col("block_number", "BIGINT"),
        "block_ts": rawcol(_BLOCK_TS_CANDIDATES, "TIMESTAMPTZ"),
        "log_index": rawcol(("log_index",), "INTEGER"),
        "tx_hash": _q("transaction_hash") if "transaction_hash" in raw else "CAST(NULL AS VARCHAR)",
        "quote_symbol": f"upper(trim({_q(qs)}))" if qs else "CAST(NULL AS VARCHAR)",
    }
    return ", ".join(f"{e} AS {k}" for k, e in exprs.items())


# ---------------------------------------------------------------------------
# Per-dataset sync
# ---------------------------------------------------------------------------

@dataclass
class SyncResult:
    rel: str
    action: str          # unchanged | append | rebuild | missing
    new_rows: int = 0
    dups_removed: int = 0
    seconds: float = 0.0


def _connect(root: Path, cfg: AppConfig) -> duckdb.DuckDBPyConnection:
    tmp = root / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, cfg.v3.duckdb_threads)}; SET memory_limit='8GB'; "
                f"SET temp_directory='{tmp}/duck'; SET TimeZone='UTC'")
    return con


def _needs_full(prev: dict[str, Any] | None, csv_path: Path, size: int, header_sha: str) -> bool:
    if prev is None or prev.get("csv_header_sha1") != header_sha:
        return True
    off = prev["csv_offset"]
    if size < off:
        return True
    return _fingerprint(csv_path, off) != prev["csv_fingerprint"]


def _write_segment(con, sql_select: str, dest: Path) -> int:
    tmp = dest.with_suffix(".parquet.tmp")
    con.execute(f"COPY ({sql_select}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE})")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    os.replace(tmp, dest)
    return n


def sync_dataset(
    con: duckdb.DuckDBPyConnection, cfg: AppConfig, rel: str, prev: dict[str, Any] | None, force_rebuild: bool = False,
) -> tuple[dict[str, Any] | None, SyncResult]:
    t0 = time.perf_counter()
    root = cfg.v3.parquet_path
    csv_path = cfg.paths.datasets_path / rel
    if not csv_path.exists():
        return prev, SyncResult(rel, "missing")

    st = csv_path.stat()
    size = st.st_size
    header_line = _read_header_line(csv_path)
    header_sha = _sha1(header_line)
    end = _last_newline_end(csv_path, size)
    # Fingerprint taken BEFORE reading: it describes the bytes we are about to consume.
    fp_before = _fingerprint(csv_path, end)
    full = force_rebuild or _needs_full(prev, csv_path, size, header_sha)
    if not full and end <= prev["csv_offset"]:
        return prev, SyncResult(rel, "unchanged", seconds=time.perf_counter() - t0)

    ddir = dataset_dir(root, rel)
    ddir.mkdir(parents=True, exist_ok=True)
    schema = csv_adapter.inspect(csv_path)
    raw_columns = schema.raw_columns

    if full:
        rn_base, segments_prev, existing = 0, [], []
        src_path = csv_path
        snapshot = None
        # Files that can change under us (in-place appends) are small: snapshot the
        # consistent prefix. Large files are replaced atomically by the extractor.
        if size < (1 << 30):
            snapshot = root / ".tmp" / f"{ddir.name}.full.csv"
            with csv_path.open("rb") as fi, snapshot.open("wb") as fo:
                remaining = end
                while remaining > 0:
                    b = fi.read(min(1 << 20, remaining)); fo.write(b); remaining -= len(b)
            src_path = snapshot
    else:
        rn_base, segments_prev = prev["n_parsed"], list(prev["segments"])
        existing = [str(ddir / s["file"]) for s in segments_prev]
        snapshot = root / ".tmp" / f"{ddir.name}.tail.csv"
        with csv_path.open("rb") as fi, snapshot.open("wb") as fo:
            fo.write(header_line)
            fi.seek(prev["csv_offset"])
            remaining = end - prev["csv_offset"]
            while remaining > 0:
                b = fi.read(min(1 << 20, remaining)); fo.write(b); remaining -= len(b)
        src_path = snapshot

    sel = _canonical_select(schema, rn_base)
    con.execute(f"CREATE OR REPLACE TEMP TABLE t AS SELECT {sel} FROM read_csv('{src_path}', {_CSV_OPTS})")
    n_parsed_new = con.execute("SELECT count(*) FROM t").fetchone()[0]

    # A full rebuild reads the live file directly when it is large: drop a trailing
    # partial line, which is always the last parsed row.
    if full and snapshot is None and end < size and n_parsed_new > 0:
        con.execute("DELETE FROM t WHERE rn = (SELECT max(rn) FROM t)")
        n_parsed_new -= 1

    # Drop rows with an unparsable timestamp, then de-duplicate swap events.
    con.execute("CREATE OR REPLACE TEMP TABLE v AS SELECT * FROM t WHERE ts IS NOT NULL")
    n_valid = con.execute("SELECT count(*) FROM v").fetchone()[0]
    if not full and existing and n_valid:
        # Events already stored (boundary overlap between two extractions).
        min_ts = con.execute("SELECT min(ts) FROM v").fetchone()[0]
        con.execute(
            f"DELETE FROM v WHERE tx_hash IS NOT NULL AND log_index IS NOT NULL AND (tx_hash, log_index) IN "
            f"(SELECT (tx_hash, log_index) FROM read_parquet({existing!r}) WHERE ts >= ?::TIMESTAMPTZ - INTERVAL 1 DAY)",
            [min_ts],
        )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE k AS SELECT * EXCLUDE (dup_rk) FROM ("
        " SELECT *, CASE WHEN tx_hash IS NOT NULL AND log_index IS NOT NULL"
        "  THEN row_number() OVER (PARTITION BY tx_hash, log_index ORDER BY rn) ELSE 1 END AS dup_rk FROM v)"
        " WHERE dup_rk = 1"
    )
    n_kept = con.execute("SELECT count(*) FROM k").fetchone()[0]
    dups = n_valid - n_kept

    seq = 1 + max([int(s["file"][4:10]) for s in segments_prev], default=0)
    segments = segments_prev
    if n_kept:
        seg_name = f"seg-{seq:06d}.parquet"
        _write_segment(con, "SELECT * FROM k ORDER BY ts, rn", ddir / seg_name)
        segments = segments_prev + [{"file": seg_name, "rows": n_kept}]

    # Compaction: merge many small daily segments into one.
    if len(segments) > cfg.v3.max_segments_before_compaction:
        files = [str(ddir / s["file"]) for s in segments]
        seg_name = f"seg-{seq + 1:06d}.parquet"
        n = _write_segment(con, f"SELECT * FROM read_parquet({files!r}) ORDER BY ts, rn", ddir / seg_name)
        segments = [{"file": seg_name, "rows": n}]

    files_now = [str(ddir / s["file"]) for s in segments]
    n_rows, lo, hi = (0, None, None)
    if files_now:
        n_rows, lo, hi = con.execute(f"SELECT count(*), min(ts), max(ts) FROM read_parquet({files_now!r})").fetchone()

    if snapshot is not None and snapshot.exists():
        snapshot.unlink()

    state = {
        "csv_name": csv_path.name,
        "raw_columns": raw_columns,
        "mapping": schema.mapping,
        "tvl_unit": schema.tvl_unit,
        "csv_header_sha1": header_sha,
        "csv_offset": end,
        "csv_fingerprint": fp_before,
        "n_parsed": rn_base + n_parsed_new,
        "n_rows": int(n_rows),
        "dups_removed": (0 if full else prev.get("dups_removed", 0)) + dups,
        "min_ts": lo.isoformat() if lo else None,
        "max_ts": hi.isoformat() if hi else None,
        "segments": segments,
    }
    # A live file that changed while we were converting it is picked up by the next run.
    action = "rebuild" if full else "append"
    return state, SyncResult(rel, action, new_rows=n_kept, dups_removed=dups, seconds=time.perf_counter() - t0)


def all_relative_paths() -> list[str]:
    rels: list[str] = []
    for a in registry.SUPPORTED_ASSETS:
        for _, r in registry.all_relative_paths(a):
            if r not in rels:
                rels.append(r)
    for r in registry.PEG_FEEDS.values():
        if r not in rels:
            rels.append(r)
    return rels


def _cleanup_orphans(root: Path, manifest: dict[str, Any], grace_seconds: int = 3600) -> None:
    keep = {
        str(dataset_dir(root, rel) / s["file"]) for rel, d in manifest["datasets"].items() for s in d["segments"]
    }
    now = time.time()
    for seg in root.glob("*/seg-*.parquet"):
        if str(seg) not in keep and now - seg.stat().st_mtime > grace_seconds:
            seg.unlink()


def run_sync(cfg: AppConfig | None = None, only: str | None = None, rebuild: bool = False,
             dry_run: bool = False) -> list[SyncResult]:
    cfg = cfg or get_config()
    root = cfg.v3.parquet_path
    root.mkdir(parents=True, exist_ok=True)
    results: list[SyncResult] = []
    with (root / ".sync.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another sync is already running")
        manifest = load_manifest(root)
        datasets = dict(manifest["datasets"])
        con = _connect(root, cfg)
        changed = False
        for rel in all_relative_paths():
            if only and only not in rel:
                continue
            if dry_run:
                continue
            state, res = sync_dataset(con, cfg, rel, datasets.get(rel), force_rebuild=rebuild)
            results.append(res)
            if state is not None and res.action in ("append", "rebuild"):
                datasets[rel] = state
                changed = True
                # Publish progressively so a crash never loses converted datasets.
                _write_manifest(root, datasets)
        if changed:
            manifest = _write_manifest(root, datasets)
        _cleanup_orphans(root, manifest)
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync the V3 Parquet store from the CSV datasets.")
    ap.add_argument("--rebuild", action="store_true", help="force a full rebuild of the selected datasets")
    ap.add_argument("--only", help="only datasets whose relative path contains this text")
    args = ap.parse_args()
    for r in run_sync(only=args.only, rebuild=args.rebuild):
        print(f"{r.action:9s} {r.rel:40s} +{r.new_rows:>9,} rows  dups_removed={r.dups_removed:<4d} {r.seconds:6.1f}s", flush=True)


if __name__ == "__main__":
    main()
