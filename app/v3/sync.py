"""CSV -> canonical Parquet store for API V3.

The CSV files are READ-ONLY inputs (they are appended to daily by the extraction
containers). This module derives a query-friendly copy under ``v3.parquet_root``:

* one directory per dataset, made of immutable, time-sorted *segments*;
* every segment is sorted by ``(ts, rn)`` where ``rn`` is the original CSV row
  number, so the CSV scan order (which V1/V2 relied on for same-timestamp ties)
  can still be replayed;
* events are de-duplicated keeping the first row: swaps on ``(tx_hash, log_index)``,
  Chainlink rounds on ``(phase, aggregator_round)`` (the CSV itself is never modified);
* the sync is incremental: only the bytes appended since the last run are read,
  after verifying what was already consumed (header, last 64 KB, and one 4 KB sample
  every 256 MB). Any mismatch triggers a full rebuild of that dataset only;
  ``--verify`` also compares a full SHA-256 of the consumed prefix;
* a partial last line (writer in the middle of an append) is never consumed;
* every line is accounted for (``quality`` in the manifest): physical lines, rows read,
  lines dropped by the tolerant reader, structural anomalies found by a strict pass,
  unreadable timestamps (repeated headers), unreadable prices (row dropped, as V1/V2 did),
  other cells that could not be converted (kept as NULL), duplicates removed;
* for Chainlink feeds, the proxy phase switches are read from the chain (see
  ``app.v3.chainlink_phases``) when ``$<v3.rpc_url_env>`` is set;
* the extraction head of every file (how far the extractor had scanned the chain on its last run, read from the
  ``extraction_timestamp_utc`` / ``node_head_block_at_extraction`` columns of the last rows) is kept in the manifest:
  it is the data coverage the API reports (``beyond_data_coverage``). A Chainlink file is also compared with the
  proxy's latest round at the chain head on every run (``oracle_complete_until_utc``): holding that round, it is
  complete up to the head even when it was extracted hours earlier;
* every dataset also gets a native DuckDB copy of its segments (``native-<hash>.duckdb``, one table in (ts, rn)
  order, ``v3.native_store``): the API reads it 2 to 3 times faster than the Parquet files. It is derived from the
  segments only, named after them (a new name whenever they change) and written once, atomically; the Parquet
  segments stay the reference, and the API falls back to them when the copy is missing or stale;
* ``manifest.json`` is swapped atomically and every published version is also kept in
  ``history/<version>.json.gz``; ``sync_log.jsonl`` records each rebuild, append and phase
  switch with its reason. The API reloads the manifest when it changes.

Usage:  python -m app.v3.sync [--rebuild] [--verify] [--only SUBSTR]
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import io
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from app import csv_adapter, registry
from app.config import AppConfig, get_config
from app.v3 import chainlink_phases

SCHEMA_VERSION = 2
FINGERPRINT_BYTES = 65536
SAMPLE_BYTES = 4096
SAMPLE_STRIDE = 256 << 20
MAX_QUALITY_SAMPLES = 20
ROW_GROUP_SIZE = 50_000
_BLOCK_TS_CANDIDATES = ("block_timestamp_utc", "block_timestamp", "block_time")
_QUOTE_SYMBOL_COLUMNS = ("quote_token_symbol", "quote_asset", "quote_currency")
_PRICE_COLUMNS = ("px_usd", "px_token_eth", "px_inv_eth")

# Same reader options as V1/V2 (duckdb_client._CSV_OPTS) + all_varchar: values are
# cast explicitly below, so a malformed cell becomes NULL instead of dropping the row.
_CSV_OPTS = (
    "delim=',', header=true, max_line_size=102400, strict_mode=false, "
    "null_padding=true, ignore_errors=true, all_varchar=true"
)
# The same tolerant read with the header's columns given explicitly (no dialect sniffing): a malformed line that
# falls in DuckDB's sniffing sample (e.g. longer than max_line_size) is then one rejected line, not a failed read.
_CSV_OPTS_EXPLICIT = (
    "delim=',', quote='\"', escape='\"', header=true, auto_detect=false, max_line_size=102400, strict_mode=false, "
    "null_padding=true, ignore_errors=true"
)
# DuckDB fills a rejects table only for a query that materialises rows, and creates it only when there is a reject.
_TOLERANT_REJECTS = "store_rejects=true, rejects_table='v3_tol_rej', rejects_scan='v3_tol_scan'"
_STRICT_REJECTS = "store_rejects=true, rejects_table='v3_strict_rej', rejects_scan='v3_strict_scan'"

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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Paths / manifest
# ---------------------------------------------------------------------------

def dataset_dir(root: Path, rel: str) -> Path:
    return root / rel.replace("/", "__").removesuffix(".csv")


def manifest_path(root: Path) -> Path:
    return root / "manifest.json"


def history_path(root: Path, version: str) -> Path:
    return root / "history" / f"{version}.json.gz"


def sync_log_path(root: Path) -> Path:
    return root / "sync_log.jsonl"


def load_manifest(root: Path) -> dict[str, Any]:
    p = manifest_path(root)
    if not p.exists():
        return {"schema_version": SCHEMA_VERSION, "version": None, "datasets": {}}
    return json.loads(p.read_text())


def load_history(root: Path, version: str) -> dict[str, Any] | None:
    if not version.isalnum():
        return None
    p = history_path(root, version)
    if not p.exists():
        return None
    with gzip.open(p, "rt") as f:
        return json.load(f)


def file_version(d: dict[str, Any]) -> str:
    """Version of one dataset: its data (segments, consumed CSV bytes, rows) and, for Chainlink, its phase switches."""
    switches = (d.get("chainlink") or {}).get("switches") or []
    stable = [d["segments"], d["csv_offset"], d["n_rows"], [[s["phase"], s["block"]] for s in switches]]
    return hashlib.sha1(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:16]


def _version_of(datasets: dict[str, Any]) -> str:
    stable = {k: v["file_version"] for k, v in sorted(datasets.items())}
    return hashlib.sha1(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:16]


def _write_manifest(root: Path, datasets: dict[str, Any]) -> dict[str, Any]:
    for d in datasets.values():
        d["file_version"] = file_version(d)
    m = {
        "schema_version": SCHEMA_VERSION if all(d.get("schema") == SCHEMA_VERSION for d in datasets.values()) else 1,
        "version": _version_of(datasets),
        "generated_at": _now(),
        "datasets": datasets,
    }
    hist = history_path(root, m["version"])
    if not hist.exists():
        hist.parent.mkdir(parents=True, exist_ok=True)
        tmp_h = hist.with_suffix(".tmp")
        with gzip.open(tmp_h, "wt") as f:
            json.dump(m, f, sort_keys=True)
        os.replace(tmp_h, hist)
    tmp = manifest_path(root).with_suffix(".json.tmp")
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True))
    os.replace(tmp, manifest_path(root))
    return m


def _append_log(root: Path, entries: list[dict[str, Any]]) -> None:
    if entries:
        with sync_log_path(root).open("a") as f:
            for e in entries:
                f.write(json.dumps(e, sort_keys=True, default=str) + "\n")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _sha1(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def _read_header_line(path: Path) -> bytes:
    with path.open("rb") as f:
        return f.readline()


def _header_names(header_line: bytes) -> list[str]:
    """Every field of the header, stripped like ``duckdb_client.describe_csv`` (empty names get a placeholder)."""
    fields = next(csv.reader(io.StringIO(header_line.decode("utf-8", errors="replace").rstrip("\r\n"))), [])
    return [f.strip() or f"_unnamed_{i}" for i, f in enumerate(fields)]


def _columns_sql(names: list[str]) -> str:
    return "{" + ", ".join(f"'{n.replace(chr(39), chr(39) * 2)}': 'VARCHAR'" for n in names) + "}"


def _fingerprint(path: Path, end: int) -> str:
    """sha1 of the last FINGERPRINT_BYTES bytes before ``end`` (detects rewrites)."""
    start = max(0, end - FINGERPRINT_BYTES)
    with path.open("rb") as f:
        f.seek(start)
        return _sha1(f.read(end - start))


def _samples(path: Path, start: int, end: int) -> list[list]:
    """[position, sha1] of a SAMPLE_BYTES block at every multiple of SAMPLE_STRIDE in [start, end - SAMPLE_BYTES]."""
    out = []
    first = -(-start // SAMPLE_STRIDE) * SAMPLE_STRIDE
    with path.open("rb") as f:
        for pos in range(first, end - SAMPLE_BYTES + 1, SAMPLE_STRIDE):
            f.seek(pos)
            out.append([pos, _sha1(f.read(SAMPLE_BYTES))])
    return out


def _samples_match(path: Path, samples: list[list]) -> bool:
    with path.open("rb") as f:
        for pos, sha in samples:
            f.seek(pos)
            if _sha1(f.read(SAMPLE_BYTES)) != sha:
                return False
    return True


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


def _scan_prefix(path: Path, end: int, check_offset: int | None = None) -> tuple[int, str, str | None]:
    """One pass over [0, end): (newline count, sha256 of [0, end), sha256 of [0, check_offset) or None)."""
    h = hashlib.sha256()
    lines, pos, at_check = 0, 0, None
    with path.open("rb") as f:
        while pos < end:
            n = min(8 << 20, end - pos)
            if check_offset is not None and at_check is None and pos < check_offset <= pos + n:
                n = check_offset - pos
            b = f.read(n)
            if not b:
                break
            h.update(b)
            lines += b.count(b"\n")
            pos += len(b)
            if check_offset is not None and pos == check_offset:
                at_check = h.copy().hexdigest()
    return lines, h.hexdigest(), at_check


def _copy_range(src: Path, dest: Path, start: int, end: int, prefix: bytes = b"") -> int:
    """Copy [start, end) of ``src`` after ``prefix`` into ``dest``; returns the newline count of the copied range."""
    lines = 0
    with src.open("rb") as fi, dest.open("wb") as fo:
        fo.write(prefix)
        fi.seek(start)
        remaining = end - start
        while remaining > 0:
            b = fi.read(min(1 << 20, remaining))
            fo.write(b)
            lines += b.count(b"\n")
            remaining -= len(b)
    return lines


_HEAD_TAIL_BYTES = 256 << 10
_BLOCK_SECONDS = 12  # post-merge slot: (head - block) x 12 s never overstates the time elapsed (missed slots)


def _ts(v: str | None) -> datetime | None:
    try:
        t = datetime.fromisoformat(v.strip()) if v and v.strip() else None
    except ValueError:
        return None
    return t.replace(tzinfo=timezone.utc) if t is not None and t.tzinfo is None else t


def extraction_head(path: Path, end: int, names: list[str]) -> str | None:
    """Chain time up to which the extractor had scanned when it wrote the last rows of ``[0, end)``: for each of the
    last rows, its ``extraction_timestamp_utc``, lowered to ``block_timestamp_utc + (node_head_block_at_extraction -
    block_number) x 12 s`` when the row is a swap; the latest over the rows of the last run (those with the
    ``extraction_run_id`` of the last row: a backfill inserts older events from a later run in the middle of the file,
    and its extraction time says nothing about the end of the file). None when the file has no extraction columns."""
    col = {n: i for i, n in enumerate(names)}
    if "extraction_timestamp_utc" not in col or end <= 0:
        return None
    start = max(0, end - _HEAD_TAIL_BYTES)
    with path.open("rb") as f:
        f.seek(start)
        lines = f.read(end - start).decode("utf-8", errors="replace").splitlines()
    if start > 0:
        lines = lines[1:]  # partial first line
    rows = [row for row in csv.reader(lines[-500:]) if len(row) == len(names)]
    if rows and "extraction_run_id" in col:
        last_run = rows[-1][col["extraction_run_id"]]
        rows = [row for row in rows if row[col["extraction_run_id"]] == last_run]
    best: datetime | None = None
    for row in rows:
        head = _ts(row[col["extraction_timestamp_utc"]])
        if head is None:
            continue
        try:
            block = int(row[col["block_number"]]) if "block_number" in col else None
            node = int(row[col["node_head_block_at_extraction"]]) if "node_head_block_at_extraction" in col else None
        except ValueError:
            block = node = None
        block_ts = _ts(row[col["block_timestamp_utc"]]) if "block_timestamp_utc" in col else None
        if block is not None and node is not None and block_ts is not None and node >= block:
            head = min(head, block_ts + timedelta(seconds=(node - block) * _BLOCK_SECONDS))
        best = head if best is None or head > best else best
    return best.isoformat() if best else None


def last_round_id(path: Path, end: int, names: list[str]) -> int | None:
    """Highest ``global_round_id`` (``phase << 64 | aggregator round``) among the last rows of ``[0, end)`` of a
    Chainlink file: the latest round the extraction wrote for the most recent phase. None without that column."""
    col = {n: i for i, n in enumerate(names)}
    if "global_round_id" not in col or end <= 0:
        return None
    start = max(0, end - _HEAD_TAIL_BYTES)
    with path.open("rb") as f:
        f.seek(start)
        lines = f.read(end - start).decode("utf-8", errors="replace").splitlines()
    if start > 0:
        lines = lines[1:]  # partial first line
    best: int | None = None
    for row in csv.reader(lines[-500:]):
        if len(row) != len(names):
            continue
        try:
            rid = int(row[col["global_round_id"]])
        except ValueError:
            continue
        best = rid if best is None or rid > best else best
    return best


def _canonical_select(schema: csv_adapter.SchemaInfo, rn_base: int) -> tuple[str, list[str]]:
    """SELECT list of a CSV read: the canonical columns stored in Parquet, then helper columns (names in the
    second return value) used for the quality counters and never stored."""
    m, raw = schema.mapping, schema.raw_columns

    def cast(c: str | None, typ: str) -> str:
        return f"TRY_CAST({_q(c)} AS {typ})" if c else f"CAST(NULL AS {typ})"

    def raw_of(names: tuple[str, ...]) -> str | None:
        return next((n for n in names if n in raw), None)

    qs = raw_of(_QUOTE_SYMBOL_COLUMNS)
    typed = {  # Parquet column -> (raw column, type)
        "ts": (m.get("timestamp"), "TIMESTAMPTZ"),
        "px_usd": (m.get("price_usd"), "DOUBLE"),
        "px_token_eth": (m.get("price_token_eth"), "DOUBLE"),
        "px_inv_eth": (m.get("price_inverse_eth"), "DOUBLE"),
        "vol_usd": (m.get("volume_usd"), "DOUBLE"),
        "vol_token": (m.get("volume_token"), "DOUBLE"),
        "tvl": (m.get("tvl_usd"), "DOUBLE"),
        "slip": (m.get("slippage"), "DOUBLE"),
        "block_number": (m.get("block_number"), "BIGINT"),
        "block_ts": (raw_of(_BLOCK_TS_CANDIDATES), "TIMESTAMPTZ"),
        "log_index": (raw_of(("log_index",)), "INTEGER"),
        "phase": (raw_of(("phase",)), "INTEGER"),
        "agg_round": (raw_of(("aggregator_round",)), "BIGINT"),
    }
    exprs = {k: cast(c, t) for k, (c, t) in typed.items()}
    exprs["rn"] = f"CAST({rn_base} + row_number() OVER () AS BIGINT)"
    exprs["tx_hash"] = _q("transaction_hash") if "transaction_hash" in raw else "CAST(NULL AS VARCHAR)"
    exprs["quote_symbol"] = f"upper(trim({_q(qs)}))" if qs else "CAST(NULL AS VARCHAR)"
    order = ["ts", "rn", "px_usd", "px_token_eth", "px_inv_eth", "vol_usd", "vol_token", "tvl", "slip", "block_number",
             "block_ts", "log_index", "tx_hash", "quote_symbol", "phase", "agg_round"]
    helpers: dict[str, str] = {
        "_ts_raw": _q(m["timestamp"]) if m.get("timestamp") else "CAST(NULL AS VARCHAR)",
        "_proxy": f"lower(trim({_q('feed_proxy_address')}))" if "feed_proxy_address" in raw else "CAST(NULL AS VARCHAR)",
    }
    for k, (c, t) in typed.items():
        if c:  # a non-empty cell that cannot be converted
            helpers[f"_bad_{k}"] = f"({_q(c)} IS NOT NULL AND trim({_q(c)}) <> '' AND TRY_CAST({_q(c)} AS {t}) IS NULL)"
    sel = ", ".join(f"{exprs[k]} AS {k}" for k in order) + ", " + ", ".join(f"{e} AS {k}" for k, e in helpers.items())
    return sel, list(helpers)


_EVENT_KEY = ("CASE WHEN tx_hash IS NOT NULL AND log_index IS NOT NULL THEN tx_hash || ':' || CAST(log_index AS VARCHAR) "
              "WHEN phase IS NOT NULL AND agg_round IS NOT NULL THEN 'round:' || CAST(phase AS VARCHAR) || ':' "
              "|| CAST(agg_round AS VARCHAR) END")


def _rejects(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[int, str, str]]:
    """(line, error type, start of the line) of the rejects of the last read, one per line and type."""
    if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [table]).fetchone()[0]:
        return []
    return con.execute(f"SELECT line, error_type, any_value(left(csv_line, 160)) FROM {table} "
                       "GROUP BY line, error_type ORDER BY line").fetchall()


def _drop_rejects(con: duckdb.DuckDBPyConnection, *tables: str) -> None:
    for t in tables:
        con.execute(f"DROP TABLE IF EXISTS {t}")


def _strict_pass(con: duckdb.DuckDBPyConnection, src: Path, names: list[str]) -> list[tuple[int, str, str]]:
    """Structural anomalies of ``src`` under an RFC 4180 parser with the header's columns (nothing is stored)."""
    if not names:
        return []
    opts = (f"delim=',', quote='\"', escape='\"', header=true, columns={_columns_sql(names)}, auto_detect=false, "
            f"max_line_size=102400, strict_mode=true, null_padding=false, {_STRICT_REJECTS}")
    _drop_rejects(con, "v3_strict_rej", "v3_strict_scan")
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE v3_strict AS SELECT {_q(names[0])} AS c FROM read_csv('{src}', {opts})")
    except duckdb.Error as e:  # e.g. mixed line endings: the whole file is anomalous for a strict parser
        first = str(e).strip().splitlines()[0][:160]
        return [(0, "STRICT PARSE FAILED", first)]
    con.execute("DROP TABLE IF EXISTS v3_strict")
    return _rejects(con, "v3_strict_rej")


def _empty_quality() -> dict[str, Any]:
    return {"physical_lines": 0, "rows_read": 0, "reader_dropped": {}, "strict_anomalies": {}, "samples": [],
            "unreadable_timestamp_rows": 0, "repeated_headers": 0, "unreadable_price_rows": 0, "cast_failures": {},
            "duplicates_removed": 0, "unaccounted_lines": 0}


def _merge_quality(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, int):
            out[k] = a.get(k, 0) + v
        elif isinstance(v, dict):
            d = dict(a.get(k, {}))
            for kk, vv in v.items():
                d[kk] = d.get(kk, 0) + vv
            out[k] = d
        elif isinstance(v, list):
            out[k] = (list(a.get(k, [])) + v)[:MAX_QUALITY_SAMPLES]
    return out


def quality_issues(q: dict[str, Any]) -> int:
    """Number of anomalies recorded (lines dropped or malformed, rows or cells that could not be read); 0 = clean."""
    return (sum(q.get("reader_dropped", {}).values()) + sum(q.get("strict_anomalies", {}).values())
            + q.get("unreadable_timestamp_rows", 0) + q.get("unreadable_price_rows", 0)
            + sum(q.get("cast_failures", {}).values()) + abs(q.get("unaccounted_lines", 0)))


# ---------------------------------------------------------------------------
# Per-dataset sync
# ---------------------------------------------------------------------------

@dataclass
class SyncResult:
    rel: str
    action: str          # unchanged | append | rebuild | verified | missing | error
    new_rows: int = 0
    dups_removed: int = 0
    seconds: float = 0.0
    reason: str | None = None
    issues: int = 0      # quality_issues() of the lines read by this run
    notes: list[str] = field(default_factory=list)


def _connect(root: Path, cfg: AppConfig) -> duckdb.DuckDBPyConnection:
    tmp = root / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, cfg.v3.duckdb_threads)}; SET memory_limit='8GB'; "
                f"SET temp_directory='{tmp}/duck'; SET TimeZone='UTC'")
    return con


def _rebuild_reason(prev: dict[str, Any] | None, csv_path: Path, size: int, header_sha: str) -> str | None:
    """Why the dataset cannot be extended incrementally (None when it can)."""
    if prev is None:
        return "first_build"
    if prev.get("schema") != SCHEMA_VERSION:
        return "schema_upgrade"
    if prev.get("csv_header_sha1") != header_sha:
        return "header_changed"
    off = prev["csv_offset"]
    if size < off:
        return "file_truncated"
    if _fingerprint(csv_path, off) != prev["csv_fingerprint"]:
        return "tail_fingerprint_mismatch"
    if not _samples_match(csv_path, prev.get("csv_samples", [])):
        return "sampled_fingerprint_mismatch"
    return None


def _write_segment(con, sql_select: str, dest: Path) -> int:
    tmp = dest.with_suffix(".parquet.tmp")
    con.execute(f"COPY ({sql_select}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE})")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    os.replace(tmp, dest)
    return n


def sync_dataset(
    con: duckdb.DuckDBPyConnection, cfg: AppConfig, rel: str, prev: dict[str, Any] | None,
    force_rebuild: bool = False, verify: bool = False,
) -> tuple[dict[str, Any] | None, SyncResult]:
    t0 = time.perf_counter()
    root = cfg.v3.parquet_path
    csv_path = cfg.paths.datasets_path / rel
    if not csv_path.exists():
        return prev, SyncResult(rel, "missing")

    size = csv_path.stat().st_size
    header_line = _read_header_line(csv_path)
    header_sha = _sha1(header_line)
    end = _last_newline_end(csv_path, size)
    # Fingerprint taken BEFORE reading: it describes the bytes we are about to consume.
    fp_before = _fingerprint(csv_path, end)
    reason = "forced" if force_rebuild else _rebuild_reason(prev, csv_path, size, header_sha)

    # --verify: full SHA-256 of the prefix hashed last time, computed in the same pass as the new one.
    verified: dict[str, Any] | None = None
    if verify and reason is None:
        old = prev.get("csv_sha256") or {}
        _, sha_end, sha_old = _scan_prefix(csv_path, end, old.get("offset"))
        if old and sha_old != old["sha256"]:
            reason = "verify_mismatch"
        else:
            verified = {"offset": end, "sha256": sha_end, "verified_at": _now()}

    full = reason is not None
    if not full and end <= prev["csv_offset"]:
        if verified:
            state = prev if verified == prev.get("csv_sha256") else {**prev, "csv_sha256": verified}
            return state, SyncResult(rel, "verified", seconds=time.perf_counter() - t0)
        return prev, SyncResult(rel, "unchanged", seconds=time.perf_counter() - t0)

    ddir = dataset_dir(root, rel)
    ddir.mkdir(parents=True, exist_ok=True)
    schema = csv_adapter.inspect(csv_path)
    names = _header_names(header_line)
    quality = _empty_quality()

    if full:
        rn_base, segments_prev, existing, lines_before = 0, [], [], 0
        src_path = csv_path
        snapshot = None
        # Files that can change under us (in-place appends) are small: snapshot the
        # consistent prefix. Large files are replaced atomically by the extractor.
        if size < (1 << 30):
            snapshot = root / ".tmp" / f"{ddir.name}.full.csv"
            _copy_range(csv_path, snapshot, 0, end)
            src_path = snapshot
        newlines, sha_end, _ = _scan_prefix(src_path, end)
        quality["physical_lines"] = max(0, newlines - 1)
        verified = {"offset": end, "sha256": sha_end, "verified_at": _now()}
    else:
        rn_base, segments_prev = prev["n_parsed"], list(prev["segments"])
        existing = [str(ddir / s["file"]) for s in segments_prev]
        lines_before = prev.get("csv_lines", prev["n_parsed"])
        snapshot = root / ".tmp" / f"{ddir.name}.tail.csv"
        quality["physical_lines"] = _copy_range(csv_path, snapshot, prev["csv_offset"], end, prefix=header_line)
        src_path = snapshot

    # Strict pass: structural anomalies with their line numbers (in the CSV file, header = line 1).
    strict = _strict_pass(con, src_path, names)
    for line, err, text in strict:
        quality["strict_anomalies"][err] = quality["strict_anomalies"].get(err, 0) + 1
        quality["samples"].append({"line": int(line) + lines_before if line else None, "error": err, "text": text})

    # Tolerant read (V1/V2 options): what is converted.
    sel, helpers = _canonical_select(schema, rn_base)
    _drop_rejects(con, "v3_tol_rej", "v3_tol_scan")
    opts = (f"{_CSV_OPTS_EXPLICIT}, columns={_columns_sql(names)}" if names and len(set(names)) == len(names)
            else _CSV_OPTS)
    con.execute(f"CREATE OR REPLACE TEMP TABLE t AS SELECT {sel} FROM read_csv('{src_path}', {opts}, {_TOLERANT_REJECTS})")
    n_parsed_new = con.execute("SELECT count(*) FROM t").fetchone()[0]
    dropped = _rejects(con, "v3_tol_rej")

    # A full rebuild reads the live file directly when it is large: drop a trailing
    # partial line, which is always the last parsed row.
    if full and snapshot is None and end < size and n_parsed_new > 0:
        con.execute("DELETE FROM t WHERE rn = (SELECT max(rn) FROM t)")
        n_parsed_new -= 1

    for line, err, text in dropped:
        quality["reader_dropped"][err] = quality["reader_dropped"].get(err, 0) + 1
        quality["samples"].append({"line": int(line) + lines_before, "error": f"dropped: {err}", "text": text})
    quality["rows_read"] = n_parsed_new
    quality["unaccounted_lines"] = quality["physical_lines"] - n_parsed_new - len({line for line, _, _ in dropped})

    bad_cols = [h for h in helpers if h.startswith("_bad_") and h != "_bad_ts"]
    ts_name = schema.mapping.get("timestamp") or ""
    bad_price = " OR ".join(f"_bad_{c}" for c in _PRICE_COLUMNS if f"_bad_{c}" in bad_cols) or "FALSE"
    counts = con.execute(
        "SELECT count(*) FILTER (WHERE ts IS NULL), count(*) FILTER (WHERE ts IS NULL AND trim(_ts_raw) = ?), "
        f"count(*) FILTER (WHERE ts IS NOT NULL AND ({bad_price}))"
        + "".join(f", count(*) FILTER (WHERE ts IS NOT NULL AND {b})" for b in bad_cols) + " FROM t", [ts_name]).fetchone()
    quality["unreadable_timestamp_rows"], quality["repeated_headers"], quality["unreadable_price_rows"] = counts[:3]
    quality["cast_failures"] = {b.removeprefix("_bad_"): n for b, n in zip(bad_cols, counts[3:]) if n}
    if bad_cols and any(counts[3:]):
        any_bad = " OR ".join(bad_cols)
        for rn, *flags in con.execute(f"SELECT rn, {', '.join(bad_cols)} FROM t WHERE ts IS NOT NULL AND ({any_bad}) "
                                      "ORDER BY rn LIMIT 5").fetchall():
            cols_bad = [b.removeprefix("_bad_") for b, f in zip(bad_cols, flags) if f]
            quality["samples"].append({"row": int(rn), "error": "unconvertible cell", "text": ", ".join(cols_bad)})

    proxies = [r[0] for r in con.execute("SELECT DISTINCT _proxy FROM t WHERE _proxy IS NOT NULL LIMIT 3").fetchall()]

    # Rows kept: a readable timestamp and, as V1/V2 did when a price cell fails to parse, a readable price.
    con.execute(f"CREATE OR REPLACE TEMP TABLE v AS SELECT * EXCLUDE ({', '.join(helpers)}), {_EVENT_KEY} AS ev "
                f"FROM t WHERE ts IS NOT NULL AND NOT ({bad_price})")
    n_valid = con.execute("SELECT count(*) FROM v").fetchone()[0]
    if not full and existing and n_valid:
        # Events already stored (boundary overlap between two extractions).
        min_ts = con.execute("SELECT min(ts) FROM v").fetchone()[0]
        con.execute(
            f"DELETE FROM v WHERE ev IS NOT NULL AND ev IN (SELECT {_EVENT_KEY} FROM read_parquet({existing!r}) "
            "WHERE ts >= ?::TIMESTAMPTZ - INTERVAL 1 DAY)",
            [min_ts],
        )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE k AS SELECT * EXCLUDE (dup_rk, ev) FROM ("
        " SELECT *, CASE WHEN ev IS NOT NULL THEN row_number() OVER (PARTITION BY ev ORDER BY rn) ELSE 1 END AS dup_rk"
        " FROM v) WHERE dup_rk = 1"
    )
    n_kept = con.execute("SELECT count(*) FROM k").fetchone()[0]
    dups = n_valid - n_kept
    quality["duplicates_removed"] = dups

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
    for tbl in ("t", "v", "k"):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    notes: list[str] = []
    chainlink = dict((prev or {}).get("chainlink") or {}) if not full or reason != "schema_upgrade" else {}
    if proxies:
        if len(proxies) > 1:
            notes.append(f"several feed_proxy_address values {proxies}: phase switches not tracked")
            chainlink = {}
        elif chainlink.get("proxy") not in (None, proxies[0]):
            notes.append(f"feed proxy changed from {chainlink['proxy']} to {proxies[0]}")
            chainlink = {"proxy": proxies[0]}
        else:
            chainlink["proxy"] = proxies[0]

    state = {
        "schema": SCHEMA_VERSION,
        "csv_name": csv_path.name,
        "raw_columns": schema.raw_columns,
        "mapping": schema.mapping,
        "tvl_unit": schema.tvl_unit,
        "csv_header_sha1": header_sha,
        "csv_offset": end,
        "csv_fingerprint": fp_before,
        "csv_samples": (_samples(csv_path, 0, end) if full else
                        prev.get("csv_samples", []) + _samples(csv_path, prev["csv_offset"], end)),
        "csv_sha256": verified if verified else prev.get("csv_sha256"),
        "csv_lines": lines_before + quality["physical_lines"],
        "n_parsed": rn_base + n_parsed_new,
        "n_rows": int(n_rows),
        "dups_removed": (0 if full else prev.get("dups_removed", 0)) + dups,
        "quality": quality if full else _merge_quality(prev.get("quality") or _empty_quality(), quality),
        "min_ts": lo.isoformat() if lo else None,
        "max_ts": hi.isoformat() if hi else None,
        "segments": segments,
        "last_change": {"at": _now(), "action": "rebuild" if full else "append", "reason": reason},
        "extraction_head_utc": extraction_head(csv_path, end, names) or (prev or {}).get("extraction_head_utc"),
    }
    if chainlink:
        state["chainlink"] = chainlink
    # A live file that changed while we were converting it is picked up by the next run.
    action = "rebuild" if full else "append"
    return state, SyncResult(rel, action, new_rows=n_kept, dups_removed=dups, seconds=time.perf_counter() - t0,
                             reason=reason, issues=quality_issues(quality), notes=notes)


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
    """Delete segments and native copies no longer named by the manifest, an hour after they were written (an API
    process may still be reading the previous version; an open file stays readable after it is unlinked)."""
    keep = {
        str(dataset_dir(root, rel) / s["file"]) for rel, d in manifest["datasets"].items() for s in d["segments"]
    }
    keep |= {str(dataset_dir(root, rel) / d["native"]) for rel, d in manifest["datasets"].items() if d.get("native")}
    now = time.time()
    for f in [*root.glob("*/seg-*.parquet"), *root.glob("*/native-*.duckdb"), *root.glob("*/.native-*.tmp*")]:
        if str(f) not in keep and now - f.stat().st_mtime > grace_seconds:
            f.unlink()


# ---------------------------------------------------------------------------
# Native DuckDB copy of a dataset (what the API reads)
# ---------------------------------------------------------------------------

NATIVE_TABLE = "t"


def native_name(d: dict[str, Any]) -> str | None:
    """File name of the native DuckDB copy of a dataset: a hash of what identifies its data (segment list, CSV bytes
    consumed and their fingerprint, rows, time of the last append or rebuild). A rebuild can reuse a segment file
    name with the same row count, so the segment list alone is not enough: any data change gives a new name and a
    copy never outlives the data it was made from."""
    if not d.get("segments"):
        return None
    identity = [d["segments"], d.get("csv_offset"), d.get("csv_fingerprint"), d.get("n_rows"),
                (d.get("last_change") or {}).get("at")]
    digest = hashlib.sha1(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()[:16]
    return f"native-{digest}.duckdb"


def build_native(root: Path, rel: str, d: dict[str, Any], threads: int) -> tuple[str | None, float]:
    """Write, once, the native copy of the dataset (table ``t``: its segments in (ts, rn) order). Returns its name and
    the seconds spent (0 when it already existed)."""
    name = native_name(d)
    if name is None:
        return None, 0.0
    ddir = dataset_dir(root, rel)
    path = ddir / name
    if path.exists():
        return name, 0.0
    t0 = time.perf_counter()
    tmp = ddir / f".{name}.tmp"
    for leftover in (tmp, Path(f"{tmp}.wal")):
        if leftover.exists():
            leftover.unlink()
    files = [str(ddir / s["file"]) for s in d["segments"]]
    con = duckdb.connect(str(tmp))
    try:
        con.execute(f"SET threads={max(1, threads)}; SET TimeZone='UTC'")
        con.execute(f"CREATE TABLE {NATIVE_TABLE} AS SELECT * FROM read_parquet({files!r}) ORDER BY ts, rn")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    os.replace(tmp, path)
    return name, time.perf_counter() - t0


def _refresh_natives(cfg: AppConfig, datasets: dict[str, Any], only: str | None) -> tuple[bool, list[dict[str, Any]],
                                                                                           list[str]]:
    """Give every dataset an up-to-date native copy. Returns (manifest changed, log entries, messages)."""
    changed, log, msgs = False, [], []
    for rel, d in datasets.items():
        if only and only not in rel:
            continue
        try:
            name, seconds = build_native(cfg.v3.parquet_path, rel, d, cfg.v3.duckdb_threads)
        except Exception as e:  # the API keeps reading the Parquet segments of this dataset
            first = (str(e).strip().splitlines() or [""])[0][:200]
            msgs.append(f"WARNING {rel}: native copy not built ({type(e).__name__}: {first}); Parquet is read instead")
            name, seconds = None, 0.0
        if name != d.get("native"):
            datasets[rel] = {**d, "native": name}
            changed = True
        if seconds:
            log.append({"at": _now(), "dataset": rel, "action": "native_copy", "file": name,
                        "rows": d.get("n_rows"), "seconds": round(seconds, 1)})
            msgs.append(f"native    {rel:40s} {d.get('n_rows', 0):>10,} rows  {seconds:6.1f}s")
    return changed, log, msgs


# ---------------------------------------------------------------------------
# Chainlink phase switches
# ---------------------------------------------------------------------------

def _refresh_phases(cfg: AppConfig, datasets: dict[str, Any], rpc: chainlink_phases.Rpc | None,
                    only: str | None) -> tuple[bool, list[dict[str, Any]], list[str]]:
    """Bring the phase table of every Chainlink dataset up to the chain head.

    Returns (switches changed, log entries, messages). Without a node the tables are kept as they are.
    """
    targets = [rel for rel, d in datasets.items() if (d.get("chainlink") or {}).get("proxy") and (not only or only in rel)]
    if not targets:
        return False, [], []
    if rpc is None:
        url = os.environ.get(cfg.v3.rpc_url_env, "")
        if not url:
            for rel in targets:
                datasets[rel]["chainlink"]["status"] = "rpc_not_configured"
            return False, [], [f"WARNING ${cfg.v3.rpc_url_env} is not set: Chainlink phase switches not checked "
                               f"({len(targets)} feeds)"]
        rpc = chainlink_phases.Rpc(url)
    changed, log, msgs = False, [], []
    for rel in targets:
        cl = datasets[rel]["chainlink"]
        try:
            new = chainlink_phases.refresh(cl, cl["proxy"], rpc)
        except chainlink_phases.RpcError as e:
            cl["status"] = f"rpc_error: {e}"
            msgs.append(f"WARNING {rel}: Chainlink phases not checked ({e}); keeping the table of "
                        f"{cl.get('verified_ts') or 'never'}")
            continue
        before = {s["phase"] for s in cl.get("switches", [])}
        for s in new["switches"]:
            if s["phase"] not in before:
                changed = True
                log.append({"at": _now(), "dataset": rel, "action": "chainlink_phase_switch", "proxy": new["proxy"],
                            "phase": s["phase"], "block": s["block"], "block_ts": s["ts"]})
        datasets[rel]["chainlink"] = {**new, "status": "ok"}
    msgs.append(f"Chainlink phases checked for {len(targets)} feeds ({rpc.calls} RPC calls)")
    return changed, log, msgs


def _refresh_complete_until(cfg: AppConfig, datasets: dict[str, Any], rpc: chainlink_phases.Rpc | None,
                            only: str | None) -> list[str]:
    """Record for every Chainlink feed the chain time up to which its file holds every round
    (``oracle_complete_until_utc``, see ``chainlink_phases.complete_until``). The API extends the coverage of the
    feed file to it. Without a node, or when the node fails, the previous value is kept (it stays true)."""
    targets = [rel for rel, d in datasets.items() if (d.get("chainlink") or {}).get("proxy") and (not only or only in rel)]
    if not targets or rpc is None:
        return []
    calls = rpc.calls
    try:
        head = rpc.block_number()
        head_time = rpc.block_time(head)
    except chainlink_phases.RpcError as e:
        return [f"WARNING Chainlink rounds not compared with the chain head ({e})"]
    msgs, complete = [], 0
    for rel in targets:
        d = datasets[rel]
        path = cfg.paths.datasets_path / rel
        try:
            last = last_round_id(path, d["csv_offset"], _header_names(_read_header_line(path)))
        except OSError:
            continue
        if last is None:
            continue
        try:
            until = chainlink_phases.complete_until(rpc, d["chainlink"]["proxy"], last, head, head_time)
        except chainlink_phases.RpcError as e:
            msgs.append(f"WARNING {rel}: rounds not compared with the chain head ({e})")
            continue
        if until is not None:
            datasets[rel] = {**d, "oracle_complete_until_utc": until.isoformat()}
            if until == head_time:
                complete += 1
    msgs.append(f"Chainlink files complete up to the chain head for {complete} of {len(targets)} feeds "
                f"({rpc.calls - calls} RPC calls)")
    return msgs


def run_sync(cfg: AppConfig | None = None, only: str | None = None, rebuild: bool = False,
             dry_run: bool = False, verify: bool = False,
             rpc: chainlink_phases.Rpc | None = None) -> list[SyncResult]:
    cfg = cfg or get_config()
    root = cfg.v3.parquet_path
    root.mkdir(parents=True, exist_ok=True)
    results: list[SyncResult] = []
    with (root / ".sync.lock").open("w") as lock:
        try:
            # A --verify run (weekly timer) waits for the regular sync instead of failing.
            fcntl.flock(lock, fcntl.LOCK_EX if verify else fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another sync is already running")
        manifest = load_manifest(root)
        datasets = dict(manifest["datasets"])
        con = _connect(root, cfg)
        changed = False
        log: list[dict[str, Any]] = []
        for rel in all_relative_paths():
            if only and only not in rel:
                continue
            if dry_run:
                continue
            prev = datasets.get(rel)
            try:
                state, res = sync_dataset(con, cfg, rel, prev, force_rebuild=rebuild, verify=verify)
            except Exception as e:  # one unreadable file must not stop the others; its previous state is kept
                first = (str(e).strip().splitlines() or [""])[0][:200]
                state, res = prev, SyncResult(rel, "error", notes=[f"sync failed, previous state kept: "
                                                                   f"{type(e).__name__}: {first}"])
            results.append(res)
            if state is not None and "extraction_head_utc" not in state:
                # Manifests written before the extraction head was recorded: read it once from the CSV tail.
                try:
                    names = _header_names(_read_header_line(cfg.paths.datasets_path / rel))
                    state = {**state, "extraction_head_utc": extraction_head(cfg.paths.datasets_path / rel,
                                                                             state["csv_offset"], names)}
                    datasets[rel] = state
                    changed = True
                except OSError:
                    pass
            if state is not None and res.action in ("append", "rebuild", "verified"):
                datasets[rel] = state
                changed = True
                if res.action != "verified":
                    log.append({"at": _now(), "dataset": rel, "action": res.action, "reason": res.reason,
                                "rows_before": (prev or {}).get("n_rows", 0), "rows_after": state["n_rows"],
                                "new_rows": res.new_rows, "duplicates_removed": res.dups_removed,
                                "quality_issues": res.issues})
                # Publish progressively so a crash never loses converted datasets.
                _write_manifest(root, datasets)
        if rpc is None and os.environ.get(cfg.v3.rpc_url_env, ""):
            rpc = chainlink_phases.Rpc(os.environ[cfg.v3.rpc_url_env])
        phases_changed, phase_log, msgs = (False, [], []) if dry_run else _refresh_phases(cfg, datasets, rpc, only)
        if not dry_run:
            msgs += _refresh_complete_until(cfg, datasets, rpc, only)
        results.append(SyncResult("chainlink phases", "checked", notes=msgs))
        log.extend(phase_log)
        if cfg.v3.native_store and not dry_run:
            natives_changed, native_log, native_msgs = _refresh_natives(cfg, datasets, only)
            changed = changed or natives_changed
            log.extend(native_log)
            results.append(SyncResult("native copies", "checked", notes=native_msgs))
        if changed or phases_changed or any((d.get("chainlink") or {}).get("status") for d in datasets.values()):
            manifest = _write_manifest(root, datasets)
        for e in log:
            e["version"] = manifest.get("version")
        _append_log(root, log)
        _cleanup_orphans(root, manifest)
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync the V3 Parquet store from the CSV datasets.")
    ap.add_argument("--rebuild", action="store_true", help="force a full rebuild of the selected datasets")
    ap.add_argument("--verify", action="store_true",
                    help="also compare a full SHA-256 of the CSV bytes consumed so far (reads every file once)")
    ap.add_argument("--only", help="only datasets whose relative path contains this text")
    args = ap.parse_args()
    for r in run_sync(only=args.only, rebuild=args.rebuild, verify=args.verify):
        if r.rel in ("chainlink phases", "native copies"):
            for m in r.notes:
                print(m, flush=True)
            continue
        why = f" ({r.reason})" if r.reason else ""
        print(f"{r.action:9s} {r.rel:40s} +{r.new_rows:>9,} rows  dups_removed={r.dups_removed:<4d} "
              f"{r.seconds:6.1f}s{why}", flush=True)
        if r.issues:
            print(f"WARNING {r.rel}: {r.issues} line(s) or cell(s) not stored as they are in the CSV; "
                  "see quality in /v3/datasets", flush=True)
        for n in r.notes:
            print(f"WARNING {r.rel}: {n}", flush=True)


if __name__ == "__main__":
    main()
