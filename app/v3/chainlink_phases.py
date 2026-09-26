"""Chainlink proxy phases: which aggregator the proxy served at T (API V3).

A Chainlink feed is read through a proxy contract that points to one aggregator at a time, its *phase*. When
Chainlink replaces the aggregator, the proxy moves to the next phase. The new aggregator usually publishes for days
or weeks before the switch, and the old one often keeps publishing after it. The extraction stores the rounds of
every phase, so the price the proxy served at T is the latest round of the phase that was active at T, not the
latest round of the file.

The switch times are not in the CSV files. They are read from the chain: ``phaseId()`` of the proxy never decreases
from one block to the next, so a binary search over block numbers finds the first block of each phase (about 25
``eth_call`` per phase; the node must serve historical state). The table lives in the V3 manifest, next to the
Parquet store, and is maintained by ``app.v3.sync``: each run reads ``phaseId()`` once per feed at the chain head
and searches the switch block only when the phase has changed. The API never calls the node.

Each sync run also compares the latest round of every feed file with the proxy's latest round at the chain head
(``complete_until``): when the file already holds it, the feed is known complete up to the head, even if it was
extracted hours earlier. A peg feed with a 24 h heartbeat publishes one or two rounds a day, so its file alone cannot
say how long no round was published after the last one.
"""

from __future__ import annotations

import bisect
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

PHASE_ID_SELECTOR = "0x58303b10"            # keccak256("phaseId()")[:4]
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"   # keccak256("latestRoundData()")[:4]
GET_ROUND_DATA_SELECTOR = "0x9a6fc8f5"      # keccak256("getRoundData(uint80)")[:4]


class RpcError(Exception):
    """A JSON-RPC failure. The message never contains the node URL (it can carry an access token)."""


class Rpc:
    def __init__(self, url: str, timeout: float = 15.0):
        self._url = url
        self._timeout = timeout
        self.calls = 0

    def _call(self, method: str, params: list[Any]) -> Any:
        self.calls += 1
        body = json.dumps({"jsonrpc": "2.0", "id": self.calls, "method": method, "params": params}).encode()
        req = urllib.request.Request(self._url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                out = json.load(resp)
        except Exception as e:  # network, HTTP or JSON error: report the kind only
            raise RpcError(f"{method}: {type(e).__name__}") from None
        if "error" in out:
            raise RpcError(f"{method}: {str(out['error'].get('message', 'error'))[:120]}")
        return out.get("result")

    def block_number(self) -> int:
        return int(self._call("eth_blockNumber", []), 16)

    def block_time(self, block: int) -> datetime:
        blk = self._call("eth_getBlockByNumber", [hex(block), False])
        return datetime.fromtimestamp(int(blk["timestamp"], 16), timezone.utc)

    def phase_id(self, proxy: str, block: int) -> int:
        """``phaseId()`` of the proxy at ``block``; 0 before the proxy existed (no code, empty return)."""
        try:
            res = self._call("eth_call", [{"to": proxy, "data": PHASE_ID_SELECTOR}, hex(block)])
        except RpcError as e:
            if "revert" in str(e).lower():
                return 0
            raise
        return int(res, 16) if res and res != "0x" else 0

    def _round(self, proxy: str, data: str, block: int) -> tuple[int, datetime] | None:
        """(global round id, updatedAt) of a ``latestRoundData()`` / ``getRoundData()`` answer at ``block``; None
        when the round has no data (revert, empty return, updatedAt = 0)."""
        try:
            res = self._call("eth_call", [{"to": proxy, "data": data}, hex(block)])
        except RpcError as e:
            if "revert" in str(e).lower():
                return None
            raise
        if not res or len(res) < 2 + 5 * 64:
            return None
        round_id, updated_at = int(res[2:66], 16), int(res[194:258], 16)
        return (round_id, datetime.fromtimestamp(updated_at, timezone.utc)) if updated_at else None

    def latest_round(self, proxy: str, block: int) -> tuple[int, datetime] | None:
        return self._round(proxy, LATEST_ROUND_DATA_SELECTOR, block)

    def round_data(self, proxy: str, round_id: int, block: int) -> tuple[int, datetime] | None:
        return self._round(proxy, GET_ROUND_DATA_SELECTOR + format(round_id, "064x"), block)


def complete_until(rpc: Rpc, proxy: str, last_round_id: int, head: int, head_time: datetime) -> datetime | None:
    """Chain time up to which a feed file holds every round the proxy published, given the latest round id of the
    file (``phase << 64 | aggregator round``).

    If the proxy's latest round at ``head`` is that round, nothing newer exists: the file is complete up to
    ``head_time``. If the proxy has moved on within the same phase, the file is complete up to just before the first
    round it lacks. After a phase change there is no answer (None). Raises RpcError when the node cannot answer.
    """
    latest = rpc.latest_round(proxy, head)
    if latest is None:
        return None
    if latest[0] == last_round_id:
        return head_time
    if latest[0] >> 64 == last_round_id >> 64 and latest[0] > last_round_id:
        first_missing = rpc.round_data(proxy, last_round_id + 1, head)
        return first_missing[1] - timedelta(seconds=1) if first_missing else None
    return None


def first_block_of_phase(rpc: Rpc, proxy: str, phase: int, lo: int, hi: int) -> int:
    """Smallest block in [lo, hi] where ``phaseId() >= phase`` (requires phaseId(hi) >= phase)."""
    while lo < hi:
        mid = (lo + hi) // 2
        if rpc.phase_id(proxy, mid) >= phase:
            hi = mid
        else:
            lo = mid + 1
    return lo


def refresh(prev: dict[str, Any] | None, proxy: str, rpc: Rpc) -> dict[str, Any]:
    """Return the phase table of ``proxy`` brought up to the chain head.

    ``switches`` lists, for every phase since the proxy was deployed (phase 1 = deployment), the first block where
    the proxy reported that phase and its timestamp. A table built earlier for the same proxy is extended, never
    recomputed. Raises RpcError when the node cannot answer; the caller then keeps ``prev``.
    """
    known = list(prev.get("switches") or []) if prev and prev.get("proxy") == proxy else []
    head = rpc.block_number()
    current = rpc.phase_id(proxy, head)
    last = known[-1]["phase"] if known else 0
    if current < last:
        raise RpcError(f"phaseId() went back from {last} to {current}")
    lo = known[-1]["block"] if known else 0
    for phase in range(last + 1, current + 1):
        block = first_block_of_phase(rpc, proxy, phase, lo, head)
        known.append({"phase": phase, "block": block, "ts": rpc.block_time(block).isoformat()})
        lo = block
    return {
        "proxy": proxy,
        "switches": known,
        "verified_block": head,
        "verified_ts": rpc.block_time(head).isoformat(),
    }


class PhaseTable:
    """Switch times of one proxy, for lookups on the request path."""

    def __init__(self, table: dict[str, Any]):
        sw = sorted(table.get("switches", []), key=lambda s: s["block"])
        self.phases = [int(s["phase"]) for s in sw]
        self.starts = [datetime.fromisoformat(s["ts"]) for s in sw]
        self.verified = datetime.fromisoformat(table["verified_ts"]) if table.get("verified_ts") else None

    def active_phase(self, t: datetime) -> int:
        """Phase the proxy served at ``t``; 0 before the proxy was deployed."""
        i = bisect.bisect_right(self.starts, t) - 1
        return self.phases[i] if i >= 0 else 0

    def start_of(self, phase: int) -> datetime | None:
        return self.starts[self.phases.index(phase)] if phase in self.phases else None

    def served_sql(self) -> str:
        """SQL predicate true for the rounds published while their phase was the one served by the proxy."""
        if not self.phases:
            return "FALSE"
        cases = " ".join(f"WHEN ts >= TIMESTAMPTZ '{start.isoformat()}' THEN {phase}"
                         for phase, start in sorted(zip(self.phases, self.starts), key=lambda x: x[1], reverse=True))
        return f"phase = CASE {cases} ELSE 0 END"
