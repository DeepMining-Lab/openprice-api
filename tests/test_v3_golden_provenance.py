"""The 245 reference responses of the test suite carry their provenance record (tests/golden/v3_golden_provenance.jsonl,
captured with capture_v3.py on v3.1.0), and the published verifier recomputes every index from it alone, without the
data or the API code, by exact equality (``tools/verify_confidence.py --strict``)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = ROOT / "tests" / "golden" / "v3_golden_provenance.jsonl"


def test_the_records_answer_the_requests_of_the_golden_file():
    ids = [json.loads(line)["id"] for line in GOLDEN.read_text().splitlines()]
    golden = [json.loads(line)["id"] for line in (ROOT / "tests" / "golden" / "v3_golden.jsonl").read_text().splitlines()]
    assert len(ids) == 245 and ids == golden


def test_the_verifier_recomputes_every_index_exactly():
    out = subprocess.run([sys.executable, "-I", str(ROOT / "tools" / "verify_confidence.py"), "--strict", str(GOLDEN)],
                         capture_output=True, text=True, timeout=600)
    assert out.returncode == 0, out.stdout[-3000:]
    assert out.stdout.splitlines()[-1] == ("233 response(s) checked, 0 with a failed check, 12 without an index "
                                           "(strict: exact equality)")
