"""The parameters written in the documentation must be the ones the API runs with.

The validity audit of 2026-09-21/22 found CLAUDE.md announcing ``seuil_TVL_min_usd`` = 1 000 000 USD and the
``linear_to_threshold`` score while ``config/openprice.yaml`` used 100 000 USD and ``log_memoire``. This test reads
every YAML block of README.md and CLAUDE.md that sets ``thresholds``, ``scoring`` or ``confidence_weights``, and every
``seuil_TVL_min_usd`` value written in the text, and compares them with the configuration file. CLAUDE.md is not
versioned (.gitignore): its checks are skipped when the file is absent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = yaml.safe_load((ROOT / "config" / "openprice.yaml").read_text())
SECTIONS = ("thresholds", "scoring", "confidence_weights")
DOCS = ("README.md", "CLAUDE.md")


def _doc(name: str) -> str:
    path = ROOT / name
    if not path.exists():
        pytest.skip(f"{name} not present")
    return path.read_text()


def _leaves(d: dict, prefix: tuple = ()):
    for k, v in d.items():
        if isinstance(v, dict):
            yield from _leaves(v, prefix + (k,))
        else:
            yield prefix + (k,), v


def _number(text: str) -> float:
    return float(re.sub(r"[ ,_ ]", "", text))


@pytest.mark.parametrize("name", DOCS)
def test_yaml_blocks_match_the_config(name):
    blocks = [yaml.safe_load(b) for b in re.findall(r"```yaml\n(.*?)```", _doc(name), re.S)]
    checked = 0
    for block in blocks:
        if not isinstance(block, dict):
            continue
        for path, value in _leaves({k: v for k, v in block.items() if k in SECTIONS}):
            node = CONFIG
            for key in path:
                node = node[key]
            assert node == value, f"{name}: {'.'.join(path)} = {value!r}, config/openprice.yaml has {node!r}"
            checked += 1
    assert checked, f"{name}: no configuration block found"


@pytest.mark.parametrize("name", DOCS)
def test_tvl_threshold_values_in_the_text_match_the_config(name):
    found = re.findall(r"seuil_TVL_min_usd`?\"?\s*(?:[:=]|\(default:)\s*\"?(\d[\d ,_ ]*\d|\d)", _doc(name))
    assert found, f"{name}: no seuil_TVL_min_usd value found"
    for value in found:
        assert _number(value) == CONFIG["thresholds"]["seuil_TVL_min_usd"], f"{name}: seuil_TVL_min_usd = {value}"


def test_readme_log_score_bounds_match_the_config():
    text = _doc("README.md")
    assert _number(re.search(r"TVL_min = ([\d,]+) USD", text).group(1)) == CONFIG["scoring"]["tvl_log_min_usd"]
    assert _number(re.search(r"TVL_ref = ([\d,]+) USD", text).group(1)) == CONFIG["scoring"]["tvl_log_ref_usd"]
