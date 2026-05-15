# OpenPrice API

A Proof of Concept API that exposes historical crypto-asset prices from CSV files — no database required.

```
CSV files → FastAPI → DuckDB (direct CSV reads) → JSON responses
```

The API never imports data into a database. Every request triggers a direct DuckDB query against the CSV files on disk. There is no setup step beyond installing Python dependencies and pointing the config at your dataset directory.

## Supported assets

`ETH`, `LINK`, `UNI`, `AAVE`, `COMP`

## Source hierarchy

The API selects the best available price source by trying levels in order until one succeeds:

| Level | Label | Description |
|-------|-------|-------------|
| `0a` | `direct_stable` | TOKEN/USDC or TOKEN/USDT Uniswap V3 pool — highest-TVL non-zombie pool wins |
| `0b` | `cross_rate` | TOKEN/WETH × WETH/USD cross-rate — both legs matched as-of timestamp |
| `2` | `alternative_amm` | Curve or SushiSwap pool (asset-specific) |
| `3` | `chainlink_fallback` | Chainlink oracle — latest observation at or before `T` |
| `4` | `unavailable` | Explicit NULL — no reliable source found |

Level `1` (alternative Uniswap pool) is treated as a candidate within the `0a`/`0b` pool selection logic rather than a separate fallback step.

### Zombie pool rules

A pool is excluded from selection when any of the following conditions is true (if the relevant column exists):

- `TVL < seuil_TVL_min_usd` (default: 1 000 000 USD)
- `volume_24h < seuil_vol_min_usd_24h` (default: 10 000 USD)
- No observation in the last `fenetre_inactivite_jours` days (default: 30)

When a required column is absent, the check is skipped and a warning is added to the response — the API never invents liquidity data.

### Cross-rate lag

For level `0b`, both the TOKEN/WETH leg and the WETH/USD leg are matched using an as-of strategy (latest observation ≤ T). If the two legs are more than `cross_rate_max_lag_seconds` apart (default: 3600 s), the cross-rate is rejected and the API falls to the next level.

## Confidence index

Each price response optionally includes three sub-scores composed as a weighted geometric mean:

```
C = S_stat^w_stat × S_liq^w_liq × S_coh^w_coh
```

Default weights: `w_stat = w_liq = w_coh ≈ 1/3` (must sum to exactly 1.0).

### S_stat — Statistical hygiene

Modified Z-score (MAD method) measuring how far the current price sits from a 7-day rolling median:

```
M_7j   = rolling median of prior 7-day VWMP values
MAD_7j = median(|p_j − M_7j|)
z_MAD  = 0.6745 × |VWMP(T) − M_7j| / MAD_7j
S_stat = exp(−z_MAD² / (2 × sigma_mad²))      sigma_mad default: 3.5
```

Falls back to `s_stat_floor` (default: 0.2) when fewer than `min_swaps_for_stat_score` observations exist.

### S_liq — Deep liquidity

Combines TVL score and slippage score into a geometric mean:

```
S_TVL = min(1, TVL / seuil_TVL_min_usd)       (linear_to_threshold mode)
S_slip = exp(−slip_1k / slip_max)              slip_max default: 0.005
S_liq  = sqrt(S_TVL × S_slip)
```

Degrades gracefully when TVL or slippage columns are absent (uses whichever is available; `null` if neither exists).

### S_coh — Coherence

For DEX branches (`0a`, `0b`, `2`): relative deviation between DEX price and Chainlink oracle:

```
δ(T)  = |P_DEX(T) − P_CL(T)| / P_CL(T)
S_coh = exp(−(δ(T) / δ_tol)²)
```

Tolerance `δ_tol` is asset-specific (default 0.5 % for most assets, 1 % for COMP).

For Chainlink fallback (level `3`): oracle staleness replaces the DEX comparison:

```
S_coh = exp(−staleness / heartbeat)       coherence_mode: "oracle_only_staleness"
```

## Installation

```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtualenv and install dependencies
uv venv .venv --python 3.11
uv pip install -e ".[dev]"
```

## Dataset path

Place your CSV files under `~/openprice/datasets/`. The path is configurable in `config/openprice.yaml`:

```yaml
paths:
  datasets_root: "~/openprice/datasets"
```

## Changing thresholds and weights

Edit `config/openprice.yaml`. All methodological parameters are there:

```yaml
thresholds:
  seuil_TVL_min_usd: 1000000        # Pool TVL below this → zombie
  seuil_vol_min_usd_24h: 10000      # 24h volume below this → zombie
  fenetre_inactivite_jours: 30      # Days with no activity → zombie
  sigma_mad: 3.5                    # MAD sensitivity for S_stat
  slip_max: 0.005                   # Max reference slippage for S_liq
  cross_rate_max_lag_seconds: 3600  # Max lag between 0b cross-rate legs

confidence_weights:
  w_stat: 0.3333333333
  w_liq: 0.3333333333
  w_coh: 0.3333333334

scoring:
  tvl_score_mode: "linear_to_threshold"  # or "binary_threshold" or "log_memoire"
```

The effective config is always visible at `GET /v1/config`.

## Starting the API

```bash
.venv/bin/uvicorn app.main:app --reload
```

The API is then available at `http://127.0.0.1:8000`.

Interactive documentation (Swagger UI): `http://127.0.0.1:8000/docs`

Alternative docs (ReDoc): `http://127.0.0.1:8000/redoc`

## Running tests

```bash
.venv/bin/python -m pytest tests/ -q
```

## API endpoints

### Health

```bash
curl http://127.0.0.1:8000/health
```

```json
{"status": "ok", "api": "OpenPrice API"}
```

### Effective configuration

```bash
curl http://127.0.0.1:8000/v1/config
```

Returns all active thresholds, weights, paths, and API version.

### Supported assets

```bash
curl http://127.0.0.1:8000/v1/assets
```

```json
{
  "assets": [
    {"asset": "ETH", "roles": ["chainlink", "level_0a_direct_stable", "eth_usd_reference", "level_2_amm"]},
    {"asset": "LINK", "roles": ["chainlink", "level_0a_direct_stable", "level_0b_cross_rate"]},
    ...
  ]
}
```

### Dataset files and existence check

```bash
curl http://127.0.0.1:8000/v1/datasets
```

```json
{
  "datasets_root": "/home/user/openprice/datasets",
  "files": [
    {"asset": "LINK", "path": "link/chainlink_link_usd.csv", "exists": true, "role": "chainlink"},
    {"asset": "LINK", "path": "link/link_usdc_uniswap_v3_03.csv", "exists": true, "role": "level_0a_direct_stable"}
  ]
}
```

### CSV schema for an asset

```bash
curl "http://127.0.0.1:8000/v1/datasets/schema?asset=LINK"
```

```json
{
  "asset": "LINK",
  "files": [
    {
      "file": "link/link_usdc_uniswap_v3_03.csv",
      "raw_columns": ["timestamp", "price_usdc_per_link", "pool_tvl_at_block", "slip_10k"],
      "canonical_mapping": {
        "timestamp": "timestamp",
        "price_usdc_per_link": "price_usd",
        "pool_tvl_at_block": "tvl_usd",
        "slip_10k": "slippage"
      },
      "warnings": []
    }
  ]
}
```

### Price at a timestamp

```bash
curl "http://127.0.0.1:8000/v1/prices/LINK/at?timestamp=2024-01-01T00:00:00Z&include_confidence=true&include_provenance=true"
```

```json
{
  "asset": "LINK",
  "timestamp_requested": "2024-01-01T00:00:00Z",
  "timestamp_observed": "2023-12-31T23:58:00Z",
  "price_usd": 14.82,
  "branch_level": "0a",
  "branch_label": "direct_stable",
  "data_status": "observed",
  "unavailable_reason": null,
  "confidence": {
    "score": 0.84,
    "S_stat": 0.91,
    "S_liq": 0.78,
    "S_coh": 0.87,
    "coherence_mode": null,
    "weights": {"w_stat": 0.3333333333, "w_liq": 0.3333333333, "w_coh": 0.3333333334},
    "parameters": {
      "seuil_TVL_min_usd": 1000000,
      "seuil_vol_min_usd_24h": 10000,
      "fenetre_inactivite_jours": 30,
      "sigma_mad": 3.5,
      "slip_max": 0.005
    },
    "warnings": []
  },
  "provenance": {
    "files_used": ["link/link_usdc_uniswap_v3_03.csv", "link/chainlink_link_usd.csv"],
    "branch_level": "0a",
    "branch_label": "direct_stable",
    "calculation_path": ["direct LINK/USDC price"],
    "token_leg_timestamp": null,
    "eth_usd_leg_timestamp": null,
    "cross_rate_lag_seconds": null,
    "parameters": {
      "seuil_TVL_min_usd": 1000000,
      "seuil_vol_min_usd_24h": 10000,
      "fenetre_inactivite_jours": 30
    },
    "warnings": []
  },
  "warnings": []
}
```

Optional query parameters:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `source` | `auto`, `dex`, `chainlink` | `auto` | Restrict source type |
| `branch` | `auto`, `0a`, `0b`, `2`, `3`, `4` | `auto` | Force a specific branch level |
| `include_confidence` | `true`, `false` | `true` | Include S_stat, S_liq, S_coh |
| `include_provenance` | `true`, `false` | `true` | Include files used and calculation path |

When no reliable source is found:

```json
{
  "price_usd": null,
  "branch_level": "4",
  "branch_label": "unavailable",
  "data_status": "unavailable",
  "unavailable_reason": "no_observation_in_window"
}
```

### Price range

```bash
curl "http://127.0.0.1:8000/v1/prices/UNI?start=2021-09-01&end=2021-09-02&limit=100"
```

Returns a list of `PriceResponse` objects at the raw timestamps present in the winning source CSV (no synthetic resampling). Confidence and provenance are disabled by default for performance.

```bash
curl "http://127.0.0.1:8000/v1/prices/LINK?start=2024-01-01&end=2024-01-31&limit=500&include_confidence=true"
```

### Confidence only

```bash
curl "http://127.0.0.1:8000/v1/confidence/LINK/at?timestamp=2024-01-01T00:00:00Z"
```

```json
{
  "score": 0.84,
  "S_stat": 0.91,
  "S_liq": 0.78,
  "S_coh": 0.87,
  "coherence_mode": null,
  "weights": {"w_stat": 0.3333333333, "w_liq": 0.3333333333, "w_coh": 0.3333333334},
  "parameters": {
    "seuil_TVL_min_usd": 1000000,
    "seuil_vol_min_usd_24h": 10000,
    "fenetre_inactivite_jours": 30,
    "sigma_mad": 3.5,
    "slip_max": 0.005
  },
  "warnings": []
}
```

### Provenance only

```bash
curl "http://127.0.0.1:8000/v1/provenance/LINK/at?timestamp=2024-01-01T00:00:00Z"
```

```json
{
  "files_used": ["link/link_weth_uniswap_v3_03.csv", "eth/eth_usdc_uniswap_v3_005.csv"],
  "branch_level": "0b",
  "branch_label": "cross_rate",
  "calculation_path": [
    "LINK/WETH leg",
    "WETH/USD leg",
    "LINK/USD = LINK/WETH * WETH/USD"
  ],
  "token_leg_timestamp": "2024-01-01T00:00:00Z",
  "eth_usd_leg_timestamp": "2023-12-31T23:57:00Z",
  "cross_rate_lag_seconds": 180.0,
  "parameters": {
    "seuil_TVL_min_usd": 1000000,
    "seuil_vol_min_usd_24h": 10000,
    "fenetre_inactivite_jours": 30
  },
  "warnings": []
}
```

### DEX vs Chainlink comparison

```bash
curl "http://127.0.0.1:8000/v1/compare/LINK?start=2024-01-01&end=2024-01-07&limit=200"
```

```json
[
  {
    "timestamp": "2024-01-01T00:00:00Z",
    "dex_price_usd": 14.82,
    "chainlink_price_usd": 14.79,
    "deviation": 0.00203,
    "dex_branch": "0a",
    "warnings": []
  }
]
```

The `deviation` field is `|DEX − CL| / CL` and corresponds directly to the `δ(T)` used in the S_coh formula.

## Design principles

- **No database**: CSVs are queried directly by DuckDB at request time.
- **No import step**: The API reads files as-is from the configured path.
- **Explicit uncertainty**: Missing data returns `price_usd: null` and `data_status: "unavailable"`, never an invented value.
- **Full provenance**: Every price exposes which files were used, the calculation path, and all active parameters.
- **Configurable**: All thresholds and weights live in `config/openprice.yaml` and take effect immediately on restart.
- **Auditable warnings**: Every viability check that cannot be evaluated (missing column) produces a structured warning instead of silently passing or failing.
