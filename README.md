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
| `0b` | `cross_rate` | TOKEN/WETH × WETH/USD cross-rate, Uniswap V3 — both legs matched as-of timestamp |
| `1` | `alternative_pool` | Same pair on Uniswap V2 — direct stable (ETH) or cross-rate (others); same endogenous auditability, older protocol version |
| `2` | `alternative_amm` | Curve (ETH via crvUSD/WETH, inverted) or SushiSwap TOKEN/ETH cross-rate for other assets |
| `3` | `chainlink_fallback` | Chainlink oracle — latest observation at or before `T` |
| `4` | `unavailable` | Explicit NULL — no reliable source found |

### Zombie pool rules

A pool is excluded from selection when any of the following conditions is true (if the relevant column exists):

- `TVL < seuil_TVL_min_usd` (default: 1 000 000 USD)
- `volume_24h < seuil_vol_min_usd_24h` (default: 10 000 USD)
- No observation in the last `fenetre_inactivite_jours` days (default: 30)

When a required column is absent, the check is skipped and a warning is added to the response — the API never invents liquidity data.

**Historically low TVL pools**: early-date observations of newer DEX
pools may show very low TVL (e.g., a Uniswap V3 LINK/WETH pool with
$3 791 TVL in February 2022, versus a $1 000 000 threshold). These pools
are correctly classified as zombie and the pipeline falls to the next
level — typically a Uniswap V2 pool whose TVL column is absent, which
allows it to pass the check (with a `missing_tvl_column` warning). This
behaviour is expected: during the early months of Uniswap V3 adoption,
liquidity was concentrated on V2, and the zombie filter reflects that.

### Cross-rate lag

For level `0b`, both the TOKEN/WETH leg and the WETH/USD leg are matched using an as-of strategy (latest observation ≤ T). If the two legs are more than `cross_rate_max_lag_seconds` apart (default: 3600 s), the cross-rate is rejected and the API falls to the next level.

## Granularity and VWMP

The API supports four price-calculation modes controlled by the `granularity` parameter.

| Granularity | Initial window | Symmetric window around T |
|-------------|---------------|--------------------------|
| `raw` | — | Latest single observation ≤ T (no aggregation) |
| `minute` | 60 s | T ± 30 s |
| `hour` | 3 600 s | T ± 30 min |
| `day` | 86 400 s | T ± 12 h |

Sub-minute granularity is intentionally excluded: Ethereum blocks are ~12 s apart, which makes a stable per-block price definition unreliable.

### Price formula (minute / hour / day)

For each `(asset, T, granularity)`, the pipeline:

1. Selects the best non-zombie pool (same pool-selection logic as the source hierarchy).
2. Queries all swaps in `[T − Δ/2, T + Δ/2]`.
3. Applies the MAD outlier filter (which also removes MEV sandwich trades) with `sigma_mad` from config.
4. Computes the **VWMP** — Volume-Weighted Median Price:

```
P(asset, T, g) = VWMP({ p_i, v_i }_{i=1..N})
```

where `p_i` is the implicit price of swap `i` and `v_i` its volume.  The VWMP is the price at which cumulative sorted volume first reaches ≥ 50 % of total volume.

For level `0b`, only the TOKEN/WETH leg uses VWMP; the ETH/USD reference is always a point read from the WETH/USDC 0.05 % pool.

### Window expansion (rule R1)

If no swap is found in the initial window, the pipeline expands progressively:

| Granularity | Expansion steps |
|-------------|----------------|
| `minute` | 60 s → 2 min → 5 min → 15 min |
| `hour` | 1 h → 2 h → 4 h → 8 h |
| `day` | no expansion — falls directly to the next source level |

After all expansion steps are exhausted, the pipeline falls to the next branch level (`0a → 0b → 2 → 3 → 4`).

### Response fields (windowed granularities)

```json
{
  "granularity": "hour",
  "swap_count": 12,
  "window_seconds": 3600
}
```

- `swap_count` — number of clean swaps used for the VWMP (after MAD filtering).
- `window_seconds` — actual window size used (may be larger than the initial window if R1 fired).
- `provenance.excluded_swaps` — number of swaps removed by the MAD filter.

## Confidence index

Each price response may include a confidence index C(asset, T) ∈ [0, 1],
computed from three bounded sub-scores:

```
C = S_stat^w_stat × S_liq^w_liq × S_coh^w_coh
```

with:

```
w_stat + w_liq + w_coh = 1
```

Default initial calibration:

```
w_stat = 1/3
w_liq  = 1/3
w_coh  = 1/3
```

These weights are the initial equal-weight calibration and are intended
to be validated empirically.

### S_stat — Statistical hygiene

S_stat measures the statistical coherence of the published VWMP against
a 7-day rolling median of prior daily VWMP values.

```
M_7j   = rolling median of prior 7-day VWMP values
MAD_7j = median(|p_j − M_7j|)
z_MAD  = 0.6745 × |VWMP(T) − M_7j| / MAD_7j

S_stat = exp(−z_MAD² / (2 × sigma_mad²))
```

Default:

```
sigma_mad = 3.5
```

The threshold z_MAD = 3.5 is used as a calibration point for continuous
penalization, not as a hard rejection rule inside the confidence index.
Hard outlier rejection is handled upstream by the curation pipeline.

If N_valid < 3, S_stat is capped at:

```
s_stat_floor = 0.2
```

This reflects high statistical uncertainty. It does not make the price
unavailable: the price is unavailable only when N_valid = 0.

### S_liq — Deep liquidity

S_liq combines a TVL score and a slippage score through a geometric mean.

```
S_TVL = max(
  0,
  min(
    1,
    log10(TVL(T) / TVL_min) / log10(TVL_ref / TVL_min)
  )
)
```

with:

```
TVL_min = 10,000 USD
TVL_ref = 1,000,000 USD
```

```
S_slip = exp(−slip_1k(T) / slip_max)
```

with:

```
slip_max = 0.005
```

```
S_liq = sqrt(S_TVL × S_slip)
```

For the cross-rate branch via WETH:

```
S_liq_cross = sqrt(S_liq(TOKEN/WETH) × S_liq(WETH/USDC))
```

When TVL or slippage columns are absent, the available sub-score is used alone;
`S_liq` is `null` only when neither column is present.

### S_coh — Inter-source coherence

For DEX branches (`0a`, `0b`, `1`, `2`), S_coh measures the relative deviation
between the DEX-derived price and the Chainlink reference feed.

```
δ(T) = |P_DEX(T) − P_CL(T)| / P_CL(T)

S_coh = exp(−(δ(T) / δ_tol(asset))²)
```

`δ_tol(asset)` is the native deviation threshold of the relevant Chainlink
feed, stored as `feed_deviation_threshold` in the provenance table.

Typical documented values include:

- 0.5% for ETH/USD
- 1% for COMP/USD

### Confidence scores by branch type

The three sub-scores are only meaningful when an independent reference
exists. The table below summarises what is computed for each branch:

| Branch | S_stat | S_liq | S_coh | Overall score |
|--------|--------|-------|-------|---------------|
| `0a` direct stable | computed | computed | DEX vs CL | computed |
| `0b` cross-rate (V3) | from CL history¹ | geometric mean of both legs² | DEX vs CL | computed |
| `1` alternative pool | from CL history¹ | geometric mean of both legs² (cross-rate) or direct | DEX vs CL | computed |
| `2` alternative AMM | from CL history¹ | geometric mean of both legs² (cross-rate) or direct | DEX vs CL | computed |
| `3` chainlink_fallback | **N/A** | **N/A** | **N/A** | **null** |
| `4` unavailable | **N/A** | **N/A** | **N/A** | **null** |

**Note 1 — S_stat for cross-rate branches**: TOKEN/WETH files do not
contain a USD price series. S_stat is therefore computed by comparing the
final USD cross-rate price against the 7-day rolling history of the asset's
Chainlink feed, which is the only available independent USD reference.

**Note 2 — S_liq for cross-rate branches**: when the TOKEN/WETH leg CSV
has no TVL or slippage columns (common for Uniswap V2 and SushiSwap files),
S_liq falls back to the ETH/USDC leg alone and the warning
`s_liq_cross_rate_token_leg_missing` is added to the response.

**Level 3 — all scores are N/A**: when Chainlink is the primary source,
there is no independent reference against which to measure statistical
coherence, liquidity, or inter-source deviation. All three sub-scores
are `null` and the overall confidence is `null`.

## Structured warnings

Every response may carry structured warnings at three levels:

- **top-level `warnings`** — deduplicated union of all warnings from every
  source; convenient for a single scan.
- **`provenance.warnings`** — warnings tied to the data source (pool
  viability checks, volume type, swap count).
- **`confidence.warnings`** — warnings tied to confidence score computation.

Each warning has the shape:

```json
{"code": "string_code", "message": "human-readable text", "severity": "info|warning|error"}
```

### Warning catalogue

| Code | Severity | Where | Meaning |
|------|----------|-------|---------|
| `volume_not_usd` | warning | provenance | Volume column is token-denominated (ETH/WETH/crvUSD). The 24 h USD volume zombie check cannot be evaluated and is skipped. Common for all TOKEN/WETH cross-rate files. |
| `low_swap_count` | info | provenance | Fewer than `min_swaps_for_stat_score` clean swaps in the window (default: 3). The VWMP is based on very few trades; reliability is reduced. |
| `mad_outliers_excluded` | info | provenance | One or more swaps were removed by the MAD outlier filter before VWMP computation. |
| `missing_tvl_column` | warning | confidence | TVL column not found in the source file. S_TVL cannot be computed; S_liq degrades to S_slip alone, or to `null` if slippage is also absent. |
| `missing_slippage_column` | warning | confidence | Slippage column not found. S_slip cannot be computed; S_liq degrades to S_TVL alone. |
| `liquidity_score_unavailable` | warning | confidence | Neither TVL nor slippage columns exist in the source file. S_liq is `null` and the overall confidence score is `null`. |
| `s_liq_cross_rate_token_leg_missing` | info | confidence | For a cross-rate branch, the TOKEN/WETH leg has no TVL or slippage data. S_liq is estimated from the ETH/USD leg only instead of the geometric mean of both legs. |
| `s_stat_insufficient_data` | warning | confidence | Fewer than `min_swaps_for_stat_score` observations in the 7-day window. S_stat is capped at `s_stat_floor` (default: 0.2). |
| `s_coh_no_chainlink_observation` | warning | confidence | No Chainlink observation found at or before the requested timestamp. S_coh is `null`. |

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
  tvl_score_mode: "log_memoire"          # or "linear_to_threshold" or "binary_threshold"
  tvl_log_min_usd: 10000                 # S_TVL = 0 at this TVL (log_memoire mode)
  tvl_log_ref_usd: 1000000              # S_TVL = 1 at this TVL (log_memoire mode)
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
  "granularity": "raw",
  "swap_count": null,
  "window_seconds": null,
  "unavailable_reason": null,
  "confidence": { ... },
  "provenance": { ... },
  "warnings": []
}
```

Optional query parameters:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `granularity` | `raw`, `minute`, `hour`, `day` | `raw` | Price calculation mode (see below) |
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
# Raw timestamps from the source CSV
curl "http://127.0.0.1:8000/v1/prices/UNI?start=2021-09-01&end=2021-09-02&limit=100"

# One VWMP point per hour
curl "http://127.0.0.1:8000/v1/prices/LINK?start=2024-01-01&end=2024-01-31&granularity=hour&limit=500"

# One VWMP point per day with confidence scores
curl "http://127.0.0.1:8000/v1/prices/LINK?start=2024-01-01&end=2024-01-31&granularity=day&include_confidence=true"
```

With `granularity=raw` (default), timestamps come from the winning source CSV with no synthetic resampling. With `minute`, `hour`, or `day`, the API generates evenly-spaced timestamps from `start` to `end` and computes a VWMP at each point.

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
