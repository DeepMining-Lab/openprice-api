# OpenPrice API

A Proof of Concept API that exposes historical crypto-asset prices from CSV files: no database required.

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
| `0a` | `direct_stable` | TOKEN/USDC or TOKEN/USDT Uniswap V3 pool: highest-TVL non-zombie pool wins |
| `0b` | `cross_rate` | TOKEN/WETH × WETH/USD cross-rate, Uniswap V3: both legs matched as-of timestamp |
| `1` | `alternative_pool` | Same pair on Uniswap V2: direct stable (ETH) or cross-rate (others); same endogenous auditability, older protocol version |
| `2` | `alternative_amm` | Curve (ETH via crvUSD/WETH, inverted) or SushiSwap TOKEN/ETH cross-rate for other assets |
| `3` | `chainlink_fallback` | Chainlink oracle: latest observation at or before `T` |
| `4` | `unavailable` | Explicit NULL: no reliable source found |

### Zombie pool rules

A pool is excluded from selection when any of the following conditions is true (if the relevant column exists):

- `TVL < seuil_TVL_min_usd` (default: 100 000 USD)
- `volume_24h < seuil_vol_min_usd_24h` (default: 10 000 USD)
- No observation in the last `fenetre_inactivite_jours` days (default: 30)

When a required column is absent, the check is skipped and a warning is added to the response: the API never invents liquidity data.

**Historically low TVL pools**: early-date observations of newer DEX
pools may show very low TVL (e.g., a Uniswap V3 LINK/WETH pool with
$3 791 TVL in February 2022, versus a $100 000 threshold). These pools
are correctly classified as zombie and the pipeline falls to the next
level: typically a Uniswap V2 pool whose TVL column is absent, which
allows it to pass the check (with a `missing_tvl_column` warning). This
behaviour is expected: during the early months of Uniswap V3 adoption,
liquidity was concentrated on V2, and the zombie filter reflects that.

### Cross-rate lag

For level `0b`, both the TOKEN/WETH leg and the WETH/USD leg are matched using an as-of strategy (latest observation ≤ T). If the two legs are more than `cross_rate_max_lag_seconds` apart (default: 3600 s), the cross-rate is rejected and the API falls to the next level.

The WETH/USD leg trades every few seconds, so for raw point reads this rule in practice requires the
token pool to have traded within the hour before T, however liquid it is. For example, AAVE at
2024-06-01T00:00Z falls back to Chainlink (level 3): the AAVE/WETH pool (TVL 1.35 M USD) last traded
7 032 s before the ETH/USD leg. V3 responses name the rule and the values that rejected each candidate
(see [V3 diagnostics](#diagnostics-added-by-v3)).

## Granularity and VWMP

The API supports four price-calculation modes controlled by the `granularity` parameter.

| Granularity | Initial window | Symmetric window around T |
|-------------|---------------|--------------------------|
| `raw` | n/a | Latest single observation ≤ T (no aggregation) |
| `minute` | 60 s | T ± 30 s |
| `hour` | 3 600 s | T ± 30 min |
| `day` | 86 400 s | T ± 12 h |

Sub-minute granularity is intentionally excluded: Ethereum blocks are ~12 s apart, which makes a stable per-block price definition unreliable.

### Price formula (minute / hour / day)

For each `(asset, T, granularity)`, the pipeline:

1. Selects the best non-zombie pool (same pool-selection logic as the source hierarchy).
2. Queries all swaps in `[T − Δ/2, T + Δ/2]`.
3. Applies the MAD outlier filter (which also removes MEV sandwich trades) with `sigma_mad` from config.
4. Computes the **VWMP** (Volume-Weighted Median Price):

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
| `day` | no expansion: falls directly to the next source level |

After all expansion steps are exhausted, the pipeline falls to the next branch level (`0a → 0b → 2 → 3 → 4`).

### Response fields (windowed granularities)

```json
{
  "granularity": "hour",
  "swap_count": 12,
  "window_seconds": 3600
}
```

- `swap_count`: number of clean swaps used for the VWMP (after MAD filtering).
- `window_seconds`: actual window size used (may be larger than the initial window if R1 fired).
- `provenance.excluded_swaps`: number of swaps removed by the MAD filter.

## Temporal provenance & reference block

To make every price fully replayable by a third party (mémoire §6.8.1, §6.2.6),
the `provenance` block carries the temporal-reconstruction metadata below. All
fields are present on both `/v1/...` and `/v2/...` responses (additive; the V1
contract is otherwise unchanged). Window fields are populated for windowed
granularities only; `raw` point reads leave them `null`.

| Field | When populated | Meaning |
|-------|----------------|---------|
| `initial_window_seconds` | windowed | Width of the initial window Δ₀ before any R1 expansion |
| `window_seconds` | windowed | Effective window width after R1 (also at top level) |
| `window_start_utc` / `window_end_utc` | windowed | UTC bounds of the effective window |
| `window_bound_policy` | windowed | Inclusion convention: always `left_closed_right_open` (`>= start AND < end`), so a swap on a boundary is never counted in two adjacent windows |
| `expansion_step` | always | R1 step that produced the result: `0` = initial window (or raw point read), `1+` = an expanded window |
| `n_raw` | windowed | Raw swap count in the window **before** MAD filtering (`raw_swap_count`) |
| `swap_count` | windowed | Swaps **retained** after filtering (`valid_swap_count`) |
| `excluded_swaps` | windowed | Swaps **removed** by the MAD/volume filter (`filtered_swap_count`) |
| `reference_block_number` | raw, DEX source | Ethereum block `b_ref(T)` of the winning observation |
| `reference_block_timestamp` | raw, DEX source | UTC timestamp of that block |

**Reference block availability.** `reference_block_*` is read from the winning
source row when the source CSV carries a `block_number` column (all DEX files).
It is `null` with a structured warning otherwise: Chainlink feeds have no block
column (`block_metadata_unavailable`), and windowed VWMP aggregates span many
blocks so no single reference exists (`block_metadata_aggregated`). The API never
fabricates a block number.

### Price data status

The `data_status` field distinguishes a directly-observed price from a
reconstructed, fallback, or rejected one: a distinction the methodology
requires for evidentiary use (mémoire §6.2.5):

| `data_status` | Meaning |
|---------------|---------|
| `observed` | Price found directly in the initial target window (or raw point read) |
| `reconstructed` | Price obtained only after the window had to expand (R1, `expansion_step ≥ 1`) |
| `rejected_outlier` | Every swap in the window was flagged by the MAD filter; the value is the unfiltered fallback and should be treated with caution |
| `oracle_fallback` | Chainlink oracle (level 3): no viable DEX source |
| `unavailable` | No reliable source (level 4); `price_usd` is `null` |

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

### S_stat: Statistical hygiene

S_stat measures the statistical coherence of the published VWMP against
the median of price observations available in the reference CSV over the
prior 7-day window `[T − 7 days, T)`.

**Reference file selection:**
- Cross-rate branches (0b, 1 cross-rate, 2): the asset's Chainlink CSV is
  used: it is the only independent USD series available for TOKEN/WETH pools.
- Direct stable branch (0a): the source pool CSV is used directly.

```
M_7j   = median of all price observations in the reference CSV over [T−7d, T)
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

**Edge case (MAD_7j = 0):** when all reference observations in the 7-day
window are identical, MAD_7j = 0 and z_MAD is undefined. `S_stat` is set
to `1.0` (perfect consistency of the reference series).

If fewer than `min_swaps_for_stat_score` observations exist in the 7-day
reference window (default: 3), S_stat is capped at:

```
s_stat_floor = 0.2
```

This floor reflects insufficient historical data for a robust median; it is
independent of the number of swaps used to compute the current VWMP. The
price is unavailable only when the current window yields zero clean swaps.

### S_liq: Deep liquidity

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
S_slip = exp(−|slip(T)| / slip_max)
```

with:

```
slip_max = 0.005
```

`slip(T)` is read from the `slip_1k` column when present (1 000 USD
reference order). If `slip_1k` is absent, `slip_10k` is used as the
canonical fallback. The absolute value is applied defensively; slippage
values are stored as decimals in the CSV files.

```
S_liq = sqrt(S_TVL × S_slip)
```

For the cross-rate branch via WETH:

```
S_liq_cross = sqrt(S_liq(TOKEN/WETH) × S_liq(WETH/USDC))
```

If the TOKEN/WETH leg CSV has no TVL or slippage columns (common for
Uniswap V2 and SushiSwap files), `S_liq_cross` falls back to the
WETH/USDC leg alone and the warning `s_liq_cross_rate_token_leg_missing`
is added. If neither leg provides liquidity data, `S_liq` is `null`.

For direct stable branches (0a), the same degradation applies within a
single leg: when only TVL or only slippage is available, the single
available sub-score is used; `S_liq` is `null` only when both are absent.

### S_coh: Inter-source coherence

For DEX branches (`0a`, `0b`, `1`, `2`), S_coh measures the relative deviation
between the DEX-derived price and the Chainlink reference feed.

```
δ(T) = |P_DEX(T) − P_CL(T)| / P_CL(T)

S_coh = exp(−(δ(T) / δ_tol(asset))²)
```

`δ_tol(asset)` is the native deviation threshold of the relevant Chainlink
feed, configured in `config/openprice.yaml` under
`chainlink.deviation_threshold_by_asset` (fallback:
`chainlink.default_deviation_threshold`). The effective value for each asset
is visible at `GET /v1/config`.

Configured values:

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

**Note 1 (S_stat for cross-rate branches):** TOKEN/WETH files do not
contain a USD price series. S_stat is therefore computed by comparing the
final USD cross-rate price against the raw price observations in the asset's
Chainlink CSV over `[T − 7 days, T)`, which is the only available
independent USD reference.

**Note 2 (S_liq for cross-rate branches):** `S_liq_cross` is the geometric
mean of both legs when both carry TVL/slippage data. If the TOKEN/WETH leg
CSV lacks both columns (common for Uniswap V2 and SushiSwap files), the
WETH/USDC leg score is used alone and `s_liq_cross_rate_token_leg_missing`
is added. If only the TOKEN/WETH leg has data, it is used alone with no
warning. If neither leg has data, `S_liq` is `null`.

**Level 3 (all scores are N/A):** when Chainlink is the primary source,
there is no independent reference against which to measure statistical
coherence, liquidity, or inter-source deviation. All three sub-scores
are `null` and the overall confidence is `null`. In V3, the `fallback_explained` warning says which
higher-priority sources were rejected and why.

### Qualitative confidence level

The research methodology defines a four-band qualitative classification
derived from the score:

| Score C | Level | Usage recommendation |
|---------|-------|----------------------|
| C ≥ 0.80 | **High** | Legally opposable without reservation |
| 0.50 ≤ C < 0.80 | **Medium** | Opposable with explicit disclosure of limits |
| C < 0.50 | **Low** | Informational only, not opposable |
| `null` | **N/A** | Chainlink fallback (level 3) or unavailable (level 4) |

> **Implementation note:** the current API (`/v1/confidence/{asset}/at`)
> returns the raw `score` and sub-scores only. The `qualitative_level`
> field is not yet exposed. Callers can derive it from the `score` field
> using the table above.

## Structured warnings

Every response may carry structured warnings at three levels:

- **top-level `warnings`**: deduplicated union of all warnings from every
  source; convenient for a single scan.
- **`provenance.warnings`**: warnings tied to the data source (pool
  viability checks, volume type, swap count).
- **`confidence.warnings`**: warnings tied to confidence score computation.

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
| `block_metadata_unavailable` | info | provenance | The source CSV has no `block_number` column (e.g. Chainlink feeds). `reference_block_number` / `reference_block_timestamp` are `null`. |
| `block_metadata_aggregated` | info | provenance | A windowed VWMP aggregates swaps from several blocks, so there is no single reference block. `reference_block_*` are `null`. |

## API V2: peg neutralization & S_peg

V2 is an **additive** evolution of the confidence index driven by the expert
review. The V1 routes (`/v1/...`) are **frozen and unchanged**:
they reproduce the original mémoire model exactly (a non-regression test
guarantees it). V2 lives under the `/v2/` prefix, reuses the same source
hierarchy / DuckDB reads / VWMP, and only changes how confidence is computed.

### What changes in V2

1. **Peg neutralization upstream of S_coh.** A stablecoin-quoted DEX price is
   not a USD price when the stablecoin itself depegs. Before measuring
   coherence, the price is neutralized by the effective quote-currency peg:

   ```
   price_neutralized_usd = price_raw_in_quote × peg(T)
   ```

   `peg(T)` is read as-of `T` from the Chainlink **USDC/USD** or **USDT/USD**
   feed (whichever matches the source pool's quote token; for cross-rate
   branches the peg of the ETH/USD reference leg is used). The neutralized
   price becomes the headline `price_usd`. S_coh is then computed on the
   neutralized price, decoupling a genuine DEX/oracle divergence from a
   stablecoin depeg.

2. **New separate sub-score S_peg.** The depeg signal is reported on its own,
   not folded into the price coherence:

   ```
   S_peg = exp(−(|peg(T) − 1| / peg_tol)²)        peg_tol default = 0.0025 (0.25 %)
   ```

   `S_peg` is published **outside** `subscores` (mode `3sub`). It is `null`
   (`s_peg_not_applicable`) when the source has no stablecoin quotation:
   Chainlink fallback (level 3) or the Curve crvUSD/WETH ETH pool.

3. **Weighted composition (same form as V1), two modes:**

   ```
   3sub (default):  C = S_stat^w_stat · S_liq^w_liq · S_coh^w_coh     (S_peg excluded)
   4sub (optional): C = ∏ S_i^(w_i / Σw)   including S_peg            (weights renormalized)
   ```

   Mode is set by `confidence_v2.composite.mode`.

4. **Fragility flag.** `fragility_flag = (C < c_threshold)`, exposed separately
   from `C`. The threshold is **uncalibrated by default** (`c_threshold: null`);
   while null, `fragility_flag` is `null` and a `fragility_threshold_uncalibrated`
   warning is added: the API imposes no qualitative verdict until the threshold
   is set empirically.

5. **Optional volatility-normalized S_stat** (`confidence_v2.s_stat.normalize_by_volatility`,
   off by default) and a configurable S_stat window (a *local anomaly* score,
   not a direct volatility measure).

### Validation case: USDC depeg, 2023-03-11 (SVB)

At `T = 2023-03-11T12:00:00Z`, USDC traded at **$0.9097** while the Chainlink
AAVE/USD feed read **$65.23**. AAVE resolves to a `0b` cross-rate quoted in USDC:

| | V1 | V2 |
|---|---|---|
| Price | 71.67 (USDC-denominated) | **65.19** (neutralized: 71.67 × 0.9097 ≈ Chainlink) |
| `S_coh` | `6e-170` (collapses, false alarm) | **0.987** (coherent) |
| `S_peg` | n/a | **0.0** (depeg reported separately) |
| `C` | `3.7e-57` | **0.93** |

V2 stops penalizing a real, well-priced DEX observation for a stablecoin event,
while still surfacing that event through `S_peg`.

### V2 endpoints

```bash
# Price at a timestamp (peg-neutralized + V2 confidence)
curl "http://127.0.0.1:8000/v2/prices/AAVE/at?timestamp=2023-03-11T12:00:00Z"

# Confidence only (V2)
curl "http://127.0.0.1:8000/v2/confidence/AAVE/at?timestamp=2023-03-11T12:00:00Z"

# Effective V2 configuration (the confidence_v2 block + peg feeds)
curl "http://127.0.0.1:8000/v2/config"
```

Example `/v2/prices/{asset}/at` response (abridged):

```json
{
  "asset": "AAVE",
  "timestamp": "2023-03-11T12:00:00Z",
  "granularity": "raw",
  "price_usd": 65.19,
  "price_raw_in_quote": 71.67,
  "price_neutralized_usd": 65.19,
  "quote_currency": "USDC",
  "quote_currency_peg": 0.90965689,
  "branch_level": "0b",
  "branch_label": "cross_rate",
  "data_status": "observed",
  "confidence": {
    "C": 0.93,
    "composition_mode": "3sub",
    "fragility_flag": null,
    "subscores": {"S_stat": 0.98, "S_liq": 0.83, "S_coh": 0.987},
    "S_peg": 0.0,
    "qualitative_level": "high",
    "weights": {"w_stat": 0.3333, "w_liq": 0.3333, "w_coh": 0.3334},
    "warnings": [
      {"code": "coh_neutralized_peg", "severity": "info", "message": "..."},
      {"code": "fragility_threshold_uncalibrated", "severity": "info", "message": "..."}
    ]
  },
  "provenance": { "...": "files used, calculation path, peg source" },
  "warnings": []
}
```

### V2 configuration

The V2 parameters live in a **separate** `confidence_v2:` block in
`config/openprice.yaml` (it never overrides the V1 keys). The effective values
are visible at `GET /v2/config`.

```yaml
confidence_v2:
  composite:
    mode: "3sub"            # 3sub (default) | 4sub
  weights:
    w_stat: 0.3333333333
    w_liq:  0.3333333333
    w_coh:  0.3333333334
    w_peg:  0.25            # used only in 4sub mode (weights renormalized)
  coh:
    default_delta_tol: 0.005
    delta_tol_by_asset: {}  # δ_tol decoupled from the feed's native threshold
  peg:
    tol: 0.0025             # 0.25 % for USDC/USDT
  s_stat:
    window_seconds: 604800  # 7 days; shorten for sensitivity tests
    volatility_estimator: "MAD"   # MAD | realized_vol
    normalize_by_volatility: false
  fragility:
    c_threshold: null       # null → fragility_flag = null + warning
```

The peg feeds are registered (not under the V1 dataset registry, V2 only):

```
stablecoins/chainlink_usdc_usd.csv
stablecoins/chainlink_usdt_usd.csv
```

### V2 warning catalogue

| Code | Severity | Meaning |
|------|----------|---------|
| `coh_neutralized_peg` | info | `S_coh` was computed on the peg-neutralized DEX price (V2 behaviour). |
| `s_peg_not_applicable` | info | Source has no stablecoin quotation (level 3 / Curve) or the peg feed is unavailable; `S_peg = null`. |
| `fragility_threshold_uncalibrated` | info | `confidence_v2.fragility.c_threshold` is not set; `fragility_flag = null`. |

## API V3: performance engine

V3 serves the **same methodology and response schema as V2** (source hierarchy, VWMP, peg
neutralization, S_stat / S_liq / S_coh / S_peg, fragility flag) from an indexed Parquet copy of the
CSV files instead of scanning the CSVs on every request. `/v1` and `/v2` are untouched and keep reading
the CSV files, so results published with them stay reproducible.

| Measured on the production host | V1/V2 (CSV) | V3 |
|---|---|---|
| ETH point price, confidence + provenance | 23 to 40 s | 0.05 to 0.25 s |
| COMP point price (cross-rate) | ~14 s | ~0.08 s |
| LINK / AAVE point price | 3 to 4 s | ~0.05 s |
| ETH before pool creation (unavailable) | ~45 s | ~0.14 s |
| `/compare` COMP, 37 points | 290 s | 0.6 s |
| repeated request (LRU cache) | n/a | ~2 ms |
| `/prices` ETH `hour`, 25 points | n/a | ~0.3 s |
| `/prices` ETH `raw`, 1 000 points (one page) | n/a | ~5 s (~60 ms from the cache) |

The point figures are per request. A range costs about 5 ms per point (uncached, `v3.range_workers: 8`
on the 12-core host), so a 1 000-point page takes seconds, not 0.1 s. The cache is emptied at every
sync that adds data (every 30 min with `deploy/`).

### What differs from V2 (on purpose)

1. **The S_stat reference window is complete.** V1/V2 read `[T-7d, T)` with `LIMIT api.max_limit`
   (10 000), i.e. only the *oldest* rows: for the ETH/USDC pool this happened on ~99.6 % of dates
   (e.g. S_stat = 0.000 instead of 0.84 on 2022-05-12). V3 reads the whole window.
2. **The windowed VWMP read is not truncated either** (it hit the same limit on ~6 % of ETH `day` windows).
3. **110 duplicated swap events** of `eth_usdc_uniswap_v3_005` (same `tx_hash` + `log_index`, only the
   extraction metadata differs) are ignored. The CSV is never modified.

No other number is meant to differ; V3 only adds the [diagnostics](#diagnostics-added-by-v3) and the
[range pagination](#ranges-pagination-and-latency) below. `v3.legacy_truncation: true` restores (1) and (2), which lets
`tests/golden/compare_v3.py --legacy` prove that V3 reproduces V2 exactly (the only remaining differences are
the windows containing the 110 duplicates). Same-timestamp ties still return the *first* row of the CSV, as V2 did.

### Validated against real V2 traffic

`tests/golden/` captures real V2 responses (`capture_v2.py`) and replays them through V3
(`compare_v3.py`). On a 245-request sample spanning the five assets, every granularity, and several
market-stress dates (LUNA, the Aug-2024 flash crash, the Mar-2023 USDC depeg):

* in `legacy_truncation` mode, **242/245 are byte-identical** to V2; the 3 remaining differences are
  entirely explained by the 110 removed duplicates (`tests/test_v3_golden.py` runs this as a permanent
  regression guard whenever the Parquet store and the golden file are present);
* in default (fixed) mode, **LINK, UNI, AAVE and COMP are unaffected** (0 differences); only ETH changes,
  through S_stat/C/`fragility_flag` and the four `day`-granularity prices listed above.

### Build and run

```bash
.venv/bin/python -m app.v3.sync        # ~2 min the first time; then incremental
.venv/bin/uvicorn app.main:app         # /v3/... is served next to /v1 and /v2
curl "http://127.0.0.1:8000/v3/prices/LINK/at?timestamp=2025-06-01T12:00:00Z"
curl  http://127.0.0.1:8000/v3/ready
```

| Endpoint | Description |
|---|---|
| `GET /v3/prices/{asset}/at` | point price (V2 schema plus the V3 diagnostics) |
| `GET /v3/prices/{asset}` | time series (`raw` = one point per distinct swap timestamp of the winning source, or `minute|hour|day`) |
| `GET /v3/confidence/{asset}/at` | V2 confidence breakdown: the confidence block of the default `/at` response |
| `GET /v3/compare/{asset}` | DEX vs Chainlink over a range (identical to `/v1/compare`) |
| `GET /v3/config`, `GET /v3/ready` | effective V3 config; readiness (503 until the store is built) |

Paths are case-sensitive (`/V3/...` returns 404); asset symbols are not (`eth` = `ETH`).

#### Response headers

| Header | `/prices/{asset}/at`, `/confidence/{asset}/at` | `/prices/{asset}` | `/compare/{asset}` |
|---|---|---|---|
| `X-Dataset-Version` | yes | yes | yes |
| `Server-Timing` (`total;dur=<ms>`) | yes | yes | yes |
| `X-Cache` | `HIT` or `MISS` | `HIT`, `MISS` or `PARTIAL` (per point) | no (not cached) |
| `X-Truncated` | no | `true` or `false` | `true` or `false` |
| `X-Next-Start`, `Link: <?…>; rel="next"` | no | only when truncated | only when truncated |

`/confidence/{asset}/at` shares the cache entry of the default `/prices/{asset}/at` request (confidence
and provenance included).

### Diagnostics added by V3

They are additive: they never change a price, a score or a branch.

| Code | Severity | When |
|---|---|---|
| `future_timestamp` | warning | `timestamp` is after the server time. The response is an explicit NULL: level `4`, `unavailable_reason: "future_timestamp"`, no confidence, never cached. Before this rule, a future date returned the last known price, even as a level `0a` "observed" price with a confidence score when it fell within 30 days of the last swap. |
| `beyond_data_coverage` | warning | The requested time (or, for `minute`/`hour`/`day`, the end of the VWMP window) is after the last synced data of a folder the price depends on (source files and peg feed). The price is still returned (as-of rule) but is provisional: it can change after the next extraction. A folder's coverage is its latest synced observation. Each asset folder holds its Chainlink feed (1 h heartbeat), so its coverage trails the extraction run by at most about an hour; for the peg feeds (`stablecoins/`, 24 h heartbeat) it can trail by up to a day, which errs on the side of flagging. |
| `fallback_explained` | info | The answer does not come from the first level of the hierarchy (or is level `4`). The message lists each rejected higher-priority candidate with the rule and the measured value. |

`provenance.rejected_candidates` lists every candidate file evaluated and not used, in evaluation order,
including siblings of the winning level (for example a zombie `0a` pool next to the one that answered):

```json
{"level": "0b", "file": "aave/aave_weth_uniswap_v3_03.csv", "rule": "cross_rate_lag",
 "message": "token leg 2024-05-31T22:02:35Z is 7,032 s from the ETH/USD leg 2024-05-31T23:59:47Z (limit 3,600 s)",
 "value": 7032.0, "threshold": 3600.0, "last_observation_utc": "2024-05-31T22:02:35Z"}
```

Rules: `zombie_tvl`, `zombie_volume_24h`, `inactive` (no observation for `fenetre_inactivite_jours`),
`cross_rate_lag`, `eth_leg_unavailable`, `no_observation` (nothing at or before T), `no_swaps_in_window`
(windowed granularities, after R1 expansion), `missing_columns`, `dataset_missing`. The diagnostics go to
the top-level `warnings` and, when there is a confidence block, to its `warnings` too (they explain a
provisional score or a `C = null` after a fallback to Chainlink). The parity tools remove them with
`strip_v3_diagnostics` before comparing with V2.

### Ranges: pagination and latency

* `limit` defaults to 1 000 points (hard cap 10 000). One day of ETH swaps is several thousand distinct
  timestamps (4 112 on 2024-03-01), and even `minute` over 24 h is 1 441 points, so a day does not fit in
  one default page.
* When the result is cut at `limit`, the response carries `X-Truncated: true` and `X-Next-Start`: repeat the
  same query with `start` set to that value. `Link: <?…>; rel="next"` gives the whole next query, relative
  to the URL you called, so it stays valid behind the gateway. The last page has `X-Truncated: false`.

```bash
curl -s -D - -o page1.json "http://127.0.0.1:8000/v3/prices/ETH?start=2024-03-01T00:00:00Z&end=2024-03-02T00:00:00Z" \
  | grep -i -E "^x-truncated|^x-next-start"
# x-truncated: true
# x-next-start: 2024-03-01T05:51:11Z      -> next page: same query with start=2024-03-01T05:51:11Z
```

* A `raw` series has one point per distinct timestamp. V1 returned one point per swap, but all the swaps
  of a timestamp get the same as-of answer (the first swap of the block), so the extra rows were identical
  copies (35 % of the ETH/USDC rows on 2024-03-01, 37 % over 2024). It also lets each page start exactly
  where the previous one stopped. `/compare` pages never split rounds that share a timestamp.
* Prefer `hour` (or `minute` over a few hours) when a coarser series is enough: 25 hourly points take ~0.3 s.

### How the store works

* `python -m app.v3.sync` converts each CSV to canonical, time-sorted Parquet segments under
  `v3.parquet_root` (default `~/openprice/parquet`; never inside the datasets directory) and writes
  `manifest.json` atomically. The CSV row number is kept (`rn`), so the CSV order (which V1/V2 relied on
  for ties) is reproduced exactly.
* It is incremental: only bytes appended since the last run are read, after checking a fingerprint of what
  was already consumed; a rewritten header, truncated or rewritten file triggers a rebuild of that dataset
  only. A partially written last line is never consumed. It is safe to run while the extraction containers
  append to the CSVs (see `deploy/`).
* The API polls the manifest (`v3.manifest_poll_seconds`) and reloads it without restart; the LRU cache key
  includes the dataset version, so it can never serve a stale price after a sync.
* Range endpoints run the same per-point engine in parallel (`v3.range_workers`, default 8; about 5 ms
  per point on the 12-core host). A set-based ASOF-join version would be faster still and is not implemented.

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
  seuil_TVL_min_usd: 100000         # Pool TVL below this → zombie
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

## Web interface

A self-contained, dependency-free web UI ships in `interface-api/index.html`
(a single static HTML file). The running API serves it directly:

```
http://127.0.0.1:8000/ui
```

It lets you, without writing any `curl`:

- pick an asset, granularity (`raw`/`minute`/`hour`/`day`) and timestamp, and
  switch between **V1**, **V2** and **V3** of the API (the info bubble next to the
  version selector explains the differences: V3 answers the V2 schema from the
  fast Parquet engine described below, in ~0.05 s instead of 3 to 40 s);
- read the price with its branch level and `data_status` (colour-coded:
  *observé* / *reconstruit* / *rejeté* / *repli oracle*);
- see the confidence gauge with the S_stat / S_liq / S_coh sub-scores (and the
  V2 `S_peg`, fragility flag and qualitative level when present);
- expand a **Provenance** panel showing files used, calculation path, leg
  timestamps, the full temporal-provenance block (window bounds, R1 expansion
  step, raw/valid/filtered swap counts) and the reference block `b_ref(T)`.

The API URL field picks its default from where the page is served:

- served by the API at `/ui` (e.g. the internal link): that same API, no key needed;
- anywhere else (GitHub Pages, or opened as a local file `file://…/index.html`): the
  Deep Mining gateway, `https://gateway.deepmining.ch/prices`, which requires an API key.

The field stays editable, e.g. `http://127.0.0.1:8000` for a local instance. A page
served over `https` cannot call a plain-`http` API (the browser blocks it), `localhost`
excepted.

### Public explorer (GitHub Pages)

The same file is published at **https://openprice-explorer.deepmining.ch/** (custom
domain of the GitHub Pages site; https://deepmining-lab.github.io/openprice-api/ redirects
to it) by `.github/workflows/pages.yml`, on every push to `main` that changes
`interface-api/` (only that folder is published). One-time setup: *Settings → Pages →
Build and deployment → Source: GitHub Actions*.

That page calls the API through the gateway, so it needs a gateway API key. During the
test phase, keys are given on request through the contact form of the OpenPrice site,
https://fair.deepmining.ch/contact (the **Request a key** link of the page opens it):

- paste it in the **API key** field; the status dot then reads `ok`, `key required`
  (401) or `invalid key` (403);
- the key is sent only in the `X-API-Key` header, never in a URL;
- it is kept in `sessionStorage` (forgotten when the tab is closed), or in
  `localStorage` when **Remember on this device** is ticked.

Through the gateway, V1/V2 computations are rate-limited (10 per minute per key, 3 in
progress across all users): prefer V3. The page relies on the gateway's CORS headers
(`Access-Control-Allow-Origin: *`, `X-API-Key` allowed by the preflight).

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
    "seuil_TVL_min_usd": 100000,
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
  "expansion_step": 0,
  "window_seconds": null,
  "window_bound_policy": null,
  "reference_block_number": 18913456,
  "reference_block_timestamp": "2024-01-01T00:00:00Z",
  "parameters": {
    "seuil_TVL_min_usd": 100000,
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
