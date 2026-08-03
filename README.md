# NautilusTraderPipeline

An event-driven research and execution pipeline built on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — the Rust-native backtesting and live-trading engine exposed to Python via PyO3 adapters. The goal is a low-latency, deterministic and repeatable path from raw exchange data to backtested strategy to live execution, with the same code path used in research and in production.

Current venue: **Bybit** (USDT linear perpetuals). Primary instrument: **ETHUSDT-LINEAR.BYBIT**.

> **Status:** research and development. Data ingestion, catalog construction and the backtest harness are working. Bar-sampling research is in progress. Live execution is not yet implemented.

---

## Design principles

**Event-driven throughout.** There is no external database. Persistent state lives in a `ParquetDataCatalog` laid out for Nautilus ingestion, and every feature is computed as events arrive — the same code path in backtest and live. Anything too heavy for the event loop (incremental batch model training, Monte Carlo filters) runs as a side job and writes its output back into the pipeline as `CustomData`.

**The main Nautilus event loop is mono-threaded.** Heavy computation is therefore pushed out of the loop rather than parallelised inside it. This constraint drives most of the architectural decisions in [`project/progressionRoadmap.md`](project/progressionRoadmap.md).

**Streaming, not loading.** The tick catalog alone is ~81 GB against 24 GB of RAM. Every pass over history — bar aggregation, statistical analysis, backtests — streams day by day or in tick chunks, carrying only the state it needs across boundaries.

**Sample on an activity clock, not a calendar clock.** Volume and dollar bars are the default sampling unit, because sampling by traded activity yields returns substantially closer to IID Gaussian than fixed time intervals (Clark, 1973; Ané & Geman, 2000; Easley, López de Prado & O'Hara, 2012; López de Prado, 2018, ch. 2). Data transformations downstream aim at the same target: as close to IID Gaussian, with stationary distributional parameters, as the data allows.

**Integrity over convenience.** Simplicity is preferred, but never at the cost of data integrity, execution integrity, or strategy logic.

---

## Data flow

```text
Bybit public endpoints
  │
  ├── project/dataDownload/*            async downloaders → raw CSV/gz on external HDD
  │
  ▼
project/dataLoadingPipeline/*.ipynb     parse, normalise, convert ms → ns,
  │                                     build Nautilus objects
  ▼
project/nautilusDataCatalog/            ParquetDataCatalog (gitignored, ~106 GB)
  │   ├── crypto_perpetual/             instrument definition (from Bybit V5 API)
  │   ├── trade_tick/                   2,077 daily files, ~81 GB
  │   ├── order_book_deltas/            89 files, ~25 GB (L2 MBO)
  │   ├── bar/                          1-MINUTE-LAST-EXTERNAL, 64 files
  │   └── funding_rate_update/          full funding history
  │
  ▼
project/backtestingPipeline/
  ├── centralNode.py                    BacktestRunner — reusable BacktestNode wrapper
  ├── barDownload.ipynb                 tick → volume bar aggregation, written back to catalog
  └── *StatisticalAnalysis.ipynb        bar specification research
```

A second catalog, `nautilusPremiumCatalog/`, holds premium index klines separately so premium data never collides with the mark/last price bars in the main catalog.

---

## Repository layout

| Path | Contents |
| :--- | :--- |
| `project/dataDownload/` | Standalone async downloaders for Bybit tick, order book, kline/premium-kline and funding-rate history. Each has its own README and CLI. No API key required — all use public endpoints. |
| `project/dataLoadingPipeline/` | Notebooks that parse raw files into Nautilus objects (`TradeTick`, `OrderBookDelta`, `Bar`, `FundingRateUpdate`) and write them to the catalog. One notebook per data type. |
| `project/backtestingPipeline/` | `BacktestRunner` harness, the volume-bar aggregation node, and the bar-sampling statistical research. |
| `project/liveDataPipeline/` | Placeholder for the live Bybit data/execution client. Not yet implemented. |
| `project/utils/getInstrument.py` | `GetInstrument` — pulls live contract specs (tick size, lot step, precisions, leverage, notional bounds) from the Bybit V5 `instruments-info` endpoint and constructs a Nautilus `CryptoPerpetual`. Avoids hard-coding venue parameters that change. |
| `project/aws_nautilus_research_architecture.md` | Target AWS layout: S3 as the permanent data lake, ECR for the research image, disposable EC2 for compute. |
| `project/progressionRoadmap.md` | Five-phase plan from offline feature cataloging to live ML execution. |
| `project/dataEngineering.md` | Intended structure of the feature/label/dataset-builder pipeline. |
| `requirements.txt` | Pinned environment (`nautilus_trader==1.230.0`). |

---

## Setup

```bash
git clone https://github.com/Dsavvi180/NautilusTraderPipeline.git
cd NautilusTraderPipeline

python3 -m venv nautilusVenv
source nautilusVenv/bin/activate
pip install -r requirements.txt
```

Create `project/nautilus.env` pointing at wherever raw downloaded data lives:

```bash
DATA_DIR="/path/to/raw/market/data"
```

The Parquet catalogs are **not** in this repository — see [Data](#data) below.

---

## Usage

### 1. Download raw history

Each downloader is independent, resumable and skips files it already has. Example:

```bash
python project/dataDownload/BybitTickData/download_bybit_tick_data_async.py \
    --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
    --start 2020-10-21 --end 2026-06-28 \
    --concurrency 6
```

See the README inside each downloader directory for its full CLI. On a spinning disk, keep concurrency low (2–4).

### 2. Ingest into the catalog

Run the relevant notebook in `project/dataLoadingPipeline/`. Each one:

1. Fetches the instrument definition from the Bybit API and writes it to the catalog.
2. Reads raw files in date order, converting Bybit millisecond timestamps to Nautilus nanoseconds.
3. Writes Nautilus objects to the `ParquetDataCatalog` in daily/weekly partitions.

The order-book notebook includes a gap check across file boundaries; the catalog is only as trustworthy as that check.

### 3. Aggregate volume bars

`project/backtestingPipeline/barDownload.ipynb` streams ticks through a `BacktestNode`, lets the `DataEngine`'s `VolumeBarAggregator` build bars tick-by-tick — the same event-driven path used live — and a collector actor re-labels the finished bars `EXTERNAL` and flushes them back to the catalog in batches.

Bars land in the **same** catalog as the ticks, so a strategy backtest sources its instrument definition and its bars from a single `catalog_path`.

Two operational cautions, both enforced in the notebook:

- A full 2020–2026 run processes ~81 GB of ticks and takes hours. Validate on a short window first.
- Re-running appends duplicate bars, so the run cell refuses to start if the output bar folder already exists.

### 4. Run a backtest

`BacktestRunner` wraps venue, data, engine and run configuration behind sensible defaults. Pass in the actors you are developing:

```python
from centralNode import BacktestRunner
from nautilus_trader.config import ImportableActorConfig

runner = BacktestRunner(
    actors=[ImportableActorConfig(actor_path="my_module:MyActor", ...)],
    instrument_id="ETHUSDT-LINEAR.BYBIT",
    start="2024-01-01",
    end="2024-02-01",
    chunk_size=1_000_000,   # ticks streamed per chunk — bounds memory
)
results = runner.run()
```

Defaults: Bybit venue, NETTING OMS, MARGIN account, 1,000,000 USDT starting balance, `TradeTick` as the data class. Fees default to Bybit's published maker/taker (0.02% / 0.055%) via `GetInstrument`.

---

## Bar sampling research

`volumeBarStatisticalAnalysis.ipynb` and `dollarBarStatisticalAnalysis.ipynb` compare fixed thresholds against thresholds recalibrated daily from trailing average daily (dollar) volume — `ADDV(d-W .. d-1) / N`, targeting `N` bars/day. Both stream the tick catalog one day at a time, holding only bars in memory.

Points worth carrying into any downstream work:

- **Do not rank on kurtosis alone.** Excess kurtosis falls monotonically with threshold size, so an argmin over a grid always returns the largest threshold — an artefact of the criterion, not a finding. The notebooks instead select *the fastest bar meeting a kurtosis tolerance*.
- **Day seams must be handled explicitly.** Building bars per day equals a global cumulative sum only if bar closes falling between the last tick of one day and the first of the next are recovered. Roughly one in three day boundaries produces such a close. The accumulator is carried as a *fraction of a bar* so it stays consistent when the threshold changes between days.
- **Cold start drops, it does not backfill.** Backfilling the first `W` days would use future volume to set past thresholds — look-ahead injected directly into bar boundaries, which then propagate into every downstream feature and label.
- **Prices and sizes are 128-bit fixed point.** This Nautilus build stores them as 16-byte binary columns scaled by 1e16. Reading them naively yields raw bytes or values 1e16 too large; the notebooks decode explicitly and sanity-check the scaling before committing to a full pass.
- **Kurtosis estimates are noisy.** The standard error is roughly `sqrt(24/n)`, so at ~20 bars/day a monthly estimate carries ±0.2 of pure noise. Quarterly periods give a far steadier read when judging thresholds.
- **Retest on a trigger, not a calendar.** Threshold reselection is evaluated walk-forward — choose on trailing data only, score on what was actually delivered forward. In production, monitor realized bars/day or rolling kurtosis against a band with hysteresis rather than reselecting on a fixed schedule; every threshold change shifts bar semantics underneath existing features and models.

There is a live tension here worth stating plainly: holding bars/day constant is adaptive thresholding, and adaptation works against the reason activity bars are normal in the first place. Sampling on an activity clock recovers normality *because* it accelerates when information flow accelerates. A threshold that rises with volume partially undoes that. The notebooks quantify the trade-off rather than assuming either side.

---

## Data

The Parquet catalogs are excluded from version control — `nautilusDataCatalog/` alone is ~106 GB. So are the raw downloads and the virtual environment. Reproduce them by running the downloaders and then the loading notebooks; both are deterministic given the same date range.

Catalog coverage currently held locally for ETHUSDT-LINEAR.BYBIT:

| Dataset | Coverage | Size |
| :--- | :--- | :--- |
| Trade ticks | 2020-10-21 → 2026-06-28 | ~81 GB |
| Order book L2 (MBO deltas) | 2023-01 onwards | ~25 GB |
| 1-minute bars | full history | ~150 MB |
| Funding rate updates | 2021-01-01 onwards | ~136 KB |
| Premium index klines | separate catalog | ~47 MB |

---

## Roadmap

Detail in [`project/progressionRoadmap.md`](project/progressionRoadmap.md). The architectural through-line is strict decoupling of heavy feature computation (external process) from deterministic, low-latency execution (inside the Nautilus loop).

| Phase | Focus |
| :--- | :--- |
| 1 | Offline feature cataloging — batch-compute log returns, fractional differentiation, GARCH, HMM regimes; persist as `CustomData`. |
| 2 | Live feature ingress over a ZeroMQ bridge into a custom Nautilus data client. |
| 3 | Meta-labeling — triple-barrier labels and a secondary classifier filtering primary-signal false positives. |
| 4 | State-space filters — Kalman as a native O(1) indicator, particle filters externally via the Phase 2 bridge. |
| 5 | Production ML inference in the live execution path. |

Deployment target is AWS: S3 as the permanent data lake, ECR holding the research image, EC2 launched only when compute is needed, and a small always-on node for live Nautilus once a strategy is ready. See [`project/aws_nautilus_research_architecture.md`](project/aws_nautilus_research_architecture.md).

---

## References

- Clark, P. K. (1973). "A Subordinated Stochastic Process Model with Finite Variance for Speculative Prices." *Econometrica* 41(1), 135–155.
- Ané, T. & Geman, H. (2000). "Order Flow, Transaction Clock, and Normality of Asset Returns." *Journal of Finance* 55(5), 2259–2284.
- Easley, D., López de Prado, M. & O'Hara, M. (2012). "The Volume Clock: Insights into the High-Frequency Paradigm." *Journal of Portfolio Management* 39(1), 19–29.
- López de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley. (Ch. 2 on bar types; Ch. 3 on triple-barrier labeling and meta-labeling.)
- [NautilusTrader documentation](https://nautilustrader.io/docs/)
- [Bybit V5 API documentation](https://bybit-exchange.github.io/docs/v5/intro)
