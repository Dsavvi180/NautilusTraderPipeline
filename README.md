# NautilusTraderPipeline

A research and execution pipeline for crypto perpetuals, built on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — a Rust-native trading engine driven from Python. Venue is Bybit; the working instrument is ETHUSDT perpetual.

The point of the whole thing is that the code which researches a strategy is the same code that trades it. Nautilus is event-driven end to end, so a feature computed tick-by-tick in a backtest runs identically against a live socket. No reimplementation, no drift between research and production.

**Where it stands:** data ingestion, catalog construction and the backtest harness work. Bar-sampling research is underway. Live execution is not built yet.

---

## The idea

Most backtests start with OHLCV candles because that is what exchanges hand you. That is a poor starting point. Fixed time intervals sample the market at a constant rate regardless of what is happening in it — a minute during a liquidation cascade gets the same weight as a minute at 4am. The resulting returns are fat-tailed and heteroscedastic, which is exactly the wrong input for most statistical models.

Sampling by traded *activity* instead of by the clock largely fixes this. Close a bar every N units of volume and the bar rate naturally speeds up when information is flowing and slows down when it is not. Returns come out far closer to IID Gaussian — a result that goes back to Clark (1973) and Ané & Geman (2000), and is the basis for López de Prado's treatment of bar types in *Advances in Financial Machine Learning*.

So this pipeline is built around volume and dollar bars rather than candles, and the ingestion layer keeps raw trade ticks so bars can be rebuilt at any threshold rather than being locked to one.

Two constraints shape everything else:

- **The Nautilus event loop is single-threaded.** Anything expensive — model training, Monte Carlo filters — has to run outside it as a side job and feed results back in, rather than being parallelised inside it.
- **The tick catalog is ~81 GB against 24 GB of RAM.** Every pass over history streams day by day or in tick chunks. Nothing ever loads whole.

---

## How data moves

```text
Bybit public endpoints
        ↓                      async downloaders, resumable, no API key
   raw CSV on disk
        ↓                      parse, normalise, ms → ns timestamps
  ParquetDataCatalog           ticks · order book L2 · 1m bars · funding · instrument
        ↓                      BacktestNode streams ticks through the engine
    volume bars                aggregated tick-by-tick, written back to the same catalog
        ↓
  strategy backtests
```

There is no database. The Parquet catalog *is* the store, laid out the way Nautilus wants to read it, and everything downstream loads from a single `catalog_path`.

---

## What's in here

| | |
| :--- | :--- |
| `project/dataDownload/` | Four standalone async downloaders — tick, order book, kline/premium-kline, funding rate. Each resumable, each with its own CLI and README. |
| `project/dataLoadingPipeline/` | One notebook per data type, turning raw files into Nautilus objects and writing them to the catalog. |
| `project/backtestingPipeline/` | `centralNode.py` (the backtest harness), the volume-bar aggregation node, and the bar-sampling research notebooks. |
| `project/utils/getInstrument.py` | Pulls live contract specs from Bybit rather than hard-coding tick sizes and lot steps that change without warning. |
| `project/liveDataPipeline/` | Placeholder for live execution. Empty for now. |
| `project/progressionRoadmap.md` | The five-phase plan, from offline feature cataloging to live ML inference. |

The Parquet catalogs are not in the repo — ~106 GB of them. They rebuild deterministically from the downloaders plus the loading notebooks.

---

## Getting started

```bash
python3 -m venv nautilusVenv && source nautilusVenv/bin/activate
pip install -r requirements.txt
echo 'DATA_DIR="/path/to/raw/data"' > project/nautilus.env
```

**1. Download.** Each script is independent and skips what it already has:

```bash
python project/dataDownload/BybitTickData/download_bybit_tick_data_async.py \
    --symbols ETHUSDT --start 2020-10-21 --end 2026-06-28 --concurrency 6
```

**2. Ingest.** Run the matching notebook in `dataLoadingPipeline/`. It fetches the instrument definition from Bybit, then converts and writes raw files into the catalog in date order.

**3. Build bars.** `barDownload.ipynb` streams ticks through a `BacktestNode` and lets the engine's aggregator build volume bars the same way it would live, then writes them back beside the ticks. A full run over the tick history takes hours — test on a short window first. The notebook refuses to run if the output folder already exists, since a second run would silently append duplicates.

**4. Backtest.** `BacktestRunner` wraps the venue, data and engine config behind defaults, so you only supply the actor you are working on:

```python
runner = BacktestRunner(
    actors=[ImportableActorConfig(actor_path="my_module:MyActor", ...)],
    instrument_id="ETHUSDT-LINEAR.BYBIT",
    start="2024-01-01", end="2024-02-01",
)
results = runner.run()
```

---

## What the bar research found

`volumeBarStatisticalAnalysis.ipynb` and `dollarBarStatisticalAnalysis.ipynb` compare fixed thresholds against thresholds recalibrated daily from trailing volume. Three things came out of it that are easy to get wrong:

**Ranking bars by kurtosis is a trap.** Excess kurtosis falls monotonically as the threshold grows, so picking the minimum always returns the largest bar in the grid. That is a property of the metric, not a discovery about the market. The right question is which is the *fastest* bar that is normal enough, so the notebooks select against a tolerance instead.

**Adaptive thresholds cut against the reason activity bars work.** Holding bars-per-day constant is a form of adaptation, and activity bars are normal precisely *because* they accelerate when the market does. Damping that partially undoes the benefit. Both notebooks quantify the trade-off rather than assuming a winner.

**Look-ahead hides in the bar boundaries.** Backfilling the warm-up window would set past thresholds from future volume — and because boundaries propagate into every feature and label built on them, that is the worst possible place for a leak. The cold-start days are dropped instead.

---

## Where it's going

Deployment target is AWS: S3 as the permanent data lake, ECR for the research image, EC2 spun up only when compute is needed, and a small always-on node for live trading once a strategy justifies it. Detail in [`project/aws_nautilus_research_architecture.md`](project/aws_nautilus_research_architecture.md).

The strategy roadmap runs through offline feature cataloging → live feature ingress over ZeroMQ → triple-barrier labeling and meta-classifiers → state-space filters → production ML inference. The architectural through-line is keeping heavy computation outside the loop and deterministic execution inside it.

---

## References

- Clark, P. K. (1973). "A Subordinated Stochastic Process Model with Finite Variance for Speculative Prices." *Econometrica* 41(1).
- Ané, T. & Geman, H. (2000). "Order Flow, Transaction Clock, and Normality of Asset Returns." *Journal of Finance* 55(5).
- Easley, D., López de Prado, M. & O'Hara, M. (2012). "The Volume Clock." *Journal of Portfolio Management* 39(1).
- López de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
