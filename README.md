# NautilusTraderPipeline

An institutional-grade research and execution pipeline for crypto perpetual futures, built on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — a Rust-native trading engine driven from Python. Venue is Bybit; the primary instrument is ETHUSDT perpetual.

**Status:** data ingestion, catalog construction and the backtest harness are operational. Bar-sampling research is in progress. Live execution is not yet implemented.

---

## Purpose

This is infrastructure for a single operator running machine-learning strategies in crypto markets, engineered to the standard a desk would expect rather than the standard a retail toolkit provides.

The reasoning is that a solo trader competes against firms whose advantage is as much operational as analytical. Their edge does not survive silent look-ahead in a backtest, timestamp drift between research and production, or a fill model that flatters a strategy which would not have filled. Those are software problems, not market problems, and they are solvable with sufficient discipline in the engineering.

The objective of this repository is therefore to eliminate the software-level failure modes entirely, so that when a strategy fails it is because the hypothesis was wrong and not because the pipeline lied. What remains is a clean surface on which to test whether an edge actually exists.

Development is deliberately AI-assisted. Specification, review and implementation are carried out in collaboration with language models, which allows a single person to maintain a codebase and a research programme that would conventionally require a team — provided every architectural decision is understood and justified rather than delegated.

---

## Design principles

**Research and production share one code path.** NautilusTrader is event-driven throughout, so a feature computed tick-by-tick against historical data executes identically against a live socket. There is no separate backtest implementation to diverge from the live one, which removes an entire category of deployment risk.

**Sampling follows market activity, not the clock.** Fixed time intervals weight a minute of liquidation cascade identically to a minute of overnight inactivity, producing returns that are fat-tailed and heteroscedastic — poorly suited to statistical modelling. Sampling by traded volume instead lets the bar rate accelerate with information flow, yielding returns materially closer to IID Gaussian (Clark, 1973; Ané & Geman, 2000; López de Prado, 2018, ch. 2). The pipeline is built on volume and dollar bars, and retains raw trade ticks so bars can be rebuilt at any threshold rather than being fixed to one.

**The event loop is single-threaded.** Computationally expensive work — model training, Monte Carlo filtering — runs as an external side job and returns its output to the loop as custom data, rather than being parallelised inside it.

**Nothing loads whole.** The tick catalog is approximately 81 GB against 24 GB of available memory. Every pass over history streams by day or in tick chunks, carrying only the state required across boundaries.

**Integrity takes precedence over convenience.** Simplicity is preferred, but never at the cost of data integrity, execution integrity or the correctness of strategy logic.

---

## Architecture

```text
Bybit public endpoints
        ↓                      async downloaders — resumable, no API key required
   raw CSV on disk
        ↓                      parse, normalise, millisecond → nanosecond timestamps
  ParquetDataCatalog           ticks · order book L2 · 1m bars · funding · instrument
        ↓                      BacktestNode streams ticks through the engine
    volume bars                aggregated tick-by-tick, written back to the same catalog
        ↓
  strategy backtests
```

There is no external database. The Parquet catalog is the store, structured for Nautilus ingestion, and every downstream component resolves from a single `catalog_path` — including the instrument definition the bars depend on.

---

## Repository structure

| | |
| :--- | :--- |
| `project/dataDownload/` | Four independent async downloaders — trade tick, order book, kline and premium kline, funding rate. Each resumable, each with its own CLI and documentation. |
| `project/dataLoadingPipeline/` | One notebook per data type, converting raw exchange files into Nautilus objects and writing them to the catalog. |
| `project/backtestingPipeline/` | The backtest harness (`centralNode.py`), the volume-bar aggregation node, and the bar-sampling research. |
| `project/utils/getInstrument.py` | Retrieves live contract specifications from the Bybit V5 API rather than hard-coding tick sizes, lot steps and margin parameters that the venue revises without notice. |
| `project/liveDataPipeline/` | Reserved for the live execution client. Not yet implemented. |
| `project/progressionRoadmap.md` | Five-phase development plan, from offline feature cataloging through to live ML inference. |

The Parquet catalogs are excluded from version control — approximately 106 GB. They reconstruct deterministically from the downloaders and loading notebooks given the same date range.

---

## Operation

```bash
python3 -m venv nautilusVenv && source nautilusVenv/bin/activate
pip install -r requirements.txt
echo 'DATA_DIR="/path/to/raw/data"' > project/nautilus.env
```

**1. Acquire data.** Each downloader runs independently and skips files already present:

```bash
python project/dataDownload/BybitTickData/download_bybit_tick_data_async.py \
    --symbols ETHUSDT --start 2020-10-21 --end 2026-06-28 --concurrency 6
```

**2. Ingest.** Run the corresponding notebook in `dataLoadingPipeline/`. It resolves the instrument definition from Bybit, then converts and writes raw files into the catalog in strict date order. The order book notebook verifies continuity across file boundaries; the catalog is only as trustworthy as that check.

**3. Aggregate bars.** `barDownload.ipynb` streams ticks through a `BacktestNode` and allows the engine's own aggregator to construct volume bars via the same event-driven path used in live trading, then persists them alongside the ticks. A full pass over the tick history runs for several hours, so validate on a constrained window first. The notebook aborts if the output directory already exists, since a repeat run would silently append duplicate bars.

**4. Backtest.** `BacktestRunner` encapsulates venue, data and engine configuration behind defaults, requiring only the actor under development:

```python
runner = BacktestRunner(
    actors=[ImportableActorConfig(actor_path="my_module:MyActor", ...)],
    instrument_id="ETHUSDT-LINEAR.BYBIT",
    start="2024-01-01", end="2024-02-01",
)
results = runner.run()
```

Defaults: Bybit venue, NETTING OMS, margin account, and the venue's published maker and taker fees resolved through `GetInstrument`.

---

## Bar sampling research

`volumeBarStatisticalAnalysis.ipynb` and `dollarBarStatisticalAnalysis.ipynb` evaluate fixed thresholds against thresholds recalibrated daily from trailing volume. Three findings materially affect how bars should be specified.

**Excess kurtosis is not a valid selection criterion in isolation.** It declines monotonically as the threshold increases, so minimising it necessarily returns the largest specification in the grid. This is a property of the estimator, not evidence about the market. The operative question is which is the fastest bar that remains sufficiently Gaussian, so selection is made against a tolerance rather than by argmin.

**Adaptive thresholds work against the mechanism that makes activity bars effective.** Holding bars-per-day constant is itself a form of adaptation, and activity sampling recovers normality precisely because it accelerates when information flow accelerates. Damping that response partially forfeits the benefit. Both notebooks quantify the trade-off rather than presuming an outcome.

**Look-ahead bias concentrates in the bar boundaries.** Backfilling the warm-up window would derive historical thresholds from future volume. Because boundaries propagate into every feature and label constructed on them, this is the most damaging location for a leak, so the cold-start period is discarded rather than reconstructed.

---

## Roadmap

The deployment target is AWS: S3 as the permanent data lake, ECR holding the research image, EC2 provisioned only when compute is required, and a small persistent node for live execution once a strategy justifies it. Specification in [`project/aws_nautilus_research_architecture.md`](project/aws_nautilus_research_architecture.md).

The strategy programme proceeds through offline feature cataloging, live feature ingress over a ZeroMQ bridge, triple-barrier labeling with meta-classification, state-space filtering, and finally production ML inference in the execution path. The architectural constant throughout is the separation of heavy computation, which runs externally, from deterministic low-latency execution, which runs inside the loop.

---

## References

- Clark, P. K. (1973). "A Subordinated Stochastic Process Model with Finite Variance for Speculative Prices." *Econometrica* 41(1).
- Ané, T. & Geman, H. (2000). "Order Flow, Transaction Clock, and Normality of Asset Returns." *Journal of Finance* 55(5).
- Easley, D., López de Prado, M. & O'Hara, M. (2012). "The Volume Clock: Insights into the High-Frequency Paradigm." *Journal of Portfolio Management* 39(1).
- López de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
