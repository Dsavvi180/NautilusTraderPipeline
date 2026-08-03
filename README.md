# NautilusTraderPipeline

A research and execution pipeline for automated crypto trading, built on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — a Rust trading engine driven from Python.

In software terms: an ETL layer that turns ~300 GB of raw exchange records into a columnar (Parquet) store, a resampling layer that converts those records into model-ready observations, and a deterministic replay engine that runs a strategy over historical data using the same code path it will later use against a live socket.

Venue is [Bybit](https://www.bybit.com); the instrument is the ETHUSDT perpetual future. **Status:** ingestion, storage and backtesting work. Sampling research is in progress. Live execution is not yet built.

---

## Vocabulary

Enough to read the rest of this document.

| Term | Meaning |
| :--- | :--- |
| **Perpetual future** | A derivative contract tracking an asset's price with no expiry date. Traders holding it pay or receive a periodic *funding rate* that keeps its price tethered to the underlying. |
| **Tick** | One executed trade: timestamp, price, quantity, side. The rawest observation available. This dataset holds ~81 GB of them. |
| **Order book (L2)** | The set of resting buy and sell orders at each price level, and every incremental change to it. Shows intent that has not yet become a trade. |
| **Bar** | An aggregation of many ticks into one row — open, high, low, close, volume. The unit most models actually consume. |
| **Backtest** | Replaying historical data through a strategy to estimate how it would have performed. Trustworthy only if the replay is free of information the strategy could not have had at the time. |
| **Look-ahead bias** | Data leakage, in the machine-learning sense: future information contaminating a past decision. The dominant way backtests produce results that do not survive contact with a live market. |

---

## Purpose

This is infrastructure for one person running machine-learning strategies in crypto markets, engineered to the standard a professional trading desk would expect rather than the standard retail tooling provides.

The reasoning is that a solo trader competes against firms whose advantage is as much operational as analytical. A genuine statistical edge does not survive leakage in the training pipeline, timestamp misalignment between research and production, or a simulator that assumes orders fill when in reality they would not have. Those are engineering defects, not market phenomena, and they are fixable with sufficient discipline.

The objective of this repository is therefore to remove the software-level failure modes entirely, so that when a strategy fails it is because the hypothesis was wrong rather than because the pipeline misreported reality. What remains is a clean surface on which to test whether an edge genuinely exists.

Development is deliberately AI-assisted. Specification, review and implementation are carried out in collaboration with language models, which lets one person maintain a codebase and research programme that would conventionally need a team — on the condition that every architectural decision is understood and justified rather than delegated.

---

## Design principles

**One code path for research and production.** The engine is event-driven throughout: data arrives as a stream of timestamped events and every component reacts to them, much like a message queue feeding a set of subscribers. A feature computed event-by-event over historical data therefore executes identically against a live feed. There is no second implementation to drift out of sync with the first, which removes a whole class of deployment bug.

**Sampling follows market activity, not the clock.** The default in finance is to summarise trades into fixed intervals — one row per minute, per hour. This is a poor design choice statistically. Market activity is extremely bursty, so a fixed interval samples heavily from quiet periods and sparsely from the volatile periods that carry the information. The resulting series has heavy tails and time-varying variance, violating the assumptions most models rest on.

Closing a bar every *N* units of traded volume instead makes the sampling rate adapt to activity: rows arrive quickly when the market is busy and slowly when it is not. Returns measured this way are substantially closer to independent and identically distributed Gaussian, which is the result established by Clark (1973) and Ané & Geman (2000). This pipeline is built on volume-sampled bars, and keeps the raw ticks so bars can be regenerated at any threshold rather than being locked to one choice made early.

**The engine loop is single-threaded.** Anything computationally heavy — training a model, running a particle filter — cannot block it. Such work runs as a separate process and returns its results into the stream as custom events.

**Nothing loads whole.** The tick history is roughly 81 GB against 24 GB of RAM. Every pass over it streams one day at a time, carrying forward only the state needed to remain consistent across boundaries.

**Correctness before convenience.** Simplicity is preferred, but never at the cost of the integrity of the data, the simulation, or the strategy logic.

---

## Architecture

```text
Bybit public endpoints
        ↓                      async downloaders — resumable, no API key required
   raw CSV on disk
        ↓                      parse, normalise, millisecond → nanosecond timestamps
  Parquet data catalog         ticks · order book · 1m bars · funding · contract spec
        ↓                      replay engine streams ticks through the aggregator
    volume bars                built event-by-event, written back to the same store
        ↓
  strategy backtests
```

There is no database. Storage is a directory of Parquet files partitioned the way the engine reads them, so every downstream component resolves from a single path — including the contract specification the bars depend on. For an append-only, read-heavy, single-writer workload, a columnar file layout is simpler and faster than a database, and it ports directly to object storage later.

---

## Repository structure

| | |
| :--- | :--- |
| `project/dataDownload/` | Four independent async downloaders — trades, order book, price bars, funding rates. Each resumable and independently documented. |
| `project/dataLoadingPipeline/` | One notebook per data type, converting raw exchange files into engine objects and writing them to the store. |
| `project/backtestingPipeline/` | The backtest harness (`centralNode.py`), the bar aggregation job, and the sampling research. |
| `project/utils/getInstrument.py` | Fetches contract specifications from the exchange API at runtime instead of hard-coding values such as minimum price increment and order size, which the venue revises without notice. |
| `project/liveDataPipeline/` | Reserved for the live execution client. Not yet implemented. |
| `project/progressionRoadmap.md` | Five-phase development plan. |

The Parquet store is excluded from version control — roughly 106 GB. It rebuilds deterministically from the downloaders and loading notebooks given the same date range.

---

## Operation

```bash
python3 -m venv nautilusVenv && source nautilusVenv/bin/activate
pip install -r requirements.txt
echo 'DATA_DIR="/path/to/raw/data"' > project/nautilus.env
```

**1. Acquire.** Each downloader runs independently and skips files already present:

```bash
python project/dataDownload/BybitTickData/download_bybit_tick_data_async.py \
    --symbols ETHUSDT --start 2020-10-21 --end 2026-06-28 --concurrency 6
```

**2. Ingest.** Run the corresponding notebook in `dataLoadingPipeline/`. It resolves the contract specification from the exchange, then converts and writes raw files into the store in strict date order. The order book notebook verifies continuity across file boundaries — the store is only as trustworthy as that check.

**3. Aggregate.** `barDownload.ipynb` streams ticks through the engine and lets its own aggregator build the bars, using the identical path that runs live, then writes them back beside the ticks. A full pass takes several hours, so validate on a short window first. The job aborts if the output directory already exists, since a repeat run would silently append duplicates.

**4. Backtest.** `BacktestRunner` wraps the venue, data and engine configuration behind defaults, so only the component under development has to be supplied:

```python
runner = BacktestRunner(
    actors=[ImportableActorConfig(actor_path="my_module:MyActor", ...)],
    instrument_id="ETHUSDT-LINEAR.BYBIT",
    start="2024-01-01", end="2024-02-01",
)
results = runner.run()
```

---

## Sampling research

`volumeBarStatisticalAnalysis.ipynb` and `dollarBarStatisticalAnalysis.ipynb` compare a fixed volume threshold against a threshold recalibrated each day from recent trading volume. Three findings shape how the threshold should be chosen.

**Kurtosis alone is not a valid selection criterion.** Excess kurtosis — a measure of how heavy the tails are relative to a normal distribution — falls monotonically as the threshold grows, because each bar then averages over more trades and the central limit theorem pulls the distribution toward normal. Minimising it therefore always selects the largest candidate in the grid, indefinitely. That is a property of the estimator, not a fact about the market. The meaningful question is which is the *fastest* sampling rate that is still close enough to normal, so selection is made against a tolerance instead of by taking the minimum.

**Adaptive thresholds work against the mechanism that makes activity sampling effective.** Recalibrating to hold the number of bars per day constant is itself a form of adaptation, and activity sampling recovers normality precisely because it speeds up when the market does. Suppressing that response forfeits part of the benefit. Both notebooks measure the trade-off rather than assuming an answer.

**Leakage concentrates in the sampling boundaries.** Backfilling the warm-up period would set historical thresholds using volume that had not yet occurred. Because the boundaries determine every observation downstream, this is the most damaging place for leakage to enter, so the warm-up days are discarded rather than reconstructed.

---

## Roadmap

Deployment target is AWS: S3 as the permanent data lake, a container registry holding the research image, compute instances provisioned only when needed, and a small persistent node for live execution once a strategy justifies it. Detail in [`project/aws_nautilus_research_architecture.md`](project/aws_nautilus_research_architecture.md).

The research programme runs through offline feature computation, streaming those features into the live engine over a ZeroMQ bridge, supervised labeling and meta-classification, state-space filtering, and finally model inference inside the execution path. The constant is the separation of heavy computation, which runs externally, from deterministic low-latency execution, which runs inside the loop.

---

## References

- Clark, P. K. (1973). "A Subordinated Stochastic Process Model with Finite Variance for Speculative Prices." *Econometrica* 41(1).
- Ané, T. & Geman, H. (2000). "Order Flow, Transaction Clock, and Normality of Asset Returns." *Journal of Finance* 55(5).
- Easley, D., López de Prado, M. & O'Hara, M. (2012). "The Volume Clock: Insights into the High-Frequency Paradigm." *Journal of Portfolio Management* 39(1).
- López de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
