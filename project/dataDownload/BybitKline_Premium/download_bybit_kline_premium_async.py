#!/usr/bin/env python3
"""
Async Bybit Kline + Premium Index Kline downloader.

This is adapted from the public-directory tick-data downloader style, but uses
Bybit's public V5 market API because Premium Index Price Kline is an API dataset
for linear USDT symbols such as BTCUSDT, ETHUSDT, SOLUSDT, LINKUSDT.

Default output structure:

/Volumes/LaCie Drive/ByBit/KlineData/
  raw/
    kline/
      BTCUSDT/
        interval=1/
          BTCUSDT_kline_1_2026-04-01_2026-04-30.csv
    premium_index_kline/
      BTCUSDT/
        interval=1/
          BTCUSDT_premium_index_kline_1_2026-04-01_2026-04-30.csv
  download_manifest.csv

Install:
    pip install aiohttp aiofiles

Example:
    python download_bybit_kline_premium_async.py \
      --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
      --datasets kline premium_index_kline \
      --interval 1 \
      --start 2026-04-01 \
      --end 2026-06-28 \
      --output-root "/Volumes/LaCie Drive/ByBit/KlineData" \
      --concurrency 3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp


BASE_URL = "https://api.bybit.com"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"]
DEFAULT_DATASETS = ["kline", "premium_index_kline"]

ENDPOINTS = {
    "kline": "/v5/market/kline",
    "premium_index_kline": "/v5/market/premium-index-price-kline",
}

# Bybit V5 intervals: 1,3,5,15,30,60,120,240,360,720,D,W,M
VALID_INTERVALS = {
    "1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"
}


@dataclass(frozen=True)
class DownloadJob:
    dataset: str
    symbol: str
    interval: str
    start: date
    end: date
    output_path: Path


@dataclass
class Counters:
    jobs: int = 0
    written: int = 0
    skipped: int = 0
    empty: int = 0
    failed: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bybit Kline and Premium Index Kline data to CSV."
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Symbols to download, e.g. BTCUSDT ETHUSDT SOLUSDT LINKUSDT.",
    )

    parser.add_argument(
        "--datasets",
        nargs="+",
        default=DEFAULT_DATASETS,
        choices=sorted(ENDPOINTS.keys()),
        help="Datasets to download: kline premium_index_kline.",
    )

    parser.add_argument(
        "--interval",
        default="1",
        help="Kline interval. Examples: 1, 5, 15, 60, D.",
    )

    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        required=True,
        help="Start date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        required=True,
        help="End date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--output-root",
        default="/Volumes/LaCie Drive/ByBit/KlineData",
        help="Output root folder on your hard drive.",
    )

    parser.add_argument(
        "--category",
        default="linear",
        choices=["linear", "inverse"],
        help="Bybit category. Use linear for USDT perps.",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Concurrent month downloads. Keep low to avoid rate limits.",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Retries per API request.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Bybit page size. Max is 1000.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing CSV files.",
    )

    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Write one file for the whole date range instead of monthly files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned jobs but do not download.",
    )

    return parser.parse_args()


def dt_utc_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def end_date_exclusive_ms(d: date) -> int:
    # End date is inclusive for the user. API end timestamp is set to next day minus 1ms.
    next_day = d + timedelta(days=1)
    return dt_utc_ms(next_day) - 1


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def month_windows(start: date, end: date):
    current = date(start.year, start.month, 1)

    while current <= end:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)

        window_start = max(start, current)
        window_end = min(end, next_month - timedelta(days=1))

        if window_start <= window_end:
            yield window_start, window_end

        current = next_month


def make_jobs(args: argparse.Namespace) -> list[DownloadJob]:
    symbols = [s.upper() for s in args.symbols]
    interval = args.interval.upper()

    if interval not in VALID_INTERVALS:
        raise SystemExit(f"Invalid interval {args.interval!r}. Valid: {sorted(VALID_INTERVALS)}")

    output_root = Path(args.output_root).expanduser()
    jobs: list[DownloadJob] = []

    if args.single_file:
        windows = [(args.start, args.end)]
    else:
        windows = list(month_windows(args.start, args.end))

    for dataset in args.datasets:
        for symbol in symbols:
            for start, end in windows:
                out_dir = output_root / "raw" / dataset / symbol / f"interval={interval}"
                filename = f"{symbol}_{dataset}_{interval}_{start.isoformat()}_{end.isoformat()}.csv"
                jobs.append(
                    DownloadJob(
                        dataset=dataset,
                        symbol=symbol,
                        interval=interval,
                        start=start,
                        end=end,
                        output_path=out_dir / filename,
                    )
                )

    return jobs


def normalise_row(dataset: str, symbol: str, interval: str, row: list[Any]) -> dict[str, Any]:
    # Bybit returns values as strings. Keep numeric values as strings in raw CSV.
    # kline rows: [startTime, open, high, low, close, volume, turnover]
    # premium index rows: [startTime, open, high, low, close]
    start_ms = int(row[0])

    record = {
        "dataset": dataset,
        "symbol": symbol,
        "interval": interval,
        "start_time_ms": start_ms,
        "start_time_utc": ms_to_iso(start_ms),
        "open": row[1] if len(row) > 1 else "",
        "high": row[2] if len(row) > 2 else "",
        "low": row[3] if len(row) > 3 else "",
        "close": row[4] if len(row) > 4 else "",
        "volume": row[5] if len(row) > 5 else "",
        "turnover": row[6] if len(row) > 6 else "",
    }

    return record


async def write_manifest_header(manifest_path: Path) -> None:
    if manifest_path.exists():
        return

    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(manifest_path, "w", encoding="utf-8") as f:
        await f.write(
            "dataset,symbol,interval,start_date,end_date,output_path,status,rows,error\n"
        )


async def append_manifest(
    manifest_path: Path,
    lock: asyncio.Lock,
    job: DownloadJob,
    status: str,
    rows: int = 0,
    error: str = "",
) -> None:
    safe_error = error.replace("\n", " ").replace("\r", " ").replace(",", ";")
    line = (
        f"{job.dataset},{job.symbol},{job.interval},"
        f"{job.start.isoformat()},{job.end.isoformat()},"
        f"{job.output_path},{status},{rows},{safe_error}\n"
    )

    async with lock:
        async with aiofiles.open(manifest_path, "a", encoding="utf-8") as f:
            await f.write(line)


async def api_get_json(
    session: aiohttp.ClientSession,
    path: str,
    params: dict[str, Any],
    retries: int,
) -> dict[str, Any]:
    url = BASE_URL + path

    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params) as response:
                text = await response.text()

                if response.status in {429, 403, 502, 503, 504}:
                    raise RuntimeError(f"HTTP {response.status}: {text[:300]}")

                response.raise_for_status()

                payload = await response.json()

                ret_code = payload.get("retCode")
                if ret_code != 0:
                    raise RuntimeError(f"Bybit retCode={ret_code}: {payload.get('retMsg')}")

                return payload

        except Exception:
            if attempt >= retries:
                raise
            await asyncio.sleep(1.5 * attempt)

    raise RuntimeError("unreachable")


async def fetch_all_rows_for_job(
    session: aiohttp.ClientSession,
    job: DownloadJob,
    category: str,
    limit: int,
    retries: int,
) -> list[dict[str, Any]]:
    path = ENDPOINTS[job.dataset]

    start_ms = dt_utc_ms(job.start)
    end_ms = end_date_exclusive_ms(job.end)

    all_rows: dict[int, dict[str, Any]] = {}

    # Bybit returns candles in reverse order. We page backwards by shrinking end.
    current_end = end_ms

    while current_end >= start_ms:
        params = {
            "category": category,
            "symbol": job.symbol,
            "interval": job.interval,
            "start": start_ms,
            "end": current_end,
            "limit": min(limit, 1000),
        }

        payload = await api_get_json(session, path, params, retries=retries)
        result = payload.get("result") or {}
        rows = result.get("list") or []

        if not rows:
            break

        min_seen_ms: int | None = None

        for row in rows:
            if not row:
                continue

            row_start_ms = int(row[0])

            if row_start_ms < start_ms or row_start_ms > end_ms:
                continue

            all_rows[row_start_ms] = normalise_row(
                dataset=job.dataset,
                symbol=job.symbol,
                interval=job.interval,
                row=row,
            )

            if min_seen_ms is None or row_start_ms < min_seen_ms:
                min_seen_ms = row_start_ms

        if min_seen_ms is None:
            break

        if len(rows) < limit:
            break

        # Move end just before the earliest candle we have seen.
        current_end = min_seen_ms - 1

        await asyncio.sleep(0.05)

    return [all_rows[k] for k in sorted(all_rows)]


async def write_csv_file(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part_path = path.with_name(path.name + ".part")

    fieldnames = [
        "dataset",
        "symbol",
        "interval",
        "start_time_ms",
        "start_time_utc",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
    ]

    async with aiofiles.open(part_path, "w", encoding="utf-8", newline="") as f:
        # aiofiles does not expose csv.writer directly cleanly; build rows manually.
        await f.write(",".join(fieldnames) + "\n")

        for row in rows:
            values = [str(row.get(col, "")) for col in fieldnames]
            # Values from Bybit are simple numeric strings, so comma-safe.
            await f.write(",".join(values) + "\n")

    part_path.replace(path)


async def process_job(
    session: aiohttp.ClientSession,
    job: DownloadJob,
    args: argparse.Namespace,
    manifest_path: Path,
    manifest_lock: asyncio.Lock,
    counters: Counters,
    counter_lock: asyncio.Lock,
) -> None:
    if job.output_path.exists() and job.output_path.stat().st_size > 0 and not args.overwrite:
        print(f"SKIP existing: {job.output_path}")
        async with counter_lock:
            counters.skipped += 1
        await append_manifest(manifest_path, manifest_lock, job, "skipped_existing")
        return

    try:
        rows = await fetch_all_rows_for_job(
            session=session,
            job=job,
            category=args.category,
            limit=args.limit,
            retries=args.retries,
        )

        if not rows:
            print(f"EMPTY: {job.dataset} {job.symbol} {job.start} to {job.end}")
            async with counter_lock:
                counters.empty += 1
            await append_manifest(manifest_path, manifest_lock, job, "empty", rows=0)
            return

        await write_csv_file(job.output_path, rows)

        print(f"WROTE: {job.output_path} ({len(rows)} rows)")
        async with counter_lock:
            counters.written += 1
        await append_manifest(manifest_path, manifest_lock, job, "written", rows=len(rows))

    except Exception as exc:
        print(f"FAILED: {job.dataset} {job.symbol} {job.start} to {job.end} -> {exc}")
        async with counter_lock:
            counters.failed += 1
        await append_manifest(manifest_path, manifest_lock, job, "failed", error=str(exc))


async def run(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    manifest_path = output_root / "download_manifest.csv"
    await write_manifest_header(manifest_path)

    jobs = make_jobs(args)

    print("Bybit kline / premium-index-kline downloader")
    print(f"Symbols:     {', '.join(s.upper() for s in args.symbols)}")
    print(f"Datasets:    {', '.join(args.datasets)}")
    print(f"Interval:    {args.interval}")
    print(f"Date range:  {args.start} to {args.end}")
    print(f"Output root: {output_root}")
    print(f"Jobs:        {len(jobs)}")
    print()

    if args.dry_run:
        for job in jobs:
            print(job.output_path)
        return 0

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    connector = aiohttp.TCPConnector(limit=max(args.concurrency * 2, 8))

    headers = {
        "User-Agent": "BybitKlineResearchDownloader/1.0",
        "Accept": "application/json",
    }

    semaphore = asyncio.Semaphore(args.concurrency)
    counters = Counters(jobs=len(jobs))
    counter_lock = asyncio.Lock()
    manifest_lock = asyncio.Lock()

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:

        async def guarded(job: DownloadJob) -> None:
            async with semaphore:
                await process_job(
                    session=session,
                    job=job,
                    args=args,
                    manifest_path=manifest_path,
                    manifest_lock=manifest_lock,
                    counters=counters,
                    counter_lock=counter_lock,
                )

        await asyncio.gather(*(guarded(job) for job in jobs))

    print("\n=== DONE ===")
    print(f"Jobs:      {counters.jobs}")
    print(f"Written:   {counters.written}")
    print(f"Skipped:   {counters.skipped}")
    print(f"Empty:     {counters.empty}")
    print(f"Failed:    {counters.failed}")
    print(f"Manifest:  {manifest_path}")

    return 0 if counters.failed == 0 else 1


def main() -> int:
    args = parse_args()

    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")

    if args.limit < 1 or args.limit > 1000:
        raise SystemExit("--limit must be between 1 and 1000")

    if args.start > args.end:
        raise SystemExit("--start must be <= --end")

    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
