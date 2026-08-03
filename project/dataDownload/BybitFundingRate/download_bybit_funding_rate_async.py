#!/usr/bin/env python3
"""
Async Bybit funding-rate history downloader.

Pulls funding rates for each symbol as far back as Bybit returns data,
or until --start, whichever comes first.

No API keys are needed. This uses the public Bybit V5 market endpoint:
    GET /v5/market/funding/history

Default symbols:
    BTCUSDT ETHUSDT SOLUSDT LINKUSDT

Default start:
    2021-01-01

Default output:
    /Volumes/LaCie Drive/ByBit/FundingRate/

Output layout:
    /Volumes/LaCie Drive/ByBit/FundingRate/
      raw/
        BTCUSDT/
          BTCUSDT_funding_rate_2021-01-01_to_2026-06-29.csv
        ETHUSDT/
        SOLUSDT/
        LINKUSDT/
      download_manifest.csv

Install:
    pip install aiohttp aiofiles

Example:
    python download_bybit_funding_rate_async.py \
      --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
      --start 2021-01-01 \
      --output-root "/Volumes/LaCie Drive/ByBit/FundingRate" \
      --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp


BASE_URL = "https://api.bybit.com"
FUNDING_ENDPOINT = "/v5/market/funding/history"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"]


@dataclass(frozen=True)
class FundingRecord:
    symbol: str
    funding_rate: str
    funding_rate_timestamp_ms: int
    funding_rate_time_utc: str


@dataclass
class Counters:
    symbols: int = 0
    written: int = 0
    empty: int = 0
    failed: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bybit funding-rate history as far back as available or until a start date."
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Symbols to download, e.g. BTCUSDT ETHUSDT SOLUSDT LINKUSDT.",
    )

    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(2021, 1, 1),
        help="Earliest desired date, YYYY-MM-DD. Default: 2021-01-01.",
    )

    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date.today(),
        help="Latest desired date, YYYY-MM-DD. Default: today.",
    )

    parser.add_argument(
        "--output-root",
        default="/Volumes/LaCie Drive/ByBit/FundingRate",
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
        default=4,
        help="Concurrent symbols to download.",
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
        default=200,
        help="Bybit page size. Max is 200 for funding history.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned output files but do not download.",
    )

    return parser.parse_args()


def start_of_day_utc_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def end_of_day_utc_ms(d: date) -> int:
    next_day = d + timedelta(days=1)
    return start_of_day_utc_ms(next_day) - 1


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def output_path_for_symbol(output_root: Path, symbol: str, start: date, end: date) -> Path:
    return (
        output_root
        / "raw"
        / symbol
        / f"{symbol}_funding_rate_{start.isoformat()}_to_{end.isoformat()}.csv"
    )


async def write_manifest_header(manifest_path: Path) -> None:
    if manifest_path.exists():
        return

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(manifest_path, "w", encoding="utf-8") as f:
        await f.write(
            "symbol,start_date,end_date,output_path,status,rows,earliest_utc,latest_utc,error\n"
        )


async def append_manifest(
    manifest_path: Path,
    lock: asyncio.Lock,
    symbol: str,
    start: date,
    end: date,
    output_path: Path,
    status: str,
    rows: int = 0,
    earliest_utc: str = "",
    latest_utc: str = "",
    error: str = "",
) -> None:
    clean_error = error.replace("\n", " ").replace("\r", " ").replace(",", ";")
    line = (
        f"{symbol},{start.isoformat()},{end.isoformat()},{output_path},"
        f"{status},{rows},{earliest_utc},{latest_utc},{clean_error}\n"
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

                if response.status in {403, 429, 500, 502, 503, 504}:
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


def parse_funding_row(row: dict[str, Any], fallback_symbol: str) -> FundingRecord | None:
    symbol = str(row.get("symbol") or fallback_symbol).upper()

    funding_rate = row.get("fundingRate")
    timestamp = row.get("fundingRateTimestamp")

    if funding_rate is None or timestamp is None:
        return None

    ts_ms = int(timestamp)

    return FundingRecord(
        symbol=symbol,
        funding_rate=str(funding_rate),
        funding_rate_timestamp_ms=ts_ms,
        funding_rate_time_utc=ms_to_iso(ts_ms),
    )


async def fetch_funding_history_for_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    category: str,
    start: date,
    end: date,
    limit: int,
    retries: int,
) -> list[FundingRecord]:
    start_ms = start_of_day_utc_ms(start)
    end_ms = end_of_day_utc_ms(end)

    current_end_ms = end_ms
    records_by_timestamp: dict[int, FundingRecord] = {}

    while current_end_ms >= start_ms:
        params = {
            "category": category,
            "symbol": symbol,
            "endTime": current_end_ms,
            "limit": min(limit, 200),
        }

        payload = await api_get_json(
            session=session,
            path=FUNDING_ENDPOINT,
            params=params,
            retries=retries,
        )

        result = payload.get("result") or {}
        rows = result.get("list") or []

        if not rows:
            break

        timestamps_in_page: list[int] = []

        for row in rows:
            record = parse_funding_row(row, fallback_symbol=symbol)
            if record is None:
                continue

            timestamps_in_page.append(record.funding_rate_timestamp_ms)

            if start_ms <= record.funding_rate_timestamp_ms <= end_ms:
                records_by_timestamp[record.funding_rate_timestamp_ms] = record

        if not timestamps_in_page:
            break

        oldest_seen = min(timestamps_in_page)

        # Stop once the API page has gone before our required start date.
        if oldest_seen < start_ms:
            break

        # Page backwards. Bybit funding history returns batches up to endTime.
        next_end = oldest_seen - 1

        if next_end >= current_end_ms:
            # Defensive guard against accidental infinite loop.
            break

        current_end_ms = next_end

        # Gentle pacing to avoid rate-limit problems.
        await asyncio.sleep(0.05)

    return [records_by_timestamp[k] for k in sorted(records_by_timestamp)]


async def write_funding_csv(path: Path, records: list[FundingRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part_path = path.with_name(path.name + ".part")

    async with aiofiles.open(part_path, "w", encoding="utf-8") as f:
        await f.write(
            "symbol,funding_rate_timestamp_ms,funding_rate_time_utc,funding_rate\n"
        )

        for record in records:
            await f.write(
                f"{record.symbol},"
                f"{record.funding_rate_timestamp_ms},"
                f"{record.funding_rate_time_utc},"
                f"{record.funding_rate}\n"
            )

    part_path.replace(path)


async def process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    args: argparse.Namespace,
    output_root: Path,
    manifest_path: Path,
    manifest_lock: asyncio.Lock,
    counters: Counters,
    counter_lock: asyncio.Lock,
) -> None:
    symbol = symbol.upper()
    output_path = output_path_for_symbol(output_root, symbol, args.start, args.end)

    if output_path.exists() and output_path.stat().st_size > 0 and not args.overwrite:
        print(f"SKIP existing: {output_path}")

        async with counter_lock:
            counters.written += 1

        await append_manifest(
            manifest_path=manifest_path,
            lock=manifest_lock,
            symbol=symbol,
            start=args.start,
            end=args.end,
            output_path=output_path,
            status="skipped_existing",
        )
        return

    try:
        records = await fetch_funding_history_for_symbol(
            session=session,
            symbol=symbol,
            category=args.category,
            start=args.start,
            end=args.end,
            limit=args.limit,
            retries=args.retries,
        )

        if not records:
            print(f"EMPTY: {symbol}")
            async with counter_lock:
                counters.empty += 1

            await append_manifest(
                manifest_path=manifest_path,
                lock=manifest_lock,
                symbol=symbol,
                start=args.start,
                end=args.end,
                output_path=output_path,
                status="empty",
            )
            return

        await write_funding_csv(output_path, records)

        earliest = records[0].funding_rate_time_utc
        latest = records[-1].funding_rate_time_utc

        print(f"WROTE: {output_path} ({len(records)} rows, {earliest} -> {latest})")

        async with counter_lock:
            counters.written += 1

        await append_manifest(
            manifest_path=manifest_path,
            lock=manifest_lock,
            symbol=symbol,
            start=args.start,
            end=args.end,
            output_path=output_path,
            status="written",
            rows=len(records),
            earliest_utc=earliest,
            latest_utc=latest,
        )

    except Exception as exc:
        print(f"FAILED: {symbol} -> {exc}")

        async with counter_lock:
            counters.failed += 1

        await append_manifest(
            manifest_path=manifest_path,
            lock=manifest_lock,
            symbol=symbol,
            start=args.start,
            end=args.end,
            output_path=output_path,
            status="failed",
            error=str(exc),
        )


async def run(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    manifest_path = output_root / "download_manifest.csv"
    await write_manifest_header(manifest_path)

    symbols = [s.upper() for s in args.symbols]

    print("Bybit funding-rate downloader")
    print(f"Symbols:     {', '.join(symbols)}")
    print(f"Category:    {args.category}")
    print(f"Date range:  {args.start} to {args.end}")
    print(f"Output root: {output_root}")
    print(f"Concurrency: {args.concurrency}")
    print()

    if args.dry_run:
        print("Dry run. Planned outputs:")
        for symbol in symbols:
            print(output_path_for_symbol(output_root, symbol, args.start, args.end))
        return 0

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    connector = aiohttp.TCPConnector(limit=max(args.concurrency * 2, 8))

    headers = {
        "User-Agent": "BybitFundingRateResearchDownloader/1.0",
        "Accept": "application/json",
    }

    counters = Counters(symbols=len(symbols))
    counter_lock = asyncio.Lock()
    manifest_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:

        async def guarded(symbol: str) -> None:
            async with semaphore:
                await process_symbol(
                    session=session,
                    symbol=symbol,
                    args=args,
                    output_root=output_root,
                    manifest_path=manifest_path,
                    manifest_lock=manifest_lock,
                    counters=counters,
                    counter_lock=counter_lock,
                )

        await asyncio.gather(*(guarded(symbol) for symbol in symbols))

    print("\n=== DONE ===")
    print(f"Symbols:   {counters.symbols}")
    print(f"Written:   {counters.written}")
    print(f"Empty:     {counters.empty}")
    print(f"Failed:    {counters.failed}")
    print(f"Manifest:  {manifest_path}")

    return 0 if counters.failed == 0 else 1


def main() -> int:
    args = parse_args()

    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")

    if args.limit < 1 or args.limit > 200:
        raise SystemExit("--limit must be between 1 and 200")

    if args.start > args.end:
        raise SystemExit("--start must be <= --end")

    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
