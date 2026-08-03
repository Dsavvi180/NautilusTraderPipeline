#!/usr/bin/env python3
"""
Download Bybit historical order book files from Bybit's history-data backend.

This script:
- supports multiple symbols in one run
- queries Bybit in date windows, default 7 days
- downloads files concurrently
- skips existing files
- writes to .part first, then atomically renames when complete
- writes a manifest CSV
- handles Ctrl+C gracefully

Example:

    python download_bybit_orderbook.py \
      --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
      --start-date 2023-01-01 \
      --end-date 2026-06-28 \
      --output-dir "/Volumes/LaCie Drive/ByBit/OrderBook/raw" \
      --workers 3

For your HDD, keep workers around 2-4.
"""

from __future__ import annotations

import argparse
import csv
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests


LIST_FILES_URL = "https://www.bybit.com/x-api/quote/public/support/download/list-files"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.bybit.com/derivatives/en/history-data",
    "Connection": "keep-alive",
}

terminate = threading.Event()
manifest_lock = threading.Lock()


@dataclass(frozen=True)
class DownloadItem:
    symbol: str
    filename: str
    url: str
    start_day: str
    end_day: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bybit historical order book files."
    )

    parser.add_argument(
        "symbol",
        nargs="?",
        default=None,
        help=(
            "Optional single symbol, e.g. BTCUSDT. "
            "If omitted, --symbols or the default symbol list is used."
        ),
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols to download, e.g. BTCUSDT ETHUSDT SOLUSDT LINKUSDT.",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2023-01-01",
        help="Start date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.today().strftime("%Y-%m-%d"),
        help="End date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--window-size",
        type=int,
        default=7,
        help="Query window size in days. Bybit UI commonly uses 7-day windows.",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/bybit/orderbook/raw",
        help="Root output directory. Symbol folders will be created inside this.",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Parallel downloads. For an HDD, use 2-4.",
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries per file.",
    )

    parser.add_argument(
        "--sleep-between-windows",
        type=float,
        default=0.2,
        help="Small delay between list-file API calls.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be downloaded, but do not download.",
    )

    return parser.parse_args()


def handle_exit(signum: int, frame: Any) -> None:
    print("\nTermination signal received. Finishing current operations safely...")
    terminate.set()


def parse_ymd(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}; expected YYYY-MM-DD") from exc


def daterange_windows(start_date: datetime, end_date: datetime, step_days: int):
    current = start_date
    while current <= end_date:
        window_end = min(current + timedelta(days=step_days - 1), end_date)
        yield current, window_end
        current = window_end + timedelta(days=1)


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbol:
        return [args.symbol.upper()]
    if args.symbols:
        return [s.upper() for s in args.symbols]
    return DEFAULT_SYMBOLS.copy()


def safe_filename(filename: str) -> str:
    # Prevent path traversal from any unexpected API response.
    return Path(filename).name


def ensure_manifest(manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if manifest_path.exists():
        return

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "symbol",
                "filename",
                "remote_url",
                "local_path",
                "query_start_day",
                "query_end_day",
                "status",
                "bytes",
                "timestamp_utc",
            ]
        )


def write_manifest(
    manifest_path: Path,
    item: DownloadItem,
    local_path: Path,
    status: str,
    num_bytes: int = 0,
) -> None:
    with manifest_lock:
        with manifest_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    item.symbol,
                    item.filename,
                    item.url,
                    str(local_path),
                    item.start_day,
                    item.end_day,
                    status,
                    num_bytes,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ]
            )


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def get_file_list(
    session: requests.Session,
    symbol: str,
    start_day: str,
    end_day: str,
) -> list[DownloadItem]:
    params = {
        "bizType": "contract",
        "productId": "orderbook",
        "symbols": symbol,
        "interval": "daily",
        "periods": "",
        "startDay": start_day,
        "endDay": end_day,
    }

    response = session.get(LIST_FILES_URL, params=params, timeout=60)
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Bybit did not return JSON for {symbol} {start_day} to {end_day}. "
            f"First 300 chars: {response.text[:300]!r}"
        ) from exc

    result = payload.get("result") or {}
    file_list = result.get("list") or []

    items: list[DownloadItem] = []
    for file_info in file_list:
        file_url = file_info.get("url")
        filename = file_info.get("filename")

        if not file_url or not filename:
            continue

        items.append(
            DownloadItem(
                symbol=symbol,
                filename=safe_filename(str(filename)),
                url=str(file_url),
                start_day=start_day,
                end_day=end_day,
            )
        )

    return items


def download_file(
    item: DownloadItem,
    output_root: Path,
    manifest_path: Path,
    max_retries: int,
) -> bool:
    symbol_dir = output_root / item.symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    final_path = symbol_dir / item.filename
    part_path = symbol_dir / f"{item.filename}.part"

    if final_path.exists() and final_path.stat().st_size > 0:
        print(f"SKIP existing: {final_path}")
        write_manifest(manifest_path, item, final_path, "skipped_existing", final_path.stat().st_size)
        return True

    for attempt in range(1, max_retries + 1):
        if terminate.is_set():
            print(f"Terminating before download: {final_path}")
            write_manifest(manifest_path, item, final_path, "terminated")
            return False

        session = make_session()
        try:
            with session.get(item.url, stream=True, timeout=(20, 180)) as response:
                if response.status_code == 404:
                    print(f"404 missing: {item.url}")
                    write_manifest(manifest_path, item, final_path, "missing_404")
                    return False

                response.raise_for_status()

                with part_path.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if terminate.is_set():
                            print(f"Terminating during download: {final_path}")
                            if part_path.exists():
                                part_path.unlink()
                            write_manifest(manifest_path, item, final_path, "terminated_partial_removed")
                            return False

                        if chunk:
                            f.write(chunk)

            if not part_path.exists() or part_path.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty")

            os.replace(part_path, final_path)

            num_bytes = final_path.stat().st_size
            print(f"SAVED: {final_path} ({num_bytes / 1024 / 1024:.2f} MB)")
            write_manifest(manifest_path, item, final_path, "downloaded", num_bytes)
            return True

        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
            RuntimeError,
            OSError,
        ) as exc:
            print(
                f"Download failed attempt {attempt}/{max_retries}: "
                f"{item.filename} -> {exc}"
            )

            if part_path.exists():
                try:
                    part_path.unlink()
                except OSError:
                    pass

            if attempt < max_retries:
                time.sleep(2 * attempt)

        finally:
            session.close()

    print(f"FAILED: {final_path}")
    write_manifest(manifest_path, item, final_path, "failed")
    return False


def main() -> int:
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        signal.signal(signal.SIGQUIT, handle_exit)
    except AttributeError:
        pass

    args = parse_args()

    if args.window_size < 1:
        raise SystemExit("--window-size must be >= 1")

    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")

    start_dt = parse_ymd(args.start_date)
    end_dt = parse_ymd(args.end_date)

    if start_dt > end_dt:
        raise SystemExit("--start-date must be <= --end-date")

    symbols = resolve_symbols(args)
    output_root = Path(args.output_dir).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    manifest_path = output_root / "download_manifest_orderbook.csv"
    ensure_manifest(manifest_path)

    print("Bybit order book downloader")
    print(f"Symbols:       {', '.join(symbols)}")
    print(f"Date range:    {args.start_date} to {args.end_date}")
    print(f"Window size:   {args.window_size} days")
    print(f"Workers:       {args.workers}")
    print(f"Output root:   {output_root}")
    print(f"Manifest:      {manifest_path}")
    print()

    list_session = make_session()
    all_items: list[DownloadItem] = []

    try:
        for symbol in symbols:
            if terminate.is_set():
                break

            print(f"=== Listing {symbol} ===")

            symbol_items: list[DownloadItem] = []

            for start, end in daterange_windows(start_dt, end_dt, args.window_size):
                if terminate.is_set():
                    break

                start_str = start.strftime("%Y-%m-%d")
                end_str = end.strftime("%Y-%m-%d")

                print(f"Requesting {symbol}: {start_str} to {end_str}")

                try:
                    items = get_file_list(list_session, symbol, start_str, end_str)
                except Exception as exc:
                    print(f"List request failed for {symbol} {start_str} to {end_str}: {exc}")
                    continue

                if not items:
                    print(f"No files found for {symbol}: {start_str} to {end_str}")
                else:
                    print(f"Found {len(items)} files for {symbol}: {start_str} to {end_str}")
                    symbol_items.extend(items)

                if args.sleep_between_windows > 0:
                    time.sleep(args.sleep_between_windows)

            seen: set[str] = set()
            unique_items: list[DownloadItem] = []

            for item in symbol_items:
                key = f"{item.symbol}/{item.filename}"
                if key in seen:
                    continue
                seen.add(key)
                unique_items.append(item)

            print(f"{symbol}: {len(unique_items)} unique files selected")
            all_items.extend(unique_items)

    finally:
        list_session.close()

    if args.dry_run:
        print("\nDry run. Files that would be downloaded:")
        for item in all_items:
            print(f"{item.symbol}/{item.filename} -> {item.url}")
        print(f"\nTotal: {len(all_items)} files")
        return 0

    if not all_items:
        print("No files to download.")
        return 0

    print(f"\nStarting downloads: {len(all_items)} files\n")

    ok_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                download_file,
                item,
                output_root,
                manifest_path,
                args.max_retries,
            )
            for item in all_items
        ]

        for future in as_completed(futures):
            try:
                ok = future.result()
            except Exception as exc:
                print(f"Unexpected worker failure: {exc}")
                ok = False

            if ok:
                ok_count += 1
            else:
                fail_count += 1

            if terminate.is_set():
                print("Termination requested; workers will stop as they check the flag.")
                break

    print("\nDONE")
    print(f"Successful/skipped: {ok_count}")
    print(f"Failed/terminated:  {fail_count}")
    print(f"Output root:        {output_root}")
    print(f"Manifest:           {manifest_path}")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
