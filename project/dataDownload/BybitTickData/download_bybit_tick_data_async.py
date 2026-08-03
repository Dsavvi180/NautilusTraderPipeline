#!/usr/bin/env python3
"""
Async Bybit public trade-history downloader.

Scrapes Bybit public trading directories, downloads .csv.gz files concurrently,
and decompresses them into symbol-specific CSV folders.

Default URLs:
- https://public.bybit.com/trading/BTCUSDT/
- https://public.bybit.com/trading/ETHUSDT/
- https://public.bybit.com/trading/SOLUSDT/
- https://public.bybit.com/trading/LINKUSDT/

Install:

    pip install aiohttp aiofiles

Run from your repo root:

    python BybitTickData/download_bybit_tick_data_async.py \
        --start 2026-04-01 \
        --end 2026-06-28 \
        --concurrency 6

Output:

    BybitTickData/
      raw/BTCUSDT/*.csv.gz
      csv/BTCUSDT/*.csv
      download_manifest.csv
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import aiofiles
import aiohttp


DEFAULT_URLS = [
    "https://public.bybit.com/trading/BTCUSDT/",
    "https://public.bybit.com/trading/ETHUSDT/",
    "https://public.bybit.com/trading/SOLUSDT/",
    "https://public.bybit.com/trading/LINKUSDT/",
]

CSV_GZ_LINK_RE = re.compile(r'href=["\']([^"\']+\.csv\.gz)["\']', re.IGNORECASE)
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class RemoteFile:
    symbol: str
    filename: str
    url: str
    file_date: date | None


@dataclass
class Counters:
    discovered: int = 0
    selected: int = 0
    downloaded_or_skipped: int = 0
    decompressed_or_skipped: int = 0
    failed: int = 0


def parse_symbol_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1].upper()


def parse_date_from_filename(filename: str) -> date | None:
    match = DATE_RE.search(filename)
    if not match:
        return None
    return date.fromisoformat(match.group(1))


def in_date_range(file: RemoteFile, start: date | None, end: date | None) -> bool:
    if file.file_date is None:
        return True
    if start is not None and file.file_date < start:
        return False
    if end is not None and file.file_date > end:
        return False
    return True


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text(errors="replace")


async def discover_files(session: aiohttp.ClientSession, directory_url: str) -> list[RemoteFile]:
    directory_url = directory_url.rstrip("/") + "/"
    symbol = parse_symbol_from_url(directory_url)
    html = await fetch_text(session, directory_url)

    files: list[RemoteFile] = []
    seen: set[str] = set()

    for href in CSV_GZ_LINK_RE.findall(html):
        filename = Path(href).name
        if filename in seen:
            continue
        seen.add(filename)
        files.append(
            RemoteFile(
                symbol=symbol,
                filename=filename,
                url=urljoin(directory_url, filename),
                file_date=parse_date_from_filename(filename),
            )
        )

    files.sort(key=lambda f: (f.file_date or date.min, f.filename))
    return files


async def write_manifest_header(manifest_path: Path) -> None:
    if manifest_path.exists():
        return

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(manifest_path, "w", encoding="utf-8") as f:
        await f.write("symbol,date,filename,remote_url,raw_path,csv_path,status\n")


async def append_manifest(manifest_path: Path, lock: asyncio.Lock, line: str) -> None:
    async with lock:
        async with aiofiles.open(manifest_path, "a", encoding="utf-8") as f:
            await f.write(line + "\n")


async def download_file(
    session: aiohttp.ClientSession,
    remote_file: RemoteFile,
    destination: Path,
    retries: int,
) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        print(f"SKIP downloaded: {destination}")
        return True

    part_path = destination.with_name(destination.name + ".part")

    for attempt in range(1, retries + 1):
        try:
            async with session.get(remote_file.url) as response:
                if response.status == 404:
                    print(f"MISSING 404: {remote_file.url}")
                    return False

                response.raise_for_status()

                async with aiofiles.open(part_path, "wb") as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        if chunk:
                            await f.write(chunk)

            if not part_path.exists() or part_path.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty")

            part_path.replace(destination)
            print(f"DOWNLOADED: {destination}")
            return True

        except Exception as exc:
            print(
                f"ERROR download attempt {attempt}/{retries}: "
                f"{remote_file.url} -> {exc}"
            )
            if part_path.exists():
                part_path.unlink()

            if attempt < retries:
                await asyncio.sleep(1.5 * attempt)

    return False


def decompress_gzip_sync(source: Path, destination: Path, overwrite: bool) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        return True

    part_path = destination.with_name(destination.name + ".part")

    try:
        with gzip.open(source, "rb") as src, open(part_path, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)

        if part_path.stat().st_size == 0:
            raise RuntimeError("Decompressed file is empty")

        part_path.replace(destination)
        return True

    except Exception:
        if part_path.exists():
            part_path.unlink()
        return False


async def decompress_gzip(source: Path, destination: Path, overwrite: bool) -> bool:
    existed = destination.exists() and destination.stat().st_size > 0 and not overwrite

    ok = await asyncio.to_thread(
        decompress_gzip_sync,
        source,
        destination,
        overwrite,
    )

    if ok and existed:
        print(f"SKIP decompressed: {destination}")
    elif ok:
        print(f"DECOMPRESSED: {destination}")
    else:
        print(f"BAD GZIP / DECOMPRESS FAILED: {source}")

    return ok


async def process_file(
    session: aiohttp.ClientSession,
    remote_file: RemoteFile,
    raw_root: Path,
    csv_root: Path,
    manifest_path: Path,
    manifest_lock: asyncio.Lock,
    counters: Counters,
    counter_lock: asyncio.Lock,
    retries: int,
    overwrite_csv: bool,
    no_decompress: bool,
) -> None:
    raw_path = raw_root / remote_file.symbol / remote_file.filename
    csv_filename = remote_file.filename.removesuffix(".gz")
    csv_path = csv_root / remote_file.symbol / csv_filename

    ok_download = await download_file(
        session=session,
        remote_file=remote_file,
        destination=raw_path,
        retries=retries,
    )

    if not ok_download:
        async with counter_lock:
            counters.failed += 1

        await append_manifest(
            manifest_path,
            manifest_lock,
            f"{remote_file.symbol},{remote_file.file_date},{remote_file.filename},"
            f"{remote_file.url},{raw_path},{csv_path},download_failed",
        )
        return

    async with counter_lock:
        counters.downloaded_or_skipped += 1

    if no_decompress:
        await append_manifest(
            manifest_path,
            manifest_lock,
            f"{remote_file.symbol},{remote_file.file_date},{remote_file.filename},"
            f"{remote_file.url},{raw_path},{csv_path},downloaded_only",
        )
        return

    ok_decompress = await decompress_gzip(
        source=raw_path,
        destination=csv_path,
        overwrite=overwrite_csv,
    )

    if ok_decompress:
        status = "ok"
        async with counter_lock:
            counters.decompressed_or_skipped += 1
    else:
        status = "decompress_failed"
        async with counter_lock:
            counters.failed += 1

    await append_manifest(
        manifest_path,
        manifest_lock,
        f"{remote_file.symbol},{remote_file.file_date},{remote_file.filename},"
        f"{remote_file.url},{raw_path},{csv_path},{status}",
    )


async def run(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root)
    raw_root = output_root / "raw"
    csv_root = output_root / "csv"
    manifest_path = output_root / "download_manifest.csv"

    await write_manifest_header(manifest_path)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=180)
    connector = aiohttp.TCPConnector(limit=max(args.concurrency * 2, 8))

    counters = Counters()
    counter_lock = asyncio.Lock()
    manifest_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)

    headers = {
        "User-Agent": "BybitTickDataResearchDownloader/2.0 async",
    }

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:
        print("Discovering files...")

        discovery_results = await asyncio.gather(
            *(discover_files(session, url) for url in args.urls),
            return_exceptions=True,
        )

        selected_files: list[RemoteFile] = []

        for url, result in zip(args.urls, discovery_results):
            symbol = parse_symbol_from_url(url)

            if isinstance(result, Exception):
                print(f"FAILED to discover {symbol}: {result}")
                counters.failed += 1
                continue

            files = result
            selected = [f for f in files if in_date_range(f, args.start, args.end)]

            counters.discovered += len(files)
            counters.selected += len(selected)

            print(f"{symbol}: found {len(files)}, selected {len(selected)}")
            selected_files.extend(selected)

        async def guarded_process(remote_file: RemoteFile) -> None:
            async with semaphore:
                await process_file(
                    session=session,
                    remote_file=remote_file,
                    raw_root=raw_root,
                    csv_root=csv_root,
                    manifest_path=manifest_path,
                    manifest_lock=manifest_lock,
                    counters=counters,
                    counter_lock=counter_lock,
                    retries=args.retries,
                    overwrite_csv=args.overwrite_csv,
                    no_decompress=args.no_decompress,
                )

        print(f"\nProcessing {len(selected_files)} files with concurrency={args.concurrency}...\n")

        await asyncio.gather(*(guarded_process(f) for f in selected_files))

    print("\n=== DONE ===")
    print(f"Discovered:             {counters.discovered}")
    print(f"Selected:               {counters.selected}")
    print(f"Downloaded/skipped:     {counters.downloaded_or_skipped}")
    print(f"Decompressed/skipped:   {counters.decompressed_or_skipped}")
    print(f"Failed:                 {counters.failed}")
    print(f"Manifest:               {manifest_path}")

    return 0 if counters.failed == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Async downloader for Bybit public trading history .csv.gz files."
    )

    parser.add_argument(
        "--urls",
        nargs="+",
        default=DEFAULT_URLS,
        help="Bybit public trading directory URLs.",
    )

    parser.add_argument(
        "--output-root",
        default=str(Path(__file__).resolve().parent),
        help="Output folder. Defaults to this BybitTickData folder.",
    )

    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=None,
        help="Optional start date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="Optional end date, YYYY-MM-DD.",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=6,
        help="Number of concurrent downloads/decompress jobs.",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Download retries per file.",
    )

    parser.add_argument(
        "--overwrite-csv",
        action="store_true",
        help="Overwrite decompressed CSV files if they already exist.",
    )

    parser.add_argument(
        "--no-decompress",
        action="store_true",
        help="Only download .csv.gz files; do not decompress to .csv.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1")

    if args.start and args.end and args.start > args.end:
        parser.error("--start must be before or equal to --end")

    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
