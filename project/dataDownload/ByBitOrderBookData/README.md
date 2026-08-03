# Bybit OrderBook Downloader

Corrected downloader for Bybit historical order book files.

## Install

```bash
pip install requests
```

## Recommended run on LaCie HDD

```bash
python download_bybit_orderbook.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --start-date 2023-01-01 \
  --end-date 2026-06-28 \
  --output-dir "/Volumes/LaCie Drive/ByBit/OrderBook/raw" \
  --window-size 7 \
  --workers 3
```

For an HDD, keep `--workers` around `2` to `4`.

## Dry run first

```bash
python download_bybit_orderbook.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --start-date 2023-01-01 \
  --end-date 2026-06-28 \
  --output-dir "/Volumes/LaCie Drive/ByBit/OrderBook/raw" \
  --dry-run
```

## Output

```text
/Volumes/LaCie Drive/ByBit/OrderBook/raw/
  BTCUSDT/
  ETHUSDT/
  SOLUSDT/
  LINKUSDT/
  download_manifest_orderbook.csv
```

## Notes

- Do not use `sudo` unless you genuinely have a permissions problem.
- The script skips existing completed files.
- Partial files are written as `.part` and only renamed after a completed download.
- This is for order book data, not public trade ticks.
