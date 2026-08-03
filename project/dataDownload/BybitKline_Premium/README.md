# Bybit Kline + Premium Index Kline Downloader

This adapts the earlier async downloader pattern to pull:

- normal OHLCV klines from `/v5/market/kline`
- premium index klines from `/v5/market/premium-index-price-kline`

It writes CSV files directly to your LaCie drive.

## Install

```bash
pip install aiohttp aiofiles
```

## Dry run

```bash
python download_bybit_kline_premium_async.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --datasets kline premium_index_kline \
  --interval 1 \
  --start 2026-04-01 \
  --end 2026-06-28 \
  --output-root "/Volumes/LaCie Drive/ByBit/KlineData" \
  --dry-run
```

## Real run

```bash
python download_bybit_kline_premium_async.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --datasets kline premium_index_kline \
  --interval 1 \
  --start 2026-04-01 \
  --end 2026-06-28 \
  --output-root "/Volumes/LaCie Drive/ByBit/KlineData" \
  --concurrency 3
```

## Output layout

```text
/Volumes/LaCie Drive/ByBit/KlineData/
  raw/
    kline/
      BTCUSDT/
        interval=1/
      ETHUSDT/
        interval=1/
      SOLUSDT/
        interval=1/
      LINKUSDT/
        interval=1/
    premium_index_kline/
      BTCUSDT/
        interval=1/
      ETHUSDT/
        interval=1/
      SOLUSDT/
        interval=1/
      LINKUSDT/
        interval=1/
  download_manifest.csv
```

## Notes

- Use `--category linear` for USDT perpetuals.
- `--interval 1` means 1-minute candles.
- The script writes monthly files by default.
- Add `--single-file` if you want one file per symbol/dataset for the whole date range.
- It skips existing files unless you pass `--overwrite`.
