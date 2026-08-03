# BybitTickData async downloader

This is the asynchronous version of the Bybit public trading-history downloader.

It scrapes:

- `https://public.bybit.com/trading/BTCUSDT/`
- `https://public.bybit.com/trading/ETHUSDT/`
- `https://public.bybit.com/trading/SOLUSDT/`
- `https://public.bybit.com/trading/LINKUSDT/`

It downloads `.csv.gz` files into `raw/<SYMBOL>/` and decompresses them into `csv/<SYMBOL>/`.

## Install

```bash
pip install aiohttp aiofiles
```

## Run

From the root of your repo:

```bash
python BybitTickData/download_bybit_tick_data_async.py \
  --start 2026-04-01 \
  --end 2026-06-28 \
  --concurrency 6
```

Download only the compressed files, without unzipping:

```bash
python BybitTickData/download_bybit_tick_data_async.py \
  --start 2026-04-01 \
  --end 2026-06-28 \
  --concurrency 8 \
  --no-decompress
```

## Output

```text
BybitTickData/
  raw/
    BTCUSDT/
      BTCUSDT2026-06-01.csv.gz
    ETHUSDT/
    SOLUSDT/
    LINKUSDT/

  csv/
    BTCUSDT/
      BTCUSDT2026-06-01.csv
    ETHUSDT/
    SOLUSDT/
    LINKUSDT/

  download_manifest.csv
```

## Notes

- These are public trade-history files, not order-book files.
- The downloader skips existing files, so it is safe to rerun.
- Start with concurrency `4` to `8`; increasing too far may get you throttled or make your disk the bottleneck.
