# Bybit Funding Rate Downloader

Downloads funding-rate history for:

- BTCUSDT
- ETHUSDT
- SOLUSDT
- LINKUSDT

It pages backwards until either:

- Bybit has no older data, or
- it reaches your `--start` date.

No API key is needed.

## Install

```bash
pip install aiohttp aiofiles
```

## Dry run

```bash
python download_bybit_funding_rate_async.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --start 2021-01-01 \
  --output-root "/Volumes/LaCie Drive/ByBit/FundingRate" \
  --dry-run
```

## Real run

```bash
python download_bybit_funding_rate_async.py \
  --symbols BTCUSDT ETHUSDT SOLUSDT LINKUSDT \
  --start 2021-01-01 \
  --output-root "/Volumes/LaCie Drive/ByBit/FundingRate" \
  --concurrency 4
```

## Output

```text
/Volumes/LaCie Drive/ByBit/FundingRate/
  raw/
    BTCUSDT/
      BTCUSDT_funding_rate_2021-01-01_to_YYYY-MM-DD.csv
    ETHUSDT/
    SOLUSDT/
    LINKUSDT/
  download_manifest.csv
```

## Notes

- Use `--category linear` for USDT perpetuals.
- The API returns at most 200 funding records per request, so the script pages backwards using `endTime`.
- Existing files are skipped unless you use `--overwrite`.
