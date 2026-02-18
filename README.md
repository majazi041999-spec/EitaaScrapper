# EitaaScrapper

Multi-thread scraper for Eitaa channels that:
- Reads channels from a text file (`@channel` per line)
- Scrapes posts in a Gregorian date range (`YYYY-MM-DD`)
- Sums view counts and counts posts per channel
- Generates a clean UTF-8 Persian report
- Writes separate logs for verbose, warning, and error

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Channel list file example

Create a txt file such as `channels.txt`:

```txt
@channel1
@channel2
@channel3
```

## Run

```bash
python eitaa_scraper.py \
  --channels-file channels.txt \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --threads 20 \
  --output result_fa.txt \
  --log-dir logs
```

### Arguments

- `--channels-file`: path to channel list txt file
- `--start-date`: start date in `YYYY-MM-DD` (example: `2025-01-01`)
- `--end-date`: end date in `YYYY-MM-DD` (example: `2025-01-31`)
- `--threads`: worker threads (for large lists, 30-40 is possible on strong hardware)
- `--output`: Persian output report path
- `--log-dir`: log folder path (`verbose.log`, `warning.log`, `error.log`)
- `--headful`: run browser visible mode (for debugging)

## Output

- Final report (UTF-8 Persian): `result_fa.txt`
- Logs:
  - `logs/verbose.log`
  - `logs/warning.log`
  - `logs/error.log`

## Notes for accuracy

- The scraper uses browser rendering (Playwright + Chromium) so JS-loaded posts can be read.
- It scrolls repeatedly to load older messages and stops when no new data is observed for several rounds.
- The scraper only counts real post cards and reads `time[datetime]` + dedicated views element to avoid false counting.
- Date parsing is strict (ISO datetime or YYYY-MM-DD/YYYY/MM/DD), so noisy message text cannot corrupt totals.
- If a post date or view cannot be parsed confidently, it is skipped and logged.
