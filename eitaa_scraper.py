#!/usr/bin/env python3
"""
Eitaa multi-thread scraper
- Reads channels from a text file (@channel per line)
- Scrapes posts between start/end dates
- Sums views and counts posts per channel
- Writes a Persian UTF-8 report
- Produces separated verbose/warning/error log files
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Optional

from dateutil import parser as date_parser
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://eitaa.com"
DEFAULT_TIMEOUT_MS = 25000
SCROLL_WAIT_SECONDS = 1.1
MAX_IDLE_SCROLLS = 8

LOGGER_NAME = "eitaa_scraper"


@dataclasses.dataclass
class ChannelResult:
    channel: str
    post_count: int = 0
    total_views: int = 0
    first_post_date: Optional[date] = None
    last_post_date: Optional[date] = None
    status: str = "موفق"
    error_message: str = ""


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    verbose_handler = logging.FileHandler(log_dir / "verbose.log", encoding="utf-8")
    verbose_handler.setLevel(logging.DEBUG)
    verbose_handler.addFilter(MaxLevelFilter(logging.INFO))
    verbose_handler.setFormatter(formatter)

    warning_handler = logging.FileHandler(log_dir / "warning.log", encoding="utf-8")
    warning_handler.setLevel(logging.WARNING)
    warning_handler.addFilter(MaxLevelFilter(logging.WARNING))
    warning_handler.setFormatter(formatter)

    error_handler = logging.FileHandler(log_dir / "error.log", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(verbose_handler)
    logger.addHandler(warning_handler)
    logger.addHandler(error_handler)
    logger.addHandler(stream_handler)

    return logger


def parse_cli_date(value: str) -> date:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Please use YYYY-MM-DD (example: 2025-01-31)."
        ) from exc


def parse_channel_file(channel_file: Path) -> list[str]:
    channels: list[str] = []
    with channel_file.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if not raw.startswith("@"):
                raw = f"@{raw}"
            channels.append(raw)
    return sorted(set(channels))


def _parse_views(text: str) -> Optional[int]:
    if not text:
        return None

    cleaned = text.replace(",", "").replace(" ", "").replace("٬", "").replace("٫", ".").lower()

    match = re.search(r"(\d+(?:\.\d+)?)([km]?)", cleaned)
    if not match:
        return None

    number = float(match.group(1))
    suffix = match.group(2)
    if suffix == "k":
        number *= 1000
    elif suffix == "m":
        number *= 1_000_000

    return int(number)


def _parse_post_date(text: str) -> Optional[date]:
    if not text:
        return None

    text = text.strip()

    numeric = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})", text)
    if numeric:
        fmt = "%Y-%m-%d" if "-" in numeric.group(1) else "%Y/%m/%d"
        try:
            return datetime.strptime(numeric.group(1), fmt).date()
        except ValueError:
            pass

    try:
        parsed = date_parser.parse(text, fuzzy=True, dayfirst=False)
        return parsed.date()
    except Exception:
        return None


def _extract_posts_payload(page) -> list[dict]:
    script = """
    () => {
      const candidates = Array.from(document.querySelectorAll('[class*=\"message\"], [class*=\"post\"], article, .tgme_widget_message_wrap'));
      const unique = [];
      const seen = new Set();
      for (const node of candidates) {
        if (!node || seen.has(node)) continue;
        seen.add(node);

        const dateNode = node.querySelector('time, [class*=\"date\"], [class*=\"meta\"]');
        const viewsNode = node.querySelector('[class*=\"view\"], [class*=\"counter\"], [class*=\"meta\"]');

        const dateText = dateNode ? (dateNode.getAttribute('datetime') || dateNode.textContent || '') : '';
        const viewText = viewsNode ? (viewsNode.textContent || '') : '';

        const combinedText = (node.textContent || '').slice(-400);

        const rawId = node.getAttribute('data-post') || node.id || '';

        unique.push({
          id: rawId,
          date_text: dateText,
          views_text: viewText,
          fallback_text: combinedText
        });
      }
      return unique;
    }
    """
    return page.evaluate(script)


def _navigate_to_channel(page, channel: str, logger: logging.Logger) -> None:
    channel_name = channel.lstrip("@")
    url = f"{BASE_URL}/{channel_name}"
    logger.info("Opening channel %s at %s", channel, url)
    page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
    page.wait_for_timeout(1500)


def _scan_channel_posts(
    page,
    channel: str,
    start_date: date,
    end_date: date,
    logger: logging.Logger,
) -> ChannelResult:
    result = ChannelResult(channel=channel)
    collected_ids: set[str] = set()
    idle_scrolls = 0
    reached_older_than_start = False

    while idle_scrolls < MAX_IDLE_SCROLLS and not reached_older_than_start:
        payload = _extract_posts_payload(page)
        before_count = len(collected_ids)

        for item in payload:
            post_id = item.get("id") or f"{item.get('date_text','')}-{item.get('views_text','')}-{hash(item.get('fallback_text', ''))}"
            if post_id in collected_ids:
                continue

            combined_date_text = f"{item.get('date_text', '')} {item.get('fallback_text', '')}"
            parsed_date = _parse_post_date(combined_date_text)
            if parsed_date is None:
                logger.debug("[%s] Skipping post with unknown date format", channel)
                continue

            if parsed_date < start_date:
                reached_older_than_start = True
                continue

            if parsed_date > end_date:
                continue

            combined_view_text = f"{item.get('views_text', '')} {item.get('fallback_text', '')}"
            views = _parse_views(combined_view_text)
            if views is None:
                logger.warning("[%s] Could not parse view count for one post (date=%s)", channel, parsed_date)
                continue

            collected_ids.add(post_id)
            result.post_count += 1
            result.total_views += views
            result.first_post_date = (
                parsed_date if result.first_post_date is None else min(result.first_post_date, parsed_date)
            )
            result.last_post_date = (
                parsed_date if result.last_post_date is None else max(result.last_post_date, parsed_date)
            )

        if len(collected_ids) == before_count:
            idle_scrolls += 1
        else:
            idle_scrolls = 0

        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(int(SCROLL_WAIT_SECONDS * 1000))

    logger.info(
        "[%s] Finished scan: posts=%d total_views=%d",
        channel,
        result.post_count,
        result.total_views,
    )
    return result


def process_channel(
    channel: str,
    start_date: date,
    end_date: date,
    headless: bool,
    logger: logging.Logger,
) -> ChannelResult:
    thread_name = threading.current_thread().name
    logger.info("[%s] Worker started in %s", channel, thread_name)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(viewport={"width": 1366, "height": 900})
            page = context.new_page()
            page.set_default_timeout(DEFAULT_TIMEOUT_MS)

            _navigate_to_channel(page, channel, logger)
            result = _scan_channel_posts(page, channel, start_date, end_date, logger)

            context.close()
            browser.close()
            return result

    except PlaywrightTimeoutError as exc:
        logger.error("[%s] Timeout error: %s", channel, exc)
        return ChannelResult(channel=channel, status="خطا", error_message=f"Timeout: {exc}")
    except Exception as exc:
        logger.exception("[%s] Unhandled error", channel)
        return ChannelResult(channel=channel, status="خطا", error_message=str(exc))


def write_report(
    output_path: Path,
    results: Iterable[ChannelResult],
    start_date: date,
    end_date: date,
    elapsed_seconds: float,
) -> None:
    items = list(results)
    total_channels = len(items)
    success_count = sum(1 for x in items if x.status == "موفق")
    failed_count = total_channels - success_count
    grand_total_views = sum(x.total_views for x in items)
    grand_total_posts = sum(x.post_count for x in items)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("گزارش نهایی اسکرپر ایتا")
    lines.append("=" * 72)
    lines.append(f"زمان تولید گزارش: {now_str}")
    lines.append(f"بازه زمانی: از {start_date.isoformat()} تا {end_date.isoformat()}")
    lines.append(f"تعداد کانال‌ها: {total_channels}")
    lines.append(f"کانال موفق: {success_count} | کانال خطادار: {failed_count}")
    lines.append(f"مجموع کل پست‌ها: {grand_total_posts}")
    lines.append(f"مجموع کل ویوها: {grand_total_views:,}")
    lines.append(f"زمان اجرا: {elapsed_seconds:.2f} ثانیه")
    lines.append("")

    for idx, item in enumerate(items, start=1):
        lines.append("-" * 72)
        lines.append(f"کانال #{idx}: {item.channel}")
        lines.append(f"وضعیت: {item.status}")
        if item.status == "موفق":
            lines.append(f"تعداد پست‌های شمرده‌شده: {item.post_count}")
            lines.append(f"مجموع ویو: {item.total_views:,}")
            lines.append(
                f"اولین تاریخ پست در بازه: {item.first_post_date.isoformat() if item.first_post_date else '-'}"
            )
            lines.append(
                f"آخرین تاریخ پست در بازه: {item.last_post_date.isoformat() if item.last_post_date else '-'}"
            )
        else:
            lines.append(f"جزئیات خطا: {item.error_message}")
        lines.append("")

    lines.append("=" * 72)
    lines.append("پایان گزارش")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Eitaa multi-thread scraper. Reads channels from text file and sums post views in a date range."
        )
    )
    parser.add_argument(
        "--channels-file",
        required=True,
        type=Path,
        help="Path to txt file containing one channel per line (example: @mychannel).",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_cli_date,
        help="Start date in YYYY-MM-DD format (example: 2025-01-01).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_cli_date,
        help="End date in YYYY-MM-DD format (example: 2025-01-31).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=min(8, (os.cpu_count() or 4) * 2),
        help="Number of worker threads (recommended 10-40 based on hardware).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("result_fa.txt"),
        help="Output report path (UTF-8 Persian text file).",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs"),
        help="Directory for log files (verbose.log, warning.log, error.log).",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser in visible mode (debug). Default is headless.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date")

    logger = setup_logger(args.log_dir)

    channels = parse_channel_file(args.channels_file)
    if not channels:
        parser.error("No channel found in channels file.")

    logger.info("Loaded %d channels from %s", len(channels), args.channels_file)
    logger.info("Date range: %s -> %s", args.start_date, args.end_date)
    logger.info("Thread count: %d", args.threads)

    start_ts = time.time()
    results: list[ChannelResult] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_map = {
            executor.submit(
                process_channel,
                channel,
                args.start_date,
                args.end_date,
                not args.headful,
                logger,
            ): channel
            for channel in channels
        }

        for future in concurrent.futures.as_completed(future_map):
            channel = future_map[future]
            try:
                item = future.result()
            except Exception as exc:
                logger.exception("[%s] unexpected worker failure", channel)
                item = ChannelResult(channel=channel, status="خطا", error_message=str(exc))
            results.append(item)

    elapsed = time.time() - start_ts
    results.sort(key=lambda x: x.channel.lower())

    write_report(args.output, results, args.start_date, args.end_date, elapsed)

    logger.info("Report written to %s", args.output)
    logger.info("Done in %.2f seconds", elapsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
