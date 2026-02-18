#!/usr/bin/env python3
"""
Eitaa multi-thread scraper (Excel input, post-link range)
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

from openpyxl import load_workbook
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

BASE_URL = "https://eitaa.com"
DEFAULT_TIMEOUT_MS = 30000
SCROLL_WAIT_SECONDS = 1.8  # intentionally slow for view load
SCROLL_PIXEL_STEP = 1100
MAX_IDLE_SCROLLS = 16

LOGGER_NAME = "eitaa_scraper"
_DIGIT_MAP = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")


@dataclasses.dataclass
class ChannelTask:
    channel: str
    start_link: str
    end_link: str
    start_id: int
    end_id: int


@dataclasses.dataclass
class ChannelResult:
    channel: str
    start_link: str
    end_link: str
    status: str = "موفق"
    error_message: str = ""

    total_posts: int = 0
    total_views: int = 0

    non_forward_posts: int = 0
    non_forward_views: int = 0

    forwarded_posts: int = 0
    forwarded_views: int = 0

    photo_posts: int = 0
    video_posts: int = 0
    sticker_posts: int = 0


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


def _sanitize_path_input(raw: str) -> Path:
    text = raw.strip()
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    if text.startswith("'") and text.endswith("'"):
        text = text[1:-1]
    return Path(text).expanduser().resolve()


def _parse_channel_post_link(link: str) -> tuple[str, int]:
    value = link.strip()
    pattern = r"https?://(?:www\.)?eitaa\.com/([A-Za-z0-9_]+)/([0-9]+)"
    m = re.fullmatch(pattern, value)
    if not m:
        raise ValueError(f"Invalid eitaa post link: {link}")
    return f"@{m.group(1)}", int(m.group(2))


def read_tasks_from_excel(excel_path: Path) -> list[ChannelTask]:
    wb = load_workbook(filename=excel_path, read_only=True, data_only=True)
    ws = wb.active
    tasks: list[ChannelTask] = []

    for row_idx, row in enumerate(ws.iter_rows(min_row=1, values_only=True), start=1):
        if not row:
            continue

        c1 = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
        c2 = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
        c3 = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""

        if not c1 and not c2 and not c3:
            continue

        # Optional header row support
        if row_idx == 1 and c1.lower() in {"channel", "کانال"}:
            continue

        if not (c1 and c2 and c3):
            raise ValueError(f"Row {row_idx}: all 3 columns are required (channel, start link, end link).")

        start_channel, start_id = _parse_channel_post_link(c2)
        end_channel, end_id = _parse_channel_post_link(c3)

        channel = c1 if c1.startswith("@") else f"@{c1}"
        if start_channel.lower() != channel.lower() or end_channel.lower() != channel.lower():
            raise ValueError(
                f"Row {row_idx}: channel mismatch. column1={channel}, start={start_channel}, end={end_channel}"
            )

        if start_id > end_id:
            start_id, end_id = end_id, start_id
            c2, c3 = c3, c2

        tasks.append(ChannelTask(channel=channel, start_link=c2, end_link=c3, start_id=start_id, end_id=end_id))

    wb.close()
    return tasks


def _normalize_number_text(text: str) -> str:
    return (
        text.translate(_DIGIT_MAP)
        .replace(",", "")
        .replace(" ", "")
        .replace("٬", "")
        .replace("٫", ".")
        .lower()
    )


def _parse_views(text: str) -> Optional[int]:
    if not text:
        return None
    cleaned = _normalize_number_text(text)
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


def _extract_posts_payload(page) -> list[dict]:
    script = """
    () => {
      const wrapSelectors = ['.tgme_widget_message_wrap', '.etme_widget_message_wrap', '.widget_message_wrap'];
      const wraps = [];
      for (const sel of wrapSelectors) {
        for (const node of document.querySelectorAll(sel)) wraps.push(node);
      }

      const rows = [];
      const seen = new Set();
      for (const wrap of wraps) {
        const msg = wrap.querySelector('[data-post], .tgme_widget_message, .etme_widget_message') || wrap;
        const postKey = msg.getAttribute('data-post') || wrap.getAttribute('data-post') || msg.id || wrap.id || '';
        if (!postKey || seen.has(postKey)) continue;
        seen.add(postKey);

        const idMatch = /\/([0-9]+)$/.exec(postKey);
        const postId = idMatch ? Number(idMatch[1]) : null;
        if (!postId) continue;

        const viewsNode = wrap.querySelector('.tgme_widget_message_views, .etme_widget_message_views, [class*="message_views"]');
        const txt = (wrap.textContent || '').toLowerCase();

        const isForwarded = !!wrap.querySelector('.tgme_widget_message_forwarded_from, .etme_widget_message_forwarded_from, [class*="forwarded"]');
        const hasPhoto = !!wrap.querySelector('a.tgme_widget_message_photo_wrap, .tgme_widget_message_photo_wrap, .etme_widget_message_photo_wrap, [class*="photo_wrap"]');
        const hasVideo = !!wrap.querySelector('video, .tgme_widget_message_video, .etme_widget_message_video, [class*="video"]');
        const hasSticker = !!wrap.querySelector('.tgme_widget_message_sticker, .etme_widget_message_sticker, [class*="sticker"]') || txt.includes('sticker');

        rows.push({
          post_id: postId,
          views_text: viewsNode ? (viewsNode.textContent || '') : '',
          is_forwarded: isForwarded,
          has_photo: hasPhoto,
          has_video: hasVideo,
          has_sticker: hasSticker
        });
      }

      rows.sort((a, b) => b.post_id - a.post_id);
      return rows;
    }
    """
    return page.evaluate(script)


def _navigate_to_channel(page, channel: str, logger: logging.Logger) -> None:
    url = f"{BASE_URL}/{channel.lstrip('@')}"
    logger.info("Opening channel %s at %s", channel, url)
    page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
    page.wait_for_timeout(2200)


def _scan_channel_posts(page, task: ChannelTask, logger: logging.Logger) -> ChannelResult:
    result = ChannelResult(channel=task.channel, start_link=task.start_link, end_link=task.end_link)
    counted_ids: set[int] = set()
    seen_ids: set[int] = set()
    idle_rounds = 0

    logger.info("[%s] target range: %d -> %d", task.channel, task.start_id, task.end_id)

    while idle_rounds < MAX_IDLE_SCROLLS:
        payload = _extract_posts_payload(page)
        new_seen = 0

        for item in payload:
            post_id = int(item.get("post_id") or 0)
            if not post_id:
                continue

            if post_id not in seen_ids:
                seen_ids.add(post_id)
                new_seen += 1

            if post_id < task.start_id or post_id > task.end_id:
                continue
            if post_id in counted_ids:
                continue

            views = _parse_views(str(item.get("views_text") or ""))
            if views is None:
                logger.warning("[%s] post=%d skipped (views unreadable: '%s')", task.channel, post_id, item.get("views_text"))
                continue

            counted_ids.add(post_id)
            result.total_posts += 1
            result.total_views += views

            if bool(item.get("is_forwarded")):
                result.forwarded_posts += 1
                result.forwarded_views += views
            else:
                result.non_forward_posts += 1
                result.non_forward_views += views

            if bool(item.get("has_photo")):
                result.photo_posts += 1
            if bool(item.get("has_video")):
                result.video_posts += 1
            if bool(item.get("has_sticker")):
                result.sticker_posts += 1

        if task.start_id in seen_ids and task.end_id in seen_ids and new_seen == 0:
            logger.debug("[%s] both boundary posts seen and page stable.", task.channel)
            break

        if task.start_id in seen_ids and task.end_id in seen_ids and len(counted_ids) >= (task.end_id - task.start_id + 1):
            logger.debug("[%s] all range posts counted by id span.", task.channel)
            break

        if new_seen == 0:
            idle_rounds += 1
        else:
            idle_rounds = 0

        logger.debug(
            "[%s] progress discovered=%d counted=%d idle=%d",
            task.channel,
            len(seen_ids),
            len(counted_ids),
            idle_rounds,
        )

        page.mouse.wheel(0, SCROLL_PIXEL_STEP)
        page.wait_for_timeout(int(SCROLL_WAIT_SECONDS * 1000))

    logger.info(
        "[%s] Finished scan: total_posts=%d total_views=%d (non_forward=%d/%d, forwarded=%d/%d)",
        task.channel,
        result.total_posts,
        result.total_views,
        result.non_forward_posts,
        result.non_forward_views,
        result.forwarded_posts,
        result.forwarded_views,
    )
    return result


def process_channel(task: ChannelTask, headless: bool, logger: logging.Logger) -> ChannelResult:
    logger.info("[%s] Worker started in %s", task.channel, threading.current_thread().name)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(viewport={"width": 1366, "height": 920})
            page = context.new_page()
            page.set_default_timeout(DEFAULT_TIMEOUT_MS)

            _navigate_to_channel(page, task.channel, logger)
            result = _scan_channel_posts(page, task, logger)

            context.close()
            browser.close()
            return result

    except PlaywrightTimeoutError as exc:
        logger.error("[%s] Timeout error: %s", task.channel, exc)
        return ChannelResult(task.channel, task.start_link, task.end_link, status="خطا", error_message=f"Timeout: {exc}")
    except Exception as exc:
        logger.exception("[%s] Unhandled error", task.channel)
        return ChannelResult(task.channel, task.start_link, task.end_link, status="خطا", error_message=str(exc))


def write_report(output_path: Path, results: Iterable[ChannelResult], elapsed_seconds: float) -> None:
    items = sorted(results, key=lambda x: x.channel.lower())

    total_channels = len(items)
    success_count = sum(1 for x in items if x.status == "موفق")
    fail_count = total_channels - success_count

    grand_posts = sum(x.total_posts for x in items)
    grand_views = sum(x.total_views for x in items)
    grand_nonf_posts = sum(x.non_forward_posts for x in items)
    grand_nonf_views = sum(x.non_forward_views for x in items)
    grand_fwd_posts = sum(x.forwarded_posts for x in items)
    grand_fwd_views = sum(x.forwarded_views for x in items)
    grand_photos = sum(x.photo_posts for x in items)
    grand_videos = sum(x.video_posts for x in items)
    grand_stickers = sum(x.sticker_posts for x in items)

    lines: list[str] = []
    lines.append("=" * 84)
    lines.append("گزارش نهایی اسکرپر ایتا (بر اساس بازه لینک پست)")
    lines.append("=" * 84)
    lines.append(f"زمان تولید گزارش: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"تعداد کانال‌ها: {total_channels}")
    lines.append(f"موفق: {success_count} | خطادار: {fail_count}")
    lines.append(f"کل پست‌ها: {grand_posts:,} | کل ویو: {grand_views:,}")
    lines.append(f"پست غیرفوروارد: {grand_nonf_posts:,} | ویو غیرفوروارد: {grand_nonf_views:,}")
    lines.append(f"پست فوروارد: {grand_fwd_posts:,} | ویو فوروارد: {grand_fwd_views:,}")
    lines.append(f"پست عکس‌دار: {grand_photos:,} | پست ویدیویی: {grand_videos:,} | پست استیکری: {grand_stickers:,}")
    lines.append(f"زمان اجرا: {elapsed_seconds:.2f} ثانیه")
    lines.append("")

    for idx, item in enumerate(items, start=1):
        lines.append("-" * 84)
        lines.append(f"کانال #{idx}: {item.channel}")
        lines.append(f"لینک شروع: {item.start_link}")
        lines.append(f"لینک پایان: {item.end_link}")
        lines.append(f"وضعیت: {item.status}")
        if item.status == "موفق":
            lines.append(f"کل پست‌ها: {item.total_posts:,}")
            lines.append(f"کل ویو: {item.total_views:,}")
            lines.append(f"غیرفوروارد -> پست: {item.non_forward_posts:,} | ویو: {item.non_forward_views:,}")
            lines.append(f"فوروارد -> پست: {item.forwarded_posts:,} | ویو: {item.forwarded_views:,}")
            lines.append(f"رسانه -> عکس: {item.photo_posts:,} | ویدیو: {item.video_posts:,} | استیکر: {item.sticker_posts:,}")
        else:
            lines.append(f"جزئیات خطا: {item.error_message}")
        lines.append("")

    lines.append("=" * 84)
    lines.append("پایان گزارش")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def _ask_user_inputs() -> tuple[Path, int, Path, Path, bool]:
    print("\n=== Eitaa Scraper (Excel Mode) ===")
    print("Excel format (3 columns per row):")
    print("1) Channel    2) Start post link    3) End post link")
    print("Example link: https://eitaa.com/mychannel/12345")
    print("Tip: you can drag & drop the Excel file path into terminal.\n")

    excel_path = _sanitize_path_input(input("Enter Excel file path: "))
    threads_raw = input("Enter thread count [default 8]: ").strip()
    output_raw = input("Enter output txt path [default result_fa.txt]: ").strip()
    log_raw = input("Enter log directory [default logs]: ").strip()
    headful_raw = input("Run browser visible? (y/N): ").strip().lower()

    threads = int(threads_raw) if threads_raw else 8
    output = _sanitize_path_input(output_raw) if output_raw else Path("result_fa.txt").resolve()
    log_dir = _sanitize_path_input(log_raw) if log_raw else Path("logs").resolve()
    headful = headful_raw in {"y", "yes", "1"}

    return excel_path, max(1, threads), output, log_dir, headful


def main() -> int:
    excel_path, threads, output_path, log_dir, headful = _ask_user_inputs()

    logger = setup_logger(log_dir)
    if not excel_path.exists():
        logger.error("Excel file not found: %s", excel_path)
        return 1

    tasks = read_tasks_from_excel(excel_path)
    if not tasks:
        logger.error("No valid rows found in excel file: %s", excel_path)
        return 1

    logger.info("Loaded %d channel rows from %s", len(tasks), excel_path)
    logger.info("Thread count: %d", threads)
    logger.info("Slow-scroll mode enabled: step=%d px, wait=%.1fs", SCROLL_PIXEL_STEP, SCROLL_WAIT_SECONDS)

    start_ts = time.time()
    results: list[ChannelResult] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_map = {executor.submit(process_channel, task, not headful, logger): task.channel for task in tasks}
        for future in concurrent.futures.as_completed(future_map):
            channel = future_map[future]
            try:
                item = future.result()
            except Exception as exc:
                logger.exception("[%s] unexpected worker failure", channel)
                item = ChannelResult(channel, "-", "-", status="خطا", error_message=str(exc))
            results.append(item)

    elapsed = time.time() - start_ts
    write_report(output_path, results, elapsed)
    logger.info("Report written to %s", output_path)
    logger.info("Done in %.2f seconds", elapsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
