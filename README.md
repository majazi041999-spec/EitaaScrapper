# EitaaScrapper

اسکرپر چندترد ایتا با ورودی **Excel** و خروجی فارسی UTF-8.

## قابلیت‌ها

- ورودی از فایل اکسل (`.xlsx`) با 3 ستون در هر ردیف:
  1. کانال
  2. **لینک پایان بازه**
  3. **لینک شروع بازه**
- الگوریتم دقیق‌تر: **Direct Sequential Access + Smart Retry**
  - هر پست مستقیماً با لینک `channel/post_id` باز می‌شود (به جای تکیه‌ی اصلی روی اسکرول)
  - برای پست‌های خطادار، retry هوشمند انجام می‌شود
- محاسبه:
  - کل پست‌ها و کل ویوها
  - فوروارد/غیرفوروارد (تعداد + ویو)
  - عکس/ویدیو/استیکر
  - not_found / service_message / failed
- خروجی پیش‌فرض: `result_fa.txt`
- لاگ پیش‌فرض: `logs/`
- اجرای تعاملی با دریافت مسیر اکسل (drag & drop path friendly)

## نصب

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## اجرای برنامه

```bash
python eitaa_scraper.py
```

ورودی‌هایی که برنامه می‌پرسد:
- مسیر فایل اکسل
- تعداد ترد
- حالت نمایش مرورگر (headful)

> مسیر لاگ و خروجی سؤال نمی‌شود و همیشه پیش‌فرض است.

## فرمت اکسل

| channel | end_link | start_link |
|---|---|---|
| @mychannel | https://eitaa.com/mychannel/1350 | https://eitaa.com/mychannel/1200 |

> اگر start/end برعکس باشد، برنامه خودش اصلاح می‌کند.

## نکته مهم برای exe (PyInstaller)

- اگر Chromium موجود نباشد، برنامه موقع اجرا خودش تلاش می‌کند `playwright install chromium` را اجرا کند.
- اگر روی سیستم مقصد محدودیت دسترسی/اینترنت باشد، یک‌بار دستی اجرا کنید:

```bash
playwright install chromium
```
