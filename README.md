# EitaaScrapper

اسکرپر چندترد ایتا با ورودی **Excel** و خروجی فارسی UTF-8.

## قابلیت‌ها

- ورودی از فایل اکسل (`.xlsx`) با 3 ستون در هر ردیف:
  1. کانال (مثال: `@mychannel`)
  2. لینک پست شروع بازه (مثال: `https://eitaa.com/mychannel/1200`)
  3. لینک پست پایان بازه (مثال: `https://eitaa.com/mychannel/1350`)
- اسکرول آهسته برای لود دقیق ویوها
- محاسبه‌ی دقیق:
  - کل پست‌ها و کل ویوها
  - آمار پست‌های فوروارد و غیرفوروارد (هم پست و هم ویو)
  - تعداد پست‌های عکس‌دار، ویدیویی و استیکری
- لاگ‌گذاری تفکیک‌شده:
  - `verbose.log`
  - `warning.log`
  - `error.log`
- اجرای کامندی تعاملی (مسیر فایل را می‌توانید با drag & drop داخل ترمینال وارد کنید)

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

بعد از اجرا، برنامه از شما این موارد را می‌پرسد:

- مسیر فایل اکسل
- تعداد ترد
- مسیر فایل خروجی گزارش
- مسیر پوشه لاگ
- حالت headful (نمایش مرورگر)

## فرمت اکسل

ستون‌ها باید به ترتیب زیر باشند:

| channel | start_link | end_link |
|---|---|---|
| @mychannel | https://eitaa.com/mychannel/1200 | https://eitaa.com/mychannel/1350 |
| @another | https://eitaa.com/another/501 | https://eitaa.com/another/620 |

> نکته: اگر لینک شروع/پایان برعکس باشد، برنامه خودش اصلاح می‌کند.

## خروجی

- گزارش نهایی فارسی UTF-8 (پیش‌فرض: `result_fa.txt`)
- فایل‌های لاگ در پوشه‌ی `logs`

