# 🌊 Luffy Panel - VLESS Proxy Manager

<div dir="rtl">

# 🌊 پنل لوفی - مدیریت پروکسی V2Ray VLESS

</div>

---

## 🇬🇧 English

### What is Luffy Panel?

Luffy Panel is a lightweight, self-hosted web dashboard for managing **VLESS over WebSocket (WS) proxy configurations**. It acts as a middleware proxy that receives VLESS traffic via WebSocket and forwards it to your upstream server — all configurable through a clean, bilingual (EN/FA) web interface.

Designed to run on **Hugging Face Spaces** (free tier), giving you a persistent proxy management panel without any server costs.

### 🚀 Deploy on Hugging Face Spaces

1. **Fork** this repository
2. Go to [huggingface.co/new-space](https://huggingface.co/new-space)
3. Choose **Docker** as the Space SDK
4. Select **Blank Docker** template
5. Link your forked repo
6. Click **Create Space** — it deploys automatically!

> ⚠️ **Important:** Make sure your Space is set to **Public** if you need external access, or keep it **Private** for personal use.

### ✨ Features

- 🗄️ **SQLite Database** — Persistent storage survives HF restarts (stored in `/data`)
- 🚦 **Rate Limiting** — Protect your panel from abuse
- 🔄 **Auto Cleanup** — Expired inbounds are removed automatically
- 📊 **Traffic History** — Daily traffic stats with 30-day retention
- 📱 **Mobile-Friendly UI** — Works great on phones and tablets
- 🌓 **Dark/Light Theme** — Auto-detects your system preference
- 📷 **Client-Side QR Codes** — Generated locally, no external API calls
- 📤 **Export Configs** — Download all your links as JSON
- 🔒 **Security Headers** — X-Frame-Options, CSP, and more
- 🌐 **Bilingual** — English & Persian (Farsi) interface

### 📖 How to Use

1. Open your Space URL (e.g., `https://yourusername-luffy-panel.hf.space`)
2. The dashboard shows your active proxy links
3. **Add a new link**: Fill in the inbound settings (port, UUID, WS path, upstream server)
4. Copy the generated VLESS URL or scan the QR code
5. Import into your V2Ray client (v2rayNG, Nekoray, etc.)
6. Monitor traffic and manage links from the dashboard

### ⚠️ Important Notes

- **Hugging Face Spaces sleep after inactivity.** The panel wakes up on the next request — there may be a cold-start delay.
- **Persistent storage** (`/data`) survives restarts but has limits (check HF docs).
- **Rate limiting** is enabled by default to prevent abuse.
- This panel is a **proxy manager**, not the proxy itself. You still need an upstream server.
- For production use, consider deploying on a dedicated server.

### 🔧 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7860` | Server port (HF Spaces default) |

---

<div dir="rtl">

## 🇮🇷 فارسی

### پنل لوفی چیست؟

پنل لوفی یک داشبورد تحت وب سبک و خودمیزبان برای مدیریت **کانفیگ‌های VLESS روی WebSocket** است. این پنل به‌عنوان یک پروکسی میانی عمل می‌کند که ترافیک VLESS را از طریق WebSocket دریافت کرده و به سرور آپ‌استریم شما ارسال می‌کند — همه از طریق یک رابط کاربری تمیز و دوزبانه (فارسی/انگلیسی) قابل تنظیم است.

طراحی شده برای اجرا روی **Hugging Face Spaces** (رایگان)، که به شما یک پنل مدیریت پروکسی پایدار بدون هیچ هزینه سرور می‌دهد.

### 🚀 نصب روی Hugging Face Spaces

۱. این ریپازیتوری را **Fork** کنید
۲. به [huggingface.co/new-space](https://huggingface.co/new-space) بروید
۳. **Docker** را به‌عنوان Space SDK انتخاب کنید
۴. قالب **Blank Docker** را انتخاب کنید
۵. ریپوی Fork شده را لینک کنید
۶. روی **Create Space** کلیک کنید — خودکار نصب می‌شود!

> ⚠️ **توجه:** مطمئن شوید Space شما روی **Public** تنظیم شده اگر نیاز به دسترسی خارجی دارید، یا برای استفاده شخصی روی **Private** بگذارید.

### ✨ ویژگی‌ها

- 🗄️ **پایگاه داده SQLite** — ذخیره‌سازی پایدار که با ری‌استارت HF از بین نمی‌رود (در `/data`)
- 🚦 **محدودیت نرخ (Rate Limiting)** — محافظت از پنل در برابر سوءاستفاده
- 🔄 **پاکسازی خودکار** — اینباندهای منقضی شده خودکار حذف می‌شوند
- 📊 **تاریخچه ترافیک** — آمار ترافیک روزانه با نگهداری ۳۰ روزه
- 📱 **رابط کاربری موبایل** — عالی روی گوشی و تبلت
- 🌓 **تم تاریک/روشن** — تشخیص خودکار تنظیمات سیستم شما
- 📷 **QR کد سمت کاربر** — تولید محلی، بدون فراخوانی API خارجی
- 📤 **خروجی کانفیگ‌ها** — دانلود همه لینک‌ها به صورت JSON
- 🔒 **هدرهای امنیتی** — X-Frame-Options و CSP و موارد دیگر
- 🌐 **دوزبانه** — رابط فارسی و انگلیسی

### 📖 روش استفاده

۱. آدرس Space خود را باز کنید (مثلاً `https://yourusername-luffy-panel.hf.space`)
۲. داشبورد لینک‌های پروکسی فعال شما را نشان می‌دهد
۳. **افزودن لینک جدید**: تنظیمات اینباند (پورت، UUID، مسیر WS، سرور آپ‌استریم) را وارد کنید
۴. لینک VLESS تولید شده را کپی کنید یا QR کد را اسکن کنید
۵. در کلاینت V2Ray خود (v2rayNG، Nekoray و غیره) ایمپورت کنید
۶. ترافیک را مانیتور کنید و لینک‌ها را از داشبورد مدیریت کنید

### ⚠️ نکات مهم

- **Hugging Face Spaces بعد از عدم فعالیت می‌خوابد.** پنل با درخواست بعدی بیدار می‌شود — ممکن است تأخیر شروع سرد وجود داشته باشد.
- **حافظه پایدار** (`/data`) با ری‌استارت از بین نمی‌رود اما محدودیت دارد (مستندات HF را ببینید).
- **محدودیت نرخ** به‌طور پیش‌فرض فعال است.
- این پنل یک **مدیریت پروکسی** است، نه خود پروکسی. شما همچنان به یک سرور آپ‌استریم نیاز دارید.
- برای استفاده تولیدی، روی یک سرور اختصاصی نصب کنید.

### 🔧 متغیرهای محیطی

| متغیر | پیش‌فرض | توضیح |
|-------|---------|-------|
| `PORT` | `7860` | پورت سرور (پیش‌فرض HF Spaces) |

</div>

---

## 📄 License

MIT — Use freely, modify, share.

## ⭐ Support

If this project helps you, give it a star ⭐ on Hugging Face!
