# داشبورد پایش عملکرد SQL Server

این پروژه یک سامانه پایش و تحلیل عملکرد SQL Server است که با استفاده از
Apache Airflow، PostgreSQL و Grafana طراحی شده است.

هدف پروژه، جمع‌آوری دوره‌ای شاخص‌های عملکرد SQL Server، ذخیره تاریخی داده‌ها
و نمایش و تحلیل آن‌ها در یک داشبورد تعاملی است.

---

## معماری پروژه

SQL Server 2022
↓
Apache Airflow
↓
PostgreSQL (monitoring_db)
↓
Grafana 9.5.2
↓
داشبورد پایش عملکرد

### نقش اجزا

- **SQL Server:** منبع اصلی داده‌های مانیتورینگ و DMVها
- **Apache Airflow:** زمان‌بندی و اجرای خودکار جمع‌آوری داده‌ها
- **PostgreSQL:** ذخیره تاریخی داده‌های مانیتورینگ
- **Grafana:** نمایش و تحلیل گرافیکی داده‌ها

---

## قابلیت‌های پروژه

### نمای کلی سرور

- نشست‌های کاربری
- درخواست‌های در حال اجرا
- درخواست‌های مسدودشده
- حافظه مصرفی SQL Server
- حافظه در دسترس سیستم
- روند فعالیت SQL Server
- روند مصرف حافظه

### پایش پایگاه داده

- وضعیت Online
- حجم Data
- حجم Log
- تعداد اتصال‌های کاربری
- انتخاب پویا پایگاه داده در Grafana

### پایش ورودی/خروجی

- عملیات خواندن و نوشتن
- حجم خواندن و نوشتن
- میانگین تأخیر خواندن
- میانگین تأخیر نوشتن
- نمایش روند I/O در طول زمان

### پایش ایندکس‌ها

- Index Fragmentation
- Index Usage
- ایندکس‌های بدون خواندن ثبت‌شده
- Missing Index Suggestions

### تحلیل Wait Statistics

انتظارهای SQL Server جمع‌آوری شده و برای تحلیل گلوگاه‌های احتمالی در گروه‌هایی مانند زیر دسته‌بندی می‌شوند:

- I/O
- CPU / Scheduler
- Locking
- Memory
- Parallelism
- Network / Client

---

## DAGهای Airflow

| DAG | وظیفه | زمان‌بندی |
|---|---|---|
| `sqlserver_server_metrics` | شاخص‌های سطح سرور | هر 5 دقیقه |
| `sqlserver_database_metrics` | شاخص‌های سطح پایگاه داده | هر 15 دقیقه |
| `sqlserver_index_usage` | استفاده از ایندکس‌ها | هر ساعت |
| `sqlserver_missing_indexes` | پیشنهادهای Missing Index | هر ساعت |
| `sqlserver_index_fragmentation` | پراکندگی ایندکس‌ها | روزانه |
| `sqlserver_wait_stats` | Wait Statistics | هر 5 دقیقه |

---

## ساختار پروژه

```text
sqlserver-monitoring-platform/
├── airflow/
│   └── dags/
├── grafana/
│   ├── dashboard/
│   └── provisioning/
├── sql/
├── requirements.txt
├── .gitignore
└── README.md
