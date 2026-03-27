# SunVolt Daily Report

Automated daily email report for solar plant generation KPIs. Runs via GitHub Actions cron schedule.

## Schedule

- **7:00 AM COL** (12:00 UTC) daily
- Reports on the previous day's data

## Configuration

All credentials are stored as GitHub Secrets:

- `PG_DSN` — PostgreSQL connection string
- `SMTP_USER` — Gmail address
- `SMTP_PASSWORD` — Gmail App Password
- `REPORT_RECIPIENT` — Destination email
