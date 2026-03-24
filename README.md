# TSP Share Price History → SQLite

Download and maintain a local SQLite database of all Thrift Savings Plan (TSP) fund share prices from [tsp.gov](https://www.tsp.gov/share-price-history/).

## Files

| File | Purpose |
|---|---|
| `tsp_initial_load.py` | One-time full download of ALL historical data (June 2003 → today) |
| `tsp_daily_update.py` | Incremental daily update — fetches only new data since the last run |

## Quick Start

```bash
# No external dependencies — uses only the Python standard library (3.10+)

# Step 1: Download all historical data
python tsp_initial_load.py

# Step 2: Run daily to keep it updated
python tsp_daily_update.py
```

Both scripts accept `--db <path>` to specify the database file (default: `tsp_fund_prices.db`).

## Database Schema

### Table: `share_prices`

| Column | Type | Description |
|---|---|---|
| `date` | TEXT | Trading date (YYYY-MM-DD) — part of primary key |
| `fund` | TEXT | Fund name (e.g. "C Fund", "L 2050") — part of primary key |
| `price` | REAL | Share price in USD |

**Primary key:** `(date, fund)`

### View: `share_prices_wide`

A pivot view with one row per date and one column per fund — convenient for analysis:

```
date        | G Fund | F Fund | C Fund | S Fund | I Fund | L Income | L 2030 | ...
2024-01-02  | 17.98  | 19.12  | 82.45  | 78.33  | 41.22  | 24.56    | 44.12  | ...
```

## Data Sources

The scripts try two download strategies in order:

1. **Full CSV** — `https://www.tsp.gov/data/fund-price-history.csv`
2. **Date-range API** — `https://www.tsp.gov/data/getSharePrices_startdate_YYYYMMDD_enddate_YYYYMMDD_Lfunds_1_InvFunds_1_download_1.csv`

If one fails, the other is used as a fallback.

## Funds Tracked

**Individual funds:** G Fund, F Fund, C Fund, S Fund, I Fund

**Lifecycle funds:** L Income, L 2025, L 2030, L 2035, L 2040, L 2045, L 2050, L 2055, L 2060, L 2065, L 2070, L 2075

## Scheduling Daily Updates

### Linux / macOS (cron)

```bash
# Run weekdays at 7:00 PM Eastern (share prices are posted after market close)
0 19 * * 1-5  cd /path/to/project && /usr/bin/python3 tsp_daily_update.py >> tsp_update.log 2>&1
```

### macOS (launchd)

Save as `~/Library/LaunchAgents/com.tsp.daily-update.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tsp.daily-update</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/tsp_daily_update.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key><integer>19</integer>
        <key>Minute</key><integer>0</integer>
    </dict>
</dict>
</plist>
```

### Windows (Task Scheduler)

```powershell
schtasks /create /tn "TSP Daily Update" /tr "python C:\path\to\tsp_daily_update.py" /sc weekly /d MON,TUE,WED,THU,FRI /st 19:00
```

### GitHub Actions (free for public repos)

```yaml
# .github/workflows/tsp-update.yml
name: TSP Daily Update
on:
  schedule:
    - cron: '0 23 * * 1-5'  # 11 PM UTC ≈ 7 PM ET
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: python tsp_daily_update.py
      - uses: actions/upload-artifact@v4
        with:
          name: tsp-database
          path: tsp_fund_prices.db
```

## Example Queries

```sql
-- Latest price for each fund
SELECT fund, date, price
FROM share_prices
WHERE date = (SELECT MAX(date) FROM share_prices)
ORDER BY fund;

-- C Fund price history for 2024
SELECT date, price
FROM share_prices
WHERE fund = 'C Fund' AND date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY date;

-- Daily returns for C Fund (percent change)
SELECT
    curr.date,
    curr.price,
    ROUND((curr.price - prev.price) / prev.price * 100, 4) AS daily_return_pct
FROM share_prices curr
JOIN share_prices prev
    ON prev.fund = curr.fund
    AND prev.date = (
        SELECT MAX(date) FROM share_prices
        WHERE fund = curr.fund AND date < curr.date
    )
WHERE curr.fund = 'C Fund'
ORDER BY curr.date DESC
LIMIT 20;

-- Year-to-date performance for all individual funds
SELECT
    sp.fund,
    first_price.price AS jan1_price,
    sp.price AS latest_price,
    ROUND((sp.price - first_price.price) / first_price.price * 100, 2) AS ytd_pct
FROM share_prices sp
JOIN (
    SELECT fund, price
    FROM share_prices
    WHERE date = (
        SELECT MIN(date) FROM share_prices
        WHERE date >= strftime('%Y', 'now') || '-01-01'
    )
) first_price ON first_price.fund = sp.fund
WHERE sp.date = (SELECT MAX(date) FROM share_prices)
    AND sp.fund IN ('G Fund', 'F Fund', 'C Fund', 'S Fund', 'I Fund')
ORDER BY ytd_pct DESC;

-- Use the wide-format view
SELECT * FROM share_prices_wide
WHERE date >= '2024-01-01'
ORDER BY date DESC
LIMIT 10;
```

## Notes

- **No external dependencies** — both scripts use only the Python 3.10+ standard library.
- The initial load uses `INSERT OR IGNORE` (safe to re-run).
- The daily update uses `INSERT OR REPLACE` to pick up any retroactive price corrections.
- TSP share prices are typically published on business days after 7 PM Eastern.
- Data goes back to **June 2, 2003** (the earliest date in TSP's system).
- Lifecycle funds that have reached maturity and rolled into L Income (e.g., L 2025) will stop receiving new prices but historical data is preserved.
