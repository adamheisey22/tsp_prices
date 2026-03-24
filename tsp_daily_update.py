#!/usr/bin/env python3
"""
TSP Share Price History - Daily Update / Append
================================================
Checks the existing SQLite database for the most recent date, then
downloads only the new data since that date and appends it.

Designed to run once per day (e.g. via cron, Windows Task Scheduler,
systemd timer, or a cloud scheduler like GitHub Actions).

Usage:
    python tsp_daily_update.py [--db tsp_fund_prices.db]

Cron example (run every weekday at 7 PM Eastern):
    0 19 * * 1-5  cd /path/to/project && python tsp_daily_update.py

systemd timer example:
    See the accompanying tsp-daily-update.service / .timer files.
"""

import argparse
import csv
import io
import json
import logging
import sqlite3
import sys
from datetime import datetime, date, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Configuration  (mirrors tsp_initial_load.py)
# ---------------------------------------------------------------------------
CSV_URL = "https://www.tsp.gov/data/fund-price-history.csv"

API_URL_TEMPLATE = (
    "https://www.tsp.gov/data/getSharePrices"
    "_startdate_{start}"
    "_enddate_{end}"
    "_Lfunds_1"
    "_InvFunds_1"
    "_download_1.csv"
)

DEFAULT_DB = "tsp_fund_prices.db"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

INDIVIDUAL_FUNDS = ["G Fund", "F Fund", "C Fund", "S Fund", "I Fund"]
LIFECYCLE_FUNDS = [
    "L Income", "L 2025", "L 2030", "L 2035", "L 2040",
    "L 2045", "L 2050", "L 2055", "L 2060", "L 2065",
    "L 2070", "L 2075",
]
ALL_FUNDS = INDIVIDUAL_FUNDS + LIFECYCLE_FUNDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers (identical to tsp_initial_load.py — you can also extract
# these into a shared module like tsp_utils.py if you prefer)
# ---------------------------------------------------------------------------
def fetch_url(url: str) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=120) as resp:
        raw = resp.read()
        try:
            return raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            return raw.decode("latin-1")


def parse_csv_text(csv_text: str) -> list[dict]:
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames:
        reader.fieldnames = [f.strip() for f in reader.fieldnames]

    for raw_row in reader:
        date_str = None
        for key in ("Date", "date", "DATE"):
            if key in raw_row:
                date_str = raw_row[key].strip()
                break
        if not date_str:
            continue

        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                date_obj = datetime.strptime(date_str, fmt)
                date_str = date_obj.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            continue

        for fund in ALL_FUNDS:
            price_str = None
            for candidate_key in (fund, fund.strip()):
                if candidate_key in raw_row:
                    price_str = raw_row[candidate_key]
                    break
            if price_str is None:
                for k in raw_row:
                    if k.strip().lower() == fund.lower():
                        price_str = raw_row[k]
                        break
            if price_str is not None:
                price_str = price_str.strip().replace("$", "").replace(",", "")
                try:
                    price = float(price_str) if price_str and price_str != "-" else None
                except ValueError:
                    price = None
            else:
                price = None

            if price is not None:
                rows.append({"date": date_str, "fund": fund, "price": price})

    return rows


def upsert_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """
    Insert or update rows.  Uses INSERT OR REPLACE so that if TSP
    retroactively corrects a price, we pick up the correction.
    Returns the count of rows affected.
    """
    before = conn.execute("SELECT COUNT(*) FROM share_prices").fetchone()[0]
    conn.executemany(
        """INSERT OR REPLACE INTO share_prices (date, fund, price)
           VALUES (?, ?, ?)""",
        [(r["date"], r["fund"], r["price"]) for r in rows],
    )
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM share_prices").fetchone()[0]
    return after - before


# ---------------------------------------------------------------------------
# Update logic
# ---------------------------------------------------------------------------
def get_latest_date(conn: sqlite3.Connection) -> str | None:
    """Return the most recent date in the database, or None if empty."""
    row = conn.execute("SELECT MAX(date) FROM share_prices").fetchone()
    return row[0] if row and row[0] else None


def download_recent_data(start_date: str) -> list[dict]:
    """
    Download data from start_date to today.
    Tries the date-range API first, then falls back to the full CSV
    (filtering client-side).
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.today()
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    # Strategy 1: date-range API
    try:
        url = API_URL_TEMPLATE.format(start=start_str, end=end_str)
        log.info("Trying date-range API: %s → %s", start_date, end_dt.strftime("%Y-%m-%d"))
        csv_text = fetch_url(url)
        rows = parse_csv_text(csv_text)
        if rows:
            log.info("Got %d records from date-range API", len(rows))
            return rows
    except Exception as e:
        log.warning("Date-range API failed: %s", e)

    # Strategy 2: full CSV, filter to recent dates only
    try:
        log.info("Falling back to full CSV download …")
        csv_text = fetch_url(CSV_URL)
        all_rows = parse_csv_text(csv_text)
        rows = [r for r in all_rows if r["date"] >= start_date]
        log.info("Filtered %d recent records from full CSV (%d total)", len(rows), len(all_rows))
        return rows
    except Exception as e:
        log.error("Full CSV download also failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Append the latest TSP share prices to the SQLite database."
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"Path to SQLite database file (default: {DEFAULT_DB})",
    )
    args = parser.parse_args()

    # --- Open database ---
    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        log.error("Cannot open database %s: %s", args.db, e)
        log.error("Run tsp_initial_load.py first to create the database.")
        sys.exit(1)

    # Check that the table exists
    table_check = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='share_prices'"
    ).fetchone()
    if not table_check:
        log.error("Table 'share_prices' not found.  Run tsp_initial_load.py first.")
        sys.exit(1)

    # --- Determine the date range to fetch ---
    latest = get_latest_date(conn)
    if latest is None:
        log.error("Database is empty.  Run tsp_initial_load.py first.")
        sys.exit(1)

    log.info("Most recent date in DB: %s", latest)

    # Start from the day after the latest record
    start_date = (
        datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    today = date.today().strftime("%Y-%m-%d")
    if start_date > today:
        log.info("Database is already up to date (latest: %s). Nothing to do.", latest)
        conn.close()
        return

    log.info("Fetching data from %s to %s …", start_date, today)
    rows = download_recent_data(start_date)

    if not rows:
        log.info("No new data available (markets may be closed today).")
        conn.close()
        return

    # Filter out any rows older than our start_date (safety net)
    rows = [r for r in rows if r["date"] >= start_date]

    # --- Upsert into database ---
    new_count = upsert_rows(conn, rows)
    new_latest = get_latest_date(conn)

    new_dates = sorted(set(r["date"] for r in rows))
    log.info("Upserted %d new records", new_count)
    log.info("New trading days added: %s", ", ".join(new_dates) if new_dates else "none")
    log.info("Latest date in DB is now: %s", new_latest)

    conn.close()
    log.info("Daily update complete.")


if __name__ == "__main__":
    main()
