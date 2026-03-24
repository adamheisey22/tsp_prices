#!/usr/bin/env python3
"""
TSP Share Price History - Initial Full Download
================================================
Downloads ALL historical share price data from tsp.gov and stores it
in a SQLite database.

Data source: https://www.tsp.gov/data/fund-price-history.csv
Fallback:    Scrapes from the share-price-history page via its internal API.

Usage:
    python tsp_initial_load.py [--db tsp_fund_prices.db]
"""

import argparse
import csv
import io
import json
import logging
import sqlite3
import sys
from datetime import datetime, date
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CSV_URL = "https://www.tsp.gov/data/fund-price-history.csv"

# The TSP website uses this internal endpoint (dates in YYYYMMDD format).
# Lfunds=1 → include Lifecycle funds, InvFunds=1 → include individual funds
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

# All known TSP fund columns (order may vary in the CSV)
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
# Database helpers
# ---------------------------------------------------------------------------
def init_db(db_path: str) -> sqlite3.Connection:
    """Create the database and table if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS share_prices (
            date          TEXT    NOT NULL,
            fund          TEXT    NOT NULL,
            price         REAL,
            PRIMARY KEY (date, fund)
        )
    """)
    # Index for quick lookups by date or fund
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_share_prices_date
        ON share_prices(date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_share_prices_fund
        ON share_prices(fund)
    """)

    # Optional: a wide-format view for convenience
    fund_cols = ",\n            ".join(
        f"MAX(CASE WHEN fund = '{f}' THEN price END) AS [{f}]"
        for f in ALL_FUNDS
    )
    conn.execute(f"""
        CREATE VIEW IF NOT EXISTS share_prices_wide AS
        SELECT
            date,
            {fund_cols}
        FROM share_prices
        GROUP BY date
        ORDER BY date
    """)

    conn.commit()
    return conn


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """
    Insert rows into the database.  Each row is a dict with keys:
      date (str YYYY-MM-DD), fund (str), price (float or None).
    Uses INSERT OR IGNORE so re-runs are safe.
    Returns the number of new rows inserted.
    """
    before = conn.execute("SELECT COUNT(*) FROM share_prices").fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO share_prices (date, fund, price) VALUES (?, ?, ?)",
        [(r["date"], r["fund"], r["price"]) for r in rows],
    )
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM share_prices").fetchone()[0]
    return after - before


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------
def fetch_url(url: str) -> str:
    """Fetch a URL and return its text content."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=120) as resp:
        raw = resp.read()
        # Try UTF-8 first, fall back to latin-1
        try:
            return raw.decode("utf-8-sig")  # handles BOM
        except UnicodeDecodeError:
            return raw.decode("latin-1")


def parse_csv_text(csv_text: str) -> list[dict]:
    """
    Parse TSP CSV text into a list of normalised row dicts.
    Expected CSV format (header row + data rows):
        Date, L Income, L 2025, ..., G Fund, F Fund, C Fund, S Fund, I Fund
        2024-01-02, 23.4567, ..., 17.9876, ...
    Handles both the full-history CSV and the date-range API CSV.
    """
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))

    # Normalise header names (strip whitespace)
    if reader.fieldnames:
        reader.fieldnames = [f.strip() for f in reader.fieldnames]

    for raw_row in reader:
        # Find the date column (could be "Date" or "date")
        date_str = None
        for key in ("Date", "date", "DATE"):
            if key in raw_row:
                date_str = raw_row[key].strip()
                break
        if not date_str:
            continue

        # Normalise date to YYYY-MM-DD
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                date_obj = datetime.strptime(date_str, fmt)
                date_str = date_obj.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            log.warning("Skipping row with unparseable date: %s", date_str)
            continue

        # Extract each fund price
        for fund in ALL_FUNDS:
            # Try exact match and common variations
            price_str = None
            for candidate_key in (fund, fund.strip(), fund.replace(" ", "  ")):
                if candidate_key in raw_row:
                    price_str = raw_row[candidate_key]
                    break

            if price_str is None:
                # Try fuzzy match on header
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

            # Only store rows where the fund actually has a price
            if price is not None:
                rows.append({"date": date_str, "fund": fund, "price": price})

    return rows


def download_full_csv() -> str:
    """Download the full history CSV from tsp.gov."""
    log.info("Downloading full CSV from %s", CSV_URL)
    return fetch_url(CSV_URL)


def download_by_date_range(start: str, end: str) -> str:
    """
    Download share prices for a date range using the TSP internal API.
    Dates should be in YYYYMMDD format.
    """
    url = API_URL_TEMPLATE.format(start=start, end=end)
    log.info("Downloading date range %s → %s", start, end)
    return fetch_url(url)


def download_all_data_chunked() -> list[dict]:
    """
    Download all historical data in yearly chunks (as a fallback if
    the single CSV download fails).  Data starts from June 2003.
    """
    all_rows = []
    start_year = 2003
    current_year = date.today().year

    for year in range(start_year, current_year + 1):
        start = f"{year}0101"
        end = f"{year}1231"
        try:
            csv_text = download_by_date_range(start, end)
            rows = parse_csv_text(csv_text)
            log.info("  %d: parsed %d fund-price records", year, len(rows))
            all_rows.extend(rows)
        except (URLError, HTTPError) as e:
            log.warning("  %d: download failed (%s), skipping", year, e)

    return all_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Download all TSP share price history into a SQLite database."
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"Path to SQLite database file (default: {DEFAULT_DB})",
    )
    args = parser.parse_args()

    log.info("Initialising database: %s", args.db)
    conn = init_db(args.db)

    # --- Strategy 1: Try the full CSV download first ---
    rows = []
    try:
        csv_text = download_full_csv()
        rows = parse_csv_text(csv_text)
        log.info("Parsed %d fund-price records from full CSV", len(rows))
    except Exception as e:
        log.warning("Full CSV download failed: %s", e)

    # --- Strategy 2: Fall back to year-by-year chunked download ---
    if not rows:
        log.info("Falling back to year-by-year download …")
        rows = download_all_data_chunked()

    if not rows:
        log.error("No data could be downloaded. Check your network / the TSP website.")
        sys.exit(1)

    # --- Insert into database ---
    new_count = insert_rows(conn, rows)
    total = conn.execute("SELECT COUNT(*) FROM share_prices").fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(date), MAX(date) FROM share_prices"
    ).fetchone()

    log.info("Inserted %d new records (%d total)", new_count, total)
    log.info("Date range: %s → %s", date_range[0], date_range[1])

    # Quick summary
    fund_counts = conn.execute(
        "SELECT fund, COUNT(*) AS n FROM share_prices GROUP BY fund ORDER BY fund"
    ).fetchall()
    log.info("Records per fund:")
    for fund, n in fund_counts:
        log.info("  %-12s %d days", fund, n)

    conn.close()
    log.info("Done!  Database saved to %s", args.db)


if __name__ == "__main__":
    main()
