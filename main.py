import time
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv

# -------------------------
# CONFIG
# -------------------------
URL = "https://www.dsebd.org/ajax/load-instrument.php"

COOKIES = {'PHPSESSID': 'u1p5h9j3aufgebs88fpa5mf0ak'}

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': '*/*',
    'Referer': 'https://www.dsebd.org/mkt_depth_3.php',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}

# -------------------------
# DB CONNECTION
# -------------------------
load_dotenv()

print("Connecting to DB...", flush=True)
conn = psycopg2.connect(os.getenv("DB_URL"))
cursor = conn.cursor()
print("Connected to DB ✅", flush=True)

# -------------------------
# INSERT FUNCTION
# -------------------------
def insert_daily(stock, date, open_price, high, low, close, volume):
    cursor.execute("""
    INSERT INTO daily_candles (stock, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stock, date)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
    """, (stock, date, open_price, high, low, close, volume))

# -------------------------
# FETCH STOCK LIST
# -------------------------
def fetch_stocks():
    print("Fetching stock list...", flush=True)
    response = requests.get(
        'https://www.dsebd.org/ajax/suggestList.php',
        params={'suggestType': 'tc'},
        cookies=COOKIES,
        headers=HEADERS
    )
    stocks = response.json()
    print(f"Total stocks fetched: {len(stocks)}", flush=True)
    return stocks

# -------------------------
# FETCH SINGLE STOCK
# -------------------------
def fetch_instrument(inst: str) -> str:
    response = requests.post(URL, data={"inst": inst}, headers=HEADERS, cookies=COOKIES)
    response.raise_for_status()
    return response.text

# -------------------------
# PARSE HTML
# -------------------------
def parse_order_book(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    def extract_table(table):
        result = []
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                price, volume = cols[0].get_text(strip=True), cols[1].get_text(strip=True)
                if price.replace('.', '', 1).isdigit():
                    result.append({"price": float(price), "volume": int(volume)})
        return result

    buy = extract_table(tables[2])
    sell = extract_table(tables[3])

    text = soup.get_text()
    match = re.search(r"Last Trade Price\s*:\s*(\d+\.?\d*)", text)
    last_price = float(match.group(1)) if match else None

    total_volume = sum([b["volume"] for b in buy] + [s["volume"] for s in sell])

    return {"last_price": last_price, "volume": total_volume}

# -------------------------
# SAVE DAILY
# -------------------------
def save_daily(stock, data):
    price = data["last_price"]

    if price is None or price == 0:
        print(f"Skipping {stock} (no price)", flush=True)
        return

    today = datetime.now().date().isoformat()

    insert_daily(
        stock=stock,
        date=today,
        open_price=price,
        high=price,
        low=price,
        close=price,
        volume=data["volume"]
    )

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("Starting scraper...", flush=True)

    stocks = fetch_stocks()

    # 🔥 TEMP: limit for testing
    # stocks = stocks[:10]

    success = 0
    failed = 0

    for i, stock in enumerate(stocks):
        try:
            print(f"[{i+1}/{len(stocks)}] Processing {stock}", flush=True)

            html = fetch_instrument(stock)
            data = parse_order_book(html)
            save_daily(stock, data)

            success += 1

        except Exception as e:
            print(f"Error {stock}: {e}", flush=True)
            conn.rollback()  # VERY IMPORTANT
            failed += 1

    # commit once at end (better performance)
    conn.commit()

    print("Finished scraping!", flush=True)
    print(f"Success: {success}, Failed: {failed}", flush=True)