import random
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv
import pytz
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("🚀 Script started", flush=True)

load_dotenv()
db_url = os.getenv("DB_URL")

session = requests.Session()
session.verify = False

HEADERS = {
    "User-Agent": f"Mozilla/5.0 ({random.randint(1,10000)})",
    "Accept": "*/*",
    "Referer": "https://www.dsebd.org/mkt_depth_3.php",
    "X-Requested-With": "XMLHttpRequest",
}

URL = "https://www.dsebd.org/ajax/load-instrument.php"

# -------------------------
# DB
# -------------------------
print("Connecting to DB...", flush=True)

conn = psycopg2.connect(db_url)
cursor = conn.cursor()

print("Connected to DB ✅", flush=True)
cursor.execute("SELECT 1;")
print("DB TEST OK ✅", flush=True)

# -------------------------
# INSERT
# -------------------------
def insert_daily(stock, date, open_price, high, low, close, volume):
    try:
        cursor.execute("""
        INSERT INTO daily_candles (stock, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock, date)
        DO UPDATE SET
            high = GREATEST(EXCLUDED.high, daily_candles.high),
            low = LEAST(EXCLUDED.low, daily_candles.low),
            close = EXCLUDED.close,
            volume = EXCLUDED.volume + daily_candles.volume
        """, (stock, date, open_price, high, low, close, volume))
    except Exception as e:
        conn.rollback()
        print("INSERT ERROR:", stock, e, flush=True)

# -------------------------
# REAL STOCK LIST (OPTION 2)
# -------------------------
def fetch_stocks():
    print("Fetching REAL stock list...", flush=True)

    try:
        r = requests.get(
            "https://www.dsebd.org/ajax/suggestList.php",
            params={"suggestType": "tc"},
            timeout=10,
            verify=False
        )

        data = r.json()

        stocks = []

        for item in data:
            if isinstance(item, dict):
                stocks.append(item.get("value") or item.get("label"))
            elif isinstance(item, str):
                stocks.append(item)

        stocks = list(set([s for s in stocks if s]))

        print(f"Total stocks fetched: {len(stocks)}", flush=True)

        if len(stocks) < 50:
            raise Exception("Too few stocks fetched, fallback triggered")

        return stocks

    except Exception as e:
        print("Stock fetch failed, using fallback:", e, flush=True)

        return [
            "GP", "BRACBANK", "BATBC", "SQURPHARMA",
            "UPGDCL", "ISLAMIBANK", "RENATA", "LHBL",
            "OLYMPIC", "WALTONHIL"
        ]

# -------------------------
# FETCH INSTRUMENT
# -------------------------
def fetch_instrument(inst):
    r = session.post(
        URL,
        data={"inst": inst},
        headers=HEADERS,
        timeout=10
    )
    return r.text

# -------------------------
# PARSE
# -------------------------
def parse_order_book(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    def extract(table):
        result = []
        if not table:
            return result

        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                p = cols[0].get_text(strip=True)
                v = cols[1].get_text(strip=True)

                if p.replace(".", "", 1).isdigit():
                    result.append({
                        "price": float(p),
                        "volume": int(v) if v.isdigit() else 0
                    })
        return result

    buy = extract(tables[2]) if len(tables) > 2 else []
    sell = extract(tables[3]) if len(tables) > 3 else []

    text = soup.get_text()
    match = re.search(r"Last Trade Price\s*:\s*(\d+\.?\d*)", text)
    last_price = float(match.group(1)) if match else None

    total_volume = sum([b["volume"] for b in buy] + [s["volume"] for s in sell])

    return {
        "last_price": last_price,
        "volume": total_volume
    }

# -------------------------
# SAVE
# -------------------------
def save_daily(stock, data):
    price = data["last_price"]
    if not price:
        return

    bd = pytz.timezone("Asia/Dhaka")
    today = datetime.now(bd).date().isoformat()

    insert_daily(stock, today, price, price, price, price, data["volume"])

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("Starting scraper...", flush=True)

    stocks = fetch_stocks()

    print("TOTAL STOCKS:", len(stocks), flush=True)

    success = 0
    failed = 0

    for i, stock in enumerate(stocks):
        try:
            print(f"[{i+1}/{len(stocks)}] {stock}", flush=True)

            html = fetch_instrument(stock)
            data = parse_order_book(html)

            save_daily(stock, data)

            conn.commit()

            print("Saved:", stock, flush=True)
            success += 1

        except Exception as e:
            conn.rollback()
            print("ERROR:", stock, e, flush=True)
            failed += 1

    cursor.close()
    conn.close()

    print("DONE ✔", flush=True)
    print("Success:", success, "Failed:", failed, flush=True)
