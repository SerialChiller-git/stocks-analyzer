import time
import requests
from bs4 import BeautifulSoup
import re
import sqlite3
from datetime import datetime

# -------------------------
# CONFIG
# -------------------------
URL = "https://www.dsebd.org/ajax/load-instrument.php"
COOKIES = {'PHPSESSID': 'u1p5h9j3aufgebs88fpa5mf0ak'}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:150.0) Gecko/20100101 Firefox/150.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.dsebd.org/mkt_depth_3.php',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://www.dsebd.org',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-GPC': '1',
    'Priority': 'u=0',
}

DB_FILE = "dse_data.db"

# -------------------------
# DATABASE FUNCTIONS
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_candles (
        stock TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (stock, date)
    )
    """)
    conn.commit()
    conn.close()

def insert_daily(stock, date, open, high, low, close, volume):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO daily_candles
    (stock, date, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (stock, date, open, high, low, close, volume))
    conn.commit()
    conn.close()

# -------------------------
# HTTP REQUEST
# -------------------------
def fetch_instrument(inst: str) -> str:
    response = requests.post(URL, data={"inst": inst}, headers=HEADERS, cookies=COOKIES)
    response.raise_for_status()
    return response.text

def fetch_stocks():
    cookies = {
        'PHPSESSID': 'u1p5h9j3aufgebs88fpa5mf0ak',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:150.0) Gecko/20100101 Firefox/150.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        # 'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://www.dsebd.org/latest_share_price_scroll_l.php',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
        # 'Cookie': 'PHPSESSID=u1p5h9j3aufgebs88fpa5mf0ak',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-GPC': '1',
    }

    params = {
        'suggestType': 'tc',
    }

    response = requests.get('https://www.dsebd.org/ajax/suggestList.php', params=params, cookies=cookies, headers=headers)
    return response.json()

# -------------------------
# PARSE HTML
# -------------------------
def parse_order_book(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    def extract_table(table) -> list:
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

    # total traded volume (sum of buy + sell)
    total_volume = sum([b["volume"] for b in buy] + [s["volume"] for s in sell])

    return {"buy": buy, "sell": sell, "last_price": last_price, "volume": total_volume}

# -------------------------
# BUILD CANDLE
# -------------------------
def update_candles(candles: dict, price: float) -> dict:
    current_time = int(time.time())
    minute = current_time - (current_time % 60)

    if minute not in candles:
        candles[minute] = {
            "time": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price
        }
    else:
        c = candles[minute]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price

    return candles

# -------------------------
# MAIN FUNCTION
# -------------------------
def run(inst: str, candles: dict):
    html = fetch_instrument(inst)
    data = parse_order_book(html)
    candles = update_candles(candles, data["last_price"])
    data["candles"] = candles
    return data

# -------------------------
# DAILY OHLC INSERT
# -------------------------
def save_daily_ohlc(inst: str, candles: dict):
    if not candles:
        return

    # Get latest candle of the day
    last_minute = max(candles.keys())
    c = candles[last_minute]

    # Skip if no valid price
    if c["close"] is None or c["close"] == 0:
        return  # don't store this candle


    today = datetime.now().date()
    insert_daily(
        stock=inst,
        date=today.isoformat(),
        open=c["open"],
        high=c["high"],
        low=c["low"],
        close=c["close"],
        volume=0  # optional: you can sum buy/sell volumes here if needed
    )

# -------------------------
# SCRIPT ENTRY POINT
# -------------------------
if __name__ == "__main__":
    init_db()
    stocks = fetch_stocks()
    for stock in stocks:
        try:
            result = run(stock, {})
            save_daily_ohlc(stock, result["candles"])
            print(f"Saved {stock}")
        except Exception as e:
            print(f"Error {stock}: {e}")



    