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
# YOUR STOCK LIST (CLEAN)
# -------------------------
def fetch_stocks():
    return [
        "TB5Y0430","TB2Y0227","LRBDL","AMBEEPHA","SALVO","JAMUNAOIL",
        "FBFIF","TB2Y0826","AMCL(PRAN)","ITC","RINGSHINE","ACTIVEFINE",
        "ECABLES","EPGL","TB10Y0535","TB2Y1126","APEXFOODS","ICB",
        "AZIZPIPES","GLOBALINS","TB5Y1029","ALLTEX","AIBL1STIMF",
        "AFTABAUTO","SBACBANK","PF1STMF","PREMIERLEA","SSSTEEL",
        "ADVENT","KPPL","RAHIMAFOOD","CENTRALPHL","TB15Y0340",
        "TB10Y1135","EASTERNINS","ARGONDENIM","PARAMOUNT",
        "BEACONPHAR","ACI","TB2Y1026","TB20Y1242","HFL","YPL",
        "MLDYEING","BDTHAIFOOD","MITHUNKNIT","1JANATAMF","TAMIJTEX",
        "TB2Y0727","DAFODILCOM","PENINSULA","GHAIL","GPHISPAT",
        "ATLASBANG","DESCO","KTL","SEMLIBBLSF","PRIMETEX",
        "USMANIAGL","BENGALWTL","TB10Y0234","SEAPEARL","DGIC",
        "RENWICKJA","PHOENIXFIN","MHSML","FIRSTFIN","AOL",
        "TB10Y0434","CAPITECGBF","BAYLEASING","SKTRIMS","GP",
        "MBL1STMF","CITYBANK","PHARMAID","NCCBLMF1","CROWNCEMNT",
        "CAPMBDBLMF","RUPALIBANK","ANWARGALV","RUPALILIFE",
        "SAMATALETH","ICBAMCL2ND","NRBCBANK","APOLOISPAT",
        "MIRAKHTER","SAPORTL","UNIONCAP","MATINSPINN",
        "SOUTHEASTB","AAMRATECH","ESQUIRENIT","CRYSTALINS",
        "JUTESPINN","ICBSONALI1","SONARGAON","MIDASFIN",
        "REPUBLIC","EBL1STMF"
    ]

# -------------------------
# DB
# -------------------------
print("Connecting DB...", flush=True)

conn = psycopg2.connect(db_url)
cursor = conn.cursor()

print("DB connected ✅", flush=True)
cursor.execute("SELECT 1;")

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
        print("DB ERROR:", stock, e, flush=True)

# -------------------------
# FETCH PRICE
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
        res = []
        if not table:
            return res

        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                p = cols[0].get_text(strip=True)
                v = cols[1].get_text(strip=True)

                if p.replace(".", "", 1).isdigit():
                    res.append({
                        "price": float(p),
                        "volume": int(v) if v.isdigit() else 0
                    })
        return res

    buy = extract(tables[2]) if len(tables) > 2 else []
    sell = extract(tables[3]) if len(tables) > 3 else []

    text = soup.get_text()
    match = re.search(r"Last Trade Price\s*:\s*(\d+\.?\d*)", text)
    last_price = float(match.group(1)) if match else None

    volume = sum([b["volume"] for b in buy] + [s["volume"] for s in sell])

    return {"last_price": last_price, "volume": volume}

# -------------------------
# SAVE
# -------------------------
def save(stock, data):
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

    print("Total stocks:", len(stocks), flush=True)

    success = 0
    failed = 0

    for i, stock in enumerate(stocks):
        try:
            print(f"[{i+1}/{len(stocks)}] {stock}", flush=True)

            html = fetch_instrument(stock)
            data = parse_order_book(html)

            save(stock, data)

            conn.commit()

            print("Saved:", stock, flush=True)
            success += 1

        except Exception as e:
            conn.rollback()
            print("ERROR:", stock, e, flush=True)
            failed += 1

    cursor.close()
    conn.close()

    print("DONE ✔")
    print("Success:", success, "Failed:", failed)
