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
"RUNNERAUTO","MHSML","KDSALTD","INDEXAGRO","ESQUIRENIT","BDLAMPS","SIPLC","DSSL","ALLTEX","ABB1STMF",
"RENWICKJA","RAKCERAMIC","SOUTHEASTB","AAMRANET","TB20Y0744","PURABIGEN","JMISMDL","BANKASIA","CAPMBDBLMF","HAMI",
"APEXFOOT","IBBLPBOND","MALEKSPIN","NFML","BPPL","BAYLEASING","OAL","NAHEEACP","ICBAGRANI1","GEMINISEA",
"ASIAINS","SHARPIND","HRTEX","BDFINANCE","SIMTEX","POPULARLIF","MONNOFABR","CVOPRL","NPOLYMER","NRBBANK",
"ALARABANK","GRAMEENS2","APEXTANRY","SEAPEARL","RELIANCINS","HWAWELLTEX","RAHIMTEXT","NATLIFEINS","MEGCONMILK","JUTESPINN",
"PLFSL","TRUSTBANK","WALTONHIL","TB20Y0845","BATBC","MPETROLEUM","UNIQUEHRL","LANKABAFIN","APOLOISPAT","TALLUSPIN",
"GPHISPAT","UNILEVERCL","MONOSPOOL","MEGHNACEM","ACMEPL","SANDHANINS","MONNOCERA","PUBALIBANK","ZAHINTEX","DGIC",
"ICBAMCL2ND","ECABLES","PADMAOIL","GREENDELT","ARAMITCEM","SAIHAMTEX","TB10Y0726","ZEALBANGLA","STANDARINS","RAHIMAFOOD",
"SONARGAON","REPUBLIC","MIRAKHTER","BEXIMCO","ENVOYTEX","SAMATALETH","TECHNODRUG","TB2Y0627","MONNOAGML","EBL",
"APSCLBOND","SPCERAMICS","SHAHJABANK","QUASEMIND","ISLAMICFIN","NBL","PHOENIXFIN","PDL","CITYGENINS","SBACBANK",
"INTECH","FAREASTFIN","LHB","DELTALIFE","QUEENSOUTH","INTRACO","FBFIF","BNICL","1STPRIMFMF","PF1STMF",
"RUPALIINS","ORIONINFU","ICB3RDNRB","BATASHOE","GSPFINANCE","OLYMPIC","CAPITECGBF","PIONEERINS","PENINSULA","ARAMIT",
"STANDBANKL","BGIC","BESTHLDNG","ARGONDENIM","KAY&QUE","VAMLRBBF","APEXFOODS","SILCOPHL","FEDERALINS","STANCERAM",
"FASFIN","KBPPWBIL","FORTUNE","CENTRALINS","PROVATIINS","PRIMEBANK","RDFOOD","FAMILYTEX","MATINSPINN","HFL",
"SHASHADNIM","UNITEDFIN","NITOLINS","EBLNRBMF","AIL","ACIFORMULA","TOSRIFA","TAMIJTEX","NCCBANK","TB20Y0143",
"NCCBLMF1","NRBCBANK","TB10Y0434","ROBI","EBL1STMF","DESCO","AFCAGRO","SALVO","BDTHAIFOOD","MAKSONSPIN",
"GQBALLPEN","PEOPLESINS","DOMINAGE","PRIME1ICBA","ASIAPACINS","ICICL","DUTCHBANGL","DELTASPINN","BERGERPBL","IFIC",
"IFIC1STMF","VFSTDL","KPCL","UTTARABANK","MIDLANDBNK","ASIATICLAB","ANLIMAYARN","POPULAR1MF","MERCINS","ICB",
"ABBANK","AZIZPIPES","SONALIANSH","TUNGHAI","PRIMEFIN","GLDNJMF","BANGAS","UPGDCL","GP","BEXGSUKUK",
"METROSPIN","BSCPLC","SQURPHARMA","SSSTEEL","GENNEXT","UNIONCAP","RUPALILIFE","LIBRAINFU","DAFODILCOM","AMANFEED",
"KEYACOSMET","APEXSPINN","MBL1STMF","RINGSHINE","PARAMOUNT","ICBSONALI1","USMANIAGL","SLIPLC","SONARBAINS","SPCL",
"TB10Y1135","ACFL","KOHINOOR","FEKDIL","SHEPHERD","PHARMAID","BPML","PREMIERLEA","BEACONPHAR","GENEXIL",
"AGRANINS","NHFIL","GREENDELMF","KPPL","PRIMEINSUR","ISLAMIINS","IDLC","UNITEDINS","MEGHNAPET","CONTININS",
"MAGURAPLEX","YPL","MEGHNAINS","SQUARETEXT","FUWANGFOOD","PREMIERBAN","ACMELAB","SAIHAMCOT","IFADAUTOS","SAPORTL",
"SEMLIBBLSF","EMERALDOIL","NAVANACNG","BEACHHATCH","PRAGATIINS","GHAIL","TITASGAS","MEGHNALIFE","LOVELLO","SUNLIFEINS",
"REGENTTEX","HAKKANIPUL","EASTERNINS","IPDC","BSRMLTD","NTLTUBES","DULAMIACOT","BXPHARMA","TB10Y0535","HEIDELBCEM",
"EASTRNLUB","ICBIBANK","ZAHEENSPIN","ACTIVEFINE","GOLDENSON","AMCL(PRAN)","ADVENT","SKTRIMS","MIRACLEIND","NTC",
"CENTRALPHL","EXIM1STMF","ETL","PRIMELIFE","EPGL","SICL","SINOBANGLA","BSC","DACCADYE","MARICO",
"JAMUNABANK","AMBEEPHA","TB15Y0339","SHYAMPSUG","MIDASFIN","FAREASTLIF","SHURWID","JANATAINS","PRIMETEX","FUWANGCER",
"JHRML","PREMIERCEM","UNIONINS","KTL","SAIFPOWER","AOL","PADMALIFE","DSHGARME","ONEBANKPLC","SINGERBD",
"TB2Y0127","TB5Y1029","RECKITTBEN","BDTHAI","WMSHIPYARD","CNATEX","ADNTEL","LRBDL","CLICL","BDAUTOCA",
"RANFOUNDRY","BDCOM","BDWELDING","ATLASBANG","PROGRESLIF","NORTHRNINS","NORTHERN","AFTABAUTO","NAVANAPHAR","BBS",
"PTL","ALIF","UCB","MITHUNKNIT","DHAKAINS","CONFIDCEM","BARKAPOWER","ICBEPMF1S1","FIRSTFIN","EASTLAND",
"TAKAFULINS","ABBLPBOND","UTTARAFIN","PHPMF1","BBSCABLES","RUPALIBANK","GBBPOWER","TILIL","POWERGRID","SAFKOSPINN",
"EHL","1JANATAMF","STYLECRAFT","LINDEBD","CAPMIBBLMF","MLDYEING","SUMITPOWER","IBP","NURANI","RENATA",
"KARNAPHULI","JAMUNAOIL","IBNSINA","OIMEX","NEWLINE","DHAKABANK","SAMORITA","TRUSTB1MF","WATACHEM","CRYSTALINS",
"ITC","FARCHEM","AAMRATECH","DESHBANDHU","DBH1STMF","LRGLOBMF1","BRACBANK","SONALIPAPR","DOREENPWR","MJLBD",
"ANWARGALV","MERCANBANK","EGEN","LEGACYFOOT","CITYBANK","AL-HAJTEX","EIL","RELIANCE1","SEMLFBSLGF","GLOBALINS",
"MTB","PRAGATILIF","SILVAPHL","BENGALWTL","AIBL1STIMF","ISNLTD","IFILISLMF1","DBH","COPPERTECH","ACI",
"FINEFOODS","BSRMSTEEL","RSRMSTEEL","SALAMCRST","BIFC","ORIONPHARM","ILFSL","GHCL","AGNISYSL","ISLAMIBANK",
"CROWNCEMNT","PHENIXINS","TB5Y1228","TB5Y1229","TB10Y0234","TB10Y0735","TB15Y0340","TB20Y0545","TB2Y0826","TB2Y1026",
"TB2Y1126","TB5Y0430","TB5Y0529","TB5Y0630","TB5Y0928","PBLPBOND","AIBLPBOND","TB2Y0727","TB5Y0429","TB2Y0227",
"SJIBLPBOND","IBBL2PBOND","TB20Y1242","TB2Y0527","TB2Y0626","TB10Y0135","TB5Y0128","TB5Y1128","SONALILIFE","TB20Y0831",
"TB5Y1130","TB5Y0930","TB10Y0634","TB2Y1127","TB5Y0527"
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
