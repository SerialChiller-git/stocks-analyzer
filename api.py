from collections import defaultdict
from flask import Flask, jsonify
from flask_cors import CORS
from psycopg2 import pool
from dotenv import load_dotenv
import os
from datetime import datetime
import pytz

# -------------------------
# INIT APP
# -------------------------
app = Flask(__name__)
CORS(app)

# -------------------------
# LOAD ENV
# -------------------------
load_dotenv()
DB_URL = os.getenv("DB_URL")

# -------------------------
# CONNECTION POOL
# -------------------------
db_pool = pool.SimpleConnectionPool(
    1,   # min connections
    10,  # max connections
    DB_URL
)

# -------------------------
# QUERY FUNCTION
# -------------------------
def query_daily(stock):
    conn = db_pool.getconn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT date, open, high, low, close, volume
            FROM daily_candles
            WHERE stock = %s
            ORDER BY date ASC
        """, (stock,))

        rows = cursor.fetchall()

        return [
            {
                "date": r[0],
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5]
            }
            for r in rows
        ]

    finally:
        cursor.close()
        db_pool.putconn(conn)   



bd_tz = pytz.timezone("Asia/Dhaka")

def to_weekly(candles):
    weekly = defaultdict(list)

    for c in candles:
        dt = datetime.fromisoformat(str(c["date"]))

        # force BD time
        if dt.tzinfo is None:
            dt = bd_tz.localize(dt)
        else:
            dt = dt.astimezone(bd_tz)

        year, week, _ = dt.isocalendar()
        weekly[(year, week)].append(c)

    result = []

    for (_, _), days in weekly.items():
        days.sort(key=lambda x: x["date"])

        result.append({
            "date": days[0]["date"],
            "open": days[0]["open"],
            "high": max(d["high"] for d in days),
            "low": min(d["low"] for d in days),
            "close": days[-1]["close"],
            "volume": sum(d["volume"] for d in days),
        })

    result.sort(key=lambda x: x["date"])
    return result
# -------------------------
# API ROUTE
# -------------------------
@app.route("/api/<stock>")
def get_stock(stock):
    try:
        data = query_daily(stock)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/<stock>/weekly")
def get_stock_weekly(stock):
    try:
        data = query_daily(stock)
        weekly = to_weekly(data)
        return jsonify(weekly)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# RUN SERVER
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)