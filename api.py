from flask import Flask, jsonify
import sqlite3
from flask_cors import CORS

app = Flask(__name__)
CORS(app) 

DB_FILE = "dse_data.db"

def query_daily(stock):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT date, open, high, low, close, volume FROM daily_candles WHERE stock=?", (stock,))
    rows = cursor.fetchall()
    conn.close()
    return [{"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in rows]

@app.route("/api/<stock>")
def get_stock(stock):
    data = query_daily(stock)
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)