from flask import Flask, jsonify
from flask_cors import CORS
from psycopg2 import pool
from dotenv import load_dotenv
import os

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

# -------------------------
# CLEAN SHUTDOWN 
# -------------------------
@app.teardown_appcontext
def close_pool(exception=None):
    try:
        db_pool.closeall()
    except:
        pass

# -------------------------
# RUN SERVER
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)