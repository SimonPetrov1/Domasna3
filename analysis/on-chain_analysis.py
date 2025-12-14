# on_chain_analysis_coingecko_optimized.py
import sqlite3
import csv
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DB_PATH = "users.db"
CSV_PATH = "../data/processed/all_coins.csv"
OUTPUT_CSV = "analysis_results_coingecko_optimized.csv"

analyzer = SentimentIntensityAnalyzer()
ETH_HASH_RATE = 120_000_000  # global ETH hash rate
COINGECKO_API = "https://api.coingecko.com/api/v3"

# ---------------- Database ----------------
def connect_db():
    return sqlite3.connect(DB_PATH)

def create_coins_table():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS coins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        time INTEGER NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        UNIQUE(symbol, time)
    )
    """)
    conn.commit()
    conn.close()
    print("coins table ready")

def insert_from_csv(csv_path):
    conn = connect_db()
    cur = conn.cursor()
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                rows.append((
                    row['symbol'],
                    int(row['time']),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['volume'])
                ))
            except:
                continue
    cur.executemany("""
        INSERT OR IGNORE INTO coins (symbol, time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    print(f"[OK] Inserted {len(rows)} records from CSV")

# ---------------- Metrics ----------------
def get_sentiment_score(text):
    return analyzer.polarity_scores(text)['compound']

def get_latest_price(symbol):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT close, volume FROM coins WHERE symbol=? ORDER BY time DESC LIMIT 1", (symbol,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row
    return 0, 0

def get_whale_movements(symbol, threshold=1000):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM coins WHERE symbol=? AND volume>=?", (symbol, threshold))
    count = cur.fetchone()[0]
    conn.close()
    return count

def get_exchange_flows(symbol):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT SUM(volume) FROM coins WHERE symbol=?", (symbol,))
    result = cur.fetchone()[0]
    conn.close()
    return result if result else 0

def get_active_addresses(symbol):
    try:
        r = requests.get(f"{COINGECKO_API}/coins/{symbol.lower()}", timeout=10).json()
        return r.get("community_data", {}).get("twitter_followers", 0)
    except:
        return 0

def get_transactions(symbol):
    try:
        r = requests.get(f"{COINGECKO_API}/coins/markets", params={
            "vs_currency": "usd",
            "ids": symbol.lower()
        }, timeout=10).json()
        if r and len(r) > 0:
            return int(r[0].get("total_volume", 0))
    except:
        return 0
    return 0

def get_tvl(symbol):
    try:
        r = requests.get(f"{COINGECKO_API}/defi/tvl", timeout=10).json()
        for d in r:
            if d["id"].lower() == symbol.lower():
                return float(d.get("tvl", 0))
    except:
        return 0
    return 0

def calculate_nvt(market_cap, volume):
    return market_cap / volume if volume else 0

def calculate_mvrv(market_cap, realized_cap):
    return market_cap / realized_cap if realized_cap else 0

# ---------------- Analyze coin ----------------
def analyze_coin(symbol):
    price, volume = get_latest_price(symbol)
    market_cap = price * volume
    whale = get_whale_movements(symbol)
    flow = get_exchange_flows(symbol)
    nvt = calculate_nvt(market_cap, volume)
    mvrv = calculate_mvrv(market_cap, volume)
    sentiment = get_sentiment_score(f"{symbol} market news today is good!")
    active_addresses = get_active_addresses(symbol)
    tx_count = get_transactions(symbol)
    hash_rate = ETH_HASH_RATE
    tvl = get_tvl(symbol)

    return {
        "symbol": symbol,
        "price": round(price, 4),
        "volume": round(volume, 4),
        "market_cap": round(market_cap, 4),
        "whale_movements": whale,
        "exchange_flow": round(flow, 4),
        "active_addresses": active_addresses,
        "tx_count": tx_count,
        "hash_rate": hash_rate,
        "tvl": tvl,
        "nvt": round(nvt, 4),
        "mvrv": round(mvrv, 4),
        "sentiment": round(sentiment, 4)
    }

# ---------------- Analyze all coins ----------------
def analyze_all_coins():
    start_time = time.time()
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM coins")
    coins = [row[0] for row in cur.fetchall()]
    conn.close()

    results = []
    # паралелно за побрзо
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analyze_coin, s) for s in coins]
        for future in as_completed(futures):
            results.append(future.result())

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["symbol", "price", "volume", "market_cap", "whale_movements",
                      "exchange_flow", "active_addresses", "tx_count", "hash_rate",
                      "tvl", "nvt", "mvrv", "sentiment"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    end_time = time.time()
    print(f"[OK] Analysis for {len(results)} coins saved to {OUTPUT_CSV}")
    print(f"[INFO] Execution time: {end_time - start_time:.2f} seconds")

# ---------------- Main ----------------
if __name__ == "__main__":
    create_coins_table()
    insert_from_csv(CSV_PATH)
    analyze_all_coins()
