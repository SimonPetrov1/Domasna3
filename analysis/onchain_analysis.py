import sqlite3
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

COINGECKO_API = "https://api.coingecko.com/api/v3"
ETH_HASH_RATE = 120_000_000


class OnChainAnalysis:
    """
    Strategy: On-Chain + Sentiment Analysis
    """

    def __init__(self, coin_symbol="BTC"):
        self.coin_symbol = coin_symbol
        self.analyzer = SentimentIntensityAnalyzer()

    def analyze(self, return_results=False):
        conn = sqlite3.connect("users.db")
        cur = conn.cursor()

        # -------- Latest price & volume --------
        cur.execute(
            "SELECT close, volume FROM coins WHERE symbol=? ORDER BY time DESC LIMIT 1",
            (self.coin_symbol,)
        )
        row = cur.fetchone()
        conn.close()

        price, volume = row if row else (0, 0)
        market_cap = price * volume

        # -------- On-chain proxies --------
        whale_movements = self._whale_movements()
        exchange_flows = self._exchange_flows()
        active_addresses = self._active_addresses()
        transactions = self._transactions()
        tvl = self._tvl()

        # -------- Ratios --------
        nvt = market_cap / volume if volume else 0
        mvrv = market_cap / volume if volume else 0

        # -------- Sentiment --------
        sentiment = self._sentiment_score()

        results = {
            "price": price,
            "volume": volume,
            "market_cap": market_cap,
            "active_addresses": active_addresses,
            "transactions": transactions,
            "exchange_flows": exchange_flows,
            "whale_movements": whale_movements,
            "hash_rate": ETH_HASH_RATE,
            "tvl": tvl,
            "nvt": nvt,
            "mvrv": mvrv,
            "sentiment": sentiment,
        }

        if return_results:
            return results

    # ---------- helpers ----------

    def _whale_movements(self, threshold=1000):
        conn = sqlite3.connect("users.db")
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM coins WHERE symbol=? AND volume>=?",
            (self.coin_symbol, threshold)
        )
        count = cur.fetchone()[0]
        conn.close()
        return count

    def _exchange_flows(self):
        conn = sqlite3.connect("users.db")
        cur = conn.cursor()
        cur.execute(
            "SELECT SUM(volume) FROM coins WHERE symbol=?",
            (self.coin_symbol,)
        )
        val = cur.fetchone()[0]
        conn.close()
        return val or 0

    def _active_addresses(self):
        try:
            r = requests.get(
                f"{COINGECKO_API}/coins/{self.coin_symbol.lower()}",
                timeout=10
            ).json()
            return r.get("community_data", {}).get("twitter_followers", 0)
        except:
            return 0

    def _transactions(self):
        try:
            r = requests.get(
                f"{COINGECKO_API}/coins/markets",
                params={"vs_currency": "usd", "ids": self.coin_symbol.lower()},
                timeout=10
            ).json()
            return int(r[0].get("total_volume", 0)) if r else 0
        except:
            return 0

    def _tvl(self):
        try:
            r = requests.get(f"{COINGECKO_API}/defi/tvl", timeout=10).json()
            for d in r:
                if d["id"].lower() == self.coin_symbol.lower():
                    return float(d.get("tvl", 0))
        except:
            pass
        return 0

    def _sentiment_score(self):
        text = f"{self.coin_symbol} crypto market news"
        return self.analyzer.polarity_scores(text)["compound"]
