
import os
import asyncio
import threading
import time
import requests
import pytz

from datetime import datetime, timezone
from collections import deque
from flask import Flask, jsonify
from waitress import serve
from pysignalr.client import SignalRClient

# ==========================================
# BRIGHT HEART AI - RENDER CLOUD VERSION
# ==========================================

APP_NAME = "BRIGHT HEART AI"

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"]
TIMEFRAMES = [15, 30, 60]

EMA_FAST = 9
EMA_SLOW = 21

MAX_SPREAD_PIPS = 2.0
MIN_CONFIDENCE = 70
ALERT_COOLDOWN_SECONDS = 300

WAT = pytz.timezone("Africa/Lagos")

# ==========================================
# RENDER HEALTH SERVER
# ==========================================

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({
        "service": APP_NAME,
        "status": "running",
        "market_feed": "Biquote SignalR",
        "pairs": PAIRS,
        "timeframes": ["15s", "30s", "1m"]
    })

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": APP_NAME
    })

def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting health server on port {port}")
    serve(app, host="0.0.0.0", port=port)

# ==========================================
# MARKET DATA STORAGE
# ==========================================

history = {
    pair: {
        seconds: deque(maxlen=300)
        for seconds in TIMEFRAMES
    }
    for pair in PAIRS
}

current_candles = {}
latest_quotes = {}
last_alert = {}

# ==========================================
# INDICATORS
# ==========================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            value * multiplier
            + result * (1 - multiplier)
        )

    return result


def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    ranges = []

    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]

        true_range = max(
            current["high"] - current["low"],
            abs(current["high"] - previous["close"]),
            abs(current["low"] - previous["close"])
        )

        ranges.append(true_range)

    return sum(ranges[-period:]) / period


def pip_size(pair):
    return 0.01 if pair.endswith("JPY") else 0.0001


# ==========================================
# DISCORD
# ==========================================

def send_discord(message):
    if not WEBHOOK_URL:
        print("DISCORD NOT CONFIGURED: Add DISCORD_WEBHOOK_URL in Render.")
        return False

    try:
        response = requests.post(
            WEBHOOK_URL,
            json={
                "username": APP_NAME,
                "content": message
            },
            timeout=15
        )

        if response.status_code in (200, 204):
            print("DISCORD ALERT SENT")
            return True

        print("DISCORD ERROR:", response.status_code)
        print(response.text[:500])
        return False

    except Exception as error:
        print("DISCORD CONNECTION ERROR:", error)
        return False


# ==========================================
# CANDLE ANALYSIS
# ==========================================

def get_analysis(pair, seconds):
    candles = list(history[pair][seconds])

    if len(candles) < 22:
        return None

    closes = [c["close"] for c in candles]

    fast = ema(closes, EMA_FAST)
    slow = ema(closes, EMA_SLOW)

    if fast is None or slow is None:
        return None

    last = candles[-1]

    return {
        "fast": fast,
        "slow": slow,
        "close": last["close"],
        "open": last["open"],
        "bullish": last["close"] > last["open"],
        "bearish": last["close"] < last["open"],
        "time": last["start"]
    }


# ==========================================
# SIGNAL ENGINE
# ==========================================

def evaluate_signal(pair, now):
    quote = latest_quotes.get(pair)

    if not quote:
        return

    bid = quote["bid"]
    ask = quote["ask"]
    mid = (bid + ask) / 2

    pip = pip_size(pair)
    spread = (ask - bid) / pip

    if spread <= 0 or spread > MAX_SPREAD_PIPS:
        print(pair, "WAIT - SPREAD TOO HIGH")
        return

    analyses = {
        seconds: get_analysis(pair, seconds)
        for seconds in TIMEFRAMES
    }

    if any(value is None for value in analyses.values()):
        print(pair, "WAIT - COLLECTING CANDLES")
        return

    # Check completed candle freshness
    for seconds, analysis in analyses.items():
        candle_close = analysis["time"] + seconds
        age = now.timestamp() - candle_close

        if age < 0 or age > seconds + 15:
            print(pair, "WAIT - OLD CANDLE DATA")
            return

    a15 = analyses[15]
    a30 = analyses[30]
    a60 = analyses[60]

    buy_conditions = (
        a60["fast"] > a60["slow"]
        and a30["fast"] > a30["slow"]
        and a15["fast"] > a15["slow"]
        and a60["close"] > a60["fast"]
        and a30["bullish"]
        and a15["bullish"]
    )

    sell_conditions = (
        a60["fast"] < a60["slow"]
        and a30["fast"] < a30["slow"]
        and a15["fast"] < a15["slow"]
        and a60["close"] < a60["fast"]
        and a30["bearish"]
        and a15["bearish"]
    )

    if not buy_conditions and not sell_conditions:
        print(pair, "WAIT - TIMEFRAMES NOT ALIGNED")
        return

    direction = "BUY" if buy_conditions else "SELL"

    # Setup strength, not a probability of winning
    separations = [
        abs(a["fast"] - a["slow"]) / pip
        for a in analyses.values()
    ]

    confidence = min(
        90,
        70 + int(min(10, sum(separations) / 3 * 2))
    )

    if confidence < MIN_CONFIDENCE:
        return

    alert_key = (pair, direction)
    previous = last_alert.get(alert_key)

    if (
        previous
        and now.timestamp() - previous
        < ALERT_COOLDOWN_SECONDS
    ):
        return

    one_minute_candles = list(history[pair][60])
    atr = calculate_atr(one_minute_candles)

    if atr is None or atr <= 0:
        print(pair, "WAIT - ATR NOT READY")
        return

    risk = atr * 1.2

    if direction == "BUY":
        stop_loss = mid - risk
        take_profit = mid + (risk * 2)
    else:
        stop_loss = mid + risk
        take_profit = mid - (risk * 2)

    entry_time = datetime.now(WAT).strftime(
        "%Y-%m-%d %H:%M:%S WAT"
    )

    decimals = 3 if pair.endswith("JPY") else 5

    message = (
        "🚨 BRIGHT HEART AI - FOREX SIGNAL\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"PAIR: {pair}\n"
        f"SIGNAL: {direction}\n"
        f"SETUP STRENGTH: {confidence}%\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"ENTRY: {mid:.{decimals}f}\n"
        f"STOP LOSS: {stop_loss:.{decimals}f}\n"
        f"TAKE PROFIT: {take_profit:.{decimals}f}\n"
        f"SPREAD: {spread:.2f} pips\n"
        f"ENTRY TIME: {entry_time}\n"
        "TRIGGER: 15-second candle\n"
        "CONFIRMATION: 30-second + 1-minute\n"
        "REVIEW WINDOW: 1 minute (suggested, not expiry)\n"
        "STRATEGY: EMA 9/21 ALIGNMENT\n"
        "RISK/REWARD: 1:2\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Setup strength is not a win probability.\n"
        "Educational signal only. Verify before entry."
    )

    print("\n" + message + "\n")

    if send_discord(message):
        last_alert[alert_key] = now.timestamp()


# ==========================================
# LIVE TICK PROCESSING
# ==========================================

def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def process_tick(tick):
    try:
        pair = tick["symbol"]

        if pair not in PAIRS:
            return

        bid = float(tick["bid"])
        ask = float(tick["ask"])
        mid = float(tick.get("mid", (bid + ask) / 2))

        timestamp = parse_timestamp(tick["timestamp"])

        if bid <= 0 or ask < bid:
            return

        latest_quotes[pair] = {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "received": time.time()
        }

        for seconds in TIMEFRAMES:
            bucket = (
                int(timestamp.timestamp()) // seconds
            ) * seconds

            key = (pair, seconds)
            candle = current_candles.get(key)

            if candle is None:
                current_candles[key] = {
                    "start": bucket,
                    "open": mid,
                    "high": mid,
                    "low": mid,
                    "close": mid
                }
                continue

            if bucket == candle["start"]:
                candle["high"] = max(candle["high"], mid)
                candle["low"] = min(candle["low"], mid)
                candle["close"] = mid

            elif bucket > candle["start"]:
                history[pair][seconds].append(candle)

                current_candles[key] = {
                    "start": bucket,
                    "open": mid,
                    "high": mid,
                    "low": mid,
                    "close": mid
                }

                if seconds == 15:
                    evaluate_signal(pair, timestamp)

    except Exception as error:
        print("TICK PROCESSING ERROR:", error)


def flatten_ticks(data):
    if isinstance(data, dict):
        return [data]

    if isinstance(data, list):
        result = []

        for item in data:
            result.extend(flatten_ticks(item))

        return result

    return []


async def receive_tick(data):
    for tick in flatten_ticks(data):
        process_tick(tick)


async def receive_state(data):
    print("SUBSCRIPTION STATUS:", data)


# ==========================================
# MARKET CONNECTION AND RECONNECTION
# ==========================================

async def run_market_feed():
    while True:
        try:
            print("Connecting to Biquote live market feed...")

            client = SignalRClient(
                "https://biquote.io/hubs/tick"
            )

            client.on("ReceiveTick", receive_tick)
            client.on(
                "ReceiveSubscriptionState",
                receive_state
            )

            client.on_open(
                lambda: client.send("Subscribe", [PAIRS])
            )

            await client.run()

            print("Market connection ended. Reconnecting...")

        except Exception as error:
            print("MARKET CONNECTION ERROR:", error)

        await asyncio.sleep(10)


# ==========================================
# START APPLICATION
# ==========================================

async def main():
    print("===================================")
    print(APP_NAME, "- RENDER CLOUD")
    print("PAIRS:", ", ".join(PAIRS))
    print("TIMEFRAMES: 15s | 30s | 1m")
    print("STRATEGY: EMA 9/21")
    print("DISCORD:", "CONFIGURED" if WEBHOOK_URL else "NOT CONFIGURED")
    print("OTC: NOT ENABLED - NO VERIFIED LIVE FEED")
    print("===================================")

    await run_market_feed()


if __name__ == "__main__":

    web_thread = threading.Thread(
        target=start_web_server,
        daemon=True
    )
    web_thread.start()

    asyncio.run(main())
