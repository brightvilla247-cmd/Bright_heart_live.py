
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
# BRIGHT HEART AI - LIVE CLOUD ENGINE
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
MIN_CANDLES = 22

LIVE_MAX_AGE = 5
STALE_MAX_AGE = 15
MONITOR_INTERVAL = 60

WAT = pytz.timezone("Africa/Lagos")
BIQUOTE_URL = "https://biquote.io"

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
last_diagnostic = {}

tick_counts = {pair: 0 for pair in PAIRS}
feed_status = {pair: "WAITING" for pair in PAIRS}

discord_status = {
    "configured": bool(WEBHOOK_URL),
    "last_test": None,
    "last_delivery": None,
    "last_error": None
}

discord_lock = threading.Lock()

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
        "historical_data": "Biquote 1m OHLC",
        "pairs": PAIRS,
        "timeframes": ["15s", "30s", "1m"],
        "discord": (
            "CONFIGURED" if WEBHOOK_URL
            else "NOT CONFIGURED"
        ),
        "discord_last_test": discord_status["last_test"]
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": APP_NAME
    })


def get_feed_state(pair):
    quote = latest_quotes.get(pair)

    if not quote:
        return "WAITING", None

    age = time.time() - quote["received"]

    if age <= LIVE_MAX_AGE:
        return "LIVE", round(age, 1)

    if age <= STALE_MAX_AGE:
        return "DELAYED", round(age, 1)

    return "STALE", round(age, 1)


@app.route("/status")
def status():
    result = {}

    for pair in PAIRS:
        state, age = get_feed_state(pair)
        feed_status[pair] = state

        result[pair] = {
            "feed": state,
            "quote_age_seconds": age,
            "ticks_received": tick_counts[pair],
            "candles": {
                "15s": len(history[pair][15]),
                "30s": len(history[pair][30]),
                "1m": len(history[pair][60])
            },
            "ready": (
                state == "LIVE"
                and all(
                    len(history[pair][seconds]) >= MIN_CANDLES
                    for seconds in TIMEFRAMES
                )
            )
        }

    return jsonify({
        "service": APP_NAME,
        "status": "running",
        "pairs": result,
        "discord": (
            "CONFIGURED" if WEBHOOK_URL
            else "NOT CONFIGURED"
        ),
        "discord_last_test": discord_status["last_test"],
        "discord_last_delivery": discord_status["last_delivery"]
    })


@app.route("/ready")
def readiness():
    result = {}

    for pair in PAIRS:
        state, age = get_feed_state(pair)

        candles_ready = all(
            len(history[pair][seconds]) >= MIN_CANDLES
            for seconds in TIMEFRAMES
        )

        result[pair] = {
            "feed": state,
            "candles_ready": candles_ready,
            "signal_ready": state == "LIVE" and candles_ready
        }

    return jsonify({
        "service": APP_NAME,
        "pairs": result
    })


# ==========================================
# DISCORD
# ==========================================

def send_discord(message):
    if not WEBHOOK_URL:
        print("[DISCORD] NOT CONFIGURED")
        discord_status["last_error"] = "Webhook missing"
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
            print("[DISCORD] MESSAGE DELIVERED")

            discord_status["last_delivery"] = (
                datetime.now(WAT).strftime("%Y-%m-%d %H:%M:%S WAT")
            )
            discord_status["last_error"] = None

            return True

        error = (
            f"HTTP {response.status_code}: "
            f"{response.text[:200]}"
        )

        print("[DISCORD] DELIVERY FAILED:", error)
        discord_status["last_error"] = error

        return False

    except Exception as error:
        print("[DISCORD] CONNECTION ERROR:", str(error)[:300])
        discord_status["last_error"] = str(error)[:300]
        return False


def discord_startup_test():
    if not WEBHOOK_URL:
        print("[DISCORD TEST] FAILED - WEBHOOK NOT CONFIGURED")
        discord_status["last_test"] = "NOT CONFIGURED"
        return False

    print("[DISCORD TEST] SENDING STARTUP TEST...")

    message = (
        "BRIGHT HEART AI\n"
        "Discord connection test\n"
        "Webhook delivery test successful.\n"
        "Market analysis is starting."
    )

    success = send_discord(message)

    if success:
        discord_status["last_test"] = "SUCCESS"
        print("[DISCORD TEST] SUCCESS - CHECK YOUR CHANNEL")
    else:
        discord_status["last_test"] = "FAILED"
        print("[DISCORD TEST] FAILED - CHECK DELIVERY ERROR")

    return success


@app.route("/discord-test", methods=["GET"])
def discord_test():
    if not WEBHOOK_URL:
        return jsonify({
            "success": False,
            "message": "Discord webhook is not configured"
        }), 503

    message = (
        "BRIGHT HEART AI\n"
        "Manual Discord delivery test successful.\n"
        "Your webhook is responding."
    )

    success = send_discord(message)

    discord_status["last_test"] = (
        "SUCCESS" if success else "FAILED"
    )

    if success:
        return jsonify({
            "success": True,
            "message": "Test message delivered to Discord"
        })

    return jsonify({
        "success": False,
        "message": "Discord delivery failed",
        "error": discord_status["last_error"]
    }), 502


# ==========================================
# RENDER SERVER
# ==========================================

def start_web_server():
    port = int(os.environ.get("PORT", 10000))
    print("Starting health server on port", port)
    serve(app, host="0.0.0.0", port=port)


# ==========================================
# LIVE FEED MONITOR
# ==========================================

def feed_monitor():
    while True:
        try:
            print("\n========== LIVE FEED MONITOR ==========")

            for pair in PAIRS:
                state, age = get_feed_state(pair)

                feed_status[pair] = state

                age_text = "NO QUOTE" if age is None else f"{age}s"

                print(
                    f"{pair}: {state} | "
                    f"QUOTE AGE: {age_text} | "
                    f"TICKS: {tick_counts[pair]}"
                )

            print("=======================================\n")

        except Exception as error:
            print("FEED MONITOR ERROR:", error)

        time.sleep(MONITOR_INTERVAL)


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
# DIAGNOSTICS
# ==========================================

def diagnostic(pair, reason):
    key = (pair, reason)
    now = time.time()

    if now - last_diagnostic.get(key, 0) >= 60:
        print(f"[DIAGNOSTIC] {pair}: {reason}")
        last_diagnostic[key] = now


# ==========================================
# HISTORICAL ONE-MINUTE DATA
# ==========================================

def seed_historical_1m():
    print("\n========== HISTORICAL DATA ==========")

    for pair in PAIRS:
        try:
            response = requests.get(
                f"{BIQUOTE_URL}/api/{pair}/ohlc",
                params={
                    "interval": "1m",
                    "limit": 100
                },
                timeout=15
            )

            response.raise_for_status()
            data = response.json()

            bars = data.get("bars", [])
            closed_bars = []

            for bar in bars:
                if bar.get("isOpen", False):
                    continue

                candle = {
                    "start": int(
                        datetime.fromisoformat(
                            bar["openTime"].replace("Z", "+00:00")
                        ).timestamp()
                    ),
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"])
                }

                closed_bars.append(candle)

            closed_bars.sort(key=lambda item: item["start"])

            history[pair][60].clear()

            for candle in closed_bars:
                history[pair][60].append(candle)

            print(
                f"{pair}: LOADED "
                f"{len(history[pair][60])} "
                f"CLOSED 1M CANDLES"
            )

        except Exception as error:
            print(
                f"{pair}: HISTORICAL DATA ERROR:",
                str(error)[:300]
            )

    print("HISTORICAL DATA LOADING FINISHED")
    print("====================================\n")


# ==========================================
# CANDLE ANALYSIS
# ==========================================

def get_analysis(pair, seconds):
    candles = list(history[pair][seconds])

    if len(candles) < MIN_CANDLES:
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
        diagnostic(pair, "WAIT - NO LIVE QUOTE")
        return

    quote_age = time.time() - quote["received"]

    if quote_age > STALE_MAX_AGE:
        diagnostic(pair, "WAIT - STALE PRICE")
        return

    bid = quote["bid"]
    ask = quote["ask"]
    mid = (bid + ask) / 2

    pip = pip_size(pair)
    spread = (ask - bid) / pip

    if spread <= 0 or spread > MAX_SPREAD_PIPS:
        diagnostic(pair, "WAIT - SPREAD TOO HIGH")
        return

    analyses = {
        seconds: get_analysis(pair, seconds)
        for seconds in TIMEFRAMES
    }

    if any(value is None for value in analyses.values()):
        diagnostic(pair, "WAIT - COLLECTING REQUIRED CANDLES")
        return

    for seconds, analysis in analyses.items():
        candle_close = analysis["time"] + seconds
        age = now.timestamp() - candle_close

        if age < 0 or age > seconds + 15:
            diagnostic(pair, "WAIT - OLD CANDLE DATA")
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
        diagnostic(pair, "WAIT - TIMEFRAMES NOT ALIGNED")
        return

    direction = "BUY" if buy_conditions else "SELL"

    separations = [
        abs(a["fast"] - a["slow"]) / pip
        for a in analyses.values()
    ]

    confidence = min(
        90,
        70 + int(min(10, sum(separations) / 3 * 2))
    )

    if confidence < MIN_CONFIDENCE:
        diagnostic(pair, "WAIT - SETUP STRENGTH TOO LOW")
        return

    alert_key = (pair, direction)
    previous = last_alert.get(alert_key)

    if (
        previous
        and now.timestamp() - previous
        < ALERT_COOLDOWN_SECONDS
    ):
        diagnostic(pair, "WAIT - ALERT COOLDOWN")
        return

    atr = calculate_atr(list(history[pair][60]))

    if atr is None or atr <= 0:
        diagnostic(pair, "WAIT - ATR NOT READY")
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

        received = time.time()

        latest_quotes[pair] = {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "received": received
        }

        tick_counts[pair] += 1
        feed_status[pair] = "LIVE"

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

            if bucket < candle["start"]:
                continue

            if bucket == candle["start"]:
                candle["high"] = max(candle["high"], mid)
                candle["low"] = min(candle["low"], mid)
                candle["close"] = mid

            elif bucket > candle["start"]:
                expected_next = candle["start"] + seconds

                if bucket == expected_next:
                    history[pair][seconds].append(candle)
                else:
                    diagnostic(
                        pair,
                        f"{seconds}s CANDLE GAP - RECOVERING"
                    )

                current_candles[key] = {
                    "start": bucket,
                    "open": mid,
                    "high": mid,
                    "low": mid,
                    "close": mid
                }

                if seconds == 15 and bucket == expected_next:
                    evaluate_signal(pair, timestamp)

    except Exception as error:
        print("TICK PROCESSING ERROR:", error)


# ==========================================
# SIGNALR EVENT HANDLERS
# ==========================================

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
# LIVE MARKET CONNECTION
# ==========================================

async def run_market_feed():
    while True:
        try:
            print("Connecting to Biquote live market feed...")

            client = SignalRClient(
                "https://biquote.io/hubs/tick"
            )

            client.on("ReceiveTick", receive_tick)
            client.on("ReceiveSubscriptionState", receive_state)

            client.on_open(
                lambda: client.send("Subscribe", [PAIRS])
            )

            await client.run()

            print("Market connection ended. Reconnecting...")

        except Exception as error:
            print("MARKET CONNECTION ERROR:", error)

        print("RECONNECTING IN 10 SECONDS...")
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
    print("HISTORICAL: BIQUOTE 1M OHLC")
    print("FEED MONITOR: ENABLED")
    print("GAP RECOVERY: ENABLED")
    print(
        "DISCORD:",
        "CONFIGURED" if WEBHOOK_URL else "NOT CONFIGURED"
    )
    print("OTC: NOT ENABLED")
    print("===================================")

    # Send one startup delivery test
    discord_startup_test()

    threading.Thread(
        target=feed_monitor,
        daemon=True
    ).start()

    await asyncio.to_thread(seed_historical_1m)

    print("Starting live market analysis...")
    print("15s and 30s candles build from live ticks.")

    await run_market_feed()

