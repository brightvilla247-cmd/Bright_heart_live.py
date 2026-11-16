
import os
import asyncio
import threading
import time
import traceback
import requests
import pytz

from datetime import datetime, timezone
from collections import deque
from flask import Flask, jsonify
from waitress import serve
from pysignalr.client import SignalRClient


# =========================================================
# BRIGHT HEART AI - LIVE FOREX SIGNALS
# =========================================================

APP_NAME = "BRIGHT HEART AI"

BIQUOTE_HUB = "https://biquote.io/hubs/tick"
BIQUOTE_URL = "https://biquote.io"

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

WAT = pytz.timezone("Africa/Lagos")

# =========================================================
# GLOBAL STATE
# =========================================================

app = Flask(__name__)

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
    "last_delivery": None,
    "last_error": None
}

state_lock = threading.Lock()


# =========================================================
# TIME AND PRICE HELPERS
# =========================================================

def wat_now():
    return datetime.now(timezone.utc).astimezone(WAT)


def pip_size(pair):
    return 0.01 if "JPY" in pair else 0.0001


def get_feed_state(pair):
    quote = latest_quotes.get(pair)

    if not quote:
        return "WAITING", None

    age = time.time() - quote["received_at"]

    if age <= LIVE_MAX_AGE:
        return "LIVE", round(age, 1)

    if age <= STALE_MAX_AGE:
        return "DELAYED", round(age, 1)

    return "STALE", round(age, 1)


def diagnostic(pair, reason):
    now = time.time()
    key = (pair, reason)

    if now - last_diagnostic.get(key, 0) >= 60:
        print(f"[DIAGNOSTIC] {pair}: {reason}", flush=True)
        last_diagnostic[key] = now


# =========================================================
# DISCORD
# =========================================================

def send_discord(message):
    if not WEBHOOK_URL:
        print("[DISCORD] Webhook is not configured.", flush=True)
        discord_status["last_error"] = "Webhook not configured"
        return False

    try:
        response = requests.post(
            WEBHOOK_URL,
            json={
                "username": APP_NAME,
                "content": message
            },
            timeout=12
        )

        if response.status_code in (200, 204):
            discord_status["last_delivery"] = wat_now().isoformat()
            discord_status["last_error"] = None
            print("[DISCORD] Message delivered.", flush=True)
            return True

        error = f"HTTP {response.status_code}: {response.text[:250]}"
        discord_status["last_error"] = error
        print("[DISCORD ERROR]", error, flush=True)
        return False

    except Exception as error:
        discord_status["last_error"] = str(error)
        print("[DISCORD CONNECTION ERROR]", error, flush=True)
        return False


def discord_startup_test():
    if not WEBHOOK_URL:
        print("[DISCORD] Not configured. Add DISCORD_WEBHOOK_URL in Render.", flush=True)
        return

    message = (
        "🟢 **BRIGHT HEART AI ONLINE**\n"
        "Forex signal service has started.\n"
        "Monitoring: EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD\n"
        "Timeframes: 15s | 30s | 1m\n"
        "Signal alerts are enabled."
    )

    send_discord(message)


# =========================================================
# EMA AND ATR
# =========================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = value * multiplier + result * (1 - multiplier)

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

    if len(ranges) < period:
        return None

    return sum(ranges[-period:]) / period


# =========================================================
# HISTORICAL 1-MINUTE CANDLES
# =========================================================

def seed_historical_1m():
    print("[HISTORY] Loading historical 1-minute candles...", flush=True)

    for pair in PAIRS:
        try:
            url = f"{BIQUOTE_URL}/api/{pair}/ohlc?interval=1m&limit=100"

            response = requests.get(url, timeout=15)
            response.raise_for_status()

            data = response.json()
            bars = data.get("bars", [])

            loaded = 0

            for bar in bars:
                if bar.get("isOpen"):
                    continue

                candle = {
                    "time": float(bar["openTime"]),
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"])
                }

                history[pair][60].append(candle)
                loaded += 1

            print(
                f"[HISTORY] {pair}: loaded {loaded} closed 1m candles",
                flush=True
            )

        except Exception as error:
            print(f"[HISTORY ERROR] {pair}: {error}", flush=True)


# =========================================================
# CANDLE BUILDING
# =========================================================

def update_candle(pair, seconds, price, timestamp):
    bucket = int(timestamp // seconds) * seconds
    key = (pair, seconds)

    candle = current_candles.get(key)

    if candle is None:
        current_candles[key] = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price
        }
        return False

    if bucket == candle["time"]:
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        return False

    if bucket > candle["time"]:
        completed = candle.copy()
        history[pair][seconds].append(completed)

        current_candles[key] = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price
        }

        # Do not manufacture missing candles.
        if bucket - completed["time"] != seconds:
            diagnostic(pair, f"{seconds}s candle gap detected")
            return False

        return seconds == 15

    return False


# =========================================================
# MARKET ANALYSIS
# =========================================================

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
        "high": last["high"],
        "low": last["low"],
        "time": last["time"]
    }


def evaluate_signal(pair, now):
    quote = latest_quotes.get(pair)

    if not quote:
        diagnostic(pair, "Waiting for quote")
        return

    age = time.time() - quote["received_at"]

    if age > LIVE_MAX_AGE:
        diagnostic(pair, "Quote is not fresh")
        return

    bid = quote["bid"]
    ask = quote["ask"]
    mid = quote["mid"]

    spread = ask - bid
    pip = pip_size(pair)
    spread_pips = spread / pip

    if spread <= 0 or spread_pips > MAX_SPREAD_PIPS:
        diagnostic(pair, f"Spread rejected: {spread_pips:.2f} pips")
        return

    analyses = {}

    for seconds in TIMEFRAMES:
        analysis = get_analysis(pair, seconds)

        if analysis is None:
            diagnostic(pair, f"Waiting for enough {seconds}s candles")
            return

        candle_age = now - (analysis["time"] + seconds)

        if candle_age > seconds * 2:
            diagnostic(pair, f"{seconds}s candle is too old")
            return

        analyses[seconds] = analysis

    a15 = analyses[15]
    a30 = analyses[30]
    a60 = analyses[60]

    buy_alignment = (
        a15["fast"] > a15["slow"]
        and a30["fast"] > a30["slow"]
        and a60["fast"] > a60["slow"]
        and a60["close"] > a60["fast"]
        and a30["close"] > a30["open"]
        and a15["close"] > a15["open"]
    )

    sell_alignment = (
        a15["fast"] < a15["slow"]
        and a30["fast"] < a30["slow"]
        and a60["fast"] < a60["slow"]
        and a60["close"] < a60["fast"]
        and a30["close"] < a30["open"]
        and a15["close"] < a15["open"]
    )

    if buy_alignment:
        direction = "BUY"
    elif sell_alignment:
        direction = "SELL"
    else:
        diagnostic(pair, "EMA/timeframe confirmation not aligned")
        return

    separations = [
        abs(a["fast"] - a["slow"]) / pip
        for a in analyses.values()
    ]

    # This is a setup-strength score, not a win probability.
    confidence = min(
        90,
        70 + int(min(10, sum(separations) / 3 * 2))
    )

    if confidence < MIN_CONFIDENCE:
        diagnostic(pair, "Setup strength below minimum")
        return

    alert_key = (pair, direction)
    last_time = last_alert.get(alert_key, 0)

    if time.time() - last_time < ALERT_COOLDOWN_SECONDS:
        diagnostic(pair, "Alert cooldown active")
        return

    one_minute_candles = list(history[pair][60])
    atr = calculate_atr(one_minute_candles)

    if atr is None or atr <= 0:
        diagnostic(pair, "Waiting for ATR data")
        return

    risk_distance = atr * 1.2

    if direction == "BUY":
        entry = ask
        stop_loss = entry - risk_distance
        take_profit = entry + (risk_distance * 2)
    else:
        entry = bid
        stop_loss = entry + risk_distance
        take_profit = entry - (risk_distance * 2)

    entry_time = wat_now().strftime("%Y-%m-%d %H:%M:%S WAT")

    message = (
        f"📊 **{APP_NAME} - FOREX SIGNAL**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"**PAIR:** {pair}\n"
        f"**DIRECTION:** {'🟢 BUY' if direction == 'BUY' else '🔴 SELL'}\n"
        f"**SETUP STRENGTH:** {confidence}%\n"
        f"**ENTRY PRICE:** {entry:.5f}\n"
        f"**STOP LOSS:** {stop_loss:.5f}\n"
        f"**TAKE PROFIT:** {take_profit:.5f}\n"
        f"**SPREAD:** {spread_pips:.2f} pips\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"**ENTRY TIME:** {entry_time}\n"
        f"**TRIGGER:** 15 seconds\n"
        f"**CONFIRMATION:** 30 seconds + 1 minute\n"
        f"**REVIEW:** Reassess after 1 minute\n"
        f"**RISK/REWARD:** 1:2\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Setup score is not a guaranteed win probability. "
        f"Demo-test before live trading."
    )

    if send_discord(message):
        last_alert[alert_key] = time.time()
        print(f"[SIGNAL SENT] {pair} {direction} {confidence}%", flush=True)


# =========================================================
# LIVE TICK PROCESSING
# =========================================================

def process_tick(tick):
    try:
        if not isinstance(tick, dict):
            return

        symbol = str(
            tick.get("symbol", tick.get("pair", ""))
        ).upper().replace("/", "")

        if symbol not in PAIRS:
            return

        bid = float(tick.get("bid", 0))
        ask = float(tick.get("ask", 0))

        if bid <= 0 or ask <= 0 or ask < bid:
            return

        mid = (bid + ask) / 2

        raw_timestamp = tick.get("timestamp")

        if raw_timestamp is None:
            timestamp = time.time()
        else:
            timestamp = float(raw_timestamp)

            # Convert milliseconds to seconds if needed.
            if timestamp > 100000000000:
                timestamp /= 1000

        with state_lock:
            latest_quotes[symbol] = {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "received_at": time.time()
            }

            tick_counts[symbol] += 1
            feed_status[symbol] = "LIVE"

        trigger = False

        for seconds in TIMEFRAMES:
            completed_15s = update_candle(
                symbol,
                seconds,
                mid,
                timestamp
            )

            if completed_15s:
                trigger = True

        if trigger:
            evaluate_signal(symbol, timestamp)

    except Exception as error:
        print("[TICK PROCESSING ERROR]", error, flush=True)


def flatten_ticks(data):
    if isinstance(data, list):
        for item in data:
            yield from flatten_ticks(item)

    elif isinstance(data, dict):
        if "ticks" in data and isinstance(data["ticks"], list):
            yield from flatten_ticks(data["ticks"])
        else:
            yield data


def receive_tick(data):
    try:
        for tick in flatten_ticks(data):
            process_tick(tick)
    except Exception as error:
        print("[RECEIVE TICK ERROR]", error, flush=True)


def receive_state(data):
    print("[SUBSCRIPTION STATUS]", data, flush=True)


# =========================================================
# MARKET FEED CONNECTION
# =========================================================

async def run_market_feed():
    while True:
        try:
            print("[FEED] Connecting to Biquote live market feed...", flush=True)

            client = SignalRClient(BIQUOTE_HUB)

            client.on("ReceiveTick", receive_tick)
            client.on("ReceiveSubscriptionState", receive_state)

            client.on_open(
                lambda: client.send("Subscribe", [PAIRS])
            )

            print("[FEED] SignalR client starting...", flush=True)
            await client.run()

            print("[FEED] Connection ended. Reconnecting...", flush=True)

        except Exception as error:
            print("[FEED CONNECTION ERROR]", error, flush=True)
            traceback.print_exc()

        await asyncio.sleep(10)


# =========================================================
# MONITOR
# =========================================================

def feed_monitor():
    while True:
        try:
            print("\n========== BRIGHT HEART FEED MONITOR ==========", flush=True)

            for pair in PAIRS:
                state, age = get_feed_state(pair)

                counts = {
                    str(seconds): len(history[pair][seconds])
                    for seconds in TIMEFRAMES
                }

                ready = (
                    state == "LIVE"
                    and all(
                        len(history[pair][seconds]) >= MIN_CANDLES
                        for seconds in TIMEFRAMES
                    )
                )

                print(
                    f"{pair} | FEED={state} | AGE={age}s "
                    f"| CANDLES={counts} | READY={ready} "
                    f"| TICKS={tick_counts[pair]}",
                    flush=True
                )

            print(
                f"DISCORD CONFIGURED: {discord_status['configured']}",
                flush=True
            )

            print("================================================\n", flush=True)

        except Exception as error:
            print("[MONITOR ERROR]", error, flush=True)

        time.sleep(60)


# =========================================================
# WEB DASHBOARD
# =========================================================

@app.route("/")
def home():
    return jsonify({
        "service": APP_NAME,
        "status": "RUNNING",
        "feed": "Biquote SignalR",
        "pairs": PAIRS,
        "timeframes": TIMEFRAMES,
        "discord_configured": discord_status["configured"]
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": APP_NAME
    })


@app.route("/status")
def status():
    result = {}

    for pair in PAIRS:
        state, age = get_feed_state(pair)

        result[pair] = {
            "feed": state,
            "quote_age_seconds": age,
            "ticks_received": tick_counts[pair],
            "candles": {
                str(seconds): len(history[pair][seconds])
                for seconds in TIMEFRAMES
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
        "pairs": result,
        "discord": discord_status
    })


@app.route("/ready")
def ready():
    result = {}

    for pair in PAIRS:
        state, age = get_feed_state(pair)

        result[pair] = {
            "feed": state,
            "quote_age_seconds": age,
            "ready": (
                state == "LIVE"
                and all(
                    len(history[pair][seconds]) >= MIN_CANDLES
                    for seconds in TIMEFRAMES
                )
            )
        }

    return jsonify(result)


@app.route("/discord-test")
def discord_test():
    success = send_discord(
        "🟢 **BRIGHT HEART AI TEST**\n"
        "Discord connection test successful."
    )

    return jsonify({
        "discord_sent": success,
        "discord": discord_status
    })


def start_web_server():
    port = int(os.environ.get("PORT", 10000))

    print(f"[WEB] Starting server on port {port}", flush=True)

    serve(
        app,
        host="0.0.0.0",
        port=port,
        threads=4
    )


# =========================================================
# MAIN
# =========================================================

async def main():
    print("\n==========================================", flush=True)
    print("       BRIGHT HEART AI - LIVE FOREX", flush=True)
    print("==========================================", flush=True)
    print("PAIRS:", ", ".join(PAIRS), flush=True)
    print("TIMEFRAMES: 15s | 30s | 1m", flush=True)
    print("EMA: 9 / 21", flush=True)
    print("FEED MONITOR: ENABLED", flush=True)
    print("GAP RECOVERY: ENABLED", flush=True)
    print(
        "DISCORD:",
        "CONFIGURED" if WEBHOOK_URL else "NOT CONFIGURED",
        flush=True
    )
    print("==========================================\n", flush=True)

    discord_startup_test()

    threading.Thread(
        target=feed_monitor,
        daemon=True
    ).start()

    try:
        await asyncio.to_thread(seed_historical_1m)
    except Exception as error:
        print("[HISTORY STARTUP ERROR]", error, flush=True)

    await run_market_feed()


if __name__ == "__main__":
    try:
        threading.Thread(
            target=start_web_server,
            daemon=True
        ).start()

        asyncio.run(main())

    except KeyboardInterrupt:
        print("[SYSTEM] Stopped.", flush=True)

    except Exception as error:
        print("[FATAL APPLICATION ERROR]", error, flush=True)
        traceback.print_exc()
        raise
    
