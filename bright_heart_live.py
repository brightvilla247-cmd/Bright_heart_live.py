
import asyncio
import requests
import pytz
import time

from datetime import datetime, timezone
from collections import deque
from pysignalr.client import SignalRClient

# ==========================================
# BRIGHT HEART AI - FOREX SIGNALS
# ==========================================

WEBHOOK_URL = "https://discord.com/api/webhooks/1571832931603845171/4cYbZ7nWNde-_5ot08eqy3rQgaIlHLO_Uoc2AfxHA6NZyTTa__XqR3ESTxLWiUCDr8_A"

PAIRS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "USDCAD"
]

TIMEFRAMES = [15, 30, 60]

EMA_FAST = 9
EMA_SLOW = 21

HISTORY_LIMIT = 100
MAX_SPREAD_PIPS = 2.0
ALERT_COOLDOWN = 300

WAT = pytz.timezone("Africa/Lagos")

# ==========================================
# DATA STORAGE
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
# TIME FUNCTIONS
# ==========================================

def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def current_wat():
    return datetime.now(WAT).strftime(
        "%Y-%m-%d %H:%M:%S WAT"
    )


# ==========================================
# HISTORICAL CANDLE LOADER
# ==========================================

def load_historical_1m():

    print("\nLOADING HISTORICAL CANDLES...")

    for pair in PAIRS:

        try:
            url = (
                f"https://biquote.io/api/{pair}/ohlc"
            )

            response = requests.get(
                url,
                params={
                    "interval": "1m",
                    "limit": HISTORY_LIMIT
                },
                timeout=20
            )

            response.raise_for_status()

            data = response.json()

            bars = data.get("bars", [])

            # Exclude unfinished candles
            closed_bars = [
                bar for bar in bars
                if not bar.get("isOpen", False)
            ]

            # Oldest first
            closed_bars.sort(
                key=lambda bar: bar["openTime"]
            )

            loaded = 0

            for bar in closed_bars:

                timestamp = parse_timestamp(
                    bar["openTime"]
                ).timestamp()

                candle = {
                    "start": int(timestamp),
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"])
                }

                history[pair][60].append(candle)
                loaded += 1

            print(
                pair,
                "- HISTORICAL 1M CANDLES:",
                loaded
            )

            if loaded < 22:
                print(
                    "WARNING:",
                    pair,
                    "has insufficient 1M history"
                )

        except Exception as error:

            print(
                pair,
                "- HISTORY ERROR:",
                error
            )


# ==========================================
# DISCORD DELIVERY
# ==========================================

def send_discord(message):

    try:

        response = requests.post(
            WEBHOOK_URL,
            json={
                "username": "Bright Heart AI",
                "content": message
            },
            timeout=15
        )

        if response.status_code == 204:

            print("DISCORD ALERT SENT")

        else:

            print(
                "DISCORD ERROR:",
                response.status_code,
                response.text
            )

    except Exception as error:

        print("DISCORD ERROR:", error)


# ==========================================
# EMA CALCULATION
# ==========================================

def calculate_ema(values, period):

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


# ==========================================
# ATR CALCULATION
# ==========================================

def calculate_atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    ranges = []

    for i in range(1, len(candles)):

        current = candles[i]
        previous = candles[i - 1]

        true_range = max(
            current["high"] - current["low"],
            abs(
                current["high"] - previous["close"]
            ),
            abs(
                current["low"] - previous["close"]
            )
        )

        ranges.append(true_range)

    return sum(ranges[-period:]) / period


def get_pip_size(pair):

    if pair.endswith("JPY"):
        return 0.01

    return 0.0001


# ==========================================
# TIMEFRAME ANALYSIS
# ==========================================

def analyze_timeframe(pair, seconds):

    candles = list(history[pair][seconds])

    if len(candles) < 22:
        return None

    closes = [
        candle["close"]
        for candle in candles
    ]

    fast = calculate_ema(closes, EMA_FAST)
    slow = calculate_ema(closes, EMA_SLOW)

    if fast is None or slow is None:
        return None

    latest = candles[-1]

    return {
        "fast": fast,
        "slow": slow,
        "close": latest["close"],
        "open": latest["open"],
        "bullish": latest["close"] > latest["open"],
        "bearish": latest["close"] < latest["open"],
        "start": latest["start"]
    }


# ==========================================
# SIGNAL ENGINE
# ==========================================

def evaluate_signal(pair, now):

    quote = latest_quotes.get(pair)

    if quote is None:
        return

    bid = quote["bid"]
    ask = quote["ask"]

    pip = get_pip_size(pair)

    spread = (ask - bid) / pip

    # Spread protection
    if spread <= 0 or spread > MAX_SPREAD_PIPS:

        print(pair, "WAIT - SPREAD TOO HIGH")
        return

    # Check quote freshness
    quote_age = now.timestamp() - quote["timestamp"]

    if quote_age < 0 or quote_age > 10:

        print(pair, "WAIT - STALE PRICE")
        return

    analyses = {
        seconds: analyze_timeframe(pair, seconds)
        for seconds in TIMEFRAMES
    }

    if any(value is None for value in analyses.values()):

        print(pair, "WAIT - COLLECTING CANDLES")
        return

    # Ensure candle data is recent
    for seconds, analysis in analyses.items():

        candle_close = analysis["start"] + seconds

        age = now.timestamp() - candle_close

        if age < 0 or age > seconds + 15:

            print(pair, "WAIT - OLD CANDLE DATA")
            return

    a15 = analyses[15]
    a30 = analyses[30]
    a60 = analyses[60]

    # ======================================
    # BUY CONDITIONS
    # ======================================

    buy_conditions = (
        a60["fast"] > a60["slow"]
        and a30["fast"] > a30["slow"]
        and a15["fast"] > a15["slow"]
        and a60["close"] > a60["fast"]
        and a30["bullish"]
        and a15["bullish"]
    )

    # ======================================
    # SELL CONDITIONS
    # ======================================

    sell_conditions = (
        a60["fast"] < a60["slow"]
        and a30["fast"] < a30["slow"]
        and a15["fast"] < a15["slow"]
        and a60["close"] < a60["fast"]
        and a30["bearish"]
        and a15["bearish"]
    )

    if not buy_conditions and not sell_conditions:

        print(pair, "WAIT - NO CONFIRMATION")
        return

    direction = (
        "BUY" if buy_conditions else "SELL"
    )

    # ======================================
    # SETUP STRENGTH SCORE
    # ======================================

    separations = [
        abs(item["fast"] - item["slow"]) / pip
        for item in analyses.values()
    ]

    strength = min(
        90,
        70 + int(
            min(10, sum(separations) / 3 * 2)
        )
    )

    # This is a setup score, not win probability.
    if strength < 70:
        return

    # ======================================
    # ALERT COOLDOWN
    # ======================================

    alert_key = (pair, direction)

    previous = last_alert.get(alert_key)

    if previous is not None:

        if time.time() - previous < ALERT_COOLDOWN:

            return

    # ======================================
    # ATR STOP LOSS / TAKE PROFIT
    # ======================================

    one_minute = list(history[pair][60])

    atr = calculate_atr(one_minute)

    if atr is None or atr <= 0:

        print(pair, "WAIT - ATR NOT READY")
        return

    entry = (
        ask if direction == "BUY"
        else bid
    )

    risk = atr * 1.2

    if direction == "BUY":

        stop_loss = entry - risk
        take_profit = entry + (risk * 2)

    else:

        stop_loss = entry + risk
        take_profit = entry - (risk * 2)

    decimals = (
        3 if pair.endswith("JPY") else 5
    )

    # ======================================
    # BUILD SIGNAL
    # ======================================

    message = (
        "🚨 BRIGHT HEART AI - FOREX SIGNAL\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"PAIR: {pair}\n"
        f"SIGNAL: {direction}\n"
        f"SETUP STRENGTH: {strength}%\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"ENTRY: {entry:.{decimals}f}\n"
        f"STOP LOSS: {stop_loss:.{decimals}f}\n"
        f"TAKE PROFIT: {take_profit:.{decimals}f}\n"
        f"SPREAD: {spread:.2f} pips\n"
        f"ENTRY TIME: {current_wat()}\n"
        "TIMEFRAMES: 15s / 30s / 1m\n"
        "STRATEGY: EMA 9 / EMA 21\n"
        "RISK/REWARD: 1:2\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "Signals are experimental. Verify before entry."
    )

    print("\n" + message + "\n")

    send_discord(message)

    last_alert[alert_key] = time.time()


# ==========================================
# LIVE TICK PROCESSING
# ==========================================

def process_tick(tick):

    try:

        pair = tick["symbol"]

        if pair not in PAIRS:
            return

        bid = float(tick["bid"])
        ask = float(tick["ask"])

        mid = float(
            tick.get("mid", (bid + ask) / 2)
        )

        timestamp = parse_timestamp(
            tick["timestamp"]
        )

        if bid <= 0 or ask < bid or mid <= 0:
            return

        latest_quotes[pair] = {
            "bid": bid,
            "ask": ask,
            "timestamp": timestamp.timestamp()
        }

        # Build 15s, 30s and 1m candles
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

                candle["high"] = max(
                    candle["high"], mid
                )

                candle["low"] = min(
                    candle["low"], mid
                )

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

                # Evaluate once a new 15-second candle starts
                if seconds == 15:

                    evaluate_signal(pair, timestamp)

    except Exception as error:

        print("TICK PROCESSING ERROR:", error)


# ==========================================
# RECEIVE TICK DATA
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
# START BOT
# ==========================================

async def main():

    print("===================================")
    print("BRIGHT HEART AI - LIVE FOREX")
    print("PAIRS:", ", ".join(PAIRS))
    print("TIMEFRAMES: 15s | 30s | 1m")
    print("STRATEGY: EMA 9 / EMA 21")
    print("HISTORICAL DATA: ENABLED")
    print("DISCORD: ENABLED")
    print("===================================")

    if (
        not WEBHOOK_URL.startswith(
            "https://discord.com/api/webhooks/"
        )
    ):

        print("ERROR: INSERT YOUR DISCORD WEBHOOK URL")
        return

    # Load historical 1-minute candles first
    load_historical_1m()

    print("\nCONNECTING TO LIVE MARKET FEED...")

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


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("BRIGHT HEART AI STOPPED")

    except Exception as error:

        print("BOT ERROR:", error)
