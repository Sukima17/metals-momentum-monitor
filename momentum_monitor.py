#!/usr/bin/env python3
"""Daily-core, multi-timeframe futures momentum monitor.

No third-party Python package is required. Domestic futures bars are read from
the same Sina endpoints used by AKShare. Daily bars drive the four-factor
model; 60-minute and weekly bars confirm the trend; 5-minute bars are alerts.
The monitored universe covers domestic precious, non-ferrous, new-energy and
ferrous main continuous futures.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dt_time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from market_universe import fetch_market_breadth
from research_backtest import compare_strategies


ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
STATE_DIR = ROOT / "state"
CONFIG_PATH = ROOT / "config.json"
LATEST_PATH = DIST / "latest.json"
HISTORY_PATH = STATE_DIR / "scan_history.jsonl"
PAPER_PATH = STATE_DIR / "paper_portfolio.json"
SIGNAL_STATE_PATH = STATE_DIR / "signal_state.json"
PERFORMANCE_PATH = DIST / "performance-report.json"
SINA_ENDPOINT = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/=/"
    "InnerFuturesNewService.getFewMinLine"
)
SINA_DAILY_ENDPOINT = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "var%20_{variable}=/InnerFuturesNewService.getDailyKLine"
)
WRITE_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with WRITE_LOCK:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        os.replace(temporary, path)


def append_history(payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    compact = {
        "generated_at": payload["generated_at"],
        "assets": [
            {
                "id": item["id"],
                "bar_time": item.get("bar_time"),
                "price": item.get("price"),
                "score": item.get("score"),
                "signal": item.get("signal"),
                "status": item.get("status"),
            }
            for item in payload["assets"]
        ],
    }
    with WRITE_LOCK, HISTORY_PATH.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(compact, ensure_ascii=False) + "\n")


def parse_sina_rows(raw: str, minimum: int, label: str) -> list[dict[str, Any]]:
    match = re.search(r"=\((.*)\);?\s*$", raw, flags=re.S)
    if not match:
        raise ValueError("行情接口返回格式异常")
    values = json.loads(match.group(1))
    bars: list[dict[str, Any]] = []
    for row in values:
        if isinstance(row, dict):
            data = {
                "datetime": row.get("d"), "open": row.get("o"), "high": row.get("h"),
                "low": row.get("l"), "close": row.get("c"), "volume": row.get("v", 0),
                "hold": row.get("p", 0), "settle": row.get("s"),
            }
        else:
            data = dict(zip(("datetime", "open", "high", "low", "close", "volume", "hold", "settle"), row))
        try:
            bars.append({
                "datetime": str(data["datetime"]), "open": float(data["open"]),
                "high": float(data["high"]), "low": float(data["low"]),
                "close": float(data["close"]), "volume": float(data.get("volume") or 0),
                "hold": float(data.get("hold") or 0),
                "settle": float(data.get("settle") or 0),
            })
        except (TypeError, ValueError, KeyError):
            continue
    bars.sort(key=lambda row: row["datetime"])
    if len(bars) < minimum:
        raise ValueError(f"有效{label}K线不足: {len(bars)}")
    return bars


def request_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 MomentumResearchMonitor/1.0",
            "Referer": "https://vip.stock.finance.sina.com.cn/",
        },
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=18) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception:
            if attempt == 2:
                raise
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError("行情请求失败")


def fetch_sina_minute_bars(symbol: str, period: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"symbol": symbol, "type": period})
    return parse_sina_rows(request_text(f"{SINA_ENDPOINT}?{query}"), 35, f"{period}分钟")


def fetch_sina_bars(symbol: str) -> list[dict[str, Any]]:
    return fetch_sina_minute_bars(symbol, "5")


def fetch_sina_daily_bars(symbol: str, now: datetime) -> list[dict[str, Any]]:
    date_key = now.strftime("%Y_%m_%d")
    variable = urllib.parse.quote(f"{symbol}{date_key}", safe="")
    url = SINA_DAILY_ENDPOINT.format(variable=variable)
    query = urllib.parse.urlencode({"symbol": symbol, "type": date_key})
    return parse_sina_rows(request_text(f"{url}?{query}"), 60, "日")


def fetch_csv_bars(relative_path: str, minimum: int = 35) -> list[dict[str, Any]]:
    path = ROOT / relative_path
    if not path.exists():
        raise FileNotFoundError(f"等待外部5分钟数据: {relative_path}")
    bars: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                bars.append(
                    {
                        "datetime": row["datetime"],
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row.get("volume") or 0),
                        "hold": float(row.get("hold") or 0),
                        "settle": float(row.get("settle") or 0),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue
    bars.sort(key=lambda row: row["datetime"])
    if len(bars) < minimum:
        raise ValueError(f"CSV有效K线不足: {len(bars)}")
    return bars


def ema(values: list[float], period: int) -> list[float]:
    alpha = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append(alpha * value + (1 - alpha) * result[-1])
    return result


def rolling_atr(bars: list[dict[str, Any]], period: int) -> list[float | None]:
    true_ranges: list[float] = []
    result: list[float | None] = []
    for index, bar in enumerate(bars):
        previous = bars[index - 1]["close"] if index else bar["close"]
        true_ranges.append(max(bar["high"] - bar["low"], abs(bar["high"] - previous), abs(bar["low"] - previous)))
        window = true_ranges[max(0, index - period + 1) : index + 1]
        result.append(statistics.fmean(window) if len(window) >= period else None)
    return result


def rolling_rsi(closes: list[float], period: int) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    for index in range(period, len(closes)):
        changes = [closes[j] - closes[j - 1] for j in range(index - period + 1, index + 1)]
        gains = sum(max(change, 0) for change in changes) / period
        losses = sum(max(-change, 0) for change in changes) / period
        result[index] = 100.0 if losses == 0 else 100 - (100 / (1 + gains / losses))
    return result


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def daily_four_factor(bars: list[dict[str, Any]], index: int, config: dict[str, Any]) -> dict[str, Any] | None:
    """Screenshot-compatible daily model: mom5, 20D breakout, MA20, RSI14."""
    momentum_bars = int(config["daily_momentum_bars"])
    lookback = int(config["daily_breakout_bars"])
    ma_period = int(config["daily_ma_period"])
    rsi_period = int(config["rsi_period"])
    if index < max(momentum_bars, lookback, ma_period, rsi_period):
        return None
    closes = [bar["close"] for bar in bars[: index + 1]]
    close = closes[-1]
    momentum = close / closes[-momentum_bars - 1] - 1
    threshold = float(config["daily_momentum_threshold_pct"]) / 100
    prior = bars[index - lookback : index]
    prior_high = max(bar["high"] for bar in prior)
    prior_low = min(bar["low"] for bar in prior)
    ma20 = statistics.fmean(closes[-ma_period:])
    changes = [closes[j] - closes[j - 1] for j in range(len(closes) - rsi_period, len(closes))]
    gains = sum(max(change, 0) for change in changes) / rsi_period
    losses = sum(max(-change, 0) for change in changes) / rsi_period
    rsi = 100.0 if losses == 0 else 100 - (100 / (1 + gains / losses))
    long_factors = {
        "momentum": momentum > threshold,
        "breakout": close > prior_high,
        "trend": close > ma20,
        "rsi": 50 <= rsi <= 70,
    }
    short_factors = {
        "momentum": momentum < -threshold,
        "breakout": close < prior_low,
        "trend": close < ma20,
        "rsi": 30 <= rsi <= 50,
    }
    long_count = sum(long_factors.values())
    short_count = sum(short_factors.values())
    required = int(config["daily_required_factors"])
    if long_count >= required:
        signal = "long"
    elif short_count >= required:
        signal = "short"
    elif long_count == required - 1 and long_count > short_count:
        signal = "watch_long"
    elif short_count == required - 1 and short_count > long_count:
        signal = "watch_short"
    else:
        signal = "neutral"
    score = int(clamp((long_count - short_count) * 25, -100, 100))
    return {
        "score": score,
        "signal": signal,
        "momentum_pct": momentum * 100,
        "threshold_pct": threshold * 100,
        "ma20": ma20,
        "rsi": rsi,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "long_factors": long_factors,
        "short_factors": short_factors,
        "long_count": long_count,
        "short_count": short_count,
    }


def aggregate_weekly(daily_bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weeks: list[dict[str, Any]] = []
    current_key: tuple[int, int] | None = None
    for bar in daily_bars:
        day = datetime.fromisoformat(bar["datetime"])
        iso = day.isocalendar()
        key = (iso.year, iso.week)
        if key != current_key:
            weeks.append(dict(bar))
            current_key = key
        else:
            week = weeks[-1]
            week["high"] = max(week["high"], bar["high"])
            week["low"] = min(week["low"], bar["low"])
            week["close"] = bar["close"]
            week["volume"] += bar["volume"]
            week["hold"] = bar["hold"]
            week["settle"] = bar.get("settle", 0)
            week["datetime"] = bar["datetime"]
    return weeks


def timeframe_trend(bars: list[dict[str, Any]], label: str) -> dict[str, Any]:
    closes = [bar["close"] for bar in bars]
    if len(closes) < 25:
        raise ValueError(f"{label}趋势数据不足")
    fast = ema(closes, 8)[-1]
    slow = ema(closes, 21)[-1]
    rsi = rolling_rsi(closes, 14)[-1]
    momentum = closes[-1] / closes[-6] - 1
    direction_votes = [1 if momentum > 0 else -1 if momentum < 0 else 0,
                       1 if closes[-1] > fast > slow else -1 if closes[-1] < fast < slow else 0,
                       1 if rsi is not None and rsi >= 55 else -1 if rsi is not None and rsi <= 45 else 0]
    score = sum(direction_votes)
    if score >= 2:
        signal = "strong_long"
    elif score == 1:
        signal = "long"
    elif score <= -2:
        signal = "strong_short"
    elif score == -1:
        signal = "short"
    else:
        signal = "neutral"
    return {"signal": signal, "vote": score, "momentum_pct": momentum * 100, "ema8": fast, "ema21": slow, "rsi": rsi}


def percentage_change(values: list[float], bars: int) -> float | None:
    if len(values) <= bars or values[-bars - 1] == 0:
        return None
    return (values[-1] / values[-bars - 1] - 1) * 100


def technical_snapshot(bars: list[dict[str, Any]]) -> dict[str, Any]:
    """Latest mainstream daily indicators, kept separate from the core signal."""
    closes = [bar["close"] for bar in bars]
    highs = [bar["high"] for bar in bars]
    lows = [bar["low"] for bar in bars]
    volumes = [bar["volume"] for bar in bars]
    if len(bars) < 60:
        raise ValueError("技术指标预热数据不足")

    ma20 = statistics.fmean(closes[-20:])
    ma60 = statistics.fmean(closes[-60:])
    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    macd_line = [fast - slow for fast, slow in zip(ema12, ema26)]
    macd_signal = ema(macd_line, 9)
    macd_hist = (macd_line[-1] - macd_signal[-1]) * 2
    atr14 = rolling_atr(bars, 14)[-1] or 0.0
    rsi14 = rolling_rsi(closes, 14)[-1]

    middle = ma20
    deviation = statistics.pstdev(closes[-20:])
    boll_upper, boll_lower = middle + 2 * deviation, middle - 2 * deviation
    boll_position = 50.0 if boll_upper == boll_lower else (closes[-1] - boll_lower) / (boll_upper - boll_lower) * 100

    k_values: list[float] = []
    for index in range(8, len(bars)):
        high9 = max(highs[index - 8 : index + 1])
        low9 = min(lows[index - 8 : index + 1])
        k_values.append(50.0 if high9 == low9 else (closes[index] - low9) / (high9 - low9) * 100)
    k_value = k_values[-1]
    d_value = statistics.fmean(k_values[-3:])

    typical = [(bar["high"] + bar["low"] + bar["close"]) / 3 for bar in bars]
    tp20 = typical[-20:]
    tp_mean = statistics.fmean(tp20)
    mean_deviation = statistics.fmean(abs(value - tp_mean) for value in tp20)
    cci20 = 0.0 if mean_deviation == 0 else (typical[-1] - tp_mean) / (0.015 * mean_deviation)

    # ADX14: simple rolling directional-movement implementation for the latest value.
    true_ranges, plus_dm, minus_dm = [], [], []
    for index in range(1, len(bars)):
        up = highs[index] - highs[index - 1]
        down = lows[index - 1] - lows[index]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        true_ranges.append(max(highs[index] - lows[index], abs(highs[index] - closes[index - 1]), abs(lows[index] - closes[index - 1])))
    dx_values = []
    for index in range(13, len(true_ranges)):
        tr_sum = sum(true_ranges[index - 13 : index + 1])
        plus_di = 0.0 if tr_sum == 0 else 100 * sum(plus_dm[index - 13 : index + 1]) / tr_sum
        minus_di = 0.0 if tr_sum == 0 else 100 * sum(minus_dm[index - 13 : index + 1]) / tr_sum
        denominator = plus_di + minus_di
        dx_values.append(0.0 if denominator == 0 else 100 * abs(plus_di - minus_di) / denominator)
    adx14 = statistics.fmean(dx_values[-14:])

    obv = [0.0]
    for index in range(1, len(bars)):
        direction = 1 if closes[index] > closes[index - 1] else -1 if closes[index] < closes[index - 1] else 0
        obv.append(obv[-1] + direction * volumes[index])
    obv_change = obv[-1] - obv[-21]
    normal_volume = statistics.fmean(volumes[-21:-1])
    volume_ratio = volumes[-1] / normal_volume if normal_volume > 0 else None

    def side(value: float, positive: float = 0.0, negative: float = 0.0) -> str:
        return "long" if value > positive else "short" if value < negative else "neutral"

    return {
        "groups": {
            "trend": [
                {"name": "均线系统", "value": f"MA20 {ma20:.2f} / MA60 {ma60:.2f}", "signal": "long" if closes[-1] > ma20 > ma60 else "short" if closes[-1] < ma20 < ma60 else "neutral", "note": "收盘与中长期均线排列"},
                {"name": "MACD", "value": f"DIF {macd_line[-1]:.2f} / 柱 {macd_hist:.2f}", "signal": side(macd_hist), "note": "EMA12、EMA26、Signal9"},
                {"name": "ADX14", "value": f"{adx14:.1f}", "signal": "long" if adx14 >= 25 and closes[-1] > ma20 else "short" if adx14 >= 25 and closes[-1] < ma20 else "neutral", "note": "≥25 表示趋势较明确"},
            ],
            "momentum": [
                {"name": "ROC5", "value": f"{percentage_change(closes, 5):+.2f}%", "signal": side(percentage_change(closes, 5) or 0), "note": "5日价格动量"},
                {"name": "RSI14", "value": f"{rsi14:.1f}", "signal": "long" if rsi14 and rsi14 >= 55 else "short" if rsi14 and rsi14 <= 45 else "neutral", "note": "超买超卖与强弱区间"},
                {"name": "KDJ(9,3)", "value": f"K {k_value:.1f} / D {d_value:.1f}", "signal": "long" if k_value > d_value else "short" if k_value < d_value else "neutral", "note": "随机指标交叉"},
                {"name": "CCI20", "value": f"{cci20:.1f}", "signal": "long" if cci20 > 100 else "short" if cci20 < -100 else "neutral", "note": "±100 为强弱参考阈值"},
            ],
            "volatility": [
                {"name": "ATR14", "value": f"{atr14:.2f} ({atr14 / closes[-1] * 100:.2f}%)", "signal": "neutral", "note": "真实波幅，用于风险线"},
                {"name": "布林带20,2", "value": f"位置 {boll_position:.1f}%", "signal": "long" if closes[-1] > boll_upper else "short" if closes[-1] < boll_lower else "neutral", "note": f"上 {boll_upper:.2f} / 下 {boll_lower:.2f}"},
                {"name": "唐奇安20", "value": f"高 {max(highs[-21:-1]):.2f} / 低 {min(lows[-21:-1]):.2f}", "signal": "long" if closes[-1] > max(highs[-21:-1]) else "short" if closes[-1] < min(lows[-21:-1]) else "neutral", "note": "20日区间突破"},
            ],
            "volume_position": [
                {"name": "量比20", "value": "—" if volume_ratio is None else f"{volume_ratio:.2f}×", "signal": "long" if volume_ratio and volume_ratio >= 1.2 and closes[-1] >= closes[-2] else "short" if volume_ratio and volume_ratio >= 1.2 and closes[-1] < closes[-2] else "neutral", "note": "当日量 / 前20日均量"},
                {"name": "OBV20方向", "value": f"{obv_change:+.0f}", "signal": side(obv_change), "note": "成交量累积方向，不代表资金流"},
            ],
        },
        "values": {"ma20": ma20, "ma60": ma60, "atr14": atr14, "boll_upper": boll_upper, "boll_lower": boll_lower, "adx14": adx14},
    }


def capital_bucket(price_month: float | None, position_week: float | None) -> str:
    """Classify price/OI quadrants from screenshot 2; OI remains a proxy."""
    if price_month is None or position_week is None or abs(price_month) < 0.15 or abs(position_week) < 0.15:
        return "divergence"
    if price_month > 0 and position_week > 0:
        return "trend_long"
    if price_month > 0 and position_week < 0:
        return "avoid"
    if price_month < 0 and position_week > 0:
        return "accumulate"
    return "weak"


def research_risk_levels(close: float, high20: float, low20: float, atr14: float) -> dict[str, float]:
    """Screenshot-compatible reference levels; these are not executable orders."""
    return {
        "entry_reference": close,
        "long_atr_stop": close - 2 * atr14,
        "short_atr_stop": close + 2 * atr14,
        "long_break_even_trigger": close * 1.002,
        "short_break_even_trigger": close * 0.998,
        "long_fib_trail": close + max(0.0, high20 - close) * 0.618,
        "short_fib_trail": close - max(0.0, close - low20) * 0.618,
    }


def prepare_bars(
    raw_bars: list[dict[str, Any]], asset: dict[str, Any], generated_at: datetime, timeframe: str
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    raw_count = len(raw_bars)
    unique = sorted({bar["datetime"]: bar for bar in raw_bars}.values(), key=lambda row: row["datetime"])
    local_now = generated_at.astimezone(ZoneInfo(asset["bar_timezone"]))
    if timeframe == "daily":
        today = local_now.date()
        after_close = local_now.time() >= dt_time(15, 5) and asset["exchange"] != "LME"
        bars = [bar for bar in unique if datetime.fromisoformat(bar["datetime"]).date() < today or
                (after_close and datetime.fromisoformat(bar["datetime"]).date() == today)]
    else:
        cutoff = local_now.replace(tzinfo=None)
        bars = [bar for bar in unique if datetime.fromisoformat(bar["datetime"]) <= cutoff]
    return bars, {"duplicate_bars_removed": raw_count - len(unique), "incomplete_bars_excluded": len(unique) - len(bars)}


def point_signal(
    bars: list[dict[str, Any]],
    index: int,
    config: dict[str, Any],
    ema_fast_values: list[float],
    ema_slow_values: list[float],
    atr_values: list[float | None],
    rsi_values: list[float | None],
) -> dict[str, Any] | None:
    lookback = int(config["history_bars"])
    momentum_bars = int(config["momentum_bars"])
    if index < max(lookback + 1, momentum_bars, int(config["atr_period"])):
        return None
    close = bars[index]["close"]
    atr = atr_values[index]
    rsi = rsi_values[index]
    if atr is None or rsi is None or atr <= 0 or close <= 0:
        return None
    previous = bars[index - momentum_bars]["close"]
    momentum = close / previous - 1
    atr_pct = atr / close
    dynamic_threshold = max(0.0008, 0.85 * atr_pct * math.sqrt(momentum_bars))
    prior = bars[index - lookback : index]
    prior_high = max(bar["high"] for bar in prior)
    prior_low = min(bar["low"] for bar in prior)
    channel_width = max(prior_high - prior_low, atr)
    channel_position = (close - prior_low) / channel_width
    volumes = [bar["volume"] for bar in bars[index - lookback : index]]
    typical_volume = statistics.median(volumes) if volumes else 0
    volume_ratio = bars[index]["volume"] / typical_volume if typical_volume > 0 else 1

    long_factors = {
        "momentum": momentum > dynamic_threshold,
        "breakout": close > prior_high,
        "trend": close > ema_fast_values[index] > ema_slow_values[index],
        "rsi": 55 <= rsi <= 75,
    }
    short_factors = {
        "momentum": momentum < -dynamic_threshold,
        "breakout": close < prior_low,
        "trend": close < ema_fast_values[index] < ema_slow_values[index],
        "rsi": 25 <= rsi <= 45,
    }

    momentum_component = 30 * clamp(momentum / (dynamic_threshold * 2), -1, 1)
    trend_component = 25 * clamp((ema_fast_values[index] - ema_slow_values[index]) / (atr * 1.5), -1, 1)
    breakout_component = 20 * clamp((channel_position - 0.5) * 2, -1, 1)
    rsi_component = 15 * clamp((rsi - 50) / 20, -1, 1)
    direction = 1 if momentum >= 0 else -1
    volume_component = 10 * direction * clamp((volume_ratio - 0.8) / 1.2, 0, 1)
    score = round(clamp(momentum_component + trend_component + breakout_component + rsi_component + volume_component, -100, 100))
    long_count = sum(long_factors.values())
    short_count = sum(short_factors.values())
    minimum = int(config["minimum_signal_score"])
    watch = int(config["watch_score"])
    if score >= minimum and long_count >= 3:
        signal = "long"
    elif score <= -minimum and short_count >= 3:
        signal = "short"
    elif abs(score) >= watch:
        signal = "watch_long" if score > 0 else "watch_short"
    else:
        signal = "neutral"
    return {
        "score": score,
        "signal": signal,
        "momentum_pct": momentum * 100,
        "threshold_pct": dynamic_threshold * 100,
        "ema_fast": ema_fast_values[index],
        "ema_slow": ema_slow_values[index],
        "rsi": rsi,
        "atr": atr,
        "atr_pct": atr_pct * 100,
        "prior_high": prior_high,
        "prior_low": prior_low,
        "volume_ratio": volume_ratio,
        "long_factors": long_factors,
        "short_factors": short_factors,
        "long_count": long_count,
        "short_count": short_count,
    }


def run_backtest(
    bars: list[dict[str, Any]],
    signals: list[dict[str, Any] | None],
    asset: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    maximum_hold = int(config["max_holding_bars"])
    cost = float(config["estimated_round_trip_cost_bps"]) / 10000
    tick = float(asset["tick"])
    returns: list[float] = []
    cursor = 1
    while cursor < len(bars) - 2:
        signal = signals[cursor]
        if not signal or signal["signal"] not in ("long", "short"):
            cursor += 1
            continue
        direction = 1 if signal["signal"] == "long" else -1
        entry_index = cursor + 1
        exit_index = min(entry_index + maximum_hold, len(bars) - 1)
        for probe in range(entry_index + 1, exit_index + 1):
            candidate = signals[probe]
            if candidate and ((direction == 1 and candidate["score"] <= -35) or (direction == -1 and candidate["score"] >= 35)):
                exit_index = probe
                break
        entry = bars[entry_index]["open"]
        exit_price = bars[exit_index]["close"]
        slippage = (2 * tick / entry) if entry else 0
        trade_return = direction * (exit_price / entry - 1) - cost - slippage
        returns.append(trade_return)
        cursor = exit_index + 1
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in returns:
        equity *= 1 + value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1)
    return {
        "trades": len(returns),
        "win_rate_pct": (sum(value > 0 for value in returns) / len(returns) * 100) if returns else None,
        "net_return_pct": (equity - 1) * 100 if returns else None,
        "max_drawdown_pct": max_drawdown * 100 if returns else None,
        "avg_trade_pct": statistics.fmean(returns) * 100 if returns else None,
        "note": "滚动样本内快速检验，开平各收万1.2并各计1跳滑点；不等同于独立样本回测。",
    }


def aggregate_strategy_performance(results: list[dict[str, Any]], generated_at: datetime, config: dict[str, Any]) -> dict[str, Any]:
    strategy_rows: dict[str, list[dict[str, Any]]] = {}
    names: dict[str, str] = {}
    for asset in results:
        for row in asset.get("strategy_comparison", []):
            if row.get("status") != "ok":
                continue
            strategy_rows.setdefault(row["key"], []).append(row)
            names[row["key"]] = row["name"]
    summary = []
    for key, rows in strategy_rows.items():
        trades = sum(row["trades"] for row in rows)
        winners = sum((row.get("win_rate_pct") or 0) * row["trades"] / 100 for row in rows)
        returns = [row["net_return_pct"] for row in rows]
        sharpes = [row["sharpe"] for row in rows]
        summary.append({
            "key": key, "name": names[key], "frequency": rows[0]["frequency"], "assets": len(rows),
            "trades": trades, "win_rate_pct": winners / trades * 100 if trades else None,
            "equal_weight_return_pct": statistics.fmean(returns), "median_return_pct": statistics.median(returns),
            "average_sharpe": statistics.fmean(sharpes), "positive_assets": sum(value > 0 for value in returns),
            "worst_drawdown_pct": min(row["max_drawdown_pct"] for row in rows),
            "sample_start": min(row["sample_start"] for row in rows), "sample_end": max(row["sample_end"] for row in rows),
        })
    summary.sort(key=lambda row: row["equal_weight_return_pct"], reverse=True)
    daily = next((row for row in summary if row["key"] == "daily_four_factor"), None)
    five = next((row for row in summary if row["key"] == "five_minute_momentum"), None)
    enough_intraday = bool(five and five["trades"] >= 30 and five["positive_assets"] >= 5)
    effective = bool(enough_intraday and five["equal_weight_return_pct"] > 0 and five["average_sharpe"] > 0.5 and (not daily or five["average_sharpe"] >= daily["average_sharpe"]))
    frequency = {
        "decision": "keep_5m" if effective else "alert_only",
        "headline": "保留5分钟执行扫描" if effective else "关闭5分钟定时交易信号，日线收盘刷新主模型",
        "reason": "5分钟样本在成本后仍具备跨品种正收益与风险调整优势。" if effective else "当前5分钟样本期较短，且尚未同时满足成本后收益、Sharpe和跨品种稳定性门槛。",
        "criteria": "5分钟总交易≥30、至少5个品种为正、等权收益>0、平均Sharpe>0.5且不低于日线四因子",
    }
    return {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "assumptions": {
            "commission": "开仓万1.2 + 平仓万1.2（单边0.012%，完整往返0.024%）",
            "slippage": "开仓1跳 + 平仓1跳",
            "execution": "信号K线收盘后生成，下一根K线开盘执行，避免前视",
            "price_series": "主力连续未复权；换月跳空会影响收益，结果仅供研究筛选",
            "portfolio": "跨品种汇总为各品种收益等权平均，不含保证金杠杆和资金容量约束",
        },
        "strategies": summary, "frequency_assessment": frequency,
        "sources": [
            {"name": "CTAAgents/FDT", "url": "https://github.com/CTAAgents/FDT", "adaptation": "参考DC20/DC55、布林带与MACD独立趋势子信号，构造透明通道共振基线"},
            {"name": "VeighNa CTA Strategy", "url": "https://github.com/vnpy/vnpy_ctastrategy", "adaptation": "参考通道突破、CCI、ATR止损及下一K线执行框架"},
        ],
        "per_asset": [{"id": item["id"], "name": item["name"], "strategies": item.get("strategy_comparison", [])} for item in results if item.get("strategy_comparison")],
    }


def update_paper_portfolio(results: list[dict[str, Any]], generated_at: datetime, config: dict[str, Any]) -> dict[str, Any]:
    """Forward-only paper ledger, persisted locally or through Actions cache."""
    valid = [item for item in results if item.get("price") and item.get("strategy_comparison")]
    strategy_names = {row["key"]: row["name"] for item in valid for row in item["strategy_comparison"] if row["key"] != "five_minute_momentum"}
    if PAPER_PATH.exists():
        try:
            with PAPER_PATH.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError):
            state = {}
    else:
        state = {}
    initial_cash = float(config.get("initial_paper_cash", 1_000_000))
    state.setdefault("started_at", generated_at.isoformat(timespec="seconds"))
    state.setdefault("initial_cash", initial_cash)
    state.setdefault("strategies", {})
    fee = float(config.get("commission_per_side_bps", 1.2)) / 10000
    slip_ticks = float(config.get("slippage_ticks_per_side", 1))
    weight = 1 / max(1, len(valid))
    output = []
    position_output: list[dict[str, Any]] = []
    recent_changes: list[dict[str, Any]] = []
    asset_by_symbol = {item["symbol"]: item for item in valid}
    tick_by_symbol = {item["symbol"]: float(item["tick"]) for item in config["assets"]}
    for key, name in strategy_names.items():
        ledger = state["strategies"].setdefault(key, {"name": name, "equity": initial_cash, "positions": {}, "last_prices": {}, "last_bars": {}, "trades": 0, "history": []})
        ledger.setdefault("entry_prices", {})
        ledger.setdefault("entry_times", {})
        ledger.setdefault("change_history", [])
        pnl, friction, changed = 0.0, 0.0, False
        for asset in valid:
            symbol, price = asset["symbol"], float(asset["price"])
            prior_price = ledger["last_prices"].get(symbol)
            prior_position = int(ledger["positions"].get(symbol, 0))
            target_row = next((row for row in asset["strategy_comparison"] if row["key"] == key), None)
            target = int(target_row.get("latest_target", 0)) if target_row else 0
            if prior_price and ledger["last_bars"].get(symbol) != asset["bar_time"]:
                pnl += weight * prior_position * (price / prior_price - 1)
                changed = True
            turnover = abs(target - prior_position)
            if turnover:
                friction += weight * turnover * (fee + slip_ticks * tick_by_symbol[symbol] / price)
                ledger["trades"] += 1 if prior_position == 0 or target != 0 else 0
                change_record = {
                    "time": generated_at.isoformat(timespec="seconds"),
                    "strategy_key": key,
                    "strategy": name,
                    "asset_id": asset["id"],
                    "asset": asset["name"],
                    "symbol": symbol,
                    "from_position": prior_position,
                    "to_position": target,
                    "price": price,
                    "bar_time": asset["bar_time"],
                }
                ledger["change_history"].append(change_record)
                recent_changes.append(change_record)
                if target:
                    ledger["entry_prices"][symbol] = price
                    ledger["entry_times"][symbol] = generated_at.isoformat(timespec="seconds")
                else:
                    ledger["entry_prices"].pop(symbol, None)
                    ledger["entry_times"].pop(symbol, None)
                changed = True
            ledger["positions"][symbol] = target
            ledger["last_prices"][symbol] = price
            ledger["last_bars"][symbol] = asset["bar_time"]
        ledger["change_history"] = ledger["change_history"][-200:]
        if changed or not ledger["history"]:
            ledger["equity"] *= max(0.01, 1 + pnl - friction)
            ledger["history"].append({"time": generated_at.isoformat(timespec="seconds"), "equity": ledger["equity"]})
            ledger["history"] = ledger["history"][-500:]
        peak, max_drawdown = 0.0, 0.0
        for point in ledger["history"]:
            peak = max(peak, point["equity"])
            if peak:
                max_drawdown = min(max_drawdown, point["equity"] / peak - 1)
        active_positions = 0
        for symbol, side in ledger["positions"].items():
            asset = asset_by_symbol.get(symbol)
            if not side or not asset:
                continue
            active_positions += 1
            entry_price = float(ledger["entry_prices"].get(symbol) or asset["price"])
            current_price = float(asset["price"])
            ledger["entry_prices"].setdefault(symbol, entry_price)
            ledger["entry_times"].setdefault(symbol, state["started_at"])
            position_output.append({
                "strategy_key": key,
                "strategy": name,
                "asset_id": asset["id"],
                "asset": asset["name"],
                "symbol": symbol,
                "side": int(side),
                "entry_price": entry_price,
                "current_price": current_price,
                "unrealized_pct": int(side) * (current_price / entry_price - 1) * 100 if entry_price else 0,
                "entry_time": ledger["entry_times"][symbol],
                "bar_time": asset["bar_time"],
            })
        output.append({"key": key, "name": name, "equity": ledger["equity"], "return_pct": (ledger["equity"] / initial_cash - 1) * 100, "max_drawdown_pct": max_drawdown * 100, "trades": ledger["trades"], "active_positions": active_positions, "observations": len(ledger["history"])})
    state["updated_at"] = generated_at.isoformat(timespec="seconds")
    atomic_json(PAPER_PATH, state)
    output.sort(key=lambda row: row["return_pct"], reverse=True)
    if not recent_changes:
        recent_changes = [record for ledger in state["strategies"].values() for record in ledger.get("change_history", [])]
    recent_changes.sort(key=lambda row: row["time"], reverse=True)
    position_output.sort(key=lambda row: (row["strategy"], row["asset"]))
    return {"mode": "forward_paper", "started_at": state["started_at"], "updated_at": state["updated_at"], "initial_cash": initial_cash, "strategies": output, "positions": position_output, "recent_changes": recent_changes[:30], "note": "无真实委托；按扫描时点最新价盯市，含开平各万1.2和每侧1跳。新加入品种从首次真实扫描建立仓位，不回填历史收益。"}


def classify_signal_change(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, str] | None:
    """Classify a cross-scan signal change without inventing observations."""
    prior_signal, signal = previous.get("signal"), current.get("signal")
    prior_daily, daily = previous.get("daily_signal"), current.get("daily_signal")
    prior_score, score = previous.get("score"), current.get("score")
    score_delta = abs(float(score) - float(prior_score)) if score is not None and prior_score is not None else 0
    side = lambda value: 1 if value in ("long", "watch_long") else -1 if value in ("short", "watch_short") else 0
    hard_side = lambda value: 1 if value == "long" else -1 if value == "short" else 0
    if side(prior_signal) * side(signal) == -1:
        return {"severity": "major", "reason": "综合信号多空直接反转"}
    if hard_side(prior_daily) * hard_side(daily) == -1:
        return {"severity": "major", "reason": "日线主模型多空直接反转"}
    if score_delta >= 50:
        return {"severity": "major", "reason": f"综合评分跃迁 {score_delta:.0f} 分"}
    if prior_signal != signal:
        return {"severity": "important" if side(prior_signal) != side(signal) else "normal", "reason": "综合信号状态变化"}
    if prior_daily != daily:
        return {"severity": "important", "reason": "日线主模型状态变化"}
    if score_delta >= 25:
        return {"severity": "important", "reason": f"综合评分变化 {score_delta:.0f} 分"}
    return None


def update_signal_change_log(results: list[dict[str, Any]], generated_at: datetime) -> dict[str, Any]:
    """Persist real cross-scan reversals; the first observation is only a baseline."""
    state: dict[str, Any] = {}
    if SIGNAL_STATE_PATH.exists():
        try:
            with SIGNAL_STATE_PATH.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError):
            state = {}
    state.setdefault("current", {})
    state.setdefault("events", [])
    if not state["current"] and LATEST_PATH.exists():
        try:
            with LATEST_PATH.open("r", encoding="utf-8") as handle:
                previous_payload = json.load(handle)
            state["current"] = {
                item["id"]: {key: item.get(key) for key in ("signal", "daily_signal", "score", "bar_time", "daily_date")}
                for item in previous_payload.get("assets", []) if item.get("signal") not in (None, "missing")
            }
        except (OSError, ValueError):
            pass
    new_events: list[dict[str, Any]] = []
    for asset in results:
        if asset.get("signal") in (None, "missing"):
            continue
        current = {key: asset.get(key) for key in ("signal", "daily_signal", "score", "bar_time", "daily_date")}
        previous = state["current"].get(asset["id"])
        observation_changed = previous and (previous.get("bar_time"), previous.get("daily_date")) != (current.get("bar_time"), current.get("daily_date"))
        classification = classify_signal_change(previous, current) if observation_changed else None
        if classification:
            event = {
                "event_id": f"{asset['id']}:{current.get('bar_time')}:{previous.get('signal')}>{current.get('signal')}",
                "time": generated_at.isoformat(timespec="seconds"),
                "observation": current.get("bar_time"),
                "asset_id": asset["id"], "asset": asset["name"], "symbol": asset["symbol"],
                "from_signal": previous.get("signal"), "to_signal": current.get("signal"),
                "from_score": previous.get("score"), "to_score": current.get("score"),
                "score_delta": (current.get("score") or 0) - (previous.get("score") or 0),
                "daily_from": previous.get("daily_signal"), "daily_to": current.get("daily_signal"),
                **classification,
            }
            if not any(existing.get("event_id") == event["event_id"] for existing in state["events"]):
                state["events"].append(event)
                new_events.append(event)
        state["current"][asset["id"]] = current
    state["events"] = state["events"][-200:]
    state["updated_at"] = generated_at.isoformat(timespec="seconds")
    atomic_json(SIGNAL_STATE_PATH, state)
    recent = list(reversed(state["events"][-30:]))
    return {
        "updated_at": state["updated_at"],
        "current_changes": new_events,
        "recent_events": recent,
        "major_count": sum(event["severity"] == "major" for event in new_events),
        "note": "仅比较不同真实行情时点；首次扫描建立基线，不生成虚构反转记录。",
    }


def analyze_asset(asset: dict[str, Any], config: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    base = {key: asset[key] for key in ("id", "name", "short_name", "symbol", "exchange", "sector", "provider", "decimals", "unit", "bar_timezone")}
    try:
        if asset["provider"] == "sina":
            raw_daily = fetch_sina_daily_bars(asset["symbol"], generated_at)
            raw_hourly = fetch_sina_minute_bars(asset["symbol"], "60")
            raw_five = fetch_sina_minute_bars(asset["symbol"], "5")
            source_mode = "live/intraday + derived close"
            source = "新浪财经日线/60分钟/5分钟（AKShare同源接口）"
        else:
            raw_daily = fetch_csv_bars(asset["daily_file"], 60)
            raw_hourly = fetch_csv_bars(asset["hourly_file"], 35)
            raw_five = fetch_csv_bars(asset["five_file"], 35)
            source_mode = "manual"
            source = "LME授权数据CSV"
        daily, daily_quality = prepare_bars(raw_daily, asset, generated_at, "daily")
        hourly, hourly_quality = prepare_bars(raw_hourly, asset, generated_at, "hourly")
        five, five_quality = prepare_bars(raw_five, asset, generated_at, "5m")
        for timeframe, raw in (("daily", raw_daily), ("60m", raw_hourly), ("5m", raw_five)):
            atomic_json(
                STATE_DIR / "raw" / f"{asset['symbol']}_{timeframe}.json",
                {
                    "retrieved_at": generated_at.isoformat(timespec="seconds"),
                    "source_mode": source_mode,
                    "source": SINA_DAILY_ENDPOINT if timeframe == "daily" and asset["provider"] == "sina" else
                              SINA_ENDPOINT if asset["provider"] == "sina" else asset.get(f"{timeframe}_file", "CSV"),
                    "symbol": asset["symbol"], "bar_timezone": asset["bar_timezone"], "bars": raw,
                },
            )
        if len(daily) < 60 or len(hourly) < 35 or len(five) < 35:
            raise ValueError("多周期指标预热数据不足")
        # Keep the full source response in state/raw, but bound model work to a
        # reproducible recent window so scheduled scans finish well within 5m.
        daily = daily[-600:]
        hourly = hourly[-600:]
        five = five[-1000:]

        daily_signals = [daily_four_factor(daily, index, config) for index in range(len(daily))]
        latest = daily_signals[-1]
        if latest is None:
            raise ValueError("日线四因子预热数据不足")
        hourly_trend = timeframe_trend(hourly, "小时")
        weekly = aggregate_weekly(daily)
        weekly_trend = timeframe_trend(weekly, "周")
        five_closes = [bar["close"] for bar in five]
        five_fast = ema(five_closes, int(config["ema_fast"]))
        five_slow = ema(five_closes, int(config["ema_slow"]))
        five_atr = rolling_atr(five, int(config["atr_period"]))
        five_rsi = rolling_rsi(five_closes, int(config["rsi_period"]))
        five_signal = point_signal(five, len(five) - 1, config, five_fast, five_slow, five_atr, five_rsi)
        if five_signal is None:
            raise ValueError("5分钟预警指标预热数据不足")

        confirmation = hourly_trend["vote"] + weekly_trend["vote"]
        if latest["signal"] == "long" and confirmation >= 0:
            overall_signal = "long"
        elif latest["signal"] == "short" and confirmation <= 0:
            overall_signal = "short"
        elif latest["signal"] == "watch_long" or confirmation >= 3:
            overall_signal = "watch_long"
        elif latest["signal"] == "watch_short" or confirmation <= -3:
            overall_signal = "watch_short"
        else:
            overall_signal = "neutral"
        overall_score = round(clamp(latest["score"] * 0.6 + hourly_trend["vote"] / 3 * 20 + weekly_trend["vote"] / 3 * 20, -100, 100))

        bar_time = datetime.fromisoformat(five[-1]["datetime"]).replace(tzinfo=ZoneInfo(asset["bar_timezone"]))
        age_minutes = max(0, (generated_at - bar_time.astimezone(generated_at.tzinfo)).total_seconds() / 60)
        status = "ok" if age_minutes <= 20 else "closed" if not in_research_session(generated_at) else "stale"
        sparkline = [
            {"time": bar["datetime"], "value": round(bar["close"], int(asset["decimals"]) + 2)}
            for bar in daily[-60:]
        ]
        daily_closes = [bar["close"] for bar in daily]
        holds = [bar["hold"] for bar in daily]
        returns = {"day": percentage_change(daily_closes, 1), "week": percentage_change(daily_closes, 5), "month": percentage_change(daily_closes, 20)}
        position_changes = {"day": percentage_change(holds, 1), "week": percentage_change(holds, 5), "month": percentage_change(holds, 20)}
        technical = technical_snapshot(daily)
        strategy_comparison = compare_strategies(daily, five, asset, config)
        result = {
            **base,
            **latest,
            "score": overall_score,
            "daily_score": latest["score"],
            "daily_signal": latest["signal"],
            "signal": overall_signal,
            "status": status,
            "status_text": "实时" if status == "ok" else "休市快照" if status == "closed" else "行情陈旧",
            "price": five_closes[-1],
            "bar_time": five[-1]["datetime"],
            "daily_date": daily[-1]["datetime"],
            "age_minutes": round(age_minutes, 1),
            "bars": {"daily": len(daily), "hourly": len(hourly), "five": len(five), "weekly": len(weekly)},
            "change_pct": (five_closes[-1] / daily_closes[-1] - 1) * 100,
            "returns": returns,
            "position_changes": position_changes,
            "capital_bucket": capital_bucket(returns["month"], position_changes["week"]),
            "technical_methods": technical["groups"],
            "risk_levels": research_risk_levels(daily_closes[-1], latest["prior_high"], latest["prior_low"], technical["values"]["atr14"]),
            "strategy_comparison": strategy_comparison,
            "timeframes": {"week": weekly_trend, "day": {"signal": latest["signal"], "vote": latest["long_count"] - latest["short_count"]}, "hour": hourly_trend, "five": {"signal": five_signal["signal"], "score": five_signal["score"]}},
            "sparkline": sparkline,
            "source": source,
            "quality": {
                "source_mode": source_mode,
                "daily": daily_quality, "hourly": hourly_quality, "five": five_quality,
                "last_actual_observation": five[-1]["datetime"],
                "last_daily_close": daily[-1]["datetime"],
                "zero_policy": "价格零值保留并在指标计算前校验；成交量/持仓量零值按真实观测保留",
            },
            "backtest": run_backtest(daily, daily_signals, asset, config),
        }
        return result
    except Exception as exc:
        return {
            **base,
            "status": "missing",
            "status_text": "缺少数据",
            "signal": "missing",
            "score": None,
            "price": None,
            "bar_time": None,
            "error": str(exc),
            "source": "新浪财经多周期行情" if asset["provider"] == "sina" else "外部授权CSV",
        }


def scan_once() -> dict[str, Any]:
    if not SCAN_LOCK.acquire(blocking=False):
        raise RuntimeError("扫描已在进行中")
    try:
        config = load_config()
        generated_at = datetime.now(ZoneInfo(config["timezone"]))
        results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            breadth_future = pool.submit(fetch_market_breadth, generated_at)
            futures = {pool.submit(analyze_asset, asset, config, generated_at): asset for asset in config["assets"]}
            for future in as_completed(futures):
                results.append(future.result())
            try:
                market_breadth = breadth_future.result()
            except Exception as exc:
                market_breadth = {"status": "error", "error": str(exc), "source": "新浪财经国内期货实时行情"}
        order = {asset["id"]: index for index, asset in enumerate(config["assets"])}
        results.sort(key=lambda item: order[item["id"]])
        counts = {key: sum(item.get("signal") == key for item in results) for key in ("long", "short", "watch_long", "watch_short", "neutral", "missing")}
        bucket_keys = ("trend_long", "avoid", "accumulate", "divergence", "weak")
        operation_summary = {
            key: [item["id"] for item in results if item.get("capital_bucket") == key]
            for key in bucket_keys
        }
        performance_report = aggregate_strategy_performance(results, generated_at, config)
        paper_trading = update_paper_portfolio(results, generated_at, config)
        signal_changes = update_signal_change_log(results, generated_at)
        payload = {
            "schema_version": 5,
            "generated_at": generated_at.isoformat(timespec="seconds"),
            "interval_seconds": int(config["scan_interval_seconds"]),
            "summary": counts,
            "operation_summary": operation_summary,
            "market_breadth": market_breadth,
            "performance": {key: performance_report[key] for key in ("assumptions", "strategies", "frequency_assessment", "sources")},
            "paper_trading": paper_trading,
            "signal_changes": signal_changes,
            "assets": results,
            "methodology": {
                "bar": "日线四因子为主；周线和60分钟确认趋势；5分钟仅作盘中预警",
                "factors": "mom5超过±3% / 突破20日高低 / 收盘相对MA20 / 日线RSI14区间",
                "decision": "日线四因子至少3项同向触发，再结合周线与小时线给出综合结论",
                "execution": "核心模型在交易日15:20后刷新；全市场行情在交易时段每15分钟刷新；5分钟策略不进入定时交易信号",
                "allocation": "七板块按主力连续当日涨跌广度与等权平均涨幅排序，相对做多前2、做空后2、其余中性",
                "technical": "主流指标层覆盖均线、MACD、ADX、ROC、RSI、KDJ、CCI、ATR、布林带、唐奇安、量比与OBV，仅作交叉验证",
                "risk": "研究参考线采用2×ATR初始止损、0.618动态跟踪与±0.2%保本触发；不自动下单",
                "cost": "所有策略对比统一按开仓万1.2、平仓万1.2，并在每一侧额外计1跳滑点",
            },
            "warnings": [
                "主连换月可能产生跳空，生产使用前应接入后复权连续合约或固定主力合约。",
                "持仓变化是主连持仓量代理，换月附近不可直接解释为资金净流入或流出。",
                "七板块配置是当日截面相对强弱，不等同于全部板块已经完成日线策略回测。",
                "黑色板块覆盖螺纹钢、热卷、线材、不锈钢、铁矿石、焦炭、焦煤、硅铁与锰硅主力连续。",
                "公开接口可能限流或中断；实盘研究建议切换至iFinD、Wind、CTP或交易所授权源。",
            ],
        }
        atomic_json(PERFORMANCE_PATH, performance_report)
        atomic_json(LATEST_PATH, payload)
        append_history(payload)
        return payload
    finally:
        SCAN_LOCK.release()


def in_research_session(now: datetime | None = None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5:
        return False
    current = now.time()
    windows = (
        (dt_time(8, 55), dt_time(11, 35)),
        (dt_time(13, 25), dt_time(15, 5)),
        (dt_time(20, 55), dt_time(23, 59, 59)),
        (dt_time(0, 0), dt_time(2, 35)),
    )
    return any(start <= current <= end for start, end in windows)


class MonitorHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(DIST), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        sys.stdout.write("[%s] %s\n" % (self.log_date_time_string(), format % args))

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] == "/api/status":
            self.send_json(read_latest())
            return
        super().do_GET()

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/api/scan":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            self.send_json(scan_once())
        except RuntimeError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def read_latest() -> dict[str, Any]:
    if not LATEST_PATH.exists():
        return scan_once()
    with LATEST_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def refresh_breadth_only() -> dict[str, Any]:
    """Refresh intraday market breadth without rerunning daily signals or paper books."""
    if not SCAN_LOCK.acquire(blocking=False):
        raise RuntimeError("扫描已在进行中")
    try:
        config = load_config()
        generated_at = datetime.now(ZoneInfo(config["timezone"]))
        payload = read_latest()
        payload["market_breadth"] = fetch_market_breadth(generated_at)
        payload["interval_seconds"] = int(config["scan_interval_seconds"])
        atomic_json(LATEST_PATH, payload)
        return payload["market_breadth"]
    finally:
        SCAN_LOCK.release()


def scheduler_loop(interval: int) -> None:
    last_breadth_slot: int | None = None
    last_daily_date: str | None = None
    while True:
        delay = 60 - (time.time() % 60) + 2
        time.sleep(delay)
        now = datetime.now(ZoneInfo(load_config()["timezone"]))
        slot = int(now.timestamp()) // max(300, interval)
        if in_research_session(now) and slot != last_breadth_slot:
            try:
                refresh_breadth_only()
                last_breadth_slot = slot
                print(f"[{now.isoformat(timespec='seconds')}] breadth refresh complete")
            except Exception:
                traceback.print_exc()
        if now.weekday() < 5 and dt_time(15, 20) <= now.time() <= dt_time(15, 35) and last_daily_date != now.date().isoformat():
            try:
                scan_once()
                last_daily_date = now.date().isoformat()
                print(f"[{now.isoformat(timespec='seconds')}] daily close scan complete")
            except Exception:
                traceback.print_exc()


def main() -> int:
    parser = argparse.ArgumentParser(description="期货日线动量与全市场广度监控")
    parser.add_argument("--scan", action="store_true", help="立即扫描一次")
    parser.add_argument("--breadth-only", action="store_true", help="仅刷新全市场涨跌广度")
    parser.add_argument("--serve", action="store_true", help="启动本地看板")
    parser.add_argument("--schedule", action="store_true", help="服务运行时每15分钟更新广度、收盘后更新核心模型")
    parser.add_argument("--respect-session", action="store_true", help="非研究时段跳过单次扫描")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    args = parser.parse_args()
    if not args.scan and not args.breadth_only and not args.serve:
        args.scan = True
    if args.breadth_only:
        breadth = refresh_breadth_only()
        print(json.dumps({key: breadth.get(key) for key in ("generated_at", "valid", "up", "down", "flat")}, ensure_ascii=False))
    if args.scan:
        if args.respect_session and not in_research_session():
            print("当前不在扫描时段，已跳过。")
        else:
            payload = scan_once()
            print(json.dumps({"generated_at": payload["generated_at"], "summary": payload["summary"]}, ensure_ascii=False))
    if args.serve:
        config = load_config()
        if not LATEST_PATH.exists():
            scan_once()
        if args.schedule:
            thread = threading.Thread(target=scheduler_loop, args=(int(config["scan_interval_seconds"]),), daemon=True)
            thread.start()
        server = ThreadingHTTPServer((args.host, args.port), MonitorHandler)
        print(f"Momentum monitor: http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
