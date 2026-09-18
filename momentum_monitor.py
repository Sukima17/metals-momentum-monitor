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
PAPER_REPORT_PATH = DIST / "paper-history.json"
FORWARD_WATCH_PATH = STATE_DIR / "strategy_forward_watch.json"
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
SINA_CONTRACT_TABLE_ENDPOINT = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQFuturesData"
)
EASTMONEY_KLINE_ENDPOINT = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_WEIGHTED_CODES = {
    "CU0": "cufi", "AL0": "alfi", "PB0": "pbfi", "ZN0": "znfi", "NI0": "nifi",
    "LC0": "lcfi", "SN0": "snfi", "AU0": "aufi", "AG0": "agfi", "RB0": "rbfi",
    "HC0": "hcfi", "SS0": "ssfi", "I0": "ifi", "J0": "jfi", "JM0": "jmfi",
    "SF0": "sffi", "SM0": "smfi",
}
SINA_MARKET_NODES = {
    "CU0": "tong_qh", "AL0": "lv_qh", "PB0": "qian_qh", "ZN0": "xing_qh",
    "NI0": "ni_qh", "LC0": "lc_qh", "SN0": "xi_qh", "AU0": "hj_qh",
    "AG0": "by_qh", "RB0": "lwg_qh", "HC0": "rzjb_qh", "SS0": "bxg_qh",
    "I0": "tks_qh", "J0": "jt_qh", "JM0": "jm_qh", "SF0": "gt_qh",
    "SM0": "mg_qh",
}
WRITE_LOCK = threading.Lock()
SCAN_LOCK = threading.Lock()
OI_FALLBACK_SEMAPHORE = threading.Semaphore(2)
EASTMONEY_SEMAPHORE = threading.Semaphore(1)
EASTMONEY_STATE_LOCK = threading.Lock()
EASTMONEY_FAILURE_UNTIL = 0.0


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


def request_eastmoney_text(url: str) -> str:
    global EASTMONEY_FAILURE_UNTIL
    with EASTMONEY_STATE_LOCK:
        if time.time() < EASTMONEY_FAILURE_UNTIL:
            raise RuntimeError("东方财富接口短暂熔断，转用全合约加总")
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json, text/plain, */*",
        },
    )
    with EASTMONEY_SEMAPHORE:
        for attempt in range(3):
            try:
                with urllib.request.urlopen(request, timeout=18) as response:
                    payload = response.read().decode("utf-8", errors="replace")
                    time.sleep(0.25)
                    return payload
            except Exception:
                if attempt == 2:
                    with EASTMONEY_STATE_LOCK:
                        EASTMONEY_FAILURE_UNTIL = time.time() + 300
                    raise
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("东方财富行情请求失败")


def fetch_sina_minute_bars(symbol: str, period: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"symbol": symbol, "type": period})
    return parse_sina_rows(request_text(f"{SINA_ENDPOINT}?{query}"), 35, f"{period}分钟")


def fetch_sina_bars(symbol: str) -> list[dict[str, Any]]:
    return fetch_sina_minute_bars(symbol, "5")


def fetch_sina_daily_bars(symbol: str, now: datetime, minimum: int = 60) -> list[dict[str, Any]]:
    date_key = now.strftime("%Y_%m_%d")
    variable = urllib.parse.quote(f"{symbol}{date_key}", safe="")
    url = SINA_DAILY_ENDPOINT.format(variable=variable)
    query = urllib.parse.urlencode({"symbol": symbol, "type": date_key})
    return parse_sina_rows(request_text(f"{url}?{query}"), minimum, "日")


def fetch_sina_contract_table(node: str) -> list[dict[str, Any]]:
    """Return live individual contracts for one variety, sorted by open interest."""
    query = urllib.parse.urlencode({"page": 1, "num": 50, "sort": "position", "asc": 0, "node": node, "base": "futures"})
    values = json.loads(request_text(f"{SINA_CONTRACT_TABLE_ENDPOINT}?{query}"))
    rows: list[dict[str, Any]] = []
    for row in values if isinstance(values, list) else []:
        symbol = str(row.get("symbol") or "").upper()
        if not re.fullmatch(r"[A-Z]+\d{3,4}", symbol):
            continue
        try:
            position = float(row.get("position") or 0)
            trade = float(row.get("trade") or 0)
            bid = float(row.get("bidprice1") or 0)
            ask = float(row.get("askprice1") or 0)
            volume = float(row.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        if position > 0:
            rows.append({
                "symbol": symbol, "name": str(row.get("name") or symbol), "position": position,
                "trade": trade, "bid": bid, "ask": ask, "volume": volume,
                "tradedate": str(row.get("tradedate") or ""), "ticktime": str(row.get("ticktime") or ""),
                "exchange": str(row.get("exchange") or "").upper(),
            })
    return sorted(rows, key=lambda item: item["position"], reverse=True)


def contract_expiry_month(symbol: str, now: datetime) -> tuple[int, int] | None:
    """Parse a listed domestic futures symbol into a comparable year/month."""
    match = re.search(r"(\d{3,4})$", symbol.upper())
    if not match:
        return None
    digits = match.group(1)
    month = int(digits[-2:])
    if month < 1 or month > 12:
        return None
    if len(digits) == 4:
        year = 2000 + int(digits[:2])
    else:
        year = (now.year // 10) * 10 + int(digits[0])
        if year < now.year - 1:
            year += 10
    return year, month


def nearby_calendar_spread(symbol: str, now: datetime, tick: float) -> dict[str, Any]:
    """Build a synchronized near-minus-next spread from live bid/ask midpoints."""
    node = SINA_MARKET_NODES.get(symbol)
    if not node:
        raise ValueError(f"缺少{symbol}的新浪品种节点")
    table = fetch_sina_contract_table(node)
    current_month = (now.year, now.month)
    candidates: list[dict[str, Any]] = []
    for row in table:
        expiry = contract_expiry_month(row["symbol"], now)
        if not expiry or expiry < current_month or row["bid"] <= 0 or row["ask"] <= 0 or row["ask"] < row["bid"]:
            continue
        candidates.append({**row, "expiry": expiry, "mid": (row["bid"] + row["ask"]) / 2})
    candidates.sort(key=lambda item: (item["expiry"], item["symbol"]))
    if len(candidates) < 2:
        raise ValueError(f"{symbol}缺少两个有效近月买卖盘")
    near, far = candidates[0], candidates[1]
    if near["tradedate"] != far["tradedate"]:
        raise ValueError(f"近远月交易日不一致: {near['tradedate']} / {far['tradedate']}")
    try:
        near_time = datetime.strptime(near["ticktime"], "%H:%M:%S")
        far_time = datetime.strptime(far["ticktime"], "%H:%M:%S")
        quote_gap_seconds = abs((near_time - far_time).total_seconds())
    except ValueError:
        quote_gap_seconds = None
    if quote_gap_seconds is not None and quote_gap_seconds > 900:
        raise ValueError(f"近远月报价时间差过大: {quote_gap_seconds:.0f}秒")
    month_gap = (far["expiry"][0] - near["expiry"][0]) * 12 + far["expiry"][1] - near["expiry"][1]
    if month_gap <= 0:
        raise ValueError("近远月到期顺序异常")
    spread = near["mid"] - far["mid"]
    spread_pct = spread / far["mid"] * 100 if far["mid"] else None
    annualized_pct = spread_pct * 12 / month_gap if spread_pct is not None else None
    neutral_band = max(float(tick) * 2, far["mid"] * 0.0002)
    structure = "backwardation" if spread > neutral_band else "contango" if spread < -neutral_band else "flat"
    structure_side = 1 if structure == "backwardation" else -1 if structure == "contango" else 0
    return {
        "status": "ok", "near_symbol": near["symbol"], "far_symbol": far["symbol"],
        "near_price": near["mid"], "far_price": far["mid"], "near_trade": near["trade"], "far_trade": far["trade"],
        "near_bid": near["bid"], "near_ask": near["ask"], "far_bid": far["bid"], "far_ask": far["ask"],
        "spread": spread, "spread_pct": spread_pct, "annualized_pct": annualized_pct,
        "short_near_long_far": near["bid"] - far["ask"],
        "long_near_short_far": near["ask"] - far["bid"],
        "month_gap": month_gap, "structure": structure, "structure_side": structure_side,
        "as_of": f"{near['tradedate']} {min(near['ticktime'], far['ticktime'])}",
        "quote_gap_seconds": quote_gap_seconds,
        "source_mode": "live/intraday",
        "source": "新浪财经分月合约实时买一/卖一",
        "formula": "月差=近月买卖盘中值−次近月买卖盘中值；近月高于远月为BACKWARDATION，近月低于远月为CONTANGO；年化为按月份间隔线性折算的研究代理。",
        "execution_note": "结构判断使用同步买卖盘中值；另保留卖近买远=近月买一−远月卖一、买近卖远=近月卖一−远月买一的可成交边界，不与收盘/结算价混用。",
    }


def fetch_eastmoney_weighted_oi(symbol: str, now: datetime) -> dict[str, Any]:
    """Read the vendor's variety-weighted series and use its aggregate OI field."""
    code = EASTMONEY_WEIGHTED_CODES.get(symbol)
    if not code:
        raise ValueError(f"缺少{symbol}的加权合约代码")
    query = urllib.parse.urlencode({
        "secid": f"159.{code}", "klt": "101", "fqt": "1", "lmt": "120", "end": "20500000",
        "iscca": "1", "fields1": "f1,f2,f3,f4,f5,f6,f7,f8",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64",
        "ut": "7eea3edcaed734bea9cbfc24409ed989", "forcect": "1",
    })
    payload = json.loads(request_eastmoney_text(f"{EASTMONEY_KLINE_ENDPOINT}?{query}"))
    data = payload.get("data") or {}
    rows: list[dict[str, Any]] = []
    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    for raw in data.get("klines") or []:
        fields = raw.split(",")
        try:
            bar_date = datetime.fromisoformat(fields[0]).date()
            completed = bar_date < local_now.date() or (bar_date == local_now.date() and local_now.time() >= dt_time(15, 5))
            hold = float(fields[12])
            if completed and hold > 0:
                rows.append({"datetime": fields[0], "hold": hold})
        except (IndexError, TypeError, ValueError):
            continue
    if len(rows) < 21:
        raise ValueError(f"{data.get('name') or code}有效持仓历史不足: {len(rows)}")
    holds = [row["hold"] for row in rows]
    return {
        "day": percentage_change(holds, 1), "week": percentage_change(holds, 5),
        "month": percentage_change(holds, 20), "mode": "weighted_contract",
        "series_name": str(data.get("name") or f"{symbol}加权"), "series_code": code,
        "contract_count": None, "coverage_pct": 100.0, "constituents": [],
        "as_of": rows[-1]["datetime"],
        "method": "优先直接读取东方财富品种加权合约的持仓量字段；该字段为品种各分月合约汇总持仓，连续合约不重复计入",
        "source": "东方财富期货加权合约日线",
    }


def aggregate_oi_change(contract_bars: list[dict[str, Any]], lag: int) -> float | None:
    """Change in the sum of OI across all eligible listed contracts."""
    eligible: list[tuple[float, float]] = []
    for item in contract_bars:
        bars = item.get("bars") or []
        if len(bars) <= lag:
            continue
        previous = float(bars[-1 - lag].get("hold") or 0)
        current = float(bars[-1].get("hold") or 0)
        if previous > 0 and current >= 0:
            eligible.append((previous, current))
    previous_total = sum(previous for previous, _ in eligible)
    current_total = sum(current for _, current in eligible)
    return (current_total / previous_total - 1) * 100 if previous_total else None


def _aggregate_open_interest_changes(symbol: str, now: datetime) -> dict[str, Any]:
    """Build a rollover-resistant OI proxy by summing every listed individual contract."""
    node = SINA_MARKET_NODES.get(symbol)
    if not node:
        raise ValueError(f"缺少{symbol}的新浪品种节点")
    table = fetch_sina_contract_table(node)
    if len(table) < 2:
        raise ValueError(f"{symbol}有效分月合约不足2个")
    total_position = sum(row["position"] for row in table)
    series: list[dict[str, Any]] = []
    failures: list[str] = []
    for row in table:
        try:
            bars = fetch_sina_daily_bars(row["symbol"], now, 6)
            series.append({**row, "bars": bars})
        except Exception as exc:
            failures.append(f"{row['symbol']}: {exc}")
    if len(series) < 2:
        raise ValueError("分月合约历史不足，无法形成全合约汇总口径")
    covered = sum(row["position"] for row in series)
    constituents = []
    for row in series:
        bars = row["bars"]
        constituents.append({
            "symbol": row["symbol"], "name": row["name"],
            "latest_hold": bars[-1]["hold"],
            "share_pct": row["position"] / covered * 100 if covered else None,
            "week_change_pct": percentage_change([bar["hold"] for bar in bars], 5),
            "as_of": bars[-1]["datetime"],
        })
    return {
        "day": aggregate_oi_change(series, 1),
        "week": aggregate_oi_change(series, 5),
        "month": aggregate_oi_change(series, 20),
        "mode": "aggregate_all_contracts",
        "contract_count": len(series),
        "coverage_pct": covered / total_position * 100 if total_position else None,
        "constituents": constituents,
        "as_of": min(row["bars"][-1]["datetime"] for row in series),
        "method": "将当前挂牌且取得历史数据的全部分月合约持仓量逐日求和，再计算品种总持仓的1/5/20日变化；连续合约本身不重复计入",
        "failures": failures,
    }


def aggregate_open_interest_changes(symbol: str, now: datetime) -> dict[str, Any]:
    """Limit fallback concurrency because it requires one history call per listed contract."""
    with OI_FALLBACK_SEMAPHORE:
        return _aggregate_open_interest_changes(symbol, now)


def cached_position_changes(asset_id: str, as_of: str) -> dict[str, Any] | None:
    """Reuse a same-day aggregate after a transient vendor failure to avoid hammering contract endpoints."""
    if not LATEST_PATH.exists():
        return None
    try:
        with LATEST_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        asset = next((row for row in payload.get("assets", []) if row.get("id") == asset_id), None)
        position = (asset or {}).get("position_changes") or {}
        if position.get("mode") in ("weighted_contract", "aggregate_all_contracts") and position.get("as_of") == as_of:
            return dict(position)
    except (OSError, ValueError):
        pass
    return None


def cached_calendar_spread(asset_id: str, now: datetime) -> dict[str, Any] | None:
    """Reuse a recent real spread snapshot only after a live-source failure."""
    if not LATEST_PATH.exists():
        return None
    try:
        with LATEST_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        generated = datetime.fromisoformat(payload.get("generated_at"))
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=now.tzinfo)
        if abs((now - generated.astimezone(now.tzinfo)).total_seconds()) > 24 * 3600:
            return None
        asset = next((row for row in payload.get("assets", []) if row.get("id") == asset_id), None)
        spread = (asset or {}).get("calendar_spread") or {}
        if spread.get("status") == "ok":
            cached = dict(spread)
            cached["source_mode"] = "terminal cache after live error"
            cached["cache_generated_at"] = payload.get("generated_at")
            return cached
    except (OSError, TypeError, ValueError):
        pass
    return None


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


def load_cached_raw_bars(symbol: str, timeframe: str, minimum: int) -> tuple[list[dict[str, Any]], str]:
    path = STATE_DIR / "raw" / f"{symbol}_{timeframe}.json"
    if not path.exists():
        raise FileNotFoundError(f"{symbol} {timeframe}无本地原始行情缓存")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    bars = payload.get("bars") or []
    if len(bars) < minimum:
        raise ValueError(f"{symbol} {timeframe}缓存K线不足: {len(bars)}")
    return bars, str(payload.get("retrieved_at") or "未知")


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
    """Classify price/OI quadrants without treating OI as directional capital flow."""
    if price_month is None or position_week is None or abs(price_month) < 0.15 or abs(position_week) < 0.15:
        return "divergence"
    if price_month > 0 and position_week > 0:
        return "trend_long"
    if price_month > 0 and position_week < 0:
        return "warn_long"
    if price_month < 0 and position_week > 0:
        return "trend_short"
    return "warn_short"


def research_risk_levels(
    close: float,
    high20: float,
    low20: float,
    atr14: float,
    ma20: float | None = None,
    boll_upper: float | None = None,
    boll_lower: float | None = None,
) -> dict[str, Any]:
    """Screenshot-compatible reference levels; these are not executable orders."""
    raw_levels = [
        ("20日低点", low20), ("MA20", ma20), ("布林下轨", boll_lower),
        ("20日高点", high20), ("布林上轨", boll_upper),
    ]

    def select_levels(is_support: bool) -> list[dict[str, float | str]]:
        candidates = [(name, float(value)) for name, value in raw_levels if value is not None and (value <= close if is_support else value >= close)]
        candidates.sort(key=lambda item: item[1], reverse=is_support)
        selected: list[dict[str, float | str]] = []
        for name, value in candidates:
            if any(abs(value - float(existing["value"])) <= max(abs(close) * 0.0001, 1e-9) for existing in selected):
                continue
            selected.append({"name": name, "value": value})
            if len(selected) == 2:
                break
        fallback_name = "1×ATR下沿" if is_support else "1×ATR上沿"
        fallback_value = close - atr14 if is_support else close + atr14
        if not selected:
            selected.append({"name": fallback_name, "value": fallback_value})
        return selected

    return {
        "entry_reference": close,
        "long_atr_stop": close - 2 * atr14,
        "short_atr_stop": close + 2 * atr14,
        "long_break_even_trigger": close * 1.002,
        "short_break_even_trigger": close * 0.998,
        "long_fib_trail": close + max(0.0, high20 - close) * 0.618,
        "short_fib_trail": close - max(0.0, close - low20) * 0.618,
        "supports": select_levels(True),
        "resistances": select_levels(False),
        "level_method": "按当前价就近筛选20日高低、MA20和布林带上下轨；突破区间时用1×ATR补充",
    }


def volatility_position_control(bars: list[dict[str, Any]], period: int = 14) -> dict[str, Any]:
    """Convert daily ATR volatility into a discrete, auditable position multiplier."""
    atr_values = rolling_atr(bars, period)
    ratios = [
        atr / bar["close"] * 100
        for atr, bar in zip(atr_values, bars)
        if atr is not None and bar["close"] > 0
    ][-252:]
    if not ratios:
        return {"atr_pct": None, "percentile_252": None, "spike_ratio": None, "regime": "unknown", "regime_text": "数据不足", "position_multiplier": 0.0, "note": "波动率数据不足，暂不持仓"}
    current = ratios[-1]
    reference_window = ratios[-21:-1] or ratios[-1:]
    reference = statistics.fmean(reference_window)
    spike_ratio = current / reference if reference > 0 else 1.0
    percentile = sum(value <= current for value in ratios) / len(ratios) * 100
    if percentile >= 90 or spike_ratio >= 1.6:
        regime, regime_text, multiplier, note = "surge", "波动飙升", 0.25, "方向保留，但目标仓位缩至25%，不因技术多头加仓"
    elif percentile >= 75 or spike_ratio >= 1.3:
        regime, regime_text, multiplier, note = "high", "高波动", 0.5, "目标仓位降至50%，等待波动回落"
    elif percentile >= 60:
        regime, regime_text, multiplier, note = "elevated", "波动偏高", 0.75, "目标仓位降至75%"
    else:
        regime, regime_text, multiplier, note = "normal", "正常波动", 1.0, "波动率未触发减仓"
    return {
        "atr_pct": current, "percentile_252": percentile, "spike_ratio": spike_ratio,
        "regime": regime, "regime_text": regime_text, "position_multiplier": multiplier, "note": note,
        "method": "ATR14/收盘价；使用最近252个交易日分位与相对20日均值的跃升倍数",
    }


def trend_quality_snapshot(bars: list[dict[str, Any]]) -> dict[str, Any]:
    """Rankable 20-day trend quality with a transparent 3-day noise penalty."""
    if len(bars) < 22:
        return {
            "status": "insufficient", "return_20d_pct": None, "return_3d_pct": None,
            "volatility_20d_pct": None, "risk_adjusted_trend": None, "noise_ratio": None,
            "quality_score": None, "direction": "neutral", "high_quality": False,
        }
    closes = [float(row["close"]) for row in bars[-22:]]
    daily_returns = [closes[index] / closes[index - 1] - 1 for index in range(1, len(closes))]
    trend_20d = closes[-1] / closes[-21] - 1
    noise_3d = closes[-1] / closes[-4] - 1
    volatility_20d = statistics.stdev(daily_returns[-20:]) * math.sqrt(20) if len(daily_returns) >= 20 else 0.0
    risk_adjusted = trend_20d / volatility_20d if volatility_20d > 0 else 0.0
    noise_ratio = abs(noise_3d) / max(abs(trend_20d), 1e-6)
    quality_score = abs(risk_adjusted) / (1 + noise_ratio)
    high_quality = abs(risk_adjusted) >= 0.75 and noise_ratio <= 0.35 and abs(trend_20d) >= 0.01
    return {
        "status": "ok",
        "return_20d_pct": trend_20d * 100,
        "return_3d_pct": noise_3d * 100,
        "volatility_20d_pct": volatility_20d * 100,
        "risk_adjusted_trend": risk_adjusted,
        "noise_ratio": noise_ratio,
        "quality_score": quality_score,
        "direction": "long" if risk_adjusted > 0 else "short" if risk_adjusted < 0 else "neutral",
        "high_quality": high_quality,
        "formula": "20日收益率 ÷ (近20日日收益率标准差 × √20)；噪音比率=|3日收益率|÷|20日收益率|",
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
        returns_by_date: dict[str, list[float]] = {}
        for row in rows:
            for point in row.get("return_series", []):
                returns_by_date.setdefault(point["date"], []).append(float(point["return"]))
        portfolio_returns = [statistics.fmean(returns_by_date[date]) for date in sorted(returns_by_date)]
        portfolio_mean = statistics.fmean(portfolio_returns) if portfolio_returns else 0.0
        portfolio_vol = statistics.stdev(portfolio_returns) if len(portfolio_returns) > 1 else 0.0
        portfolio_sharpe = portfolio_mean / portfolio_vol * math.sqrt(252) if portfolio_vol > 0 else 0.0
        portfolio_equity, portfolio_peak, portfolio_drawdown = 1.0, 1.0, 0.0
        for value in portfolio_returns:
            portfolio_equity *= 1 + value
            portfolio_peak = max(portfolio_peak, portfolio_equity)
            portfolio_drawdown = min(portfolio_drawdown, portfolio_equity / portfolio_peak - 1)
        summary.append({
            "key": key, "name": names[key], "frequency": rows[0]["frequency"], "assets": len(rows),
            "trades": trades, "win_rate_pct": winners / trades * 100 if trades else None,
            "equal_weight_return_pct": statistics.fmean(returns), "median_return_pct": statistics.median(returns),
            "average_sharpe": statistics.fmean(sharpes), "positive_assets": sum(value > 0 for value in returns),
            "worst_drawdown_pct": min(row["max_drawdown_pct"] for row in rows),
            "portfolio_return_pct": (portfolio_equity - 1) * 100,
            "portfolio_sharpe": portfolio_sharpe,
            "portfolio_max_drawdown_pct": portfolio_drawdown * 100,
            "sample_start": min(row["sample_start"] for row in rows), "sample_end": max(row["sample_end"] for row in rows),
        })
    summary.sort(key=lambda row: row["portfolio_sharpe"], reverse=True)
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
    eligible = [row for row in summary if row["frequency"] == "daily" and row["trades"] >= int(config.get("minimum_ranked_strategy_trades", 20))]
    top_count = int(config.get("top_strategy_count", 4))
    top_strategies = eligible[:top_count]
    for rank, row in enumerate(top_strategies, 1):
        row["sharpe_rank"] = rank
    return {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "assumptions": {
            "commission": "开仓万1.2 + 平仓万1.2（单边0.012%，完整往返0.024%）",
            "slippage": "开仓1跳 + 平仓1跳",
            "execution": "信号K线收盘后生成，下一根K线开盘执行，避免前视",
            "price_series": "主力连续未复权；换月跳空会影响收益，结果仅供研究筛选",
            "portfolio": "按交易日对各有效品种策略收益等权合成组合净值，再计算组合收益、Sharpe和最大回撤；不含保证金杠杆和资金容量约束",
            "window": f"最近{int(config.get('backtest_calendar_days', 365))}个自然日；指标预热使用窗口前历史，但绩效只统计窗口内",
        },
        "strategies": top_strategies,
        "all_strategies": summary,
        "selection": {
            "metric": "portfolio_sharpe", "top_n": top_count,
            "minimum_total_trades": int(config.get("minimum_ranked_strategy_trades", 20)),
            "note": "仅展示最近一年17品种逐日等权组合Sharpe靠前且交易数达标的日线策略；这是同窗筛选结果，不能视为独立样本证明。",
        },
        "frequency_assessment": frequency,
        "sources": [
            {"name": "CTAAgents/FDT", "url": "https://github.com/CTAAgents/FDT", "adaptation": "参考DC20/DC55、布林带与MACD独立趋势子信号，构造透明通道共振基线"},
            {"name": "VeighNa CTA Strategy", "url": "https://github.com/vnpy/vnpy_ctastrategy", "adaptation": "参考Turtle 20/10、布林突破、CCI和下一K线执行框架"},
            {"name": "TrendFollowingSystems", "url": "https://github.com/ArturSepp/TrendFollowingSystems", "adaptation": "参考多周期EWMAC、TSMOM与波动率缩放的可复现研究设计"},
            {"name": "Trend Atlas", "url": "https://github.com/0xpg/crypto-trend-following", "adaptation": "参考多速度趋势投票、风险预算、仓位上限与无前视执行原则；未采用其加密资产数据"},
        ],
        "per_asset": [{"id": item["id"], "name": item["name"], "strategies": item.get("strategy_comparison", [])} for item in results if item.get("strategy_comparison")],
    }


def select_asset_strategy(strategy_rows: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    """Select only eligible methods; keep strong low-sample results on a forward watchlist."""
    minimum_trades = int(config.get("minimum_asset_strategy_trades", 8))
    sharpe_floors = config.get("strategy_min_sharpe", {})
    default_sharpe_floor = float(sharpe_floors.get("default", 0))
    watch_sharpe = float(config.get("high_performance_low_sample_sharpe", 0.3))
    candidates: list[dict[str, Any]] = []
    for row in strategy_rows:
        if row.get("status") != "ok" or row.get("frequency") != "daily" or not row.get("ranking_eligible", True):
            continue
        trades = int(row.get("trades") or 0)
        sharpe = float(row.get("sharpe") or 0)
        net_return = float(row.get("net_return_pct") or 0)
        drawdown = abs(float(row.get("max_drawdown_pct") or 0))
        reliability = min(1.0, math.sqrt(trades / 20)) if trades > 0 else 0.0
        return_component = 0.15 * clamp(net_return / 20, -1, 1)
        drawdown_penalty = 0.15 * max(0.0, drawdown / 25 - 1)
        selection_score = sharpe * reliability + return_component - drawdown_penalty
        minimum_sharpe = float(sharpe_floors.get(row["key"], default_sharpe_floor))
        trade_pass = trades >= minimum_trades
        sharpe_pass = sharpe >= minimum_sharpe if minimum_sharpe > 0 else sharpe > 0
        return_pass = net_return > 0
        passed = trade_pass and sharpe_pass and return_pass
        low_turnover = bool(row.get("low_turnover"))
        high_performance_low_sample = low_turnover and not trade_pass and sharpe >= watch_sharpe and return_pass
        if passed:
            eligibility_status = "passed"
            status_text = "通过门槛"
        elif high_performance_low_sample:
            eligibility_status = "high_performance_low_sample"
            status_text = "高绩效但样本不足 · 前向观察"
        elif not trade_pass:
            eligibility_status = "sample_insufficient"
            status_text = f"样本不足（{trades}/{minimum_trades}笔）"
        else:
            eligibility_status = "quality_below_threshold"
            status_text = f"质量未达标（Sharpe需{'≥' if minimum_sharpe > 0 else '>'}{minimum_sharpe:.2f}且收益>0）"
        failure_reasons = []
        if not trade_pass:
            failure_reasons.append(f"交易数{trades}<{minimum_trades}")
        if not sharpe_pass:
            failure_reasons.append(f"Sharpe {sharpe:.2f}未达{minimum_sharpe:.2f}")
        if not return_pass:
            failure_reasons.append("最近一年收益≤0")
        candidates.append({
            "key": row["key"], "name": row["name"], "frequency": row.get("frequency", "daily"), "trades": trades,
            "win_rate_pct": row.get("win_rate_pct"), "net_return_pct": net_return,
            "sharpe": sharpe, "max_drawdown_pct": row.get("max_drawdown_pct"),
            "profit_factor": row.get("profit_factor"), "latest_target": int(row.get("latest_target") or 0),
            "sample_start": row.get("sample_start"), "sample_end": row.get("sample_end"),
            "reliability": reliability, "selection_score": selection_score, "passed": passed,
            "minimum_sharpe": minimum_sharpe, "trade_pass": trade_pass, "sharpe_pass": sharpe_pass,
            "return_pass": return_pass, "low_turnover": low_turnover,
            "high_performance_low_sample": high_performance_low_sample,
            "eligibility_status": eligibility_status, "status_text": status_text,
            "failure_reasons": failure_reasons, "long_horizon": row.get("long_horizon"),
        })
    status_priority = {"passed": 3, "high_performance_low_sample": 2, "sample_insufficient": 1, "quality_below_threshold": 0}
    candidates.sort(key=lambda item: (status_priority[item["eligibility_status"]], item["selection_score"], item["trades"]), reverse=True)
    eligible_rank = 0
    for item in candidates:
        if item["passed"]:
            eligible_rank += 1
            item["eligible_rank"] = eligible_rank
        else:
            item["eligible_rank"] = None
    selected = next((item for item in candidates if item["passed"]), None)
    forward_watch = [item for item in candidates if item["high_performance_low_sample"]]
    if selected:
        confidence = "较高" if selected["trades"] >= 15 and selected["sharpe"] >= 0.75 and abs(float(selected["max_drawdown_pct"] or 0)) <= 20 else "观察"
        verdict = f"沿用{selected['name']}作为该品种研究方法；仍需前向模拟验证"
    else:
        confidence = "不足"
        verdict = "暂无可靠方法；最近一年没有方法同时通过交易数、收益和策略质量门槛"
        if forward_watch:
            verdict += f"；{forward_watch[0]['name']}为高绩效低样本候选，仅进入前向观察"
    return {
        "selected": selected, "confidence": confidence, "minimum_trades": minimum_trades,
        "method": "可用排名仅包含通过门槛的方法并优先展示；最近一年交易数至少达标、收益为正，日线四因子Sharpe须≥0.30，其他方法Sharpe须>0，再按可靠性折扣后的综合分排序。",
        "warning": "高绩效但不足8笔的低换手方法只进入前向观察，不直接替换；另列真实3年/5年结果作长期稳定性背景，不用于放宽最近一年门槛。同窗选优仍存在选择偏差。",
        "quality_thresholds": {"default_sharpe": default_sharpe_floor, "daily_four_factor_sharpe": float(sharpe_floors.get("daily_four_factor", 0.3)), "minimum_trades": minimum_trades},
        "verdict": verdict, "candidates": candidates, "forward_watch": forward_watch,
    }


def update_strategy_forward_watch(
    results: list[dict[str, Any]], generated_at: datetime, config: dict[str, Any]
) -> dict[str, Any]:
    """Forward-only observer for high-performance, low-sample methods; never backfill returns."""
    try:
        with FORWARD_WATCH_PATH.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
    except (FileNotFoundError, OSError, ValueError):
        state = {"started_at": generated_at.isoformat(timespec="seconds"), "records": {}}
    records = state.setdefault("records", {})
    fee = float(config.get("commission_per_side_bps", 1.2)) / 10000
    slip_ticks = float(config.get("slippage_ticks_per_side", 1))
    assets = {item["id"]: item for item in results if item.get("price") and item.get("daily_open")}
    current_keys: set[str] = set()

    for asset in assets.values():
        candidates = asset.get("strategy_selection", {}).get("forward_watch", [])
        for candidate in candidates:
            record_key = f"{asset['id']}:{candidate['key']}"
            current_keys.add(record_key)
            daily_date = str(asset["daily_date"])
            daily_open = float(asset["daily_open"])
            vol_multiplier = float(asset.get("volatility_control", {}).get("position_multiplier") or 0)
            next_target = int(candidate.get("latest_target") or 0) * vol_multiplier
            record = records.get(record_key)
            if not record:
                record = {
                    "asset_id": asset["id"], "asset": asset["name"], "symbol": asset["symbol"],
                    "strategy_key": candidate["key"], "strategy": candidate["name"],
                    "started_at": generated_at.isoformat(timespec="seconds"), "status": "watching",
                    "equity": 1.0, "position": 0.0, "pending_target": next_target,
                    "last_daily_date": daily_date, "last_open": daily_open,
                    "trades": 0, "observations": 0, "history": [],
                }
                records[record_key] = record
            elif daily_date > str(record.get("last_daily_date") or ""):
                prior_position = float(record.get("position") or 0)
                prior_open = float(record.get("last_open") or daily_open)
                target = float(record.get("pending_target") or 0)
                holding_return = prior_position * (daily_open / prior_open - 1) if prior_open > 0 else 0.0
                turnover = abs(target - prior_position)
                friction = turnover * (fee + slip_ticks * float(asset["tick"]) / daily_open) if daily_open > 0 else 0.0
                record["equity"] = max(0.01, float(record.get("equity") or 1) * (1 + holding_return - friction))
                if turnover:
                    record["trades"] = int(record.get("trades") or 0) + 1
                record["position"] = target
                record["pending_target"] = next_target
                record["last_daily_date"] = daily_date
                record["last_open"] = daily_open
                record["observations"] = int(record.get("observations") or 0) + 1
                record.setdefault("history", []).append({
                    "date": daily_date, "equity": record["equity"], "position": target,
                    "holding_return": holding_return, "friction": friction,
                })
                record["history"] = record["history"][-750:]
            else:
                record["pending_target"] = next_target
            record["status"] = "watching"
            record["updated_at"] = generated_at.isoformat(timespec="seconds")

    for record_key, record in records.items():
        if record_key in current_keys or record.get("status") == "closed":
            continue
        asset = assets.get(record.get("asset_id"))
        if not asset:
            continue
        daily_date = str(asset["daily_date"])
        if daily_date > str(record.get("last_daily_date") or ""):
            daily_open = float(asset["daily_open"])
            prior_position = float(record.get("position") or 0)
            prior_open = float(record.get("last_open") or daily_open)
            holding_return = prior_position * (daily_open / prior_open - 1) if prior_open > 0 else 0.0
            friction = abs(prior_position) * (fee + slip_ticks * float(asset["tick"]) / daily_open) if daily_open > 0 else 0.0
            record["equity"] = max(0.01, float(record.get("equity") or 1) * (1 + holding_return - friction))
            if prior_position:
                record["trades"] = int(record.get("trades") or 0) + 1
            record.update({"position": 0.0, "pending_target": 0.0, "last_daily_date": daily_date, "last_open": daily_open, "status": "closed"})
            record["observations"] = int(record.get("observations") or 0) + 1
        else:
            record["status"] = "pending_exit"
        record["updated_at"] = generated_at.isoformat(timespec="seconds")

    rows = []
    for record_key, record in records.items():
        row = {
            "record_key": record_key, "asset_id": record["asset_id"], "asset": record["asset"],
            "strategy_key": record["strategy_key"], "strategy": record["strategy"],
            "status": record.get("status"), "started_at": record.get("started_at"),
            "return_pct": (float(record.get("equity") or 1) - 1) * 100,
            "trades": int(record.get("trades") or 0), "observations": int(record.get("observations") or 0),
            "position": float(record.get("position") or 0), "pending_target": float(record.get("pending_target") or 0),
            "last_daily_date": record.get("last_daily_date"),
        }
        rows.append(row)
        asset = assets.get(record["asset_id"])
        if asset:
            candidate = next((item for item in asset.get("strategy_selection", {}).get("candidates", []) if item["key"] == record["strategy_key"]), None)
            if candidate:
                candidate["forward_test"] = row
            watch_candidate = next((item for item in asset.get("strategy_selection", {}).get("forward_watch", []) if item["key"] == record["strategy_key"]), None)
            if watch_candidate:
                watch_candidate["forward_test"] = row
    rows.sort(key=lambda item: (item["status"] == "watching", item["return_pct"]), reverse=True)
    state["updated_at"] = generated_at.isoformat(timespec="seconds")
    atomic_json(FORWARD_WATCH_PATH, state)
    return {
        "updated_at": state["updated_at"], "rows": rows,
        "method": "高绩效低样本方法首次进入观察时不回填；信号在观察日收盘登记、下一交易日开盘执行，开平各万1.2并每侧1跳。该账本与1000万元正式模拟组合隔离。",
    }


def build_operation_research_view(asset: dict[str, Any]) -> dict[str, Any]:
    """Combine technical evidence first, then use OI structure only as context."""
    signal = asset.get("signal", "neutral")
    technical_side = 1 if signal in ("long", "watch_long") else -1 if signal in ("short", "watch_short") else 0
    hard_signal = signal in ("long", "short")
    selected = asset.get("strategy_selection", {}).get("selected")
    method_side = int(selected.get("latest_target") or 0) if selected else 0
    risk_adjusted = float(asset.get("trend_quality", {}).get("risk_adjusted_trend") or 0)
    trend_side = 1 if risk_adjusted >= 0.25 else -1 if risk_adjusted <= -0.25 else 0
    calendar_spread = asset.get("calendar_spread", {})
    spread_side = int(calendar_spread.get("structure_side") or 0)
    spread_labels = {
        "backwardation": "BACKWARDATION · 近月升水", "contango": "CONTANGO · 近月贴水",
        "flat": "近远月平水", "missing": "近月月差缺失",
    }
    capital = asset.get("capital_bucket", "divergence")
    capital_labels = {
        "trend_long": "涨价增仓 · 多头结构参考", "trend_short": "跌价增仓 · 空头结构参考",
        "warn_long": "涨价减仓 · 警惕继续追多", "warn_short": "跌价减仓 · 警惕继续追空",
        "divergence": "价格或总持仓变化不显著",
    }
    if technical_side and selected and method_side == technical_side and trend_side == technical_side:
        action = "顺势做多" if technical_side > 0 and hard_signal else "顺势做空" if technical_side < 0 and hard_signal else "观察偏多" if technical_side > 0 else "观察偏空"
        reason = "综合技术、品种选优方法与风险调整趋势三者一致"
    elif technical_side and selected and method_side == technical_side:
        action, reason = "等待趋势确认", "技术方向与品种选优方法一致，但20日风险调整趋势未确认"
    elif technical_side and selected and method_side == -technical_side:
        action, reason = "暂缓开仓", "综合技术方向与品种选优方法冲突"
    elif technical_side and selected and method_side == 0:
        action, reason = "等待方法确认", "技术方向已形成，但品种选优方法当前为空仓"
    elif technical_side and not selected:
        action, reason = "仅观察技术方向", "该品种尚无通过可靠性门槛的固定方法"
    else:
        action, reason = "观望", "综合技术方向尚未形成"
    if technical_side and spread_side == technical_side:
        spread_alignment = "同向参考"
        reason += "；近月月差结构同向，仅作风险参考"
    elif technical_side and spread_side == -technical_side:
        spread_alignment = "风险提示"
        reason += "；近月月差结构背离，仅提示风险、不改变方向"
    elif calendar_spread.get("status") == "ok":
        spread_alignment = "中性"
        reason += "；近远月接近平水"
    else:
        spread_alignment = "数据缺失"
        reason += "；近月月差暂缺，不据此反向"
    if technical_side > 0:
        flow_alignment = "确认" if capital == "trend_long" else "背离" if capital in ("trend_short", "warn_long") else "中性"
    elif technical_side < 0:
        flow_alignment = "确认" if capital == "trend_short" else "背离" if capital in ("trend_long", "warn_short") else "中性"
    else:
        flow_alignment = "仅供参考"
    multiplier = float(asset.get("volatility_control", {}).get("position_multiplier") or 0)
    if technical_side and multiplier <= 0.5:
        reason += f"；波动率偏高，仓位上限缩放至{multiplier * 100:.0f}%"
    return {
        "action": action, "reason": reason, "technical_signal": signal, "technical_side": technical_side,
        "risk_adjusted_trend": risk_adjusted, "trend_side": trend_side,
        "calendar_reference": spread_labels.get(calendar_spread.get("structure"), "近月月差缺失"),
        "calendar_alignment": spread_alignment, "calendar_spread": calendar_spread,
        "selected_method": selected, "capital_reference": capital_labels.get(capital, capital_labels["divergence"]),
        "capital_alignment": flow_alignment, "position_multiplier": multiplier,
        "priority": "综合技术、品种选优方法与风险调整趋势决定方向；近月月差背离仅作风险提示，不改变方向或仓位；价格×总持仓只作资金结构交叉验证。",
    }


def update_paper_portfolio(results: list[dict[str, Any]], generated_at: datetime, config: dict[str, Any]) -> dict[str, Any]:
    """Forward-only paper ledger, persisted locally or through Actions cache."""
    all_valid = [item for item in results if item.get("price") and item.get("strategy_comparison")]
    paper_asset_ids = set(config.get("paper_asset_ids", []))
    valid = [item for item in all_valid if not paper_asset_ids or item["id"] in paper_asset_ids]
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
    asset_by_symbol = {item["symbol"]: item for item in all_valid}
    paper_asset_by_symbol = {item["symbol"]: item for item in valid}
    paper_symbols = {item["symbol"] for item in config["assets"] if item["id"] in paper_asset_ids}
    tick_by_symbol = {item["symbol"]: float(item["tick"]) for item in config["assets"]}
    for key, name in strategy_names.items():
        ledger = state["strategies"].setdefault(key, {"name": name, "equity": initial_cash, "positions": {}, "last_prices": {}, "last_bars": {}, "trades": 0, "history": []})
        ledger.setdefault("entry_prices", {})
        ledger.setdefault("entry_times", {})
        ledger.setdefault("change_history", [])
        pnl, friction, changed = 0.0, 0.0, False
        # Close positions that are no longer in the explicitly approved paper scope.
        for symbol, stored_position in list(ledger["positions"].items()):
            prior_position = float(stored_position)
            if not prior_position or symbol in paper_symbols:
                continue
            asset = asset_by_symbol.get(symbol)
            prior_price = ledger["last_prices"].get(symbol)
            price = float(asset["price"]) if asset else float(prior_price or 0)
            current_bar = asset.get("bar_time") if asset else ledger["last_bars"].get(symbol)
            if asset and prior_price and ledger["last_bars"].get(symbol) != current_bar:
                pnl += weight * prior_position * (price / prior_price - 1)
            if price:
                friction += weight * abs(prior_position) * (fee + slip_ticks * tick_by_symbol.get(symbol, 0) / price)
            change_record = {
                "time": generated_at.isoformat(timespec="seconds"), "strategy_key": key, "strategy": name,
                "asset_id": asset["id"] if asset else symbol, "asset": asset["name"] if asset else symbol,
                "symbol": symbol, "from_position": prior_position, "to_position": 0,
                "price": price or None, "bar_time": current_bar, "reason": "scope_removed",
            }
            ledger["change_history"].append(change_record)
            ledger["trades"] += 1
            ledger["positions"][symbol] = 0
            if price:
                ledger["last_prices"][symbol] = price
            ledger["last_bars"][symbol] = current_bar
            ledger["entry_prices"].pop(symbol, None)
            ledger["entry_times"].pop(symbol, None)
            changed = True
        for asset in valid:
            symbol, price = asset["symbol"], float(asset["price"])
            prior_price = ledger["last_prices"].get(symbol)
            prior_position = float(ledger["positions"].get(symbol, 0))
            target_row = next((row for row in asset["strategy_comparison"] if row["key"] == key), None)
            raw_target = int(target_row.get("latest_target", 0)) if target_row else 0
            volatility_multiplier = float(asset.get("volatility_control", {}).get("position_multiplier", 0))
            target = raw_target * volatility_multiplier
            if prior_price and ledger["last_bars"].get(symbol) != asset["bar_time"]:
                pnl += weight * prior_position * (price / prior_price - 1)
                changed = True
            turnover = abs(target - prior_position)
            if turnover:
                friction += weight * turnover * (fee + slip_ticks * tick_by_symbol[symbol] / price)
                ledger["trades"] += 1
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
                    "reason": "open" if prior_position == 0 else "close" if target == 0 else "reverse" if prior_position * target < 0 else "vol_reduce" if abs(target) < abs(prior_position) else "vol_increase",
                    "volatility_regime": asset.get("volatility_control", {}).get("regime"),
                    "volatility_multiplier": volatility_multiplier,
                }
                ledger["change_history"].append(change_record)
                if target and (prior_position == 0 or prior_position * target < 0):
                    ledger["entry_prices"][symbol] = price
                    ledger["entry_times"][symbol] = generated_at.isoformat(timespec="seconds")
                elif target and abs(target) > abs(prior_position):
                    old_entry = float(ledger["entry_prices"].get(symbol) or price)
                    added_size = abs(target) - abs(prior_position)
                    ledger["entry_prices"][symbol] = (old_entry * abs(prior_position) + price * added_size) / abs(target)
                    ledger["entry_times"].setdefault(symbol, generated_at.isoformat(timespec="seconds"))
                else:
                    if not target:
                        ledger["entry_prices"].pop(symbol, None)
                        ledger["entry_times"].pop(symbol, None)
                changed = True
            ledger["positions"][symbol] = target
            ledger["last_prices"][symbol] = price
            ledger["last_bars"][symbol] = asset["bar_time"]
        ledger["change_history"] = ledger["change_history"][-2000:]
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
            asset = paper_asset_by_symbol.get(symbol)
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
                "side": 1 if side > 0 else -1,
                "position_size_pct": abs(float(side)) * 100,
                "entry_price": entry_price,
                "current_price": current_price,
                "unrealized_pct": (1 if side > 0 else -1) * (current_price / entry_price - 1) * 100 if entry_price else 0,
                "volatility_control": asset.get("volatility_control"),
                "entry_time": ledger["entry_times"][symbol],
                "bar_time": asset["bar_time"],
            })
        output.append({"key": key, "name": name, "equity": ledger["equity"], "return_pct": (ledger["equity"] / initial_cash - 1) * 100, "max_drawdown_pct": max_drawdown * 100, "trades": ledger["trades"], "active_positions": active_positions, "observations": len(ledger["history"])})
    state["updated_at"] = generated_at.isoformat(timespec="seconds")
    state["paper_asset_ids"] = sorted(paper_asset_ids)
    atomic_json(PAPER_PATH, state)
    output.sort(key=lambda row: row["return_pct"], reverse=True)
    all_changes = [record for ledger in state["strategies"].values() for record in ledger.get("change_history", [])]
    all_changes.sort(key=lambda row: row["time"], reverse=True)
    position_output.sort(key=lambda row: (row["strategy"], row["asset"]))
    scope_rows = [{"id": asset["id"], "name": asset["name"], "symbol": asset["symbol"]} for asset in config["assets"] if asset["id"] in paper_asset_ids]
    paper_report = {
        "mode": "forward_paper", "started_at": state["started_at"], "updated_at": state["updated_at"],
        "initial_cash": initial_cash, "scope": scope_rows, "strategies": output,
        "positions": position_output, "position_changes": all_changes,
        "equity_history": {key: ledger.get("history", []) for key, ledger in state["strategies"].items()},
        "assumptions": "仅限指定10个品种；开平各万1.2、每侧1跳；方向由策略决定，仓位再按ATR14波动分位缩放为100%/75%/50%/25%；按扫描时点盯市，无真实委托。",
    }
    atomic_json(PAPER_REPORT_PATH, paper_report)
    return {
        **{key: paper_report[key] for key in ("mode", "started_at", "updated_at", "initial_cash", "scope", "strategies", "positions")},
        "recent_changes": all_changes[:30], "history_count": len(all_changes), "history_url": "paper-history.json",
        "note": "无真实委托；仅跟踪指定10个品种，含开平各万1.2和每侧1跳。方向与仓位分离，高波动可只减仓不改方向；完整净值与头寸变化可下载追溯，不回填历史收益。",
    }


def _capped_allocation_weights(raw_scores: dict[str, float], cap: float, gross_target: float = 1.0) -> dict[str, float]:
    """Normalize positive scores into capped gross weights without forced leverage."""
    positive = {key: value for key, value in raw_scores.items() if value > 0}
    if not positive:
        return {key: 0.0 for key in raw_scores}
    weights = {key: 0.0 for key in raw_scores}
    remaining = set(positive)
    remaining_budget = gross_target
    while remaining and remaining_budget > 1e-9:
        total = sum(positive[key] for key in remaining)
        if total <= 0:
            break
        provisional = {key: remaining_budget * positive[key] / total for key in remaining}
        capped = [key for key, value in provisional.items() if value > cap]
        if not capped:
            for key, value in provisional.items():
                weights[key] = value
            break
        for key in capped:
            weights[key] = cap
            remaining_budget -= cap
            remaining.remove(key)
    return weights


def build_allocation_targets(
    results: list[dict[str, Any]], performance_report: dict[str, Any], config: dict[str, Any], equity: float
) -> list[dict[str, Any]]:
    """Construct a transparent time-series allocation; cross-sectional rank never sets direction."""
    paper_ids = set(config.get("paper_asset_ids", []))
    assets = [item for item in results if item.get("price") and item["id"] in paper_ids]
    raw_scores: dict[str, float] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for asset in assets:
        rows = {row["key"]: row for row in asset.get("strategy_comparison", [])}
        selected = asset.get("strategy_selection", {}).get("selected")
        selected_keys = [selected["key"]] if selected and selected.get("key") in rows else []
        votes = [int(rows[key].get("latest_target", 0)) for key in selected_keys]
        consensus = statistics.fmean(votes) if votes else 0.0
        trend = asset.get("trend_quality", {})
        risk_adjusted = float(trend.get("risk_adjusted_trend") or 0)
        noise_ratio = float(trend.get("noise_ratio") or 99)
        trend_side = 1 if risk_adjusted > 0 else -1 if risk_adjusted < 0 else 0
        vote_side = 1 if consensus > 0 else -1 if consensus < 0 else 0
        signal = asset.get("signal", "neutral")
        technical_side = 1 if signal in ("long", "watch_long") else -1 if signal in ("short", "watch_short") else 0
        spread = asset.get("calendar_spread", {})
        spread_side = int(spread.get("structure_side") or 0)
        base_aligned = vote_side != 0 and vote_side == trend_side == technical_side and abs(consensus) >= 0.25 and abs(risk_adjusted) >= 0.25
        spread_warning = spread.get("status") == "ok" and spread_side == -technical_side
        aligned = base_aligned
        volatility = max(float(trend.get("volatility_20d_pct") or 0) / 100, 0.03)
        noise_penalty = 1 / (1 + noise_ratio)
        vol_multiplier = float(asset.get("volatility_control", {}).get("position_multiplier", 0))
        raw_score = abs(consensus) * min(abs(risk_adjusted), 2.0) * noise_penalty * vol_multiplier / volatility if aligned else 0.0
        raw_scores[asset["id"]] = raw_score
        diagnostics[asset["id"]] = {
            "direction": vote_side if aligned else 0,
            "technical_side": technical_side,
            "calendar_spread_side": spread_side,
            "calendar_spread_status": spread.get("status", "missing"),
            "consensus": consensus,
            "votes": votes,
            "selected_strategies": selected_keys,
            "risk_adjusted_trend": risk_adjusted,
            "noise_ratio": noise_ratio,
            "volatility_20d_pct": trend.get("volatility_20d_pct"),
            "volatility_multiplier": vol_multiplier,
            "calendar_spread_warning": spread_warning,
            "reason": ("综合技术、品种选优方法与20日风险调整趋势一致；近月月差背离仅作风险提示，不改变配置" if aligned and spread_warning else "综合技术、品种选优方法与20日风险调整趋势一致；近月月差仅作风险参考" if aligned else "综合技术、品种选优方法与趋势方向未形成一致，保持空仓"),
        }
    weights = _capped_allocation_weights(
        raw_scores, float(config.get("paper_max_weight_per_asset", 0.20)), float(config.get("paper_gross_target", 1.0))
    )
    max_net = float(config.get("paper_max_net_exposure", 0.40))
    signed = {asset_id: diagnostics[asset_id]["direction"] * weight for asset_id, weight in weights.items()}
    long_total = sum(value for value in signed.values() if value > 0)
    short_total = abs(sum(value for value in signed.values() if value < 0))
    net = long_total - short_total
    if net > max_net and long_total > 0:
        long_scale = (max_net + short_total) / long_total
        signed = {key: value * long_scale if value > 0 else value for key, value in signed.items()}
    elif net < -max_net and short_total > 0:
        short_scale = (max_net + long_total) / short_total
        signed = {key: value * short_scale if value < 0 else value for key, value in signed.items()}
    targets = []
    for asset in assets:
        weight = signed.get(asset["id"], 0.0)
        multiplier = float(asset.get("multiplier", 1))
        price = float(asset["price"])
        contract_notional = price * multiplier
        reference_lots = math.floor(abs(weight) * equity / contract_notional) if contract_notional > 0 else 0
        signed_lots = reference_lots if weight > 0 else -reference_lots if weight < 0 else 0
        actual_notional = abs(signed_lots) * contract_notional
        targets.append({
            "asset": asset,
            "target_lots": signed_lots,
            "target_weight": (actual_notional / equity if equity > 0 else 0) * (1 if signed_lots > 0 else -1 if signed_lots < 0 else 0),
            "target_notional": actual_notional,
            "contract_multiplier": multiplier,
            **diagnostics[asset["id"]],
        })
        if weight and reference_lots == 0:
            targets[-1]["reason"] += "；目标金额不足主连参考价1手，暂不建仓"
    return targets


def update_allocation_paper_portfolio(
    results: list[dict[str, Any]], generated_at: datetime, config: dict[str, Any], performance_report: dict[str, Any]
) -> dict[str, Any]:
    """Forward-only CNY10m portfolio with integer reference lots and auditable allocation."""
    initial_cash = float(config.get("initial_paper_cash", 10_000_000))
    if PAPER_PATH.exists():
        try:
            with PAPER_PATH.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError):
            state = {}
    else:
        state = {}
    portfolio = state.setdefault("allocation_portfolio_v1", {
        "name": "1000万动态多空组合", "started_at": generated_at.isoformat(timespec="seconds"),
        "initial_cash": initial_cash, "equity": initial_cash, "positions": {}, "last_prices": {},
        "last_bars": {}, "entry_prices": {}, "entry_times": {}, "history": [], "change_history": [], "trades": 0,
    })
    paper_ids = set(config.get("paper_asset_ids", []))
    valid = [item for item in results if item.get("price") and item["id"] in paper_ids]
    by_symbol = {item["symbol"]: item for item in valid}
    fee = float(config.get("commission_per_side_bps", 1.2)) / 10000
    slip_ticks = float(config.get("slippage_ticks_per_side", 1))
    pnl = 0.0
    for symbol, stored_lots in portfolio["positions"].items():
        asset = by_symbol.get(symbol)
        prior_price = portfolio["last_prices"].get(symbol)
        if not asset or prior_price is None or portfolio["last_bars"].get(symbol) == asset["bar_time"]:
            continue
        pnl += float(stored_lots) * float(asset.get("multiplier", 1)) * (float(asset["price"]) - float(prior_price))
    marked_equity = max(1.0, float(portfolio["equity"]) + pnl)
    targets = build_allocation_targets(valid, performance_report, config, marked_equity)
    costs = 0.0
    changes: list[dict[str, Any]] = []
    target_symbols = {row["asset"]["symbol"] for row in targets}
    for symbol in set(portfolio["positions"]) - target_symbols:
        portfolio["positions"][symbol] = 0
    for target in targets:
        asset = target["asset"]
        symbol, price = asset["symbol"], float(asset["price"])
        multiplier = float(target["contract_multiplier"])
        prior_lots = int(portfolio["positions"].get(symbol, 0))
        target_lots = int(target["target_lots"])
        delta_lots = target_lots - prior_lots
        if delta_lots:
            turnover_notional = abs(delta_lots) * price * multiplier
            costs += turnover_notional * fee + abs(delta_lots) * slip_ticks * float(asset["tick"]) * multiplier
            reason = "open" if prior_lots == 0 else "close" if target_lots == 0 else "reverse" if prior_lots * target_lots < 0 else "rebalance"
            record = {
                "time": generated_at.isoformat(timespec="seconds"), "strategy_key": "allocation_portfolio_v1",
                "strategy": portfolio["name"], "asset_id": asset["id"], "asset": asset["name"], "symbol": symbol,
                "from_lots": prior_lots, "to_lots": target_lots,
                "from_position": prior_lots, "to_position": target_lots,
                "target_weight_pct": target["target_weight"] * 100, "price": price, "bar_time": asset["bar_time"],
                "reason": reason, "allocation_reason": target["reason"], "strategy_votes": target["votes"],
                "risk_adjusted_trend": target["risk_adjusted_trend"], "noise_ratio": target["noise_ratio"],
            }
            changes.append(record)
            portfolio["change_history"].append(record)
            portfolio["trades"] += 1
            if target_lots and (prior_lots == 0 or prior_lots * target_lots < 0):
                portfolio["entry_prices"][symbol] = price
                portfolio["entry_times"][symbol] = generated_at.isoformat(timespec="seconds")
            elif target_lots == 0:
                portfolio["entry_prices"].pop(symbol, None)
                portfolio["entry_times"].pop(symbol, None)
        portfolio["positions"][symbol] = target_lots
        portfolio["last_prices"][symbol] = price
        portfolio["last_bars"][symbol] = asset["bar_time"]
    portfolio["equity"] = max(1.0, marked_equity - costs)
    portfolio["history"].append({
        "time": generated_at.isoformat(timespec="seconds"), "equity": portfolio["equity"],
        "pnl": pnl, "costs": costs,
    })
    portfolio["history"] = portfolio["history"][-1000:]
    portfolio["change_history"] = portfolio["change_history"][-3000:]
    positions = []
    for target in targets:
        asset, lots = target["asset"], int(target["target_lots"])
        symbol, price = asset["symbol"], float(asset["price"])
        entry_price = float(portfolio["entry_prices"].get(symbol) or price)
        multiplier = float(target["contract_multiplier"])
        notional = abs(lots) * price * multiplier
        unrealized = lots * multiplier * (price - entry_price)
        positions.append({
            "strategy_key": "allocation_portfolio_v1", "strategy": portfolio["name"], "asset_id": asset["id"],
            "asset": asset["name"], "symbol": symbol, "side": 1 if lots > 0 else -1 if lots < 0 else 0,
            "lots": abs(lots), "signed_lots": lots, "contract_multiplier": multiplier,
            "position_size_pct": notional / portfolio["equity"] * 100 if portfolio["equity"] else 0,
            "target_weight_pct": target["target_weight"] * 100, "notional_cny": notional,
            "entry_price": entry_price, "current_price": price,
            "unrealized_cny": unrealized, "unrealized_pct": unrealized / notional * 100 if notional else 0,
            "volatility_control": asset.get("volatility_control"), "trend_quality": asset.get("trend_quality"),
            "allocation_reason": target["reason"], "strategy_votes": target["votes"],
            "entry_time": portfolio["entry_times"].get(symbol), "bar_time": asset["bar_time"],
        })
    gross_notional = sum(row["notional_cny"] for row in positions)
    net_notional = sum(row["notional_cny"] * row["side"] for row in positions)
    peak, max_drawdown = initial_cash, 0.0
    for point in portfolio["history"]:
        peak = max(peak, float(point["equity"]))
        if peak:
            max_drawdown = min(max_drawdown, float(point["equity"]) / peak - 1)
    summary = [{
        "key": "allocation_portfolio_v1", "name": portfolio["name"], "equity": portfolio["equity"],
        "return_pct": (portfolio["equity"] / initial_cash - 1) * 100, "max_drawdown_pct": max_drawdown * 100,
        "trades": portfolio["trades"], "active_positions": sum(row["side"] != 0 for row in positions),
        "long_positions": sum(row["side"] > 0 for row in positions), "short_positions": sum(row["side"] < 0 for row in positions),
        "gross_exposure_pct": gross_notional / portfolio["equity"] * 100 if portfolio["equity"] else 0,
        "net_exposure_pct": net_notional / portfolio["equity"] * 100 if portfolio["equity"] else 0,
        "cash_buffer_cny": max(0.0, portfolio["equity"] - gross_notional), "observations": len(portfolio["history"]),
    }]
    all_changes = sorted(portfolio["change_history"], key=lambda row: row["time"], reverse=True)
    scope_rows = [{"id": asset["id"], "name": asset["name"], "symbol": asset["symbol"]} for asset in config["assets"] if asset["id"] in paper_ids]
    state["updated_at"] = generated_at.isoformat(timespec="seconds")
    state["paper_asset_ids"] = sorted(paper_ids)
    atomic_json(PAPER_PATH, state)
    paper_report = {
        "mode": "forward_allocation_paper", "started_at": portfolio["started_at"], "updated_at": state["updated_at"],
        "initial_cash": initial_cash, "scope": scope_rows, "strategies": summary, "positions": positions,
        "position_changes": all_changes, "equity_history": {"allocation_portfolio_v1": portfolio["history"]},
        "selected_strategies": {item["id"]: item.get("strategy_selection", {}).get("selected") for item in valid},
        "assumptions": "1000万元初始权益；仅指定10个品种；综合技术、各品种选优方法与20日风险调整趋势三者同向时配置，再按逆波动率分配；近月月差背离只作风险提示，不改变方向或仓位；单品种≤20%、组合净敞口≤40%、总名义敞口≤100%；主连价仅用于模拟盘参考手数折算；开平各万1.2、每侧1跳。",
    }
    atomic_json(PAPER_REPORT_PATH, paper_report)
    return {
        **{key: paper_report[key] for key in ("mode", "started_at", "updated_at", "initial_cash", "scope", "strategies", "positions")},
        "recent_changes": all_changes[:30], "history_count": len(all_changes), "history_url": "paper-history.json",
        "allocation_method": paper_report["assumptions"],
        "note": "单一1000万元组合账本；方向来自品种自身时间序列信号，不使用板块截面强弱。参考手数按主连价和交易单位折算，不代表可成交合约或真实委托。",
    }


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
    base = {key: asset[key] for key in ("id", "name", "short_name", "symbol", "exchange", "sector", "provider", "tick", "multiplier", "decimals", "unit", "bar_timezone")}
    try:
        if asset["provider"] == "sina":
            cache_errors: dict[str, str] = {}
            cache_times: dict[str, str] = {}

            def live_or_cache(timeframe: str, minimum: int, fetcher: Any) -> list[dict[str, Any]]:
                try:
                    return fetcher()
                except Exception as exc:
                    bars, retrieved_at = load_cached_raw_bars(asset["symbol"], timeframe, minimum)
                    cache_errors[timeframe] = str(exc)
                    cache_times[timeframe] = retrieved_at
                    return bars

            raw_daily = live_or_cache("daily", 60, lambda: fetch_sina_daily_bars(asset["symbol"], generated_at))
            raw_hourly = live_or_cache("60m", 35, lambda: fetch_sina_minute_bars(asset["symbol"], "60"))
            raw_five = live_or_cache("5m", 35, lambda: fetch_sina_minute_bars(asset["symbol"], "5"))
            source_mode = "cached_after_live_error" if cache_errors else "live/intraday + derived close"
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
            if asset["provider"] == "sina" and timeframe in cache_errors:
                continue
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
        daily_history = daily
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
        continuous_position_changes = {"day": percentage_change(holds, 1), "week": percentage_change(holds, 5), "month": percentage_change(holds, 20)}
        position_changes = cached_position_changes(asset["id"], daily[-1]["datetime"])
        if position_changes and position_changes.get("mode") == "weighted_contract":
            position_changes["cache_status"] = "same_day_weighted_reuse"
            position_changes["continuous_proxy"] = continuous_position_changes
        else:
            try:
                position_changes = fetch_eastmoney_weighted_oi(asset["symbol"], generated_at)
                position_changes["continuous_proxy"] = continuous_position_changes
            except Exception as weighted_exc:
                if position_changes:
                    position_changes["cache_status"] = "same_day_reuse"
                    position_changes["weighted_contract_error"] = str(weighted_exc)
                    position_changes["continuous_proxy"] = continuous_position_changes
                else:
                    try:
                        position_changes = aggregate_open_interest_changes(asset["symbol"], generated_at)
                        position_changes["weighted_contract_error"] = str(weighted_exc)
                        position_changes["continuous_proxy"] = continuous_position_changes
                    except Exception as aggregate_exc:
                        position_changes = {
                            **continuous_position_changes,
                            "mode": "continuous_fallback", "contract_count": 1, "coverage_pct": None,
                            "constituents": [], "as_of": daily[-1]["datetime"],
                            "method": "加权合约与全合约加总均不可用，明确降级为主力连续持仓变化",
                            "error": f"加权合约: {weighted_exc}; 全合约加总: {aggregate_exc}",
                        }
        try:
            calendar_spread = nearby_calendar_spread(asset["symbol"], generated_at, float(asset["tick"]))
        except Exception as spread_exc:
            calendar_spread = cached_calendar_spread(asset["id"], generated_at) or {
                "status": "missing", "structure": "missing", "structure_side": 0,
                "source_mode": "missing", "error": str(spread_exc),
                "formula": "月差=近月买卖盘中值−次近月买卖盘中值",
            }
            if calendar_spread.get("status") == "ok":
                calendar_spread["live_error"] = str(spread_exc)
        technical = technical_snapshot(daily)
        oi_week = position_changes.get("week")
        oi_mode = "加权合约" if position_changes.get("mode") == "weighted_contract" else "全合约汇总" if position_changes.get("mode") == "aggregate_all_contracts" else "主连降级"
        technical["groups"]["volume_position"].append({
            "name": "OI 5日变化",
            "value": "—" if oi_week is None else f"{oi_week:+.2f}%",
            "signal": "long" if oi_week is not None and oi_week > 0.15 else "short" if oi_week is not None and oi_week < -0.15 else "neutral",
            "note": f"{oi_mode} · {position_changes.get('contract_count', 0)}合约 · 覆盖率" + ("—" if position_changes.get("coverage_pct") is None else f"{position_changes['coverage_pct']:.1f}%"),
        })
        spread_labels = {"backwardation": "BACKWARDATION", "contango": "CONTANGO", "flat": "平水", "missing": "缺失"}
        technical["groups"]["volume_position"].append({
            "name": "近月月差结构",
            "value": spread_labels.get(calendar_spread.get("structure"), "缺失"),
            "signal": "long" if calendar_spread.get("structure_side") == 1 else "short" if calendar_spread.get("structure_side") == -1 else "neutral",
            "note": f"{calendar_spread.get('near_symbol', '—')}−{calendar_spread.get('far_symbol', '—')} = " + ("—" if calendar_spread.get("spread") is None else f"{calendar_spread['spread']:+.{int(asset['decimals'])}f}") + "；实时买卖盘中值，非结算价",
        })
        volatility_control = volatility_position_control(daily, int(config["atr_period"]))
        trend_quality = trend_quality_snapshot(daily)
        strategy_comparison = compare_strategies(daily, five, asset, config, extended_daily=daily_history)
        strategy_selection = select_asset_strategy(strategy_comparison, config)
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
            "daily_open": daily[-1]["open"],
            "daily_close": daily[-1]["close"],
            "age_minutes": round(age_minutes, 1),
            "bars": {"daily": len(daily), "daily_history": len(daily_history), "hourly": len(hourly), "five": len(five), "weekly": len(weekly)},
            "change_pct": (five_closes[-1] / daily_closes[-1] - 1) * 100,
            "returns": returns,
            "position_changes": position_changes,
            "calendar_spread": calendar_spread,
            "capital_bucket": capital_bucket(returns["month"], position_changes["week"]),
            "technical_methods": technical["groups"],
            "volatility_control": volatility_control,
            "trend_quality": trend_quality,
            "strategy_selection": strategy_selection,
            "risk_levels": research_risk_levels(
                daily_closes[-1], latest["prior_high"], latest["prior_low"], technical["values"]["atr14"],
                technical["values"]["ma20"], technical["values"]["boll_upper"], technical["values"]["boll_lower"],
            ),
            "strategy_comparison": strategy_comparison,
            "timeframes": {"week": weekly_trend, "day": {"signal": latest["signal"], "vote": latest["long_count"] - latest["short_count"]}, "hour": hourly_trend, "five": {"signal": five_signal["signal"], "score": five_signal["score"]}},
            "sparkline": sparkline,
            "source": source,
            "quality": {
                "source_mode": source_mode,
                "daily": daily_quality, "hourly": hourly_quality, "five": five_quality,
                "cache_errors": cache_errors if asset["provider"] == "sina" else {},
                "cache_retrieved_at": cache_times if asset["provider"] == "sina" else {},
                "last_actual_observation": five[-1]["datetime"],
                "last_daily_close": daily[-1]["datetime"],
                "zero_policy": "价格零值保留并在指标计算前校验；成交量/持仓量零值按真实观测保留",
            },
            "backtest": run_backtest(daily, daily_signals, asset, config),
        }
        result["operation_view"] = build_operation_research_view(result)
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
        with ThreadPoolExecutor(max_workers=4) as pool:
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
        bucket_keys = ("trend_long", "warn_long", "trend_short", "warn_short", "divergence")
        capital_flow_summary = {
            key: [item["id"] for item in results if item.get("capital_bucket") == key]
            for key in bucket_keys
        }
        trend_rows = [
            {"id": item["id"], "name": item["name"], "short_name": item["short_name"], "symbol": item["symbol"], **item["trend_quality"]}
            for item in results if item.get("trend_quality", {}).get("status") == "ok"
        ]
        trend_rows.sort(key=lambda row: row["risk_adjusted_trend"], reverse=True)
        for rank, row in enumerate(trend_rows, 1):
            row["rank"] = rank
        trend_ranking = {
            "as_of": generated_at.isoformat(timespec="seconds"),
            "rows": trend_rows,
            "high_quality": [row for row in trend_rows if row["high_quality"]],
            "method": "20日趋势=20日收益率；20日波动率=近20日日收益率标准差×√20；风险调整后趋势=20日趋势÷20日波动率；噪音比率=|3日收益率|÷|20日收益率|。",
            "quality_rule": "高质量趋势：|风险调整后趋势|≥0.75、噪音比率≤35%、|20日收益率|≥1%。",
        }
        forward_watch = update_strategy_forward_watch(results, generated_at, config)
        performance_report = aggregate_strategy_performance(results, generated_at, config)
        paper_trading = update_allocation_paper_portfolio(results, generated_at, config, performance_report)
        for item in results:
            for strategy in item.get("strategy_comparison", []):
                strategy.pop("return_series", None)
        signal_changes = update_signal_change_log(results, generated_at)
        payload = {
            "schema_version": 13,
            "generated_at": generated_at.isoformat(timespec="seconds"),
            "interval_seconds": int(config["scan_interval_seconds"]),
            "summary": counts,
            "capital_flow_summary": capital_flow_summary,
            "trend_ranking": trend_ranking,
            "market_breadth": market_breadth,
            "performance": {key: performance_report[key] for key in ("assumptions", "strategies", "selection", "frequency_assessment", "sources")},
            "paper_trading": paper_trading,
            "forward_watch": forward_watch,
            "signal_changes": signal_changes,
            "assets": results,
            "methodology": {
                "bar": "日线四因子为主；周线和60分钟确认趋势；5分钟仅作盘中预警",
                "factors": "mom5超过±3% / 突破20日高低 / 收盘相对MA20 / 日线RSI14区间",
                "decision": "操作总结以日线四因子、周线/小时确认、品种选优方法和20日风险调整趋势共同决定方向；近月月差与价格×总持仓只作风险参考，不改变方向或仓位",
                "execution": "核心模型在交易日15:20后刷新；全市场行情在交易时段每15分钟刷新；回测绩效只统计最近365个自然日，5分钟策略不进入定时交易信号",
                "capital_structure": "价格上涨且总持仓增加归为多头增仓；价格下跌且总持仓增加归为空头增仓；上涨缩仓提示警惕追多，下跌缩仓提示警惕追空。该分类只描述价格与总持仓组合，不判定多空持仓归属",
                "trend_quality": "20日收益率刻画中期趋势，3日收益率相对20日趋势的绝对比例刻画短期噪音；20日收益率除以20日同期限波动率得到风险调整后趋势",
                "strategy_selection": "所有品种统一先按最近一年硬门槛筛选，再排可用名次：至少8笔、收益>0；日线四因子Sharpe≥0.30，其他方法Sharpe>0。高绩效但低于8笔的方法仅列入前向观察；低换手方法另用真实5年、数据不足时3年历史作背景核验，不直接替换。",
                "allocation": "1000万元模拟组合要求综合技术、品种选优方法与20日风险调整趋势三者同向，再按逆波动率配置；近月月差背离仅作风险提示，不改变配置；单品种≤20%、净敞口≤40%、总名义敞口≤100%",
                "open_interest": "优先读取行情商的品种加权合约持仓字段；若无该序列，将全部挂牌分月合约持仓逐日求和；两者均失败才明确标记主连降级",
                "calendar_spread": "按到期月份选择最近两个仍挂牌且有有效买卖盘的分月合约；月差=近月买卖盘中值−次近月买卖盘中值，正值为BACKWARDATION、负值为CONTANGO；保留两侧可成交边界并核对报价交易日和时间差",
                "technical": "主流指标层覆盖均线、MACD、ADX、ROC、RSI、KDJ、CCI、ATR、布林带、唐奇安、量比与OBV，仅作交叉验证",
                "risk": "支撑压力综合20日高低、MA20、布林带与ATR；方向信号与仓位分离，ATR波动率升至历史高分位时分档降至75%/50%/25%，不自动下单",
                "cost": "所有策略对比统一按开仓万1.2、平仓万1.2，并在每一侧额外计1跳滑点",
            },
            "warnings": [
                "斐波那契/ATR追踪止盈仅使用上一根及更早的已完成K线更新；日线OHLC无法确定同一根K线内高低点先后，禁止用当日极值更新追踪线后再在当日触发，否则会产生未来路径偏差并虚增胜率。",
                "主连换月可能产生跳空，生产使用前应接入后复权连续合约或固定主力合约。",
                "品种加权合约或全分月合约汇总持仓可降低主力换月扰动，但只能作为资金流向参考，不能直接识别多空双方，也不单独构成操作建议。",
                "近月月差使用实时买卖盘中值而非官方结算价；它仅作为期限结构风险提示，不改变技术方向、模拟盘配置或仓位。",
                "黑色板块覆盖螺纹钢、热卷、不锈钢、铁矿石、焦炭、焦煤、硅铁与锰硅主力连续；已按要求删除线材。",
                "最近一年按Sharpe选择策略属于同窗筛选，存在选择偏差；应继续观察前向模拟盘，不能把排名直接外推为未来收益。",
                "低换手方法的3年/5年结果只用于稳定性背景；最近一年样本不足时仍不能直接替换当前方法，避免事后放宽门槛。",
                "模拟盘参考手数由主力连续价和交易单位折算；主力连续不是可成交月份，实盘前必须映射具体合约并复核保证金、手续费和平今规则。",
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
