"""Dependency-free, no-lookahead strategy comparison for futures research."""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta
from typing import Any, Callable


SignalFn = Callable[[list[dict[str, Any]], int], int]

LOW_TURNOVER_STRATEGIES = {"ma_trend", "turtle_20_10", "ewmac_ensemble", "tsmom_ensemble"}


def _ema(values: list[float], period: int) -> list[float]:
    alpha = 2 / (period + 1)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1 - alpha) * output[-1])
    return output


def _rsi(values: list[float], index: int, period: int = 14) -> float:
    changes = [values[j] - values[j - 1] for j in range(index - period + 1, index + 1)]
    gains = sum(max(value, 0) for value in changes) / period
    losses = sum(max(-value, 0) for value in changes) / period
    return 100.0 if losses == 0 else 100 - 100 / (1 + gains / losses)


def _atr(bars: list[dict[str, Any]], index: int, period: int = 14) -> float:
    ranges = []
    for cursor in range(index - period + 1, index + 1):
        previous = bars[cursor - 1]["close"] if cursor else bars[cursor]["close"]
        bar = bars[cursor]
        ranges.append(max(bar["high"] - bar["low"], abs(bar["high"] - previous), abs(bar["low"] - previous)))
    return statistics.fmean(ranges)


def _daily_four_factor(bars: list[dict[str, Any]], index: int) -> int:
    if index < 21:
        return 0
    closes = [bar["close"] for bar in bars]
    close = closes[index]
    prior = bars[index - 20 : index]
    rsi = _rsi(closes, index)
    long_count = sum((close / closes[index - 5] - 1 > 0.03, close > max(row["high"] for row in prior), close > statistics.fmean(closes[index - 19 : index + 1]), 50 <= rsi <= 70))
    short_count = sum((close / closes[index - 5] - 1 < -0.03, close < min(row["low"] for row in prior), close < statistics.fmean(closes[index - 19 : index + 1]), 30 <= rsi <= 50))
    return 1 if long_count >= 3 else -1 if short_count >= 3 else 0


def _ma_trend(bars: list[dict[str, Any]], index: int) -> int:
    if index < 59:
        return 0
    closes = [row["close"] for row in bars]
    ma10 = statistics.fmean(closes[index - 9 : index + 1])
    ma30 = statistics.fmean(closes[index - 29 : index + 1])
    ma60 = statistics.fmean(closes[index - 59 : index + 1])
    return 1 if ma10 > ma30 > ma60 else -1 if ma10 < ma30 < ma60 else 0


def _fibonacci_channel(bars: list[dict[str, Any]], index: int) -> int:
    if index < 55:
        return 0
    prior = bars[index - 55 : index]
    high = max(row["high"] for row in prior)
    low = min(row["low"] for row in prior)
    upper = low + (high - low) * 0.618
    lower = high - (high - low) * 0.618
    ma20 = statistics.fmean(row["close"] for row in bars[index - 19 : index + 1])
    close = bars[index]["close"]
    return 1 if close > upper and close > ma20 else -1 if close < lower and close < ma20 else 0


def _fdt_channel_baseline(bars: list[dict[str, Any]], index: int) -> int:
    """Adapted research baseline from FDT's independently described trend sub-signals."""
    if index < 60:
        return 0
    closes = [row["close"] for row in bars[: index + 1]]
    close = closes[-1]
    votes = []
    for window, upper_cut, lower_cut in ((20, 0.95, 0.05), (55, 0.80, 0.20)):
        sample = bars[index - window + 1 : index + 1]
        high, low = max(row["high"] for row in sample), min(row["low"] for row in sample)
        position = 0.5 if high == low else (close - low) / (high - low)
        votes.append(1 if position > upper_cut else -1 if position < lower_cut else 0)
    sample20 = closes[-20:]
    mean20, deviation = statistics.fmean(sample20), statistics.pstdev(sample20)
    percent_b = 0.5 if deviation == 0 else (close - (mean20 - 2 * deviation)) / (4 * deviation)
    votes.append(1 if percent_b > 0.95 else -1 if percent_b < 0.05 else 0)
    fast, slow = _ema(closes, 12), _ema(closes, 26)
    macd = [left - right for left, right in zip(fast, slow)]
    signal = _ema(macd, 9)
    votes.append(1 if macd[-1] > signal[-1] else -1)
    return 1 if sum(value > 0 for value in votes) >= 3 else -1 if sum(value < 0 for value in votes) >= 3 else 0


def _make_turtle_signal(entry_window: int = 20, exit_window: int = 10) -> SignalFn:
    """VeighNa-style Turtle 20/10 channel state, evaluated sequentially."""
    position = 0

    def signal(bars: list[dict[str, Any]], index: int) -> int:
        nonlocal position
        if index < entry_window:
            return 0
        close = bars[index]["close"]
        entry_sample = bars[index - entry_window : index]
        exit_sample = bars[index - exit_window : index]
        if position > 0 and close < min(row["low"] for row in exit_sample):
            position = 0
        elif position < 0 and close > max(row["high"] for row in exit_sample):
            position = 0
        if position == 0:
            if close > max(row["high"] for row in entry_sample):
                position = 1
            elif close < min(row["low"] for row in entry_sample):
                position = -1
        return position

    return signal


def _make_bollinger_cci_signal(window: int = 20, deviation: float = 2.0) -> SignalFn:
    """Persistent Bollinger breakout gated by CCI; exits at the middle band."""
    position = 0

    def signal(bars: list[dict[str, Any]], index: int) -> int:
        nonlocal position
        if index < window:
            return 0
        sample = bars[index - window + 1 : index + 1]
        closes = [row["close"] for row in sample]
        typical = [(row["high"] + row["low"] + row["close"]) / 3 for row in sample]
        middle = statistics.fmean(closes)
        std = statistics.pstdev(closes)
        typical_mean = statistics.fmean(typical)
        mean_deviation = statistics.fmean(abs(value - typical_mean) for value in typical)
        cci = 0.0 if mean_deviation == 0 else (typical[-1] - typical_mean) / (0.015 * mean_deviation)
        close = closes[-1]
        if position > 0 and close < middle:
            position = 0
        elif position < 0 and close > middle:
            position = 0
        if position == 0 and std > 0:
            if close > middle + deviation * std and cci > 0:
                position = 1
            elif close < middle - deviation * std and cci < 0:
                position = -1
        return position

    return signal


def _ewmac_ensemble(bars: list[dict[str, Any]], index: int) -> int:
    """Multi-speed EWMAC vote inspired by volatility-scaled managed-futures systems."""
    if index < 192:
        return 0
    closes = [row["close"] for row in bars[: index + 1]]
    votes = []
    for fast_period, slow_period in ((16, 48), (32, 96), (64, 192)):
        fast = _ema(closes, fast_period)[-1]
        slow = _ema(closes, slow_period)[-1]
        votes.append(1 if fast > slow else -1 if fast < slow else 0)
    score = sum(votes)
    return 1 if score > 0 else -1 if score < 0 else 0


def _tsmom_ensemble(bars: list[dict[str, Any]], index: int) -> int:
    """Time-series momentum vote across 20/60/120 trading-day horizons."""
    if index < 120:
        return 0
    close = bars[index]["close"]
    votes = [1 if close > bars[index - lag]["close"] else -1 if close < bars[index - lag]["close"] else 0 for lag in (20, 60, 120)]
    score = sum(votes)
    return 1 if score > 0 else -1 if score < 0 else 0


def _risk_adjusted_trend_signal(bars: list[dict[str, Any]], index: int) -> int:
    """20-day trend divided by 20-day horizon volatility, filtered by 3-day noise."""
    if index < 21:
        return 0
    closes = [row["close"] for row in bars[index - 21 : index + 1]]
    daily_returns = [closes[cursor] / closes[cursor - 1] - 1 for cursor in range(1, len(closes))]
    trend = closes[-1] / closes[0] - 1
    horizon_vol = statistics.stdev(daily_returns) * math.sqrt(20) if len(daily_returns) > 1 else 0.0
    noise = closes[-1] / closes[-4] - 1
    risk_adjusted = trend / horizon_vol if horizon_vol > 0 else 0.0
    noise_ratio = abs(noise) / max(abs(trend), 1e-6)
    if abs(risk_adjusted) < 0.5 or noise_ratio > 0.75:
        return 0
    return 1 if risk_adjusted > 0 else -1


def _five_minute_momentum(bars: list[dict[str, Any]], index: int) -> int:
    if index < 22:
        return 0
    closes = [row["close"] for row in bars]
    close = closes[index]
    atr = _atr(bars, index)
    threshold = max(0.0008, 0.85 * atr / close * math.sqrt(6))
    momentum = close / closes[index - 6] - 1
    ema8, ema21 = _ema(closes[: index + 1], 8)[-1], _ema(closes[: index + 1], 21)[-1]
    rsi = _rsi(closes, index)
    prior = bars[index - 20 : index]
    long_count = sum((momentum > threshold, close > max(row["high"] for row in prior), close > ema8 > ema21, 55 <= rsi <= 75))
    short_count = sum((momentum < -threshold, close < min(row["low"] for row in prior), close < ema8 < ema21, 25 <= rsi <= 45))
    return 1 if long_count >= 3 else -1 if short_count >= 3 else 0


def run_strategy(
    bars: list[dict[str, Any]], signal_fn: SignalFn, tick: float, frequency: str,
    fee_per_side: float = 0.00012, slippage_ticks: float = 1.0, warmup: int = 60,
    evaluation_start: str | None = None,
) -> dict[str, Any]:
    """Signal at close[i], execute at open[i+1]; every side pays fee and one tick."""
    if len(bars) < warmup + 5:
        return {"status": "insufficient", "frequency": frequency, "trades": 0, "net_return_pct": None}
    targets = [0] * len(bars)
    for index in range(warmup, len(bars) - 1):
        targets[index + 1] = signal_fn(bars, index)

    evaluation_index = warmup + 1
    if evaluation_start:
        evaluation_index = max(evaluation_index, next((index for index, row in enumerate(bars) if row["datetime"] >= evaluation_start), len(bars) - 1))
    equity, peak, max_drawdown = 1.0, 1.0, 0.0
    interval_returns: list[float] = []
    return_series: list[dict[str, Any]] = []
    previous_target = 0
    for index in range(evaluation_index, len(bars) - 1):
        price = bars[index]["open"]
        next_price = bars[index + 1]["open"]
        target = targets[index]
        turnover = abs(target - previous_target)
        friction = turnover * (fee_per_side + slippage_ticks * tick / price) if price > 0 else 0.0
        period_return = target * (next_price / price - 1) - friction
        interval_returns.append(period_return)
        if frequency == "daily":
            return_series.append({"date": bars[index + 1]["datetime"][:10], "return": period_return})
        equity *= 1 + period_return
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1)
        previous_target = target

    trades: list[float] = []
    entry_price: float | None = None
    side = 0
    for index in range(evaluation_index, len(bars)):
        target = targets[index]
        execution = bars[index]["open"]
        if target == side:
            continue
        if side and entry_price:
            costs = 2 * fee_per_side + 2 * slippage_ticks * tick / entry_price
            trades.append(side * (execution / entry_price - 1) - costs)
        entry_price = execution if target else None
        side = target
    if side and entry_price:
        costs = 2 * fee_per_side + 2 * slippage_ticks * tick / entry_price
        trades.append(side * (bars[-1]["close"] / entry_price - 1) - costs)

    positive = sum(value > 0 for value in trades)
    gross_profit = sum(value for value in trades if value > 0)
    gross_loss = abs(sum(value for value in trades if value < 0))
    bars_per_year = 252
    if frequency == "5m":
        counts: dict[str, int] = {}
        for row in bars:
            day = row["datetime"][:10]
            counts[day] = counts.get(day, 0) + 1
        bars_per_year = 252 * max(1, round(statistics.median(counts.values())))
    mean_return = statistics.fmean(interval_returns) if interval_returns else 0.0
    volatility = statistics.stdev(interval_returns) if len(interval_returns) > 1 else 0.0
    sharpe = mean_return / volatility * math.sqrt(bars_per_year) if volatility > 0 else 0.0
    return {
        "status": "ok", "frequency": frequency, "trades": len(trades),
        "win_rate_pct": positive / len(trades) * 100 if trades else None,
        "net_return_pct": (equity - 1) * 100,
        "annual_return_pct": (equity ** (bars_per_year / max(1, len(interval_returns))) - 1) * 100 if equity > 0 else -100.0,
        "max_drawdown_pct": max_drawdown * 100, "sharpe": sharpe,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
        "avg_trade_pct": statistics.fmean(trades) * 100 if trades else None,
        "positive_trades": positive, "latest_target": targets[-1],
        "sample_start": bars[evaluation_index]["datetime"], "sample_end": bars[-1]["datetime"],
        "observations": len(interval_returns), "return_series": return_series,
    }


def _extended_horizon_result(
    bars: list[dict[str, Any]], signal_factory: Callable[[], SignalFn], tick: float,
    fee: float, slip: float, warmup: int,
) -> dict[str, Any]:
    """Use the longest fully covered 5y/3y window; never synthesize missing history."""
    if not bars:
        return {"status": "insufficient", "window_label": "长周期数据不足", "selection_role": "context_only"}
    latest = datetime.fromisoformat(bars[-1]["datetime"])
    parsed = [datetime.fromisoformat(row["datetime"]) for row in bars]
    for years in (5, 3):
        evaluation_start = latest - timedelta(days=round(365.25 * years))
        evaluation_index = next((index for index, value in enumerate(parsed) if value >= evaluation_start), len(bars))
        if evaluation_index < warmup + 1 or len(bars) - evaluation_index < 200 * years:
            continue
        scoped = bars[max(0, evaluation_index - warmup - 5) :]
        result = run_strategy(
            scoped, signal_factory(), tick, "daily", fee, slip, warmup,
            evaluation_start.isoformat(sep=" "),
        )
        result.pop("return_series", None)
        return {
            **result,
            "window_years": years,
            "window_label": f"{years}年",
            "selection_role": "context_only",
            "source_mode": "同一行情源的主力连续日线（真实历史，未补造）",
            "note": "仅用于低换手方法的长期稳定性核验，不替代最近一年门槛，也不直接触发方法切换。",
        }
    return {
        "status": "insufficient", "window_label": "长周期数据不足", "selection_role": "context_only",
        "available_start": bars[0]["datetime"], "available_end": bars[-1]["datetime"],
        "note": "真实日线覆盖不足3年，未补造历史。",
    }


def compare_strategies(
    daily: list[dict[str, Any]], five: list[dict[str, Any]], asset: dict[str, Any], config: dict[str, Any],
    extended_daily: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    fee = float(config.get("commission_per_side_bps", 1.2)) / 10000
    slip = float(config.get("slippage_ticks_per_side", 1))
    latest_daily = datetime.fromisoformat(daily[-1]["datetime"])
    evaluation_start = (latest_daily - timedelta(days=int(config.get("backtest_calendar_days", 365)))).isoformat(sep=" ")
    definitions: list[tuple[str, str, list[dict[str, Any]], Callable[[], SignalFn], str, int]] = [
        ("daily_four_factor", "日线四因子", daily, lambda: _daily_four_factor, "daily", 60),
        ("ma_trend", "移动均线10/30/60", daily, lambda: _ma_trend, "daily", 60),
        ("fibonacci_0618", "斐波那契0.618通道", daily, lambda: _fibonacci_channel, "daily", 60),
        ("fdt_channel", "FDT启发通道共振", daily, lambda: _fdt_channel_baseline, "daily", 60),
        ("turtle_20_10", "Turtle 20/10通道", daily, lambda: _make_turtle_signal(), "daily", 60),
        ("bollinger_cci", "布林突破 + CCI", daily, lambda: _make_bollinger_cci_signal(), "daily", 60),
        ("ewmac_ensemble", "多周期EWMAC", daily, lambda: _ewmac_ensemble, "daily", 200),
        ("tsmom_ensemble", "TSMOM 20/60/120", daily, lambda: _tsmom_ensemble, "daily", 130),
        ("risk_adjusted_trend", "20日风险调整趋势", daily, lambda: _risk_adjusted_trend_signal, "daily", 60),
        ("five_minute_momentum", "5分钟动量", five, lambda: _five_minute_momentum, "5m", 60),
    ]
    output = []
    minimum_trades = int(config.get("minimum_asset_strategy_trades", 8))
    history = extended_daily or daily
    for key, name, bars, signal_factory, frequency, warmup in definitions:
        result = run_strategy(bars, signal_factory(), float(asset["tick"]), frequency, fee, slip, warmup, evaluation_start if frequency == "daily" else None)
        low_turnover = frequency == "daily" and (key in LOW_TURNOVER_STRATEGIES or int(result.get("trades") or 0) < minimum_trades)
        row = {"key": key, "name": name, "ranking_eligible": frequency == "daily", "low_turnover": low_turnover, **result}
        if low_turnover:
            row["long_horizon"] = _extended_horizon_result(history, signal_factory, float(asset["tick"]), fee, slip, warmup)
        output.append(row)
    return output
