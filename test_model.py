"""Small deterministic checks for the signal engine; run with `python test_model.py`."""

from momentum_monitor import (
    EASTMONEY_WEIGHTED_CODES,
    capital_bucket,
    classify_signal_change,
    daily_four_factor,
    ema,
    load_config,
    point_signal,
    research_risk_levels,
    rolling_atr,
    rolling_rsi,
    technical_snapshot,
    volatility_position_control,
    aggregate_oi_change,
)
from market_universe import UNIVERSE
from research_backtest import compare_strategies, run_strategy


def make_bars(direction: int) -> list[dict]:
    bars = []
    price = 100.0 if direction > 0 else 140.0
    for index in range(100):
        change = direction * (0.16 + (index % 4) * 0.01)
        opening = price
        price += change
        bars.append(
            {
                "datetime": f"2026-09-15 09:{index % 60:02d}:00",
                "open": opening,
                "high": max(opening, price) + 0.04,
                "low": min(opening, price) - 0.04,
                "close": price,
                "volume": 100 + index,
                "hold": 1000,
            }
        )
    return bars


def latest_signal(direction: int) -> dict:
    config = load_config()
    bars = make_bars(direction)
    closes = [bar["close"] for bar in bars]
    fast = ema(closes, config["ema_fast"])
    slow = ema(closes, config["ema_slow"])
    atr = rolling_atr(bars, config["atr_period"])
    rsi = rolling_rsi(closes, config["rsi_period"])
    return point_signal(bars, len(bars) - 1, config, fast, slow, atr, rsi)


if __name__ == "__main__":
    bullish = latest_signal(1)
    bearish = latest_signal(-1)
    assert bullish and bullish["score"] > 0 and bullish["long_count"] >= 3
    assert bearish and bearish["score"] < 0 and bearish["short_count"] >= 3
    assert -100 <= bullish["score"] <= 100
    assert -100 <= bearish["score"] <= 100
    config = load_config()
    daily_bullish = daily_four_factor(make_bars(1), 99, config)
    daily_bearish = daily_four_factor(make_bars(-1), 99, config)
    assert daily_bullish and daily_bullish["score"] > 0
    assert daily_bearish and daily_bearish["score"] < 0
    technical = technical_snapshot(make_bars(1))
    assert set(technical["groups"]) == {"trend", "momentum", "volatility", "volume_position"}
    assert sum(len(group) for group in technical["groups"].values()) == 12
    assert capital_bucket(3.0, 2.0) == "trend_long"
    assert capital_bucket(3.0, -2.0) == "avoid"
    assert capital_bucket(-3.0, 2.0) == "accumulate"
    assert capital_bucket(-3.0, -2.0) == "weak"
    levels = research_risk_levels(100, 110, 90, 2)
    assert levels["long_atr_stop"] == 96 and levels["short_atr_stop"] == 104
    assert levels["long_break_even_trigger"] == 100.2
    comparison = compare_strategies(make_bars(1), make_bars(1), {"tick": 0.1}, config)
    assert len(comparison) == 5
    assert {row["frequency"] for row in comparison} == {"daily", "5m"}
    friction_test = run_strategy(make_bars(1), lambda bars, index: 1, 0.1, "daily", 0.00012, 1, 60)
    assert friction_test["trades"] == 1 and friction_test["net_return_pct"] is not None
    assert {row[2] for row in UNIVERSE} >= {"SHFE", "SHFE/INE", "DCE", "CZCE", "GFEX", "CFFEX"}
    assert {row[3] for row in UNIVERSE} >= {"precious", "nonferrous", "ferrous", "energy", "agriculture", "new_energy", "financial"}
    assert len(config["assets"]) == 17
    assert "cobalt" not in {asset["id"] for asset in config["assets"]}
    assert sum(asset["sector"] == "ferrous" for asset in config["assets"]) == 8
    assert "wire_rod" not in {asset["id"] for asset in config["assets"]}
    assert len(config["paper_asset_ids"]) == 10
    assert set(config["paper_asset_ids"]) <= {asset["id"] for asset in config["assets"]}
    assert {asset["symbol"] for asset in config["assets"]} == set(EASTMONEY_WEIGHTED_CODES)
    detailed_levels = research_risk_levels(100, 110, 90, 2, ma20=98, boll_upper=108, boll_lower=92)
    assert detailed_levels["supports"][0]["value"] == 98
    assert detailed_levels["resistances"][0]["value"] == 108
    volatile_bars = make_bars(1) * 3
    for index, bar in enumerate(volatile_bars):
        bar["datetime"] = f"2026-01-{index + 1:03d}"
        if index >= len(volatile_bars) - 14:
            bar["high"] = bar["close"] + 12
            bar["low"] = bar["close"] - 12
    volatility = volatility_position_control(volatile_bars)
    assert volatility["regime"] == "surge" and volatility["position_multiplier"] == 0.25
    oi_series = [
        {"bars": [{"hold": value} for value in (100, 110, 120, 130, 140, 150)]},
        {"bars": [{"hold": value} for value in (300, 306, 312, 318, 324, 330)]},
    ]
    assert abs(aggregate_oi_change(oi_series, 5) - 20.0) < 1e-9
    direct_reversal = classify_signal_change(
        {"signal": "long", "daily_signal": "long", "score": 70},
        {"signal": "short", "daily_signal": "short", "score": -65},
    )
    score_jump = classify_signal_change(
        {"signal": "neutral", "daily_signal": "neutral", "score": 0},
        {"signal": "watch_long", "daily_signal": "neutral", "score": 55},
    )
    assert direct_reversal and direct_reversal["severity"] == "major"
    assert score_jump and score_jump["severity"] == "major"
    print("signal, volatility sizing, aggregate OI, strategy comparison, market universe and risk checks OK")
