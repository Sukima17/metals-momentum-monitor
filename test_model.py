"""Small deterministic checks for the signal engine; run with `python test_model.py`."""

from momentum_monitor import (
    capital_bucket,
    daily_four_factor,
    ema,
    load_config,
    point_signal,
    research_risk_levels,
    rolling_atr,
    rolling_rsi,
    technical_snapshot,
)


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
    print("signal, technical toolkit, capital quadrant and risk checks OK")
