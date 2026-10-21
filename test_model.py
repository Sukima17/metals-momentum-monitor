"""Small deterministic checks for the signal engine; run with `python test_model.py`."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
    trend_quality_snapshot,
    volatility_position_control,
    aggregate_oi_change,
    _capped_allocation_weights,
    build_allocation_targets,
    build_operation_research_view,
    build_live_daily_bar,
    forced_asset_direction,
    forced_asset_lots,
    update_individual_forced_books,
    contract_expiry_month,
    select_asset_strategy,
)
from market_universe import UNIVERSE
import research_backtest
from research_backtest import _extended_horizon_result, _ma_trend, compare_strategies, run_strategy, run_four_factor_fib_atr


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
    completed = [{"datetime": "2026-10-16", "open": 100, "high": 105, "low": 99, "close": 104, "volume": 20, "hold": 1000}]
    live_five = [
        {"datetime": "2026-10-16 21:05:00", "open": 105, "high": 107, "low": 104, "close": 106, "volume": 4, "hold": 1001},
        {"datetime": "2026-10-19 09:05:00", "open": 106, "high": 109, "low": 105, "close": 108, "volume": 6, "hold": 1004},
        {"datetime": "2026-10-19 14:55:00", "open": 108, "high": 110, "low": 107, "close": 109, "volume": 8, "hold": 1008},
    ]
    forming = build_live_daily_bar(completed, completed, live_five, {"bar_timezone": "Asia/Shanghai", "exchange": "SHFE"}, datetime(2026, 10, 19, 14, 56, tzinfo=ZoneInfo("Asia/Shanghai")))
    assert forming and forming["datetime"] == "2026-10-19" and forming["open"] == 105
    assert forming["high"] == 110 and forming["low"] == 104 and forming["close"] == 109
    assert forming["status"] == "forming" and forming["source_mode"] == "derived_from_real_5m"
    closed_five = live_five + [{"datetime": "2026-10-19 15:00:00", "open": 109, "high": 109, "low": 108, "close": 108.5, "volume": 2, "hold": 1009}]
    observed = build_live_daily_bar(completed, completed, closed_five, {"bar_timezone": "Asia/Shanghai", "exchange": "SHFE"}, datetime(2026, 10, 19, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai")))
    assert observed and observed["status"] == "close_observed" and observed["close"] == 108.5
    assert completed[-1]["datetime"] == "2026-10-16", "盘中聚合不得覆盖已确认日线"
    bars = make_bars(direction)
    closes = [bar["close"] for bar in bars]
    fast = ema(closes, config["ema_fast"])
    slow = ema(closes, config["ema_slow"])
    atr = rolling_atr(bars, config["atr_period"])
    rsi = rolling_rsi(closes, config["rsi_period"])
    return point_signal(bars, len(bars) - 1, config, fast, slow, atr, rsi)


def make_daily_history(count: int) -> list[dict]:
    bars = make_bars(1)[:1] * count
    start = datetime(2020, 1, 1)
    output = []
    price = 100.0
    for index, _ in enumerate(bars):
        opening = price
        price += 0.06 + (index % 5) * 0.005
        output.append({
            "datetime": (start + timedelta(days=index)).strftime("%Y-%m-%d"),
            "open": opening, "high": price + 0.05, "low": opening - 0.05,
            "close": price, "volume": 100 + index, "hold": 1000,
        })
    return output


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
    assert capital_bucket(3.0, -2.0) == "warn_long"
    assert capital_bucket(-3.0, 2.0) == "trend_short"
    assert capital_bucket(-3.0, -2.0) == "warn_short"
    levels = research_risk_levels(100, 110, 90, 2)
    assert levels["long_atr_stop"] == 96 and levels["short_atr_stop"] == 104
    assert levels["long_break_even_trigger"] == 100.2
    comparison = compare_strategies(make_bars(1), make_bars(1), {"tick": 0.1}, config)
    assert len(comparison) == 11
    assert {row["frequency"] for row in comparison} == {"daily", "5m"}
    friction_test = run_strategy(make_bars(1), lambda bars, index: 1, 0.1, "daily", 0.00012, 1, 60)
    assert friction_test["trades"] == 0 and friction_test["open_position"] == 1
    assert friction_test["open_trade_unrealized_pct"] is not None and friction_test["net_return_pct"] is not None

    # A trailing level learned from today's high may only be used tomorrow.
    # If today's high and low were both used in sequence, this daily-OHLC case
    # would manufacture a large winning exit despite unknown intraday ordering.
    path_bars = []
    for index in range(9):
        high, low = (120.0, 99.0) if index == 2 else (101.0, 99.0)
        path_bars.append({
            "datetime": (datetime(2026, 1, 1) + timedelta(days=index)).strftime("%Y-%m-%d"),
            "open": 100.0, "high": high, "low": low, "close": 100.0,
            "volume": 100, "hold": 1000,
        })
    original_four_factor = research_backtest._daily_four_factor
    original_atr = research_backtest._atr
    try:
        research_backtest._daily_four_factor = lambda bars, index: 1 if index == 1 else 0
        research_backtest._atr = lambda bars, index: 10.0
        lagged_trailing = run_four_factor_fib_atr(path_bars, 0.1, warmup=1)
    finally:
        research_backtest._daily_four_factor = original_four_factor
        research_backtest._atr = original_atr
    assert lagged_trailing["trades"] == 1 and lagged_trailing["stop_exits"] == 1
    assert lagged_trailing["avg_trade_pct"] < 0
    assert {row[2] for row in UNIVERSE} >= {"SHFE", "SHFE/INE", "DCE", "CZCE", "GFEX", "CFFEX"}
    assert {row[3] for row in UNIVERSE} >= {"precious", "nonferrous", "ferrous", "energy", "agriculture", "new_energy", "financial"}
    assert len(config["assets"]) == 17
    assert "cobalt" not in {asset["id"] for asset in config["assets"]}
    assert sum(asset["sector"] == "ferrous" for asset in config["assets"]) == 8
    assert "wire_rod" not in {asset["id"] for asset in config["assets"]}
    assert len(config["paper_asset_ids"]) == 10
    assert config["initial_paper_cash"] == 10_000_000
    assert all(asset["multiplier"] > 0 for asset in config["assets"])
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
    assert volatility["regime"] == "surge" and volatility["position_multiplier"] == 0.35
    trend_quality = trend_quality_snapshot(make_bars(1))
    assert trend_quality["risk_adjusted_trend"] > 0 and trend_quality["noise_ratio"] >= 0
    capped = _capped_allocation_weights({"a": 9, "b": 1, "c": 1, "d": 1, "e": 1}, 0.20)
    assert max(capped.values()) <= 0.20 + 1e-12 and sum(capped.values()) <= 1.0 + 1e-12
    oi_series = [
        {"bars": [{"hold": value} for value in (100, 110, 120, 130, 140, 150)]},
        {"bars": [{"hold": value} for value in (300, 306, 312, 318, 324, 330)]},
    ]
    assert abs(aggregate_oi_change(oi_series, 5) - 20.0) < 1e-9
    shanghai_now = datetime(2026, 9, 16)
    assert contract_expiry_month("CU2610", shanghai_now) == (2026, 10)
    assert contract_expiry_month("MA701", shanghai_now) == (2027, 1)
    assert contract_expiry_month("CU0", shanghai_now) is None
    selection = select_asset_strategy([
        {"key": "one_trade", "name": "单笔高收益", "status": "ok", "frequency": "daily", "ranking_eligible": True, "low_turnover": True,
         "trades": 1, "sharpe": 4.0, "net_return_pct": 20, "max_drawdown_pct": -2, "latest_target": 1},
        {"key": "robust", "name": "稳健趋势", "status": "ok", "frequency": "daily", "ranking_eligible": True,
         "trades": 12, "sharpe": 0.8, "net_return_pct": 9, "max_drawdown_pct": -12, "latest_target": 1},
    ], config)
    assert selection["selected"]["key"] == "robust"
    assert selection["candidates"][0]["eligible_rank"] == 1
    assert next(row for row in selection["candidates"] if row["key"] == "one_trade")["eligibility_status"] == "high_performance_low_sample"
    weak_daily = select_asset_strategy([{
        "key": "daily_four_factor", "name": "日线四因子", "status": "ok", "frequency": "daily", "ranking_eligible": True,
        "trades": 15, "sharpe": 0.29, "net_return_pct": 8, "max_drawdown_pct": -5, "latest_target": 1,
    }], config)
    assert weak_daily["selected"] is None and weak_daily["candidates"][0]["eligibility_status"] == "quality_below_threshold"
    long_result = _extended_horizon_result(make_daily_history(2300), lambda: _ma_trend, 0.1, 0.00012, 1, 60)
    assert long_result["status"] == "ok" and long_result["window_years"] == 5 and long_result["selection_role"] == "context_only"
    operation = build_operation_research_view({
        "signal": "long", "capital_bucket": "trend_long", "strategy_selection": selection,
        "volatility_control": {"position_multiplier": 0.5, "regime_text": "波动偏高"},
        "trend_quality": {"risk_adjusted_trend": 0.8},
        "calendar_spread": {"status": "ok", "structure": "backwardation", "structure_side": 1},
    })
    assert operation["action"] == "顺势做多" and operation["capital_alignment"] == "确认" and operation["calendar_alignment"] == "同向参考"
    assert len(operation["votes"]) == 4 and not operation["conflicts"]
    assert any("仓位上限" in item for item in operation["risk_warnings"])
    spread_warning = build_operation_research_view({
        "signal": "long", "capital_bucket": "trend_long", "strategy_selection": selection,
        "volatility_control": {"position_multiplier": 1.0},
        "trend_quality": {"risk_adjusted_trend": 0.8},
        "calendar_spread": {"status": "ok", "structure": "contango", "structure_side": -1},
    })
    assert spread_warning["action"] == "顺势做多" and spread_warning["calendar_alignment"] == "风险提示"
    assert any("月差" in item for item in spread_warning["risk_warnings"])
    all_conflicts = build_operation_research_view({
        "signal": "long", "capital_bucket": "warn_long", "strategy_selection": selection,
        "volatility_control": {"position_multiplier": 0.5, "regime_text": "波动偏高"},
        "trend_quality": {"risk_adjusted_trend": -0.7},
        "calendar_spread": {"status": "ok", "structure": "contango", "structure_side": -1},
    })
    assert all_conflicts["action"] == "暂缓开仓"
    assert any("风险调整趋势" in item and "相反" in item for item in all_conflicts["conflicts"])
    assert len(all_conflicts["risk_warnings"]) == 3
    allocation_with_spread_warning = build_allocation_targets([{
        "id": "copper", "price": 100_000, "multiplier": 5, "signal": "long",
        "strategy_comparison": [{"key": "robust", "latest_target": 1}],
        "strategy_selection": selection,
        "trend_quality": {"risk_adjusted_trend": 0.8, "noise_ratio": 0.2, "volatility_20d_pct": 4.0},
        "volatility_control": {"position_multiplier": 1.0},
        "calendar_spread": {"status": "ok", "structure": "contango", "structure_side": -1},
    }], {}, config, 10_000_000)[0]
    assert allocation_with_spread_warning["direction"] == 1 and allocation_with_spread_warning["target_lots"] > 0
    assert allocation_with_spread_warning["calendar_spread_warning"] is True
    forced = forced_asset_direction({
        "signal": "watch_long", "strategy_selection": {"selected": {"name": "测试方法", "latest_target": -1}},
        "trend_quality": {"risk_adjusted_trend": 0.8, "return_20d_pct": 3.0},
    })
    assert forced["direction"] == 1 and forced["votes"] == {"method": -1, "technical": 1, "trend": 1}
    forced_sizing = forced_asset_lots({
        "daily_close": 100, "price": 100, "multiplier": 10,
        "trend_quality": {"volatility_20d_pct": 5.0},
        "volatility_control": {"position_multiplier": 0.35},
    }, 1_000_000, -1, config)
    assert forced_sizing["signed_lots"] < 0 and abs(forced_sizing["signed_lots"]) >= 1
    assert forced_sizing["margin_rate"] > 0 and forced_sizing["margin_per_contract"] < forced_sizing["contract_notional"]
    assert forced_sizing["limiting_rule"] in ("保证金预算", "止损风险预算")
    copper_sizing = forced_asset_lots({
        "daily_close": 109_550, "price": 109_150, "multiplier": 5, "sim_margin_rate": 0.13,
        "risk_levels": {"entry_reference": 109_550, "long_atr_stop": 107_345.71428571429},
        "volatility_control": {"position_multiplier": 1.0},
    }, 1_000_000, 1, config)
    assert copper_sizing["signed_lots"] == 4
    assert copper_sizing["stop_atr_multiple"] == 1.5
    assert copper_sizing["risk_budget_pct"] == 4.0
    high_vol_copper = forced_asset_lots({
        "daily_close": 109_550, "price": 109_150, "multiplier": 5, "sim_margin_rate": 0.13,
        "risk_levels": {"entry_reference": 109_550, "long_atr_stop": 107_345.71428571429},
        "volatility_control": {"position_multiplier": 0.5},
    }, 1_000_000, 1, config)
    assert 1 <= high_vol_copper["signed_lots"] < copper_sizing["signed_lots"]
    forced_state = {}
    forced_asset = {
        "id": "test", "name": "测试品种", "short_name": "测试", "symbol": "T0", "price": 102,
        "daily_date": "2026-10-19", "daily_open": 100, "daily_close": 102, "daily_settle": 101,
        "live_daily": {"close": 102}, "multiplier": 10, "tick": 1, "signal": "watch_long",
        "strategy_selection": {"selected": {"name": "测试方法", "latest_target": 1}},
        "trend_quality": {"risk_adjusted_trend": 0.8, "return_20d_pct": 3.0, "volatility_20d_pct": 5.0},
        "volatility_control": {"position_multiplier": 1.0, "regime": "normal", "regime_text": "正常波动"},
    }
    first_forced = update_individual_forced_books(forced_state, [forced_asset], datetime(2026, 10, 20, 9), config)
    assert len(first_forced["accounts"]) == 1 and first_forced["accounts"][0]["side"] != 0
    assert first_forced["accounts"][0]["margin_usage_pct"] < first_forced["accounts"][0]["notional_exposure_pct"]
    assert first_forced["accounts"][0]["history"] and "historical_pnl_cny" in first_forced["accounts"][0]
    forced_asset_next = {**forced_asset, "daily_date": "2026-10-20", "daily_open": 103, "daily_close": 104, "daily_settle": 104, "price": 104, "live_daily": {"close": 104}}
    second_forced = update_individual_forced_books(forced_state, [forced_asset_next], datetime(2026, 10, 21, 9), config)
    assert second_forced["accounts"][0]["observations"] == 2 and second_forced["accounts"][0]["side"] != 0
    stop_state = {}
    stop_asset = {
        **forced_asset, "daily_date": "2026-10-19", "daily_open": 100, "daily_close": 100,
        "daily_settle": 100, "price": 100,
        "risk_levels": {"entry_reference": 100, "long_atr_stop": 96},
        "live_daily": {"date": "2026-10-19", "open": 100, "high": 101, "low": 99, "close": 100},
    }
    opened = update_individual_forced_books(stop_state, [stop_asset], datetime(2026, 10, 19, 15), config)
    assert opened["accounts"][0]["side"] > 0
    stop_asset_live = {
        **stop_asset,
        "live_daily": {"date": "2026-10-20", "open": 100, "high": 101, "low": 96.5, "close": 97},
    }
    stopped = update_individual_forced_books(stop_state, [stop_asset_live], datetime(2026, 10, 20, 10), config)
    assert stopped["accounts"][0]["side"] == 0
    assert stopped["accounts"][0]["stop_status"] == "硬止损已退出"
    assert stopped["accounts"][0]["stop_events"][0]["mode"] == "hard_exit_live"
    assert stopped["accounts"][0]["pending_lots"] > 0
    two_of_three = build_allocation_targets([{
        "id": "copper", "sector": "nonferrous", "price": 100_000, "multiplier": 5, "signal": "long",
        "strategy_comparison": [{"key": "robust", "latest_target": 1}], "strategy_selection": selection,
        "trend_quality": {"risk_adjusted_trend": -0.8, "noise_ratio": 0.2, "volatility_20d_pct": 4.0},
        "volatility_control": {"position_multiplier": 1.0}, "calendar_spread": {"status": "missing"},
    }], {}, config, 10_000_000)[0]
    assert two_of_three["direction"] == 1 and two_of_three["conviction"] == 0.55 and two_of_three["target_lots"] > 0
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
    print("signal, per-asset method selection, calendar spread, operation summary, allocation, aggregate OI, strategy comparison, market universe and risk checks OK")
