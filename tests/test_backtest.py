from __future__ import annotations

from datetime import date

import pandas as pd

from nifty_backtest.backtest import run_backtest
from nifty_backtest.models import (
    BacktestRunConfig,
    Contract,
    FillTiming,
    ReversalProxyConfig,
)


def build_contract() -> Contract:
    return Contract(
        exchange="NSE",
        segment="FNO",
        trading_symbol="NIFTYTEST",
        instrument_id="TEST-INSTRUMENT-ID",
        underlying_symbol="NIFTY",
        expiry_date=date(2026, 3, 12),
        strike_price=20000.0,
        instrument_type="CE",
        lot_size=75,
    )


def test_backtest_uses_next_open_fill_and_forced_eod_exit() -> None:
    contract = build_contract()
    candles = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-09 09:15:00+05:30",
                "open": 200.0,
                "high": 210.0,
                "low": 140.0,
                "close": 165.0,
                "volume": 100,
                "open_interest": 10,
            },
            {
                "timestamp": "2026-03-09 09:16:00+05:30",
                "open": 166.0,
                "high": 180.0,
                "low": 160.0,
                "close": 175.0,
                "volume": 120,
                "open_interest": 11,
            },
            {
                "timestamp": "2026-03-09 15:29:00+05:30",
                "open": 175.0,
                "high": 176.0,
                "low": 168.0,
                "close": 170.0,
                "volume": 90,
                "open_interest": 9,
            },
        ]
    )

    result = run_backtest(
        contract_candles={contract: candles},
        strategy_config=ReversalProxyConfig(),
        run_config=BacktestRunConfig(lots_per_trade=1, fill_timing=FillTiming.NEXT_CANDLE_OPEN),
    )

    assert result.aggregate_summary.trade_count == 1
    trade = result.trade_log.iloc[0]
    assert trade["entry_price"] == 160.0
    assert trade["exit_price"] == 170.0
    assert trade["exit_reason"] == "forced_eod"
    assert trade["pnl_rupees"] == 750.0


def test_backtest_uses_signal_close_fill_when_selected() -> None:
    contract = build_contract()
    candles = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-09 09:15:00+05:30",
                "open": 200.0,
                "high": 210.0,
                "low": 140.0,
                "close": 165.0,
                "volume": 100,
                "open_interest": 10,
            },
            {
                "timestamp": "2026-03-09 09:16:00+05:30",
                "open": 165.0,
                "high": 210.0,
                "low": 160.0,
                "close": 195.0,
                "volume": 120,
                "open_interest": 11,
            },
        ]
    )

    result = run_backtest(
        contract_candles={contract: candles},
        strategy_config=ReversalProxyConfig(),
        run_config=BacktestRunConfig(
            lots_per_trade=1,
            fill_timing=FillTiming.SIGNAL_CANDLE_CLOSE,
        ),
    )

    assert result.aggregate_summary.trade_count == 1
    trade = result.trade_log.iloc[0]
    assert trade["entry_price"] == 160.0
    assert trade["exit_price"] == 190.0
    assert trade["exit_reason"] == "take_profit"


def test_backtest_does_not_create_consecutive_buys_while_position_is_open() -> None:
    contract = build_contract()
    candles = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-09 09:15:00+05:30",
                "open": 200.0,
                "high": 185.0,
                "low": 140.0,
                "close": 165.0,
                "volume": 100,
                "open_interest": 10,
            },
            {
                "timestamp": "2026-03-09 09:16:00+05:30",
                "open": 180.0,
                "high": 182.0,
                "low": 120.0,
                "close": 150.0,
                "volume": 100,
                "open_interest": 10,
            },
            {
                "timestamp": "2026-03-09 09:17:00+05:30",
                "open": 150.0,
                "high": 280.0,
                "low": 148.0,
                "close": 180.0,
                "volume": 100,
                "open_interest": 10,
            },
        ]
    )

    result = run_backtest(
        contract_candles={contract: candles},
        strategy_config=ReversalProxyConfig(),
        run_config=BacktestRunConfig(
            lots_per_trade=1,
            fill_timing=FillTiming.SIGNAL_CANDLE_CLOSE,
        ),
    )

    assert result.aggregate_summary.trade_count == 1
    assert len(result.trade_log) == 1
    trade = result.trade_log.iloc[0]
    assert trade["entry_price"] == 160.0
    assert trade["exit_price"] == 190.0
    assert trade["exit_reason"] == "take_profit"


def test_backtest_uses_stop_loss_relative_to_buy_price() -> None:
    contract = build_contract()
    candles = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-09 09:15:00+05:30",
                "open": 200.0,
                "high": 205.0,
                "low": 140.0,
                "close": 165.0,
                "volume": 100,
                "open_interest": 10,
            },
            {
                "timestamp": "2026-03-09 09:16:00+05:30",
                "open": 166.0,
                "high": 170.0,
                "low": 145.0,
                "close": 150.0,
                "volume": 120,
                "open_interest": 11,
            },
        ]
    )

    result = run_backtest(
        contract_candles={contract: candles},
        strategy_config=ReversalProxyConfig(
            take_profit_threshold=40.0,
            stop_loss_threshold=10.0,
        ),
        run_config=BacktestRunConfig(
            lots_per_trade=1,
            fill_timing=FillTiming.NEXT_CANDLE_OPEN,
        ),
    )

    assert result.aggregate_summary.trade_count == 1
    trade = result.trade_log.iloc[0]
    assert trade["entry_price"] == 160.0
    assert trade["exit_price"] == 150.0
    assert trade["exit_reason"] == "stop_loss"
