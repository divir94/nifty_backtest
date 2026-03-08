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
    assert trade["entry_price"] == 166.0
    assert trade["exit_price"] == 170.0
    assert trade["exit_reason"] == "forced_eod"
    assert trade["pnl_rupees"] == 300.0


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
    assert trade["entry_price"] == 165.0
    assert trade["exit_price"] == 195.0
    assert trade["exit_reason"] == "signal_close"
