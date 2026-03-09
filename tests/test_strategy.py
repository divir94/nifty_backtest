from __future__ import annotations

import pandas as pd

from nifty_backtest.models import ReversalProxyConfig
from nifty_backtest.strategy import ReversalProxyStrategy


def test_reversal_proxy_signals_match_thresholds() -> None:
    candles = pd.DataFrame(
        [
            {"open": 200.0, "high": 240.0, "low": 140.0, "close": 170.0},
            {"open": 170.0, "high": 176.0, "low": 160.0, "close": 165.0},
        ]
    )
    strategy = ReversalProxyStrategy(
        ReversalProxyConfig(
            buy_drop_threshold=50.0,
            buy_revert_threshold=20.0,
            take_profit_threshold=30.0,
        )
    )

    entries = strategy.entry_signal(candles)
    entry_prices = strategy.entry_trigger_price(candles)

    assert entries.tolist() == [True, False]
    assert entry_prices.tolist() == [160.0, 180.0]
