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
            sell_rise_threshold=30.0,
            sell_revert_threshold=10.0,
        )
    )

    entries = strategy.entry_signal(candles)
    exits = strategy.exit_signal(candles)

    assert entries.tolist() == [True, False]
    assert exits.tolist() == [True, False]
