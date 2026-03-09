from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from nifty_backtest.models import ReversalProxyConfig


@dataclass(frozen=True)
class StrategyDefinition:
    key: str
    label: str
    description: str


REVERSAL_PROXY_DEFINITION = StrategyDefinition(
    key="reversal_proxy",
    label="Reversal Proxy",
    description=(
        "Long-only, 1-minute candle proxy for an intraminute reversal idea. "
        "Entry uses open/low/close thresholds and exits use take-profit and optional "
        "stop-loss levels relative to the filled buy price."
    ),
)


class ReversalProxyStrategy:
    def __init__(self, config: ReversalProxyConfig) -> None:
        self.config = config

    def annotate(self, candles: pd.DataFrame) -> pd.DataFrame:
        frame = candles.copy()
        frame["entry_signal"] = self.entry_signal(frame)
        frame["entry_trigger_price"] = self.entry_trigger_price(frame).where(frame["entry_signal"])
        return frame

    def entry_signal(self, candles: pd.DataFrame) -> pd.Series:
        return (
            (candles["open"] - candles["low"] > self.config.buy_drop_threshold)
            & (candles["close"] - candles["low"] > self.config.buy_revert_threshold)
        )

    def entry_trigger_price(self, candles: pd.DataFrame) -> pd.Series:
        return candles["low"] + self.config.buy_revert_threshold
