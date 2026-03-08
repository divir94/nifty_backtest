from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

import pandas as pd


class FillTiming(str, Enum):
    NEXT_CANDLE_OPEN = "next_candle_open"
    SIGNAL_CANDLE_CLOSE = "signal_candle_close"


@dataclass(frozen=True)
class Contract:
    exchange: str
    segment: str
    trading_symbol: str
    instrument_id: str
    underlying_symbol: str
    expiry_date: date
    strike_price: float | None
    instrument_type: str
    lot_size: int
    exchange_token: str | None = None

    @property
    def display_label(self) -> str:
        strike = f"{self.strike_price:.0f}" if self.strike_price is not None else "NA"
        expiry_label = self.expiry_date.strftime("%d %b %Y")
        return (
            f"{self.trading_symbol} | {self.instrument_type} | "
            f"{strike} | {expiry_label} | lot {self.lot_size}"
        )


@dataclass(frozen=True)
class ReversalProxyConfig:
    buy_drop_threshold: float = 50.0
    buy_revert_threshold: float = 20.0
    sell_rise_threshold: float = 30.0
    sell_revert_threshold: float = 10.0


@dataclass(frozen=True)
class BacktestRunConfig:
    lots_per_trade: int = 1
    fill_timing: FillTiming = FillTiming.NEXT_CANDLE_OPEN
    timezone: str = "Asia/Kolkata"


@dataclass(frozen=True)
class Trade:
    contract_symbol: str
    instrument_id: str
    instrument_type: str
    expiry_date: date
    strike_price: float | None
    lot_size: int
    lots: int
    entry_signal_time: datetime
    entry_time: datetime
    entry_price: float
    exit_signal_time: datetime
    exit_time: datetime
    exit_price: float
    exit_reason: str
    points: float
    pnl_rupees: float


@dataclass(frozen=True)
class DataQualityReport:
    row_count: int
    start_time: datetime | None
    end_time: datetime | None
    duplicate_timestamps: int
    missing_intervals: int
    zero_volume_rows: int
    zero_open_interest_rows: int

    def warning_messages(self, contract_label: str) -> list[str]:
        if self.row_count == 0:
            return [f"{contract_label}: no candles returned for the selected range."]

        warnings: list[str] = []
        if self.duplicate_timestamps:
            warnings.append(
                f"{contract_label}: {self.duplicate_timestamps} duplicate timestamps detected."
            )
        if self.missing_intervals:
            warnings.append(
                f"{contract_label}: {self.missing_intervals} missing 1-minute intervals detected."
            )
        if self.zero_volume_rows:
            warnings.append(
                f"{contract_label}: {self.zero_volume_rows} candles have zero volume."
            )
        if self.zero_open_interest_rows:
            warnings.append(
                f"{contract_label}: {self.zero_open_interest_rows} candles have zero open interest."
            )
        return warnings


@dataclass(frozen=True)
class ContractSummary:
    trading_symbol: str
    instrument_id: str
    instrument_type: str
    expiry_date: date
    strike_price: float | None
    lot_size: int
    trade_count: int
    win_rate: float
    gross_points: float
    gross_pnl_rupees: float
    average_points: float
    average_pnl_rupees: float
    max_drawdown_rupees: float


@dataclass(frozen=True)
class RunSummary:
    contract_count: int
    trade_count: int
    win_rate: float
    gross_points: float
    gross_pnl_rupees: float
    average_points: float
    average_pnl_rupees: float
    max_drawdown_rupees: float


@dataclass
class ContractBacktestResult:
    contract: Contract
    diagnostics: DataQualityReport
    summary: ContractSummary
    trade_log: pd.DataFrame
    candles: pd.DataFrame


@dataclass
class BacktestResult:
    aggregate_summary: RunSummary
    per_contract_summary: pd.DataFrame
    trade_log: pd.DataFrame
    equity_curve: pd.DataFrame
    daily_pnl: pd.DataFrame
    diagnostics: pd.DataFrame
    contract_results: list[ContractBacktestResult]
