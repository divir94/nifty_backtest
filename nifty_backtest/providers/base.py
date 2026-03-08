from __future__ import annotations

from datetime import datetime
from typing import Protocol

import pandas as pd

from nifty_backtest.models import Contract


class ProviderError(RuntimeError):
    """Base provider error."""


class ProviderConfigurationError(ProviderError):
    """Raised when provider configuration is incomplete."""


class ProviderPermissionError(ProviderError):
    """Raised when the API denies access to a requested resource."""


class DataProvider(Protocol):
    discovery_warning: str | None

    def list_underlyings(self, exchange: str = "NSE") -> list[str]: ...

    def get_expiries(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        year: int | None = None,
        month: int | None = None,
    ) -> list[str]: ...

    def get_contracts(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        expiry_date: str,
    ) -> list[Contract]: ...

    def get_candles(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        candle_interval: str,
    ) -> pd.DataFrame: ...
