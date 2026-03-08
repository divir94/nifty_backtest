from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
import gzip
import json
from io import BytesIO
from urllib.parse import quote

import pandas as pd
import requests

from nifty_backtest.cache import LocalCandleCache
from nifty_backtest.config import UpstoxCredentials, get_cache_dir
from nifty_backtest.models import Contract
from nifty_backtest.providers.base import (
    ProviderConfigurationError,
    ProviderError,
    ProviderPermissionError,
)


UPSTOX_COMPLETE_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)
UPSTOX_HISTORICAL_V3_URL = "https://api.upstox.com/v3/historical-candle"
INDIA_TIMEZONE = "Asia/Kolkata"


@dataclass
class UpstoxProvider:
    credentials: UpstoxCredentials | None = None
    cache: LocalCandleCache | None = None

    def __post_init__(self) -> None:
        self.cache = self.cache or LocalCandleCache(get_cache_dir() / "upstox" / "candles")
        self.discovery_warning = (
            "Upstox contract discovery uses the current public instrument master. "
            "Expired option contracts are not currently discoverable in this app."
        )

    def list_underlyings(self, exchange: str = "NSE") -> list[str]:
        instruments = self._instruments.loc[self._instruments["exchange"] == exchange]
        underlyings = sorted(set(instruments["underlying_symbol"].tolist()))
        if "NIFTY" in underlyings:
            underlyings = ["NIFTY"] + [item for item in underlyings if item != "NIFTY"]
        return underlyings

    def get_expiries(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        year: int | None = None,
        month: int | None = None,
    ) -> list[str]:
        instruments = self._instruments.loc[
            (self._instruments["exchange"] == exchange)
            & (self._instruments["underlying_symbol"] == underlying_symbol)
        ]
        expiries = sorted(set(instruments["expiry_date"].tolist()))
        if year is not None:
            expiries = [expiry for expiry in expiries if expiry.startswith(f"{year:04d}-")]
        if month is not None:
            expiries = [expiry for expiry in expiries if expiry[5:7] == f"{month:02d}"]
        return expiries

    def get_contracts(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        expiry_date: str,
    ) -> list[Contract]:
        instruments = self._instruments.loc[
            (self._instruments["exchange"] == exchange)
            & (self._instruments["underlying_symbol"] == underlying_symbol)
            & (self._instruments["expiry_date"] == expiry_date)
        ]
        contracts = [
            Contract(
                exchange=str(row["exchange"]),
                segment="FNO",
                trading_symbol=str(row["trading_symbol"]),
                instrument_id=str(row["instrument_key"]),
                underlying_symbol=str(row["underlying_symbol"]),
                expiry_date=date.fromisoformat(str(row["expiry_date"])),
                strike_price=float(row["strike_price"]),
                instrument_type=str(row["instrument_type"]),
                lot_size=int(row["lot_size"]),
                exchange_token=str(row["exchange_token"]) or None,
            )
            for _, row in instruments.iterrows()
        ]
        return sorted(
            contracts,
            key=lambda contract: (
                contract.instrument_type,
                contract.strike_price if contract.strike_price is not None else 0.0,
                contract.trading_symbol,
            ),
        )

    def get_candles(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        candle_interval: str,
    ) -> pd.DataFrame:
        start_time = ensure_timezone(start_time)
        end_time = ensure_timezone(end_time)
        if end_time < start_time:
            raise ProviderError("end_time must be greater than or equal to start_time.")

        cached, missing_days = self.cache.load_range(
            exchange=exchange,
            segment=segment,
            instrument_id=instrument_id,
            candle_interval=candle_interval,
            start_time=start_time,
            end_time=end_time,
        )

        for trading_day in missing_days:
            fetched = self._fetch_candle_day(
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                trading_day=trading_day,
            )
            self.cache.store_range(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                candles=fetched,
                requested_start=trading_day,
                requested_end=trading_day,
            )

        cached, _ = self.cache.load_range(
            exchange=exchange,
            segment=segment,
            instrument_id=instrument_id,
            candle_interval=candle_interval,
            start_time=start_time,
            end_time=end_time,
        )
        if cached.empty:
            return cached

        cached["timestamp"] = pd.to_datetime(cached["timestamp"])
        cached = cached.loc[
            (cached["timestamp"] >= start_time)
            & (cached["timestamp"] <= end_time)
        ].sort_values("timestamp")
        cached = cached.drop_duplicates(subset=["timestamp"], keep="last")
        return cached.reset_index(drop=True)

    @property
    def _headers(self) -> dict[str, str]:
        if self.credentials is None:
            raise ProviderConfigurationError(
                "Upstox access token is required for historical candle requests."
            )
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.credentials.access_token}",
        }

    @property
    def _instruments(self) -> pd.DataFrame:
        return load_upstox_instruments()

    def _fetch_candle_day(
        self,
        *,
        instrument_id: str,
        candle_interval: str,
        trading_day: date,
    ) -> pd.DataFrame:
        path = build_interval_path(
            instrument_id=instrument_id,
            candle_interval=candle_interval,
            trading_day=trading_day,
        )
        response = requests.get(
            path,
            headers=self._headers,
            timeout=30,
        )
        if response.status_code == 401:
            raise ProviderConfigurationError(
                "Upstox access token is invalid or expired."
            )
        if response.status_code == 403:
            raise ProviderPermissionError(
                "Upstox denied access to historical candles for this token."
            )
        if not response.ok:
            raise ProviderError(
                f"Upstox historical candle request failed: {response.text[:400]}"
            )
        payload = response.json()
        candle_rows = payload.get("data", {}).get("candles", [])
        return normalize_upstox_candles(candle_rows)


@lru_cache(maxsize=1)
def load_upstox_instruments() -> pd.DataFrame:
    response = requests.get(UPSTOX_COMPLETE_INSTRUMENTS_URL, timeout=30)
    response.raise_for_status()
    payload = json.load(gzip.GzipFile(fileobj=BytesIO(response.content)))
    frame = pd.DataFrame(payload).fillna("")
    frame = frame.loc[
        (frame["segment"] == "NSE_FO")
        & frame["instrument_type"].isin(["CE", "PE"])
    ].copy()
    frame["expiry_date"] = (
        pd.to_datetime(frame["expiry"], unit="ms", utc=True)
        .dt.tz_convert(INDIA_TIMEZONE)
        .dt.strftime("%Y-%m-%d")
    )
    frame["strike_price"] = pd.to_numeric(frame["strike_price"], errors="coerce")
    frame["lot_size"] = (
        pd.to_numeric(frame["lot_size"], errors="coerce")
        .fillna(1)
        .astype(int)
    )
    return frame


def build_interval_path(
    *,
    instrument_id: str,
    candle_interval: str,
    trading_day: date,
) -> str:
    quoted_instrument_id = quote(instrument_id, safe="")
    if candle_interval == "1minute":
        return (
            f"{UPSTOX_HISTORICAL_V3_URL}/{quoted_instrument_id}/minutes/1/"
            f"{trading_day.isoformat()}/{trading_day.isoformat()}"
        )
    if candle_interval == "1day":
        return (
            f"https://api.upstox.com/v2/historical-candle/{quoted_instrument_id}/day/"
            f"{trading_day.isoformat()}/{trading_day.isoformat()}"
        )
    raise ProviderError(f"Unsupported Upstox candle interval: {candle_interval}")


def normalize_upstox_candles(candle_rows: list[list[object]]) -> pd.DataFrame:
    columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
    ]
    if not candle_rows:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(candle_rows, columns=columns[: len(candle_rows[0])])
    if "open_interest" not in frame.columns:
        frame["open_interest"] = 0.0
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    if frame["timestamp"].dt.tz is None:
        frame["timestamp"] = frame["timestamp"].dt.tz_localize(INDIA_TIMEZONE)
    else:
        frame["timestamp"] = frame["timestamp"].dt.tz_convert(INDIA_TIMEZONE)
    for column in ["open", "high", "low", "close", "volume", "open_interest"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
    return frame.reset_index(drop=True)


def ensure_timezone(value: datetime) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(INDIA_TIMEZONE)
    else:
        timestamp = timestamp.tz_convert(INDIA_TIMEZONE)
    return timestamp.to_pydatetime()
