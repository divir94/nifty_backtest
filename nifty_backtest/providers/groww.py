from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import cached_property
from typing import Any

import pandas as pd

from nifty_backtest.cache import LocalCandleCache, daterange
from nifty_backtest.config import GrowwCredentials, get_cache_dir
from nifty_backtest.models import Contract
from nifty_backtest.providers.base import (
    ProviderConfigurationError,
    ProviderError,
    ProviderPermissionError,
)


INSTRUMENTS_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
INDIA_TIMEZONE = "Asia/Kolkata"
ALIAS_MAP = {
    "NIFTY 50": "NIFTY",
    "NIFTY": "NIFTY",
}


@dataclass
class GrowwProvider:
    credentials: GrowwCredentials | None = None
    cache: LocalCandleCache | None = None

    def __post_init__(self) -> None:
        self.cache = self.cache or LocalCandleCache(get_cache_dir() / "groww" / "candles")
        self.discovery_warning: str | None = None
        self._client: Any | None = None

    def list_underlyings(self, exchange: str = "NSE") -> list[str]:
        instruments = self._instruments
        filtered = instruments.loc[
            (instruments["exchange"] == exchange)
            & (instruments["segment"] == "FNO")
            & instruments["underlying_symbol"].notna()
        ]
        underlyings = sorted(set(filtered["underlying_symbol"].tolist()))
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
        underlying_symbol = normalize_underlying_symbol(underlying_symbol)
        try:
            payload = self._response_payload(
                self._get_client().get_expiries(
                    exchange=exchange,
                    underlying_symbol=underlying_symbol,
                    year=year,
                    month=month,
                )
            )
            expiries = self._normalize_expiries(payload)
            if expiries:
                self.discovery_warning = None
                return expiries
        except Exception as exc:
            normalized = self._normalize_error(exc, "expiry discovery")
            if not isinstance(
                normalized,
                (ProviderPermissionError, ProviderConfigurationError),
            ):
                raise normalized from exc
            if isinstance(normalized, ProviderPermissionError):
                self.discovery_warning = (
                    "Groww historical discovery returned 403. Falling back to the public "
                    "instrument catalog for expiry discovery."
                )
            else:
                self.discovery_warning = (
                    "Groww credentials are missing, so expiry discovery is using the public "
                    "instrument catalog."
                )

        return self._fallback_expiries(
            exchange=exchange,
            underlying_symbol=underlying_symbol,
            year=year,
            month=month,
        )

    def get_contracts(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        expiry_date: str,
    ) -> list[Contract]:
        underlying_symbol = normalize_underlying_symbol(underlying_symbol)
        try:
            payload = self._response_payload(
                self._get_client().get_contracts(
                    exchange=exchange,
                    underlying_symbol=underlying_symbol,
                    expiry_date=expiry_date,
                )
            )
            contracts = self._normalize_contracts(
                payload=payload,
                exchange=exchange,
                underlying_symbol=underlying_symbol,
                expiry_date=expiry_date,
            )
            if contracts:
                self.discovery_warning = None
                return contracts
        except Exception as exc:
            normalized = self._normalize_error(exc, "contract discovery")
            if not isinstance(
                normalized,
                (ProviderPermissionError, ProviderConfigurationError),
            ):
                raise normalized from exc
            if isinstance(normalized, ProviderPermissionError):
                self.discovery_warning = (
                    "Groww historical discovery returned 403. Falling back to the public "
                    "instrument catalog for contract discovery."
                )
            else:
                self.discovery_warning = (
                    "Groww credentials are missing, so contract discovery is using the public "
                    "instrument catalog."
                )

        return self._fallback_contracts(
            exchange=exchange,
            underlying_symbol=underlying_symbol,
            expiry_date=expiry_date,
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

        for chunk_start, chunk_end in chunk_missing_days(missing_days):
            fetched = self._fetch_candle_chunk(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                start_time=max(
                    start_time,
                    datetime.combine(
                        chunk_start,
                        datetime.min.time(),
                        tzinfo=start_time.tzinfo,
                    ),
                ),
                end_time=min(
                    end_time,
                    datetime.combine(
                        chunk_end,
                        datetime.max.time().replace(microsecond=0),
                        tzinfo=end_time.tzinfo,
                    ),
                ),
                candle_interval=candle_interval,
            )
            self.cache.store_range(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                candles=fetched,
                requested_start=chunk_start,
                requested_end=chunk_end,
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

    def _fetch_candle_chunk(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        candle_interval: str,
    ) -> pd.DataFrame:
        client = self._get_client()
        try:
            response = client.get_historical_candles(
                exchange=exchange,
                segment=segment,
                groww_symbol=instrument_id,
                start_time=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time=end_time.strftime("%Y-%m-%d %H:%M:%S"),
                candle_interval=candle_interval,
            )
        except Exception as exc:
            raise self._normalize_error(exc, "historical candles") from exc

        return self._normalize_candles(self._response_payload(response))

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self.credentials is None:
            raise ProviderConfigurationError(
                "Groww credentials are required for authenticated historical requests."
            )

        try:
            from growwapi import GrowwAPI
        except ImportError as exc:
            raise ProviderConfigurationError(
                "growwapi is not installed. Install dependencies with requirements.txt."
            ) from exc

        try:
            access_token = GrowwAPI.get_access_token(
                api_key=self.credentials.api_key,
                secret=self.credentials.secret,
            )
            self._client = GrowwAPI(access_token)
            return self._client
        except Exception as exc:
            raise self._normalize_error(exc, "authentication") from exc

    @cached_property
    def _instruments(self) -> pd.DataFrame:
        instruments = pd.read_csv(INSTRUMENTS_URL, dtype="str").fillna("")
        if "underlying_symbol" in instruments.columns:
            instruments["underlying_symbol"] = instruments["underlying_symbol"].replace(
                ALIAS_MAP
            )
        return instruments

    def _fallback_expiries(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        year: int | None,
        month: int | None,
    ) -> list[str]:
        instruments = self._instruments.loc[
            (self._instruments["exchange"] == exchange)
            & (self._instruments["segment"] == "FNO")
            & (self._instruments["underlying_symbol"] == underlying_symbol)
            & self._instruments["expiry_date"].ne("")
        ]
        expiries = sorted(set(instruments["expiry_date"].tolist()))
        if year is not None:
            expiries = [expiry for expiry in expiries if expiry.startswith(f"{year:04d}-")]
        if month is not None:
            expiries = [expiry for expiry in expiries if expiry[5:7] == f"{month:02d}"]
        return expiries

    def _fallback_contracts(
        self,
        *,
        exchange: str,
        underlying_symbol: str,
        expiry_date: str,
    ) -> list[Contract]:
        instruments = self._instruments.loc[
            (self._instruments["exchange"] == exchange)
            & (self._instruments["segment"] == "FNO")
            & (self._instruments["underlying_symbol"] == underlying_symbol)
            & (self._instruments["expiry_date"] == expiry_date)
            & self._instruments["instrument_type"].isin(["CE", "PE"])
        ]
        contracts = [
            self._contract_from_record(record.to_dict())
            for _, record in instruments.iterrows()
        ]
        return sort_contracts(contracts)

    def _normalize_expiries(self, payload: Any) -> list[str]:
        if isinstance(payload, dict):
            for key in ("expiries", "items", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    payload = value
                    break

        if not isinstance(payload, list):
            return []

        expiries: list[str] = []
        for item in payload:
            if isinstance(item, str):
                expiries.append(item[:10])
            elif isinstance(item, dict):
                expiry = (
                    item.get("expiry_date")
                    or item.get("expiry")
                    or item.get("date")
                    or item.get("value")
                )
                if expiry:
                    expiries.append(str(expiry)[:10])
        return sorted(set(expiries))

    def _normalize_contracts(
        self,
        *,
        payload: Any,
        exchange: str,
        underlying_symbol: str,
        expiry_date: str,
    ) -> list[Contract]:
        if isinstance(payload, dict):
            for key in ("contracts", "items", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    payload = value
                    break

        if not isinstance(payload, list):
            return []

        contracts: list[Contract] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            merged = dict(item)
            merged.setdefault("exchange", exchange)
            merged.setdefault("segment", "FNO")
            merged.setdefault("underlying_symbol", underlying_symbol)
            merged.setdefault("expiry_date", expiry_date)
            try:
                contracts.append(self._contract_from_record(merged))
            except ValueError:
                continue
        return sort_contracts(contracts)

    def _contract_from_record(self, record: dict[str, Any]) -> Contract:
        trading_symbol = str(
            record.get("trading_symbol") or record.get("tradingSymbol") or ""
        ).strip()
        groww_symbol = str(
            record.get("groww_symbol") or record.get("growwSymbol") or ""
        ).strip()
        lookup_row = self._instrument_lookup(groww_symbol, trading_symbol)

        underlying_symbol = normalize_underlying_symbol(
            str(
                record.get("underlying_symbol")
                or record.get("underlyingSymbol")
                or lookup_row.get("underlying_symbol", "")
            ).strip()
        )
        expiry_value = (
            record.get("expiry_date")
            or record.get("expiry")
            or record.get("expiryDate")
            or lookup_row.get("expiry_date")
        )
        if not trading_symbol or not groww_symbol or not expiry_value:
            raise ValueError("Incomplete contract record.")

        instrument_type = str(
            record.get("instrument_type")
            or record.get("option_type")
            or record.get("instrumentType")
            or lookup_row.get("instrument_type", "")
        ).strip()
        strike_price = to_optional_float(
            record.get("strike_price")
            or record.get("strike")
            or record.get("strikePrice")
            or lookup_row.get("strike_price")
        )
        lot_size = int(
            float(
                record.get("lot_size")
                or record.get("lotSize")
                or lookup_row.get("lot_size")
                or 1
            )
        )
        exchange = str(record.get("exchange") or lookup_row.get("exchange") or "NSE")
        segment = str(record.get("segment") or lookup_row.get("segment") or "FNO")

        return Contract(
            exchange=exchange,
            segment=segment,
            trading_symbol=trading_symbol,
            instrument_id=groww_symbol,
            underlying_symbol=underlying_symbol,
            expiry_date=date.fromisoformat(str(expiry_value)[:10]),
            strike_price=strike_price,
            instrument_type=instrument_type,
            lot_size=lot_size,
            exchange_token=str(
                record.get("exchange_token") or lookup_row.get("exchange_token") or ""
            )
            or None,
        )

    def _instrument_lookup(self, groww_symbol: str, trading_symbol: str) -> dict[str, str]:
        instruments = self._instruments
        if groww_symbol:
            match = instruments.loc[instruments["groww_symbol"] == groww_symbol]
            if not match.empty:
                return match.iloc[0].to_dict()
        if trading_symbol:
            match = instruments.loc[instruments["trading_symbol"] == trading_symbol]
            if not match.empty:
                return match.iloc[0].to_dict()
        return {}

    def _normalize_candles(self, payload: Any) -> pd.DataFrame:
        if isinstance(payload, dict):
            for key in ("candles", "items", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    payload = value
                    break

        if not isinstance(payload, list) or not payload:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "open_interest",
                ]
            )

        first = payload[0]
        if isinstance(first, dict):
            frame = pd.DataFrame(payload)
            rename_map = {
                "time": "timestamp",
                "start_time": "timestamp",
                "datetime": "timestamp",
                "ts": "timestamp",
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
                "oi": "open_interest",
            }
            frame = frame.rename(columns=rename_map)
        else:
            width = len(first)
            columns = [
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
            ][:width]
            frame = pd.DataFrame(payload, columns=columns)

        for column in ["timestamp", "open", "high", "low", "close"]:
            if column not in frame.columns:
                raise ProviderError("Groww candle response is missing required OHLC fields.")

        if "volume" not in frame.columns:
            frame["volume"] = 0.0
        if "open_interest" not in frame.columns:
            frame["open_interest"] = 0.0

        frame["timestamp"] = parse_timestamp_series(frame["timestamp"])
        for column in ["open", "high", "low", "close", "volume", "open_interest"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
        frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")
        return frame.reset_index(drop=True)

    def _response_payload(self, response: Any) -> Any:
        if isinstance(response, dict) and response.get("response") is not None:
            return response["response"]
        return response

    def _normalize_error(self, exc: Exception, action: str) -> ProviderError:
        message = str(exc)
        lowered = message.lower()
        if "403" in message or "forbidden" in lowered:
            return ProviderPermissionError(
                f"Groww denied access for {action}. Check whether this API key has "
                "historical market-data permissions enabled."
            )
        if "401" in message or "authentication" in lowered or "authorisation" in lowered:
            return ProviderConfigurationError(
                f"Groww authentication failed during {action}. Check the API key and secret."
            )
        return ProviderError(f"Groww {action} request failed: {message}")


def normalize_underlying_symbol(value: str) -> str:
    cleaned = value.strip()
    return ALIAS_MAP.get(cleaned, cleaned)


def chunk_missing_days(days: list[date], max_span_days: int = 30) -> list[tuple[date, date]]:
    if not days:
        return []

    chunks: list[tuple[date, date]] = []
    current_start = days[0]
    current_end = days[0]
    for current_day in days[1:]:
        next_expected = current_end + timedelta(days=1)
        span_days = (current_day - current_start).days + 1
        if current_day == next_expected and span_days <= max_span_days:
            current_end = current_day
            continue
        chunks.append((current_start, current_end))
        current_start = current_end = current_day
    chunks.append((current_start, current_end))
    return chunks


def sort_contracts(contracts: list[Contract]) -> list[Contract]:
    return sorted(
        contracts,
        key=lambda contract: (
            contract.instrument_type,
            contract.strike_price if contract.strike_price is not None else 0.0,
            contract.trading_symbol,
        ),
    )


def to_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def ensure_timezone(value: datetime) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(INDIA_TIMEZONE)
    else:
        timestamp = timestamp.tz_convert(INDIA_TIMEZONE)
    return timestamp.to_pydatetime()


def parse_timestamp_series(values: pd.Series) -> pd.Series:
    string_values = values.astype(str).str.strip()
    if not string_values.empty and string_values.str.fullmatch(r"\d+").all():
        numeric = pd.to_numeric(string_values, errors="coerce")
        unit = "ms" if string_values.str.len().max() >= 13 else "s"
        parsed = pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")
        return parsed.dt.tz_convert(INDIA_TIMEZONE)

    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(INDIA_TIMEZONE)
    return parsed.dt.tz_convert(INDIA_TIMEZONE)
