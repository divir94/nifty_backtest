from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd


class LocalCandleCache:
    def __init__(self, root: Path) -> None:
        self.root = root

    def load_range(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        candle_interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[pd.DataFrame, list[date]]:
        frames: list[pd.DataFrame] = []
        missing_dates: list[date] = []

        for trading_day in daterange(start_time.date(), end_time.date()):
            parquet_path = self._parquet_path(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                trading_day=trading_day,
            )
            empty_path = self._empty_marker_path(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                trading_day=trading_day,
            )
            if parquet_path.exists():
                frames.append(pd.read_parquet(parquet_path))
                continue
            if empty_path.exists():
                continue
            missing_dates.append(trading_day)

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            combined["timestamp"] = pd.to_datetime(combined["timestamp"])
            combined = combined.loc[
                (combined["timestamp"] >= start_time)
                & (combined["timestamp"] <= end_time)
            ].sort_values("timestamp")
        else:
            combined = pd.DataFrame(
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
        return combined, missing_dates

    def store_range(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        candle_interval: str,
        candles: pd.DataFrame,
        requested_start: date,
        requested_end: date,
    ) -> None:
        fetched_dates: set[date] = set()

        if not candles.empty:
            grouped = candles.groupby(candles["timestamp"].dt.date, sort=True)
            for trading_day, day_frame in grouped:
                fetched_dates.add(trading_day)
                parquet_path = self._parquet_path(
                    exchange=exchange,
                    segment=segment,
                    instrument_id=instrument_id,
                    candle_interval=candle_interval,
                    trading_day=trading_day,
                )
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                day_frame.sort_values("timestamp").to_parquet(parquet_path, index=False)
                empty_path = self._empty_marker_path(
                    exchange=exchange,
                    segment=segment,
                    instrument_id=instrument_id,
                    candle_interval=candle_interval,
                    trading_day=trading_day,
                )
                if empty_path.exists():
                    empty_path.unlink()

        for trading_day in daterange(requested_start, requested_end):
            if trading_day in fetched_dates:
                continue
            empty_path = self._empty_marker_path(
                exchange=exchange,
                segment=segment,
                instrument_id=instrument_id,
                candle_interval=candle_interval,
                trading_day=trading_day,
            )
            empty_path.parent.mkdir(parents=True, exist_ok=True)
            empty_path.write_text("")

    def _parquet_path(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        candle_interval: str,
        trading_day: date,
    ) -> Path:
        return (
            self.root
            / exchange
            / segment
            / candle_interval
            / instrument_id
            / f"{trading_day.isoformat()}.parquet"
        )

    def _empty_marker_path(
        self,
        *,
        exchange: str,
        segment: str,
        instrument_id: str,
        candle_interval: str,
        trading_day: date,
    ) -> Path:
        return (
            self.root
            / exchange
            / segment
            / candle_interval
            / instrument_id
            / f"{trading_day.isoformat()}.empty"
        )


def daterange(start: date, end: date) -> list[date]:
    total_days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(total_days + 1)]
