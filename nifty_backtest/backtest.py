from __future__ import annotations

from dataclasses import asdict
from datetime import date

import pandas as pd

from nifty_backtest.models import (
    BacktestResult,
    BacktestRunConfig,
    Contract,
    ContractBacktestResult,
    ContractSummary,
    DataQualityReport,
    FillTiming,
    ReversalProxyConfig,
    RunSummary,
    Trade,
)
from nifty_backtest.strategy import ReversalProxyStrategy


def run_backtest(
    *,
    contract_candles: dict[Contract, pd.DataFrame],
    strategy_config: ReversalProxyConfig,
    run_config: BacktestRunConfig,
) -> BacktestResult:
    strategy = ReversalProxyStrategy(strategy_config)
    contract_results: list[ContractBacktestResult] = []

    for contract, candles in contract_candles.items():
        contract_results.append(
            _backtest_contract(
                contract=contract,
                candles=candles,
                strategy=strategy,
                strategy_config=strategy_config,
                run_config=run_config,
            )
        )

    trade_frames = [result.trade_log for result in contract_results if not result.trade_log.empty]
    combined_trade_log = (
        pd.concat(trade_frames, ignore_index=True).sort_values("exit_time")
        if trade_frames
        else empty_trade_log()
    )
    if not combined_trade_log.empty:
        combined_trade_log["exit_date"] = combined_trade_log["exit_time"].dt.date

    per_contract_summary = pd.DataFrame(
        [asdict(result.summary) for result in contract_results]
    )
    aggregate_summary = build_run_summary(
        contract_count=len(contract_results),
        trade_log=combined_trade_log,
    )
    equity_curve = build_equity_curve(combined_trade_log)
    daily_pnl = build_daily_pnl(combined_trade_log)
    diagnostics = pd.DataFrame(
        [
            {
                "trading_symbol": result.contract.trading_symbol,
                "instrument_id": result.contract.instrument_id,
                **asdict(result.diagnostics),
                "warnings": " | ".join(
                    result.diagnostics.warning_messages(result.contract.trading_symbol)
                ),
            }
            for result in contract_results
        ]
    )

    return BacktestResult(
        aggregate_summary=aggregate_summary,
        per_contract_summary=per_contract_summary,
        trade_log=combined_trade_log,
        equity_curve=equity_curve,
        daily_pnl=daily_pnl,
        diagnostics=diagnostics,
        contract_results=contract_results,
    )


def _backtest_contract(
    *,
    contract: Contract,
    candles: pd.DataFrame,
    strategy: ReversalProxyStrategy,
    strategy_config: ReversalProxyConfig,
    run_config: BacktestRunConfig,
) -> ContractBacktestResult:
    prepared = prepare_candles(candles, strategy)
    diagnostics = analyze_candle_quality(prepared)
    trades = simulate_trades(
        contract=contract,
        candles=prepared,
        strategy_config=strategy_config,
        run_config=run_config,
    )
    trade_log = pd.DataFrame([asdict(trade) for trade in trades]) if trades else empty_trade_log()
    summary = build_contract_summary(contract=contract, trade_log=trade_log)
    return ContractBacktestResult(
        contract=contract,
        diagnostics=diagnostics,
        summary=summary,
        trade_log=trade_log,
        candles=prepared,
    )


def prepare_candles(candles: pd.DataFrame, strategy: ReversalProxyStrategy) -> pd.DataFrame:
    if candles.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
                "entry_signal",
                "entry_trigger_price",
                "trade_date",
            ]
        )

    frame = candles.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    frame = frame.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
    frame = strategy.annotate(frame)
    frame["trade_date"] = frame["timestamp"].dt.date
    return frame.reset_index(drop=True)


def analyze_candle_quality(candles: pd.DataFrame) -> DataQualityReport:
    if candles.empty:
        return DataQualityReport(
            row_count=0,
            start_time=None,
            end_time=None,
            duplicate_timestamps=0,
            missing_intervals=0,
            zero_volume_rows=0,
            zero_open_interest_rows=0,
        )

    timestamps = candles["timestamp"].sort_values()
    missing_intervals = 0
    for _, day_frame in candles.groupby(candles["timestamp"].dt.date, sort=True):
        deltas = day_frame["timestamp"].sort_values().diff().dropna()
        if deltas.empty:
            continue
        delta_minutes = deltas.dt.total_seconds().div(60).astype(int)
        missing_intervals += int(delta_minutes.sub(1).clip(lower=0).sum())

    return DataQualityReport(
        row_count=len(candles),
        start_time=timestamps.iloc[0],
        end_time=timestamps.iloc[-1],
        duplicate_timestamps=int(candles["timestamp"].duplicated().sum()),
        missing_intervals=missing_intervals,
        zero_volume_rows=int((candles["volume"] <= 0).sum()),
        zero_open_interest_rows=int((candles["open_interest"] <= 0).sum()),
    )


def simulate_trades(
    *,
    contract: Contract,
    candles: pd.DataFrame,
    strategy_config: ReversalProxyConfig,
    run_config: BacktestRunConfig,
) -> list[Trade]:
    if candles.empty:
        return []

    trades: list[Trade] = []
    pending_entry: dict[str, object] | None = None
    position: dict[str, object] | None = None

    for _, day_frame in candles.groupby("trade_date", sort=True):
        rows = day_frame.reset_index(drop=True)
        for index, row in rows.iterrows():
            timestamp = row["timestamp"]
            is_last_row = index == len(rows) - 1

            if pending_entry is not None:
                if position is None:
                    position = {
                        "entry_signal_time": pending_entry["signal_time"],
                        "entry_time": timestamp,
                        "entry_price": float(pending_entry["trigger_price"]),
                        "entry_fill_timing": FillTiming.NEXT_CANDLE_OPEN,
                    }
                pending_entry = None

                exit_event = resolve_exit(
                    row=row,
                    position=position,
                    strategy_config=strategy_config,
                )
                if exit_event is not None:
                    trades.append(
                        build_trade(
                            contract=contract,
                            run_config=run_config,
                            position=position,
                            exit_signal_time=timestamp,
                            exit_time=timestamp,
                            exit_price=exit_event["price"],
                            exit_reason=exit_event["reason"],
                        )
                    )
                    position = None
            entry_signal = bool(row["entry_signal"])

            if run_config.fill_timing == FillTiming.SIGNAL_CANDLE_CLOSE:
                if position is None:
                    if entry_signal:
                        position = {
                            "entry_signal_time": timestamp,
                            "entry_time": timestamp,
                            "entry_price": float(row["entry_trigger_price"]),
                            "entry_fill_timing": FillTiming.SIGNAL_CANDLE_CLOSE,
                        }
                        exit_event = resolve_exit(
                            row=row,
                            position=position,
                            strategy_config=strategy_config,
                        )
                        if exit_event is not None:
                            trades.append(
                                build_trade(
                                    contract=contract,
                                    run_config=run_config,
                                    position=position,
                                    exit_signal_time=timestamp,
                                    exit_time=timestamp,
                                    exit_price=exit_event["price"],
                                    exit_reason=exit_event["reason"],
                                )
                            )
                            position = None
                else:
                    exit_event = resolve_exit(
                        row=row,
                        position=position,
                        strategy_config=strategy_config,
                    )
                    if exit_event is not None:
                        trades.append(
                            build_trade(
                                contract=contract,
                                run_config=run_config,
                                position=position,
                                exit_signal_time=timestamp,
                                exit_time=timestamp,
                                exit_price=exit_event["price"],
                                exit_reason=exit_event["reason"],
                            )
                        )
                        position = None
            else:
                if position is None:
                    if entry_signal and not is_last_row:
                        pending_entry = {
                            "signal_time": timestamp,
                            "trigger_price": float(row["entry_trigger_price"]),
                        }
                else:
                    exit_event = resolve_exit(
                        row=row,
                        position=position,
                        strategy_config=strategy_config,
                    )
                    if exit_event is not None:
                        trades.append(
                            build_trade(
                                contract=contract,
                                run_config=run_config,
                                position=position,
                                exit_signal_time=timestamp,
                                exit_time=timestamp,
                                exit_price=exit_event["price"],
                                exit_reason=exit_event["reason"],
                            )
                        )
                        position = None

            if is_last_row:
                pending_entry = None
                if position is not None:
                    trades.append(
                        build_trade(
                            contract=contract,
                            run_config=run_config,
                            position=position,
                            exit_signal_time=timestamp,
                            exit_time=timestamp,
                            exit_price=float(row["close"]),
                            exit_reason="forced_eod",
                        )
                    )
                    position = None

    return trades


def resolve_exit(
    *,
    row: pd.Series,
    position: dict[str, object],
    strategy_config: ReversalProxyConfig,
) -> dict[str, object] | None:
    entry_price = float(position["entry_price"])
    target_price = entry_price + strategy_config.take_profit_threshold
    stop_loss_threshold = strategy_config.stop_loss_threshold
    stop_price = entry_price - stop_loss_threshold if stop_loss_threshold is not None else None

    is_signal_candle_entry = (
        position.get("entry_fill_timing") == FillTiming.SIGNAL_CANDLE_CLOSE
        and position.get("entry_time") == row["timestamp"]
    )

    target_hit = float(row["high"]) >= target_price
    if stop_price is None:
        stop_hit = False
    elif is_signal_candle_entry:
        stop_hit = float(row["close"]) <= stop_price
    else:
        stop_hit = float(row["low"]) <= stop_price

    if stop_hit:
        return {"price": float(stop_price), "reason": "stop_loss"}
    if target_hit:
        return {"price": float(target_price), "reason": "take_profit"}
    return None


def build_trade(
    *,
    contract: Contract,
    run_config: BacktestRunConfig,
    position: dict[str, object],
    exit_signal_time: pd.Timestamp,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
) -> Trade:
    entry_price = float(position["entry_price"])
    points = exit_price - entry_price
    pnl_rupees = points * contract.lot_size * run_config.lots_per_trade
    return Trade(
        contract_symbol=contract.trading_symbol,
        instrument_id=contract.instrument_id,
        instrument_type=contract.instrument_type,
        expiry_date=contract.expiry_date,
        strike_price=contract.strike_price,
        lot_size=contract.lot_size,
        lots=run_config.lots_per_trade,
        entry_signal_time=position["entry_signal_time"],
        entry_time=position["entry_time"],
        entry_price=entry_price,
        exit_signal_time=exit_signal_time,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=exit_reason,
        points=points,
        pnl_rupees=pnl_rupees,
    )


def build_contract_summary(contract: Contract, trade_log: pd.DataFrame) -> ContractSummary:
    if trade_log.empty:
        return ContractSummary(
            trading_symbol=contract.trading_symbol,
            instrument_id=contract.instrument_id,
            instrument_type=contract.instrument_type,
            expiry_date=contract.expiry_date,
            strike_price=contract.strike_price,
            lot_size=contract.lot_size,
            trade_count=0,
            win_rate=0.0,
            gross_points=0.0,
            gross_pnl_rupees=0.0,
            average_points=0.0,
            average_pnl_rupees=0.0,
            max_drawdown_rupees=0.0,
        )

    return ContractSummary(
        trading_symbol=contract.trading_symbol,
        instrument_id=contract.instrument_id,
        instrument_type=contract.instrument_type,
        expiry_date=contract.expiry_date,
        strike_price=contract.strike_price,
        lot_size=contract.lot_size,
        trade_count=int(len(trade_log)),
        win_rate=float((trade_log["pnl_rupees"] > 0).mean()),
        gross_points=float(trade_log["points"].sum()),
        gross_pnl_rupees=float(trade_log["pnl_rupees"].sum()),
        average_points=float(trade_log["points"].mean()),
        average_pnl_rupees=float(trade_log["pnl_rupees"].mean()),
        max_drawdown_rupees=compute_max_drawdown(trade_log["pnl_rupees"]),
    )


def build_run_summary(*, contract_count: int, trade_log: pd.DataFrame) -> RunSummary:
    if trade_log.empty:
        return RunSummary(
            contract_count=contract_count,
            trade_count=0,
            win_rate=0.0,
            gross_points=0.0,
            gross_pnl_rupees=0.0,
            average_points=0.0,
            average_pnl_rupees=0.0,
            max_drawdown_rupees=0.0,
        )

    return RunSummary(
        contract_count=contract_count,
        trade_count=int(len(trade_log)),
        win_rate=float((trade_log["pnl_rupees"] > 0).mean()),
        gross_points=float(trade_log["points"].sum()),
        gross_pnl_rupees=float(trade_log["pnl_rupees"].sum()),
        average_points=float(trade_log["points"].mean()),
        average_pnl_rupees=float(trade_log["pnl_rupees"].mean()),
        max_drawdown_rupees=compute_max_drawdown(trade_log["pnl_rupees"]),
    )


def build_equity_curve(trade_log: pd.DataFrame) -> pd.DataFrame:
    if trade_log.empty:
        return pd.DataFrame(columns=["exit_time", "equity_rupees"])
    equity_curve = trade_log[["exit_time", "pnl_rupees"]].copy()
    equity_curve["equity_rupees"] = equity_curve["pnl_rupees"].cumsum()
    return equity_curve[["exit_time", "equity_rupees"]]


def build_daily_pnl(trade_log: pd.DataFrame) -> pd.DataFrame:
    if trade_log.empty:
        return pd.DataFrame(columns=["exit_date", "pnl_rupees"])
    daily = (
        trade_log.assign(exit_date=trade_log["exit_time"].dt.date)
        .groupby("exit_date", as_index=False)["pnl_rupees"]
        .sum()
    )
    return daily


def compute_max_drawdown(pnl_rupees: pd.Series) -> float:
    if pnl_rupees.empty:
        return 0.0
    equity = pnl_rupees.cumsum()
    drawdown = equity - equity.cummax()
    return float(abs(drawdown.min()))


def empty_trade_log() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "contract_symbol",
            "instrument_id",
            "instrument_type",
            "expiry_date",
            "strike_price",
            "lot_size",
            "lots",
            "entry_signal_time",
            "entry_time",
            "entry_price",
            "exit_signal_time",
            "exit_time",
            "exit_price",
            "exit_reason",
            "points",
            "pnl_rupees",
        ]
    )
