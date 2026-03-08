from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from nifty_backtest.backtest import run_backtest
from nifty_backtest.config import (
    CredentialsError,
    load_groww_credentials,
    load_upstox_credentials,
)
from nifty_backtest.models import BacktestRunConfig, FillTiming, ReversalProxyConfig
from nifty_backtest.providers.base import (
    DataProvider,
    ProviderConfigurationError,
    ProviderError,
    ProviderPermissionError,
)
from nifty_backtest.providers.groww import GrowwProvider
from nifty_backtest.providers.upstox import UpstoxProvider
from nifty_backtest.strategy import REVERSAL_PROXY_DEFINITION


st.set_page_config(page_title="NIFTY Options Proxy Backtester", layout="wide")


def main() -> None:
    st.title("NIFTY Options Proxy Backtester")

    selection_col, settings_col = st.columns([1.3, 1.0], gap="large")

    with selection_col:
        st.subheader("Market Selection")
        provider_key = st.segmented_control(
            "Historical data provider",
            options=["upstox", "groww"],
            default=["upstox", "groww"][default_provider_index()],
            format_func=lambda value: value.title(),
            selection_mode="single",
        )
        provider = build_provider(provider_key or "upstox")
        underlying = build_underlying_input(provider)

        default_end = date.today()
        default_start = default_end - timedelta(days=7)
        selected_dates = st.date_input(
            "Backtest date range",
            value=(default_start, default_end),
            min_value=date(2020, 1, 1),
        )
        start_date, end_date = normalize_date_range(selected_dates)

        expiries = load_expiries(
            provider=provider,
            underlying_symbol=underlying,
            start_date=start_date,
            end_date=end_date,
        )
        expiry = st.selectbox("Expiry", options=expiries, disabled=not expiries)

        contracts = (
            provider.get_contracts(
                exchange="NSE",
                underlying_symbol=underlying,
                expiry_date=expiry,
            )
            if expiry
            else []
        )
        contracts = [contract for contract in contracts if contract.instrument_type in {"CE", "PE"}]

        option_types = st.multiselect("Option types", options=["CE", "PE"], default=["CE", "PE"])
        filtered_contracts = [
            contract for contract in contracts if contract.instrument_type in option_types
        ]
        selected_contract_labels = st.multiselect(
            "Contracts",
            options=[contract.display_label for contract in filtered_contracts],
            default=[contract.display_label for contract in filtered_contracts[:2]],
            help="Use search inside the multiselect to narrow strikes quickly.",
        )
        selected_contracts = [
            contract
            for contract in filtered_contracts
            if contract.display_label in selected_contract_labels
        ]
        st.caption(
            "Some strikes can return no candles when the provider has no trades for that contract "
            "during the selected dates."
        )

    with settings_col:
        st.subheader("Strategy Settings")
        st.caption(REVERSAL_PROXY_DEFINITION.description)
        buy_drop_threshold = st.number_input("Buy drop threshold", min_value=0.0, value=50.0, step=1.0)
        buy_revert_threshold = st.number_input(
            "Buy revert threshold", min_value=0.0, value=20.0, step=1.0
        )
        sell_rise_threshold = st.number_input(
            "Sell rise threshold", min_value=0.0, value=30.0, step=1.0
        )
        sell_revert_threshold = st.number_input(
            "Sell revert threshold", min_value=0.0, value=10.0, step=1.0
        )

        st.subheader("Execution Settings")
        lots_per_trade = int(st.number_input("Lots per trade", min_value=1, value=1, step=1))
        fill_timing = st.radio(
            "Signal fill timing",
            options=[FillTiming.NEXT_CANDLE_OPEN, FillTiming.SIGNAL_CANDLE_CLOSE],
            format_func=format_fill_timing,
        )

    run_button = st.button("Fetch data and run backtest", type="primary")

    if run_button:
        if not selected_contracts:
            st.error("Select at least one contract before running the backtest.")
            return

        strategy_config = ReversalProxyConfig(
            buy_drop_threshold=buy_drop_threshold,
            buy_revert_threshold=buy_revert_threshold,
            sell_rise_threshold=sell_rise_threshold,
            sell_revert_threshold=sell_revert_threshold,
        )
        run_config = BacktestRunConfig(
            lots_per_trade=lots_per_trade,
            fill_timing=fill_timing,
        )

        start_dt = datetime.combine(start_date, time(9, 15))
        end_dt = datetime.combine(end_date, time(15, 30))

        try:
            with st.spinner("Fetching candles and running the proxy backtest..."):
                contract_candles = {
                contract: provider.get_candles(
                        exchange=contract.exchange,
                        segment=contract.segment,
                        instrument_id=contract.instrument_id,
                        start_time=start_dt,
                        end_time=end_dt,
                        candle_interval="1minute",
                    )
                    for contract in selected_contracts
                }
                result = run_backtest(
                    contract_candles=contract_candles,
                    strategy_config=strategy_config,
                    run_config=run_config,
                )
        except ProviderConfigurationError as exc:
            st.error(str(exc))
            return
        except ProviderPermissionError as exc:
            st.error(str(exc))
            return
        except ProviderError as exc:
            st.error(str(exc))
            return

        render_results(result, contract_candles)


def build_provider(provider_key: str) -> DataProvider:
    if provider_key == "upstox":
        try:
            credentials = load_upstox_credentials(required=False)
        except CredentialsError as exc:
            st.error(str(exc))
            credentials = None
        return UpstoxProvider(credentials=credentials)

    try:
        credentials = load_groww_credentials(required=False)
    except CredentialsError as exc:
        st.error(str(exc))
        credentials = None
    return GrowwProvider(credentials=credentials)


def build_underlying_input(provider: DataProvider) -> str:
    underlyings = provider.list_underlyings(exchange="NSE")
    default_index = underlyings.index("NIFTY") if "NIFTY" in underlyings else 0
    return st.selectbox(
        "Underlying",
        options=underlyings,
        index=default_index,
        help="NIFTY is the NSE NIFTY 50 index option underlying used in this v1 app.",
    )


def load_expiries(
    *,
    provider: DataProvider,
    underlying_symbol: str,
    start_date: date,
    end_date: date,
) -> list[str]:
    years = range(start_date.year, end_date.year + 1)
    expiries: set[str] = set()
    for year in years:
        expiries.update(
            provider.get_expiries(
                exchange="NSE",
                underlying_symbol=underlying_symbol,
                year=year,
            )
        )

    ordered = sorted(expiries)
    if ordered:
        return ordered

    return []


def default_provider_index() -> int:
    try:
        has_upstox = load_upstox_credentials(required=False) is not None
    except CredentialsError:
        has_upstox = False
    try:
        has_groww = load_groww_credentials(required=False) is not None
    except CredentialsError:
        has_groww = False

    if has_upstox:
        return 0
    if has_groww:
        return 1
    return 0


def normalize_date_range(selected_dates: tuple[date, date] | list[date] | date) -> tuple[date, date]:
    if isinstance(selected_dates, tuple):
        return selected_dates
    if isinstance(selected_dates, list):
        if len(selected_dates) == 2:
            return selected_dates[0], selected_dates[1]
        if len(selected_dates) == 1:
            return selected_dates[0], selected_dates[0]
    return selected_dates, selected_dates


def render_results(result, contract_candles: dict) -> None:
    st.subheader("Summary")
    summary = result.aggregate_summary
    metric_cols = st.columns(6)
    metric_cols[0].metric("Contracts", summary.contract_count)
    metric_cols[1].metric("Trades", summary.trade_count)
    metric_cols[2].metric("Win rate", f"{summary.win_rate:.1%}")
    metric_cols[3].metric("Gross points", f"{summary.gross_points:.2f}")
    metric_cols[4].metric("Gross P&L", f"Rs {summary.gross_pnl_rupees:,.2f}")
    metric_cols[5].metric("Max drawdown", f"Rs {summary.max_drawdown_rupees:,.2f}")

    st.subheader("Per-Contract Summary")
    st.dataframe(result.per_contract_summary, width="stretch")

    st.subheader("Candle Chart")
    render_candle_chart(result)

    st.subheader("Equity Curve")
    if result.equity_curve.empty:
        st.caption("No completed trades for the selected settings.")
    else:
        render_equity_curve(result.equity_curve)

    st.subheader("Daily P&L")
    if result.daily_pnl.empty:
        st.caption("No daily P&L to display.")
    else:
        daily_series = result.daily_pnl.set_index("exit_date")["pnl_rupees"]
        st.bar_chart(daily_series, width="stretch")

    st.subheader("Trade Log")
    st.dataframe(result.trade_log, width="stretch")

    st.subheader("Diagnostics")
    st.dataframe(result.diagnostics, width="stretch")

    st.subheader("Candle Preview")
    preview_frames: list[pd.DataFrame] = []
    for contract, candles in contract_candles.items():
        preview = candles.head(25).copy()
        if preview.empty:
            continue
        preview.insert(0, "contract_symbol", contract.trading_symbol)
        preview_frames.append(preview)
    if preview_frames:
        st.dataframe(pd.concat(preview_frames, ignore_index=True), width="stretch")
    else:
        st.caption("No candles returned for preview.")


def render_candle_chart(result) -> None:
    if not result.contract_results:
        return

    default_index = next(
        (index for index, item in enumerate(result.contract_results) if not item.candles.empty),
        0,
    )
    selected_label = st.selectbox(
        "Chart contract",
        options=[item.contract.display_label for item in result.contract_results],
        index=default_index,
    )
    selected_result = next(
        item for item in result.contract_results if item.contract.display_label == selected_label
    )

    if selected_result.candles.empty:
        st.caption("No candle data returned for the selected contract and date range.")
        return

    frame = selected_result.candles.copy()
    frame["buy_price"] = frame["low"]
    frame["sell_price"] = frame["high"]
    frame["timestamp"] = to_ist_series(frame["timestamp"])
    frame["timestamp_label"] = frame["timestamp"].dt.strftime("%d %b %Y %H:%M")
    frame["hover_label"] = frame["timestamp"].dt.strftime("%d %b %Y %H:%M IST")

    figure = go.Figure()
    figure.add_trace(
        go.Candlestick(
            x=frame["timestamp_label"],
            open=frame["open"],
            high=frame["high"],
            low=frame["low"],
            close=frame["close"],
            name="Price",
            increasing_line_color="#1f9d55",
            increasing_fillcolor="#1f9d55",
            decreasing_line_color="#d64545",
            decreasing_fillcolor="#d64545",
            customdata=frame[["hover_label", "volume", "open_interest"]].to_numpy(),
            hovertemplate=(
                "Time: %{customdata[0]}<br>"
                "Open: %{open:.2f}<br>"
                "High: %{high:.2f}<br>"
                "Low: %{low:.2f}<br>"
                "Close: %{close:.2f}<br>"
                "Volume: %{customdata[1]:,.0f}<br>"
                "Open interest: %{customdata[2]:,.0f}<extra></extra>"
            ),
        )
    )

    buy_signals = frame.loc[frame["entry_signal"]]
    if not buy_signals.empty:
        figure.add_trace(
            go.Scatter(
                x=buy_signals["timestamp_label"],
                y=buy_signals["buy_price"],
                mode="markers",
                name="Buy Signal",
                marker={
                    "symbol": "triangle-up",
                    "size": 11,
                    "color": "#1f9d55",
                    "line": {"width": 1, "color": "#0f5132"},
                },
                customdata=buy_signals[["hover_label"]].to_numpy(),
                hovertemplate=(
                    "Buy signal<br>"
                    "Time: %{customdata[0]}<br>"
                    "Price: %{y:.2f}<extra></extra>"
                ),
            )
        )

    sell_signals = frame.loc[frame["exit_signal"]]
    if not sell_signals.empty:
        figure.add_trace(
            go.Scatter(
                x=sell_signals["timestamp_label"],
                y=sell_signals["sell_price"],
                mode="markers",
                name="Sell Signal",
                marker={
                    "symbol": "triangle-down",
                    "size": 11,
                    "color": "#d64545",
                    "line": {"width": 1, "color": "#7f1d1d"},
                },
                customdata=sell_signals[["hover_label"]].to_numpy(),
                hovertemplate=(
                    "Sell signal<br>"
                    "Time: %{customdata[0]}<br>"
                    "Price: %{y:.2f}<extra></extra>"
                ),
            )
        )

    figure.update_layout(
        height=560,
        template="plotly_dark",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis={
            "rangeslider": {"visible": False},
            "type": "category",
            "categoryorder": "array",
            "categoryarray": frame["timestamp_label"].tolist(),
            "showgrid": True,
            "tickmode": "auto",
        },
        yaxis={"fixedrange": False, "showgrid": True},
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config={"displaylogo": False, "responsive": True},
    )


def render_equity_curve(equity_curve: pd.DataFrame) -> None:
    frame = equity_curve.copy()
    frame["exit_time"] = to_ist_series(frame["exit_time"])
    frame["exit_label"] = frame["exit_time"].dt.strftime("%d %b %Y %H:%M")
    frame["hover_label"] = frame["exit_time"].dt.strftime("%d %b %Y %H:%M IST")

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["exit_label"],
            y=frame["equity_rupees"],
            mode="lines+markers",
            name="Equity",
            line={"color": "#4c9aff", "width": 2},
            marker={"size": 6},
            customdata=frame[["hover_label"]].to_numpy(),
            hovertemplate=(
                "Time: %{customdata[0]}<br>"
                "Equity: Rs %{y:,.2f}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        height=360,
        template="plotly_dark",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        xaxis_title="Time",
        yaxis_title="Equity (Rs)",
        xaxis={"type": "category", "showgrid": True},
        yaxis={"showgrid": True},
        hovermode="x unified",
        showlegend=False,
    )
    st.plotly_chart(
        figure,
        width="stretch",
        config={"displaylogo": False, "responsive": True},
    )


def to_ist_series(values: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(values, errors="coerce")
    if timestamps.dt.tz is None:
        return timestamps.dt.tz_localize("Asia/Kolkata")
    return timestamps.dt.tz_convert("Asia/Kolkata")


def format_fill_timing(value: FillTiming) -> str:
    if value == FillTiming.NEXT_CANDLE_OPEN:
        return "Next candle open"
    return "Signal candle close"


if __name__ == "__main__":
    main()
