# NIFTY Options Proxy Backtester

Streamlit app for rough, candle-based backtesting of NIFTY weekly expiry options using Groww or Upstox market data.

The app now supports both `Groww` and `Upstox` as historical data providers. You can switch between them from the provider selector in the UI.

## Caveats

- This is a proxy backtest built on 1-minute candles.
- It does not reconstruct the true intraminute price path.
- Option spreads, stale prints, and intermittent trades can make candle-only results look better than real execution.

## Setup

1. Ensure `credentials.yaml` exists at the repository root.
   - `groww.api_key` and `groww.secret` are used for Groww
   - `upstox.access_token` is used for Upstox
2. Create the environment with micromamba:

```bash
MICROMAMBA_BIN="${MICROMAMBA_BIN:-$HOME/micromamba/bin/micromamba}"
MAMBA_ROOT_PREFIX="${MAMBA_ROOT_PREFIX:-$HOME/micromamba}"

"$MICROMAMBA_BIN" create -f environment.yml
```

3. Run the app:

```bash
"$MICROMAMBA_BIN" run -n nifty-backtest streamlit run app.py
```

## Notebook Kernel

Install the notebook kernel into the environment and register it once:

```bash
"$MICROMAMBA_BIN" run -n nifty-backtest python -m pip install -r requirements.txt
"$MICROMAMBA_BIN" run -n nifty-backtest python -m ipykernel install --user --name nifty-backtest --display-name "Python (nifty-backtest)"
```

Then select the `Python (nifty-backtest)` kernel when opening the diagnostics notebook.

## Tests

```bash
"$MICROMAMBA_BIN" run -n nifty-backtest pytest
```
