# NIFTY Options Proxy Backtester

Streamlit app for rough, candle-based backtesting of NIFTY weekly expiry options using Groww or Upstox market data.

The app now supports both `Groww` and `Upstox` as historical data providers. You can switch between them from the provider selector in the UI.

## Caveats

- This is a proxy backtest built on 1-minute candles.
- It does not reconstruct the true intraminute price path.
- Option spreads, stale prints, and intermittent trades can make candle-only results look better than real execution.

## Setup

1. Provide credentials using either local `credentials.yaml`, environment variables, or Streamlit Cloud secrets.
   - Local file:

```yaml
groww:
  api_key: "YOUR_GROWW_API_KEY"
  secret: "YOUR_GROWW_SECRET"

upstox:
  access_token: "YOUR_UPSTOX_ACCESS_TOKEN"
```

   - Environment variables:
     - `GROWW_API_KEY`
     - `GROWW_SECRET`
     - `UPSTOX_ACCESS_TOKEN`
   - Streamlit Cloud secrets:

```toml
[groww]
api_key = "YOUR_GROWW_API_KEY"
secret = "YOUR_GROWW_SECRET"

[upstox]
access_token = "YOUR_UPSTOX_ACCESS_TOKEN"
```

   - Flat Streamlit secrets also work:

```toml
GROWW_API_KEY = "YOUR_GROWW_API_KEY"
GROWW_SECRET = "YOUR_GROWW_SECRET"
UPSTOX_ACCESS_TOKEN = "YOUR_UPSTOX_ACCESS_TOKEN"
```
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
