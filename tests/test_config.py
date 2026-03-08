from __future__ import annotations

import sys
import types
from pathlib import Path

from nifty_backtest.config import (
    GrowwCredentials,
    UpstoxCredentials,
    load_groww_credentials,
    load_upstox_credentials,
)


def test_load_upstox_credentials_from_yaml(tmp_path: Path) -> None:
    credentials_path = tmp_path / "credentials.yaml"
    credentials_path.write_text(
        "upstox:\n"
        "  access_token: yaml-token\n"
    )

    credentials = load_upstox_credentials(credentials_path=credentials_path)

    assert credentials == UpstoxCredentials(access_token="yaml-token")


def test_load_groww_credentials_from_streamlit_section(monkeypatch) -> None:
    fake_streamlit = types.SimpleNamespace(
        secrets={
            "groww": {
                "api_key": "section-api-key",
                "secret": "section-secret",
            }
        }
    )
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    credentials = load_groww_credentials(credentials_path=Path("/does/not/exist"))

    assert credentials == GrowwCredentials(
        api_key="section-api-key",
        secret="section-secret",
    )


def test_load_upstox_credentials_from_streamlit_top_level(monkeypatch) -> None:
    fake_streamlit = types.SimpleNamespace(secrets={"UPSTOX_ACCESS_TOKEN": "top-level-token"})
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    credentials = load_upstox_credentials(credentials_path=Path("/does/not/exist"))

    assert credentials == UpstoxCredentials(access_token="top-level-token")
