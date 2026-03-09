from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path

from nifty_backtest.config import UpstoxCredentials, load_upstox_credentials


class FakeSecretsSection(Mapping):
    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> str:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def get(self, key: str, default=None):
        return self._data.get(key, default)


class FakeSecretsRoot(Mapping):
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)


def test_load_upstox_credentials_from_streamlit_mapping_section(monkeypatch) -> None:
    fake_streamlit = type(
        "FakeStreamlit",
        (),
        {
            "secrets": FakeSecretsRoot(
                {
                    "upstox": FakeSecretsSection({"access_token": "mapping-token"}),
                }
            )
        },
    )()
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    credentials = load_upstox_credentials(credentials_path=Path("/does/not/exist"))

    assert credentials == UpstoxCredentials(access_token="mapping-token")
