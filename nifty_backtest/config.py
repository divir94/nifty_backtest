from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CREDENTIALS_PATH = REPO_ROOT / "credentials.yaml"
DEFAULT_CACHE_DIR = REPO_ROOT / ".cache" / "nifty_backtest"


class CredentialsError(RuntimeError):
    """Raised when provider credentials are missing or invalid."""


@dataclass(frozen=True)
class GrowwCredentials:
    api_key: str
    secret: str


@dataclass(frozen=True)
class UpstoxCredentials:
    access_token: str


def load_groww_credentials(
    credentials_path: Path | None = None,
    *,
    required: bool = True,
) -> GrowwCredentials | None:
    env_api_key = os.getenv("GROWW_API_KEY")
    env_secret = os.getenv("GROWW_SECRET")
    if env_api_key and env_secret:
        return GrowwCredentials(api_key=env_api_key, secret=env_secret)

    secrets_api_key, secrets_secret = _load_streamlit_section_values(
        "groww",
        "api_key",
        "secret",
    )
    if secrets_api_key and secrets_secret:
        return GrowwCredentials(api_key=secrets_api_key, secret=secrets_secret)

    config = _load_yaml(credentials_path or DEFAULT_CREDENTIALS_PATH)
    groww_config = config.get("groww", {})
    api_key = str(groww_config.get("api_key", "")).strip()
    secret = str(groww_config.get("secret", "")).strip()
    if api_key and secret:
        return GrowwCredentials(api_key=api_key, secret=secret)

    if required:
        raise CredentialsError(
            "Groww credentials were not found. Use credentials.yaml, "
            "GROWW_API_KEY/GROWW_SECRET, or Streamlit secrets."
        )
    return None


def load_upstox_credentials(
    credentials_path: Path | None = None,
    *,
    required: bool = True,
) -> UpstoxCredentials | None:
    env_access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
    if env_access_token:
        return UpstoxCredentials(access_token=env_access_token)

    (secrets_access_token,) = _load_streamlit_section_values("upstox", "access_token")
    if secrets_access_token:
        return UpstoxCredentials(access_token=secrets_access_token)

    config = _load_yaml(credentials_path or DEFAULT_CREDENTIALS_PATH)
    upstox_config = config.get("upstox", {})
    access_token = str(upstox_config.get("access_token", "")).strip()
    if access_token:
        return UpstoxCredentials(access_token=access_token)

    if required:
        raise CredentialsError(
            "Upstox credentials were not found. Use credentials.yaml, "
            "UPSTOX_ACCESS_TOKEN, or Streamlit secrets."
        )
    return None


def get_cache_dir() -> Path:
    return DEFAULT_CACHE_DIR


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise CredentialsError(f"{path} must contain a mapping at the top level.")
    return data


def _load_streamlit_section_values(section: str, *keys: str) -> tuple[str | None, ...]:
    try:
        import streamlit as st
    except Exception:
        return tuple(None for _ in keys)

    try:
        secrets = st.secrets
    except Exception:
        return tuple(None for _ in keys)

    try:
        section_values = secrets.get(section, {})
    except Exception:
        return tuple(None for _ in keys)
    if isinstance(section_values, dict):
        resolved = tuple(str(section_values.get(key, "")).strip() or None for key in keys)
        if all(resolved):
            return resolved

    top_level_resolved = tuple(
        _lookup_top_level_streamlit_secret(secrets, section, key) for key in keys
    )
    return top_level_resolved


def _lookup_top_level_streamlit_secret(
    secrets: Any,
    section: str,
    key: str,
) -> str | None:
    candidate_names = (
        f"{section}_{key}",
        f"{section}_{key}".upper(),
        key,
        key.upper(),
    )
    for candidate in candidate_names:
        try:
            value = secrets.get(candidate, "")
        except Exception:
            return None
        cleaned = str(value).strip() if value is not None else ""
        if cleaned:
            return cleaned
    return None
