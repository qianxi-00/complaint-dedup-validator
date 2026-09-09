from pathlib import Path

import pytest
from pydantic import ValidationError

from complaint_dedup.config import Settings, get_settings


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = Settings.model_construct().model_dump()
    values["_env_file"] = None
    values.update(overrides)
    return Settings(**values)


def test_settings_load_active_defaults() -> None:
    settings = make_settings()

    assert settings.app_host == "127.0.0.1"
    assert settings.app_port == 8765
    assert settings.app_timezone == "Asia/Shanghai"
    assert settings.database_mode == "sqlite"
    assert settings.database_path == Path("runtime/app.db")
    assert settings.db_name == "gongdan"
    assert settings.db_pool_size == 10
    assert settings.db_max_overflow == 10
    assert settings.max_total_rows == 200_000
    assert settings.http_max_connections == 24
    assert settings.http_max_keepalive_connections == 12
    assert str(settings.llm_base_url).rstrip("/") == "http://127.0.0.1:8000/v1"
    assert settings.llm_api_key == ""
    assert settings.llm_model == ""
    assert settings.llm_concurrency == 2
    assert settings.llm_timeout_seconds == 180
    assert settings.llm_max_retries == 3
    assert settings.llm_temperature == 0
    assert settings.llm_max_tokens == 4096
    assert settings.llm_enable_thinking is False
    assert settings.llm_send_enable_thinking is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("app_port", 0),
        ("db_pool_size", 0),
        ("max_total_rows", 0),
        ("http_max_connections", 0),
        ("http_max_keepalive_connections", 0),
        ("llm_concurrency", 0),
        ("llm_timeout_seconds", 0),
        ("llm_max_retries", 0),
        ("llm_max_tokens", 0),
    ],
)
def test_settings_rejects_non_positive_limits(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


def test_settings_rejects_missing_llm_base_url() -> None:
    with pytest.raises(ValidationError):
        make_settings(llm_base_url="")


def test_settings_rejects_invalid_timezone() -> None:
    with pytest.raises(ValidationError, match="APP_TIMEZONE"):
        make_settings(app_timezone="Mars/Base")


def test_settings_rejects_keepalive_larger_than_connection_pool() -> None:
    with pytest.raises(ValidationError, match="HTTP_MAX_KEEPALIVE_CONNECTIONS"):
        make_settings(http_max_connections=4, http_max_keepalive_connections=5)


def test_settings_are_frozen() -> None:
    settings = make_settings()

    with pytest.raises(ValidationError):
        settings.app_port = 9999


def test_get_settings_returns_cached_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_BASE_URL", "http://127.0.0.1:9000/v1")
    get_settings.cache_clear()

    first = get_settings()
    second = get_settings()

    assert first is second
    get_settings.cache_clear()
