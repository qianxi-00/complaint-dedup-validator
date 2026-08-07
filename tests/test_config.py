from pathlib import Path

import pytest
from pydantic import ValidationError

from complaint_dedup.config import Settings, get_settings


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "llm_base_url": "http://127.0.0.1:8000/v1",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


def test_settings_loads_approved_defaults() -> None:
    settings = make_settings()

    assert settings.app_host == "127.0.0.1"
    assert settings.app_port == 8765
    assert settings.database_path == Path("runtime/app.db")
    assert settings.max_total_rows == 10_000
    assert settings.default_match_preset == "balanced"
    assert settings.default_time_window_days == 0
    assert settings.max_candidates_per_record == 50
    assert settings.broad_key_max_matches == 200
    assert str(settings.llm_base_url).rstrip("/") == "http://127.0.0.1:8000/v1"
    assert settings.llm_api_key == ""
    assert settings.llm_model == ""
    assert settings.llm_concurrency == 2
    assert settings.llm_timeout_seconds == 180
    assert settings.llm_max_retries == 3
    assert settings.llm_extraction_batch_size == 20
    assert settings.llm_judgement_batch_size == 20
    assert settings.llm_temperature == 0
    assert settings.llm_redact_pii is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("app_port", 0),
        ("max_total_rows", 0),
        ("max_candidates_per_record", 0),
        ("broad_key_max_matches", 0),
        ("llm_concurrency", 0),
        ("llm_timeout_seconds", 0),
        ("llm_max_retries", 0),
        ("llm_extraction_batch_size", 0),
        ("llm_judgement_batch_size", 0),
    ],
)
def test_settings_rejects_non_positive_limits(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


def test_settings_rejects_missing_llm_base_url() -> None:
    with pytest.raises(ValidationError):
        make_settings(llm_base_url="")


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
