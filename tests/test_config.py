from pathlib import Path

import pytest
from pydantic import ValidationError

from complaint_dedup.config import Settings, get_settings


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = Settings.model_construct().model_dump()
    values["_env_file"] = None
    values.update(overrides)
    return Settings(**values)


def test_settings_loads_approved_defaults() -> None:
    settings = make_settings()

    assert settings.app_host == "127.0.0.1"
    assert settings.app_port == 8765
    assert settings.database_path == Path("runtime/app.db")
    assert settings.database_mode == "sqlite"
    assert settings.db_name == "gongdan"
    assert settings.milvus_db == "gongdan"
    assert settings.max_total_rows == 200_000
    assert settings.default_match_preset == "balanced"
    assert settings.default_time_window_days == 0
    assert settings.max_candidates_per_record == 50
    assert settings.broad_key_max_matches == 200
    assert str(settings.llm_base_url).rstrip("/") == "http://127.0.0.1:8000/v1"
    assert settings.llm_api_key == ""
    assert settings.llm_model == ""
    assert settings.llm_extraction_model == ""
    assert settings.llm_judgement_model == ""
    assert settings.llm_send_enable_thinking is True
    assert settings.llm_concurrency == 2
    assert settings.job_concurrency == 2
    assert settings.max_inflight_batches_per_job == 2
    assert settings.embedding_concurrency == 4
    assert settings.milvus_concurrency == 8
    assert settings.rerank_concurrency == 4
    assert settings.llm_extraction_concurrency == 2
    assert settings.llm_judgement_concurrency == 2
    assert settings.db_pool_size == 10
    assert settings.db_max_overflow == 10
    assert settings.http_max_connections == 24
    assert settings.http_max_keepalive_connections == 12
    assert settings.job_lease_seconds == 300
    assert settings.job_heartbeat_seconds == 30
    assert settings.llm_timeout_seconds == 180
    assert settings.llm_max_retries == 3
    assert settings.llm_extraction_batch_size == 20
    assert settings.llm_judgement_batch_size == 20
    assert settings.llm_temperature == 0
    assert settings.llm_max_tokens == 4096
    assert settings.llm_enable_thinking is False
    assert settings.llm_redact_pii is False
    assert settings.pipeline_version == "event_cluster_v2"
    assert settings.event_vector_top_k == 30
    assert settings.event_rerank_top_n == 12
    assert settings.event_raw_group_limit == 20
    assert settings.event_component_max_size == 200
    assert settings.event_llm_concurrency == 2
    assert settings.auto_merge_enabled is True
    assert settings.auto_merge_confidence == 0.95
    assert settings.auto_merge_max_members == 20
    assert settings.pipeline_mode == "corpus_incremental"
    assert settings.history_freeze is True
    assert settings.daily_overlap_policy == "reject"
    assert settings.correction_batch_enabled is True
    assert settings.dictionary_review_required is True
    assert settings.pg_trgm_enabled is True
    assert settings.normalization_vector_enabled is False
    assert settings.normalization_llm_enabled is True
    assert settings.normalization_llm_concurrency == 2
    assert settings.parse_concurrency == 4
    assert settings.daily_batch_concurrency == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("app_port", 0),
        ("max_total_rows", 0),
        ("max_candidates_per_record", 0),
        ("broad_key_max_matches", 0),
        ("llm_concurrency", 0),
        ("job_concurrency", 0),
        ("max_inflight_batches_per_job", 0),
        ("embedding_concurrency", 0),
        ("milvus_concurrency", 0),
        ("rerank_concurrency", 0),
        ("llm_extraction_concurrency", 0),
        ("llm_judgement_concurrency", 0),
        ("db_pool_size", 0),
        ("http_max_connections", 0),
        ("http_max_keepalive_connections", 0),
        ("job_lease_seconds", 0),
        ("job_heartbeat_seconds", 0),
        ("llm_timeout_seconds", 0),
        ("llm_max_retries", 0),
        ("llm_extraction_batch_size", 0),
        ("llm_judgement_batch_size", 0),
        ("llm_max_tokens", 0),
        ("event_vector_top_k", 0),
        ("event_rerank_top_n", 0),
        ("event_raw_group_limit", 0),
        ("event_component_max_size", 0),
        ("event_llm_concurrency", 0),
        ("auto_merge_max_members", 0),
        ("normalization_llm_concurrency", 0),
        ("parse_concurrency", 0),
        ("daily_batch_concurrency", 0),
    ],
)
def test_settings_rejects_non_positive_limits(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


def test_settings_rejects_missing_llm_base_url() -> None:
    with pytest.raises(ValidationError):
        make_settings(llm_base_url="")


def test_settings_rejects_database_pool_smaller_than_job_concurrency() -> None:
    with pytest.raises(ValidationError, match="DB_POOL_SIZE"):
        make_settings(job_concurrency=4, db_pool_size=3)


def test_specific_llm_concurrency_can_override_legacy_value() -> None:
    settings = make_settings(
        llm_concurrency=3,
        llm_extraction_concurrency=5,
        llm_judgement_concurrency=7,
    )

    assert settings.llm_concurrency == 3
    assert settings.llm_extraction_concurrency == 5
    assert settings.llm_judgement_concurrency == 7


def test_legacy_llm_concurrency_fills_unspecified_stage_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LLM_CONCURRENCY", "7")
    monkeypatch.delenv("LLM_EXTRACTION_CONCURRENCY", raising=False)
    monkeypatch.delenv("LLM_JUDGEMENT_CONCURRENCY", raising=False)

    settings = Settings(_env_file=None)

    assert settings.llm_extraction_concurrency == 7
    assert settings.llm_judgement_concurrency == 7


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
