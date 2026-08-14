from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8765, gt=0, le=65_535)
    pipeline_mode: Literal["corpus_incremental"] = "corpus_incremental"
    history_freeze: bool = True
    daily_overlap_policy: Literal["reject", "allow"] = "reject"
    correction_batch_enabled: bool = True
    dictionary_review_required: bool = True
    pg_trgm_enabled: bool = True
    normalization_vector_enabled: bool = False
    normalization_llm_enabled: bool = True
    normalization_llm_concurrency: int = Field(default=2, gt=0)
    parse_concurrency: int = Field(default=4, gt=0)
    daily_batch_concurrency: int = Field(default=2, gt=0)
    database_mode: Literal["sqlite", "postgresql"] = "sqlite"
    database_path: Path = Path("runtime/app.db")
    db_host: str = "127.0.0.1"
    db_port: int = Field(default=5432, gt=0, le=65_535)
    db_user: str = "postgres"
    db_password: str = ""
    db_name: str = "gongdan"
    db_pool_size: int = Field(default=10, gt=0)
    db_max_overflow: int = Field(default=10, ge=0)

    milvus_host: str = "127.0.0.1"
    milvus_port: int = Field(default=19530, gt=0, le=65_535)
    milvus_user: str = "root"
    milvus_password: str = ""
    milvus_db: str = "gongdan"
    milvus_collection: str = "complaint_records_bge_m3_v1"
    embedding_dimension: int = Field(default=1024, gt=0)
    max_total_rows: int = Field(default=200_000, gt=0)
    default_match_preset: Literal["strict", "balanced", "loose"] = "balanced"
    default_time_window_days: int = Field(default=0, ge=0)
    max_candidates_per_record: int = Field(default=50, gt=0)
    broad_key_max_matches: int = Field(default=200, gt=0)
    vector_top_k: int = Field(default=50, gt=0)
    rerank_top_n: int = Field(default=12, gt=0)
    rerank_enabled: bool = True
    pipeline_version: Literal["pair_v1", "event_cluster_v2"] = "event_cluster_v2"
    event_vector_top_k: int = Field(default=30, gt=0)
    event_rerank_top_n: int = Field(default=12, gt=0)
    event_raw_group_limit: int = Field(default=20, gt=0)
    event_component_max_size: int = Field(default=200, gt=0)
    event_llm_concurrency: int = Field(default=2, gt=0)
    auto_merge_enabled: bool = True
    auto_merge_confidence: float = Field(default=0.95, ge=0, le=1)
    auto_merge_max_members: int = Field(default=20, gt=0)

    job_concurrency: int = Field(default=2, gt=0)
    max_inflight_batches_per_job: int = Field(default=2, gt=0)
    embedding_concurrency: int = Field(default=4, gt=0)
    milvus_concurrency: int = Field(default=8, gt=0)
    rerank_concurrency: int = Field(default=4, gt=0)
    http_max_connections: int = Field(default=24, gt=0)
    http_max_keepalive_connections: int = Field(default=12, gt=0)
    job_lease_seconds: int = Field(default=300, gt=0)
    job_heartbeat_seconds: int = Field(default=30, gt=0)

    llm_base_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1")
    llm_api_key: str = ""
    llm_model: str = ""
    llm_extraction_model: str = ""
    llm_judgement_model: str = ""
    llm_concurrency: int = Field(default=2, gt=0)
    llm_extraction_concurrency: int | None = Field(default=None, gt=0)
    llm_judgement_concurrency: int | None = Field(default=None, gt=0)
    llm_timeout_seconds: float = Field(default=180, gt=0)
    llm_max_retries: int = Field(default=3, gt=0)
    llm_extraction_batch_size: int = Field(default=20, gt=0)
    llm_judgement_batch_size: int = Field(default=20, gt=0)
    llm_temperature: float = Field(default=0, ge=0)
    llm_max_tokens: int = Field(default=4096, gt=0)
    llm_enable_thinking: bool = False
    llm_send_enable_thinking: bool = True
    llm_redact_pii: bool = False

    llm_response_format: Literal["text", "json_object"] = "text"
    embedding_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1/embeddings")
    embedding_model: str = "bge-m3"
    embedding_timeout: float = Field(default=120, gt=0)
    embedding_retries: int = Field(default=3, gt=0)
    embedding_retry_backoff: float = Field(default=1.5, gt=0)
    embedding_batch_size: int = Field(default=16, gt=0)
    rerank_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1/rerank")
    rerank_model: str = "bge-reranker-v2-m3"
    rerank_timeout: float = Field(default=60, gt=0)
    rerank_retries: int = Field(default=3, gt=0)
    rerank_retry_doc_limit: int = Field(default=10, gt=0)

    @model_validator(mode="after")
    def validate_concurrency_capacity(self) -> "Settings":
        if self.llm_extraction_concurrency is None:
            object.__setattr__(self, "llm_extraction_concurrency", self.llm_concurrency)
        if self.llm_judgement_concurrency is None:
            object.__setattr__(self, "llm_judgement_concurrency", self.llm_concurrency)
        if self.db_pool_size < self.job_concurrency:
            raise ValueError("DB_POOL_SIZE must be at least JOB_CONCURRENCY")
        if self.job_heartbeat_seconds >= self.job_lease_seconds:
            raise ValueError("JOB_HEARTBEAT_SECONDS must be less than JOB_LEASE_SECONDS")
        if self.rerank_top_n > self.vector_top_k:
            raise ValueError("RERANK_TOP_N must not exceed VECTOR_TOP_K")
        if self.event_rerank_top_n > self.event_vector_top_k:
            raise ValueError("EVENT_RERANK_TOP_N must not exceed EVENT_VECTOR_TOP_K")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
