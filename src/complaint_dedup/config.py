from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    app_timezone: str = "Asia/Shanghai"

    database_mode: Literal["sqlite", "postgresql"] = "sqlite"
    database_path: Path = Path("runtime/app.db")
    db_host: str = "127.0.0.1"
    db_port: int = Field(default=5432, gt=0, le=65_535)
    db_user: str = "postgres"
    db_password: str = ""
    db_name: str = "gongdan"
    db_pool_size: int = Field(default=10, gt=0)
    db_max_overflow: int = Field(default=10, ge=0)

    max_total_rows: int = Field(default=200_000, gt=0)

    log_level: str = "INFO"
    log_dir: Path = Path("runtime/logs")
    log_retention_days: int = Field(default=30, gt=0)

    http_max_connections: int = Field(default=24, gt=0)
    http_max_keepalive_connections: int = Field(default=12, gt=0)
    llm_base_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1")
    llm_api_key: str = ""
    llm_model: str = ""
    llm_concurrency: int = Field(default=8, gt=0)
    llm_timeout_seconds: float = Field(default=180, gt=0)
    llm_max_retries: int = Field(default=3, gt=0)
    llm_temperature: float = Field(default=0, ge=0)
    llm_max_tokens: int = Field(default=4096, gt=0)
    llm_enable_thinking: bool = False
    llm_send_enable_thinking: bool = True
    dedup_llm_enabled: bool = True
    dedup_max_candidates: int = Field(default=30, gt=0)
    dedup_cards_per_batch: int = Field(default=16, ge=2, le=32)
    dedup_max_requests: int = Field(default=400, ge=0)
    dedup_max_concurrency: int = Field(default=8, gt=0, le=200)
    dedup_max_seconds: float = Field(default=1800, ge=0)
    dedup_min_confidence: float = Field(default=0.7, ge=0, le=1)

    @model_validator(mode="after")
    def validate_runtime_capacity(self) -> "Settings":
        try:
            ZoneInfo(self.app_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("APP_TIMEZONE must be a valid IANA timezone") from exc
        if self.http_max_keepalive_connections > self.http_max_connections:
            raise ValueError(
                "HTTP_MAX_KEEPALIVE_CONNECTIONS must not exceed HTTP_MAX_CONNECTIONS"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
