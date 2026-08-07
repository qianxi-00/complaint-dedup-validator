from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl
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
    database_path: Path = Path("runtime/app.db")
    max_total_rows: int = Field(default=10_000, gt=0)
    default_match_preset: Literal["strict", "balanced", "loose"] = "balanced"
    default_time_window_days: int = Field(default=0, ge=0)
    max_candidates_per_record: int = Field(default=50, gt=0)
    broad_key_max_matches: int = Field(default=200, gt=0)

    llm_base_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1")
    llm_api_key: str = ""
    llm_model: str = ""
    llm_concurrency: int = Field(default=2, gt=0)
    llm_timeout_seconds: float = Field(default=180, gt=0)
    llm_max_retries: int = Field(default=3, gt=0)
    llm_extraction_batch_size: int = Field(default=20, gt=0)
    llm_judgement_batch_size: int = Field(default=20, gt=0)
    llm_temperature: float = Field(default=0, ge=0)
    llm_redact_pii: bool = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
