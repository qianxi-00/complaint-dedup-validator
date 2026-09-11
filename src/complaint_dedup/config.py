from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, HttpUrl, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """运行配置。所有配置项均可通过 .env 文件或环境变量覆盖。"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ------------------------------ 服务运行参数 ------------------------------
    app_host: str = "127.0.0.1"  # API 监听地址
    app_port: int = Field(default=8765, gt=0, le=65_535)  # API 监听端口
    app_timezone: str = "Asia/Shanghai"  # 业务时区，用于自然日计算与授权日期

    # ------------------------------- 数据库配置 -------------------------------
    database_mode: Literal["sqlite", "postgresql"] = "postgresql"  # 数据库类型（运行时统一 PostgreSQL）
    database_path: Path = Path("runtime/app.db")  # 运行数据目录基准路径（上传/导出等使用其父目录；SQLite 仅测试用）
    db_host: str = "127.0.0.1"  # PostgreSQL 主机地址
    db_port: int = Field(default=5432, gt=0, le=65_535)  # PostgreSQL 端口
    db_user: str = "postgres"  # PostgreSQL 用户名
    db_password: str = ""  # PostgreSQL 密码（只写入 .env，禁止提交 Git）
    db_name: str = "gongdan"  # PostgreSQL 数据库名
    db_pool_size: int = Field(default=10, gt=0)  # 数据库常驻连接池大小
    db_max_overflow: int = Field(default=10, ge=0)  # 连接池允许的临时溢出连接数

    # ------------------------------- 导入与日志 -------------------------------
    max_total_rows: int = Field(default=200_000, gt=0)  # 单次全量文件最大数据行数
    log_level: str = "INFO"  # 日志级别：DEBUG、INFO、WARNING、ERROR
    log_dir: Path = Path("runtime/logs")  # 日志文件目录
    log_retention_days: int = Field(default=30, gt=0)  # 日志文件保留天数

    # ---------------------------- HTTP 连接池与模型并发 ----------------------------
    http_max_connections: int = Field(default=24, gt=0)  # 对外 HTTP 最大连接数
    http_max_keepalive_connections: int = Field(default=12, gt=0)  # 长连接保留数

    # --------------------------- OpenAI 兼容模型服务 ----------------------------
    llm_base_url: HttpUrl = HttpUrl("http://127.0.0.1:8000/v1")  # 模型服务地址（含 /v1）
    llm_api_key: str = ""  # 模型访问密钥，内网服务不校验时可填 EMPTY
    llm_model: str = ""  # 模型名称，必须与模型服务暴露的名称一致
    llm_concurrency: int = Field(default=8, gt=0)  # 模型请求并发数
    llm_timeout_seconds: float = Field(default=200, gt=0)  # 单次模型请求超时（秒）
    llm_max_retries: int = Field(default=3, gt=0)  # 单次模型请求最大尝试次数
    llm_temperature: float = Field(default=0, ge=0)  # 采样温度，判重建议为 0
    llm_max_tokens: int = Field(default=4096, gt=0)  # 单次响应最大生成 token 数
    llm_enable_thinking: bool = False  # 是否允许模型思考模式推理
    llm_send_enable_thinking: bool = True  # 是否向模型发送 enable_thinking 字段
    llm_json_mode: Literal["prompt", "json_object", "guided_json"] = "prompt"  # JSON 输出模式

    # ----------------------------- 自动判重参数 ---------------------------------
    dedup_llm_enabled: bool = True  # 是否启用事件卡 LLM 判重；关闭后仅用硬匹配和保守规则
    dedup_max_candidates: int = Field(default=30, gt=0)  # 每张事件卡保留的候选上限
    dedup_cards_per_batch: int = Field(default=16, ge=2, le=32)  # 每次发送给模型的卡片数
    dedup_max_requests: int = Field(default=0, ge=0)  # 单次比对模型请求上限；0=不限制
    dedup_max_concurrency: int = Field(default=8, gt=0, le=200)  # 判重模型并发数
    dedup_max_seconds: float = Field(default=0, ge=0)  # 单次比对模型总时长上限；0=不限制
    dedup_min_confidence: float = Field(default=0.7, ge=0, le=1)  # 模型合并的最低置信度
    dedup_text_duplicate_enabled: bool = True  # 高相似文本确定性合并开关
    dedup_text_duplicate_threshold: float = Field(default=0.9, ge=0, le=1)  # 文本重复判定阈值
    dedup_fallback_max_span_days: int = Field(default=90, ge=0)  # 回退合并时间跨度护栏（天）；0=关闭
    dedup_fallback_span_shadow: bool = False  # 时间跨度护栏影子模式；已按评估结论默认生效

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
