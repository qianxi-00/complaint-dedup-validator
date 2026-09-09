"""loguru 日志体系:控制台 + 滚动文件双 sink,拦截标准库日志。

- API 进程写入 ``runtime/logs/api-*.log``
- 10MB 轮转 + zip 压缩 + 按天文件名,保留天数可配
- ``enqueue=True`` 保证多线程/多进程下文件写入不丢不乱
- uvicorn / sqlalchemy / alembic 的标准库日志统一汇入 loguru
"""
from __future__ import annotations

import inspect
import logging
import sys
from pathlib import Path

from loguru import logger

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{extra[process]}</cyan> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


class InterceptHandler(logging.Handler):
    """把标准库 logging 记录转发给 loguru。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(
    *,
    log_dir: str | Path,
    process_name: str,
    level: str = "INFO",
    retention_days: int = 30,
) -> None:
    directory = Path(log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.configure(extra={"process": process_name})
    logger.add(
        sys.stderr,
        level=level.upper(),
        format=_LOG_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    logger.add(
        directory / f"{process_name}-{{time:YYYY-MM-DD}}.log",
        level=level.upper(),
        format=_LOG_FORMAT,
        rotation="10 MB",
        retention=f"{max(int(retention_days), 1)} days",
        compression="zip",
        enqueue=True,
        encoding="utf-8",
        backtrace=False,
        diagnose=False,
    )
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    for name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "alembic",
        "aiosqlite",
    ):
        std_logger = logging.getLogger(name)
        std_logger.handlers = [InterceptHandler()]
        std_logger.propagate = False
