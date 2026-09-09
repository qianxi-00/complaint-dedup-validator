"""授权有效期检查(上海时区)。

到期日期由构建期生成的 ``licensing_conf.py`` 烧录进混淆包;
本地开发环境缺失该文件时回退到远期默认值并打印警告。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

SHANGHAI = ZoneInfo("Asia/Shanghai")
_DEFAULT_EXPIRE_DATE = date(2099, 12, 31)
_CLOCK_ROLLBACK_TOLERANCE = timedelta(days=1)

logger = logging.getLogger(__name__)


class LicenseExpiredError(RuntimeError):
    """授权已到期或检测到系统时间异常。"""


def _load_conf():
    from complaint_dedup import licensing_conf

    return licensing_conf


def expire_date() -> date:
    try:
        return _load_conf().EXPIRE_DATE
    except ImportError:
        return _DEFAULT_EXPIRE_DATE


def now_shanghai() -> datetime:
    return datetime.now(SHANGHAI)


def is_expired(now: datetime | None = None) -> bool:
    current = now if now is not None else now_shanghai()
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    else:
        current = current.astimezone(SHANGHAI)
    return current.date() > expire_date()


def ensure_not_expired(now: datetime | None = None) -> None:
    if is_expired(now):
        raise LicenseExpiredError(
            f"服务授权已于 {expire_date().isoformat()} 到期（Asia/Shanghai），"
            "系统已停止服务，请联系供应商续期"
        )


async def ensure_license_ok(database) -> None:
    """启动期检查:到期日 + 时钟回拨守卫(需数据库可用)。

    ``database`` 是应用共享的 :class:`~complaint_dedup.async_database.AsyncDatabase`。
    """
    ensure_not_expired()
    current = now_shanghai()
    async with database.engine.begin() as connection:
        row = (
            await connection.execute(
                text("SELECT first_started_at FROM license_state WHERE id = 1")
            )
        ).first()
        if row is None:
            await connection.execute(
                text(
                    "INSERT INTO license_state (id, first_started_at, expire_date, updated_at) "
                    "VALUES (1, :now, :expire_date, :now)"
                ),
                {"now": current, "expire_date": expire_date()},
            )
            return
        first_started_at = row[0]
        if first_started_at is None:
            return
        if first_started_at.tzinfo is None:
            first_started_at = first_started_at.replace(tzinfo=SHANGHAI)
        if current < first_started_at - _CLOCK_ROLLBACK_TOLERANCE:
            raise LicenseExpiredError(
                "检测到系统时间早于首次启动时间超过 1 天，疑似时钟回拨，"
                "系统已拒绝启动，请校准服务器时间后重试"
            )
