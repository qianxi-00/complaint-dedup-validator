from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from complaint_dedup import licensing
from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.full_corpus_web import create_full_corpus_app

SHANGHAI = ZoneInfo("Asia/Shanghai")


class _UnusedDatabase:
    engine = None


def test_expire_date_source() -> None:
    try:
        from complaint_dedup import licensing_conf
    except ImportError:
        assert licensing.expire_date() == date(2099, 12, 31)
    else:
        assert licensing.expire_date() == licensing_conf.EXPIRE_DATE
    assert licensing.is_expired() is (
        licensing.expire_date() < datetime.now(SHANGHAI).date()
    )


def test_expiry_boundary_uses_shanghai_calendar_day(monkeypatch) -> None:
    monkeypatch.setattr(licensing, "expire_date", lambda: date(2027, 9, 1))
    fixed = {"now": datetime(2027, 9, 1, 23, 59, 0, tzinfo=SHANGHAI)}
    monkeypatch.setattr(licensing, "now_shanghai", lambda: fixed["now"])

    licensing.ensure_not_expired()

    fixed["now"] = datetime(2027, 9, 2, 0, 0, 0, tzinfo=SHANGHAI)
    with pytest.raises(licensing.LicenseExpiredError) as excinfo:
        licensing.ensure_not_expired()
    assert "2027-09-01" in str(excinfo.value)


def test_expiry_compares_shanghai_instant(monkeypatch) -> None:
    expire = date(2027, 9, 1)
    monkeypatch.setattr(licensing, "expire_date", lambda: expire)
    utc_after = datetime(2027, 9, 1, 16, 30, tzinfo=timezone.utc)
    utc_before = datetime(2027, 9, 1, 15, 59, tzinfo=timezone.utc)

    assert licensing.is_expired(utc_after) is True
    assert licensing.is_expired(utc_before) is False


def test_expired_requests_return_403(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(licensing, "is_expired", lambda now=None: True)
    monkeypatch.setattr(licensing, "expire_date", lambda: date(2027, 9, 1))
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "app.db",
        app_timezone="Asia/Shanghai",
        _env_file=None,
    )
    app = create_full_corpus_app(settings, database=_UnusedDatabase())
    client = TestClient(app)

    response = client.get("/")
    assert response.status_code == 403
    assert "服务授权已到期" in response.text
    assert "2027-09-01" in response.text


def test_active_requests_pass_gate(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(licensing, "is_expired", lambda now=None: False)
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "app.db",
        app_timezone="Asia/Shanghai",
        _env_file=None,
    )
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")
    try:
        app = create_full_corpus_app(settings, database=database)
        with TestClient(app) as client:
            response = client.get("/")
            assert response.status_code == 200
    finally:
        asyncio.run(database.close())
