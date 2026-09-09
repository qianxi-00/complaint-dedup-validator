from io import BytesIO
from pathlib import Path

from fastapi.testclient import TestClient
from openpyxl import Workbook
from openpyxl import load_workbook

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.full_corpus_web import create_full_corpus_app
from complaint_dedup.full_corpus_web import _local_date
from zoneinfo import ZoneInfo
from datetime import datetime, timezone


def workbook_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["工单编号", "受理时间", "办结时间", "诉求标题", "事发地点", "事项分类", "市民诉求", "处理部门"])
    sheet.append(["WO-1", "2026-09-01 08:00:00", "2026-09-02 08:00:00", "德昌电机门口积水", "江海区礼乐街道德昌电机门口", "道路积水", "地址：江海区礼乐街道德昌电机门口。", "礼乐街道办事处"])
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def test_default_window_date_uses_configured_local_timezone():
    value = datetime(2026, 8, 31, 16, 30, tzinfo=timezone.utc)
    assert _local_date(value, ZoneInfo("Asia/Shanghai")).isoformat() == "2026-09-01"


def test_full_corpus_web_upload_and_create_comparison(tmp_path: Path):
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")
    app = create_full_corpus_app(settings, database=database)
    with TestClient(app) as client:
        response = client.post(
            "/sync",
            files={"file": ("all.xlsx", workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            follow_redirects=False,
        )
        assert response.status_code == 303
        home = client.get("/")
        assert home.status_code == 200
        assert "当前工单数" in home.text
        result = client.post(
            "/comparisons",
            data={"time_field": "completed_at", "target_from": "2026-09-02", "target_to": "2026-09-02"},
            follow_redirects=False,
        )
        assert result.status_code == 303
        detail = client.get(result.headers["location"])
        assert detail.status_code == 200
        assert "办结时间" in detail.text
        assert "处理部门" in detail.text
        assert "礼乐街道办事处" in detail.text
        exported = client.get(f"/comparisons/{result.headers['location'].rsplit('/', 1)[-1]}/export")
        assert exported.status_code == 200
        workbook = load_workbook(BytesIO(exported.content))
        assert workbook.sheetnames == ["重复项", "孤立工单"]
        assert workbook["孤立工单"]["A1"].value == "数据侧"


def test_full_corpus_web_rejects_self_cannot_link(tmp_path: Path):
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")
    app = create_full_corpus_app(settings, database=database)
    with TestClient(app) as client:
        client.post(
            "/sync",
            files={"file": ("all.xlsx", workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            follow_redirects=False,
        )
        result = client.post(
            "/comparisons",
            data={"time_field": "completed_at", "target_from": "2026-09-02", "target_to": "2026-09-02"},
            follow_redirects=False,
        )
        comparison_id = result.headers["location"].rsplit("/", 1)[-1]
        saved = client.post(
            f"/comparisons/{comparison_id}/cannot-links",
            data={
                "left_record_key": "wo:WO-1",
                "right_record_key": "wo:WO-1",
                "reason": "test",
            },
            follow_redirects=False,
        )
        assert saved.status_code == 400
        assert "不能与自身建立禁止关系" in saved.text


def test_full_corpus_web_rejects_cannot_link_outside_comparison(tmp_path: Path):
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")
    app = create_full_corpus_app(settings, database=database)
    with TestClient(app) as client:
        client.post(
            "/sync",
            files={"file": ("all.xlsx", workbook_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            follow_redirects=False,
        )
        result = client.post(
            "/comparisons",
            data={"time_field": "completed_at", "target_from": "2026-09-02", "target_to": "2026-09-02"},
            follow_redirects=False,
        )
        comparison_id = result.headers["location"].rsplit("/", 1)[-1]
        saved = client.post(
            f"/comparisons/{comparison_id}/cannot-links",
            data={
                "left_record_key": "wo:WO-1",
                "right_record_key": "wo:missing",
                "reason": "test",
            },
            follow_redirects=False,
        )
        assert saved.status_code == 400
        assert "不能与自身建立禁止关系" not in saved.text
        assert "必须属于当前比对任务" in saved.text
