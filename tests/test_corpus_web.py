from io import BytesIO
from pathlib import Path
import time

from fastapi.testclient import TestClient
from openpyxl import Workbook
from openpyxl import load_workbook

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.corpus_web import create_corpus_app


def workbook_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(
        [
            "工单编号",
            "受理时间",
            "诉求标题",
            "事发地点",
            "事项分类四级",
            "事项分类",
            "所属部门",
            "市民诉求",
            "联系电话",
        ]
    )
    sheet.append(
        [
            "WO-1",
            "2026-08-12 08:00:00",
            "德昌电机门口积水",
            "江海区礼乐街道德昌电机门口",
            "道路积水",
            "道路积水",
            "礼乐街道办事处",
            "地址：江海区礼乐街道德昌电机门口。\n事项：道路积水。",
            "13800138000",
        ]
    )
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def make_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "app.db",
        max_total_rows=200_000,
        _env_file=None,
    )
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")
    return TestClient(create_corpus_app(settings, database=database))


def wait_for_text(client: TestClient, url: str, expected: str, timeout: float = 5) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = client.get(url).text
        if expected in text:
            return text
        time.sleep(0.05)
    raise AssertionError(f"等待页面出现文本超时: {expected}")


def test_upload_is_queued_without_parsing_in_request(
    tmp_path: Path, monkeypatch
):
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "queued.db",
        max_total_rows=200_000,
        _env_file=None,
    )
    database = AsyncDatabase(f"sqlite+aiosqlite:///{settings.database_path}")

    async def fail_if_called(*args, **kwargs):
        raise AssertionError("上传请求不应解析 Excel")

    app = create_corpus_app(settings, database=database, embedded_worker=False)
    monkeypatch.setattr(app.state.processor, "stage_records", fail_if_called)
    with TestClient(app) as client:
        response = client.post(
            "/batches",
            data={"mode": "bootstrap_history", "name": "异步历史"},
            files={
                "file_history": (
                    "history.xlsx",
                    workbook_bytes(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        page = client.get(response.headers["location"])
        assert "等待后台处理" in page.text


def test_home_shows_corpus_batch_modes(tmp_path: Path):
    with make_client(tmp_path) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "历史库冷启动" in response.text
    assert "每日新增" in response.text
    assert "补录或更正" in response.text


def test_bootstrap_upload_review_approve_and_event_page(tmp_path: Path):
    with make_client(tmp_path) as client:
        response = client.post(
            "/batches",
            data={"mode": "bootstrap_history", "name": "历史测试"},
            files={
                "file_history": (
                    "history.xlsx",
                    workbook_bytes(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        batch_url = response.headers["location"]
        batch_text = wait_for_text(client, batch_url, "待审核词典")
        assert "历史库冷启动" in batch_text
        assert "bootstrap_history" not in batch_text
        dictionary_page = client.get(f"{batch_url}/dictionary?dimension=anchor")
        assert dictionary_page.status_code == 200
        assert "词典审核工作台" in dictionary_page.text
        assert "德昌电机门口" in dictionary_page.text
        assert "证据数" in dictionary_page.text
        item_detail = client.get(f"{batch_url}/dictionary/anchor/1")
        assert item_detail.status_code == 200
        assert "样例工单" in item_detail.text
        assert "WO-1" in item_detail.text
        assert "合并到其他标准项" in item_detail.text
        assert "拆分所选别名" in item_detail.text
        seed_export = client.get(f"{batch_url}/dictionary/export")
        assert seed_export.status_code == 200
        assert load_workbook(BytesIO(seed_export.content), read_only=True).sheetnames == [
            "标准街道",
            "标准锚点",
            "标准问题",
            "审核队列",
            "抽取统计",
        ]
        reviewed = client.post(
            f"{batch_url}/dictionary/anchor/1/approve",
            follow_redirects=False,
        )
        assert reviewed.status_code == 303
        assert "已通过" in client.get(
            f"{batch_url}/dictionary?dimension=anchor&status=approved"
        ).text

        approve = client.post(f"{batch_url}/approve", follow_redirects=False)
        assert approve.status_code == 303
        wait_for_text(client, batch_url, "批次已完成")
        events = client.get("/events")
        assert "德昌电机门口" in events.text
        assert '<option value="江海区"' in events.text
        assert '<option value="礼乐街道"' in events.text
        assert '<option value="道路积水"' in events.text
        assert "/events/1" in events.text
        assert "德昌电机门口" in client.get(
            "/events?region=江海区&street=礼乐街道&issue=道路积水"
        ).text
        assert "德昌电机门口" in client.get("/events?event_name=德昌电机").text
        assert "德昌电机门口" not in client.get("/events?event_name=不存在").text
        detail = client.get("/events/1")
        assert detail.status_code == 200
        assert "受理时间" in detail.text
        assert "所属部门" in detail.text
        assert "数据来源" in detail.text
        assert "成员置信度" not in detail.text

        renamed = client.post(
            "/events/1/name",
            data={"name": "礼乐街道｜德昌电机门口｜道路积水"},
            follow_redirects=False,
        )
        assert renamed.status_code == 303
        assert "礼乐街道｜德昌电机门口｜道路积水" in client.get("/events/1").text
        exported = client.get("/exports/corpus")
        assert exported.status_code == 200
        workbook = load_workbook(BytesIO(exported.content))
        assert workbook.sheetnames == ["重复项", "孤立工单"]
