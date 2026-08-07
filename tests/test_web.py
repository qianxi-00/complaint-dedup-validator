import io
import re
from pathlib import Path

import pandas as pd

import pandas as pd

from fastapi.testclient import TestClient

from complaint_dedup.config import Settings
from complaint_dedup.database import connect_database
from complaint_dedup.pipeline import InputRecord
from complaint_dedup.web import create_app


def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "app.db",
        llm_model="test-model",
    )


def test_home_page_renders_application_title(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "投诉重复验证台" in response.text


def test_upload_inspection_persists_two_files(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)
    csv_a = "工单编号,诉求标题,市民诉求\nA1,甲公司欠薪,地址：金瓯路188号\n".encode()
    csv_b = "工单编号,诉求标题,市民诉求\nB1,甲公司工资未发,地址：金瓯路188号\n".encode()

    with TestClient(app) as client:
        response = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.csv", csv_a, "text/csv"),
                "file_b": ("b.csv", csv_b, "text/csv"),
            },
        )

    assert response.status_code == 200
    assert "字段映射" in response.text
    assert "session_id" in response.text


def test_mapping_partial_uses_selected_sheet_columns(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)
    first = pd.DataFrame({"首号": ["A1"], "首标题": ["首表投诉"], "首内容": ["首表内容"]})
    second = pd.DataFrame({"二号": ["A2"], "二标题": ["二表投诉"], "二内容": ["二表内容"]})
    workbook = io.BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        first.to_excel(writer, sheet_name="第一表", index=False)
        second.to_excel(writer, sheet_name="第二表", index=False)
    workbook.seek(0)

    with TestClient(app) as client:
        response = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.xlsx", workbook.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                "file_b": ("b.xlsx", workbook.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            },
        )
        assert response.status_code == 200
        session_id = re.search(r'name="session_id" value="([^"]+)"', response.text).group(1)

        mapping = client.get(
            f"/uploads/{session_id}/mapping/a",
            params={"sheet": "第二表"},
        )

    assert mapping.status_code == 200
    assert "二号" in mapping.text
    assert "首号" not in mapping.text


def test_settings_page_reports_model_configuration(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    assert "模型状态" in response.text
    assert "已配置" in response.text
    assert "API Key" in response.text


def test_create_job_rejects_when_model_is_not_configured(tmp_path: Path) -> None:
    app_settings = Settings(database_path=tmp_path / "app.db")
    app = create_app(app_settings, start_worker=False)
    csv_a = "工单编号,诉求标题,市民诉求\nA1,甲公司欠薪,地址：金瓯路188号\n".encode()
    csv_b = "工单编号,诉求标题,市民诉求\nB1,甲公司工资未发,地址：金瓯路188号\n".encode()

    with TestClient(app) as client:
        inspection = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.csv", csv_a, "text/csv"),
                "file_b": ("b.csv", csv_b, "text/csv"),
            },
        )
        session_id = re.search(r'name="session_id" value="([^"]+)"', inspection.text).group(1)
        response = client.post(
            "/jobs",
            data={
                "session_id": session_id,
                "sheet_a": "CSV",
                "sheet_b": "CSV",
                "a_work_order_id": "工单编号",
                "a_title": "诉求标题",
                "a_appeal_text": "市民诉求",
                "b_work_order_id": "工单编号",
                "b_title": "诉求标题",
                "b_appeal_text": "市民诉求",
            },
        )

    assert response.status_code == 400
    assert "模型未配置" in response.text


def test_job_page_contains_auto_refresh_targets(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)
    processor = app.state.processor
    job_id = processor.create_job(
        "test",
        [],
        [],
    )

    with TestClient(app) as client:
        response = client.get(f"/jobs/{job_id}")

    assert response.status_code == 200
    assert f"/jobs/{job_id}/status" in response.text
    assert "every 2s" in response.text


def test_pair_list_is_paginated_and_contains_both_record_texts(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)
    processor = app.state.processor
    job_id = processor.create_job(
        "review",
        [InputRecord("A", 2, "A1", "甲方标题", "其他", "甲方投诉原文")],
        [InputRecord("B", 2, "B1", "乙方标题", "其他", "乙方投诉原文")],
    )
    with connect_database(app.state.settings.database_path) as connection:
        rows = connection.execute("SELECT id FROM records ORDER BY id").fetchall()
        connection.execute(
            "INSERT INTO candidate_pairs (job_id, record_a_id, record_b_id, candidate_key) VALUES (?, ?, ?, 'test')",
            (job_id, rows[0]["id"], rows[1]["id"]),
        )

    with TestClient(app) as client:
        response = client.get(f"/jobs/{job_id}/pairs", params={"page": 1})

    assert response.status_code == 200
    assert "甲方投诉原文" in response.text
    assert "乙方投诉原文" in response.text
    assert "第 1 页" in response.text


def test_upload_inspection_allows_selecting_rows_from_large_multi_sheet_files(tmp_path: Path) -> None:
    limited = Settings(database_path=tmp_path / "app.db", llm_model="test-model", max_total_rows=2)
    app = create_app(limited, start_worker=False)
    workbook = tmp_path / "multi.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"第一表字段": range(5)}).to_excel(writer, sheet_name="大表", index=False)
        pd.DataFrame({"第二表字段": [1]}).to_excel(writer, sheet_name="小表", index=False)

    with TestClient(app) as client:
        response = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.xlsx", workbook.read_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                "file_b": ("b.xlsx", workbook.read_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            },
        )

    assert response.status_code == 200
    assert 'hx-get="/uploads/' in response.text
    assert "大表" in response.text
    assert "小表" in response.text


def test_sheet_mapping_endpoint_uses_selected_sheet_columns(tmp_path: Path) -> None:
    app = create_app(settings(tmp_path), start_worker=False)
    workbook = tmp_path / "multi.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"第一表字段": [1]}).to_excel(writer, sheet_name="第一表", index=False)
        pd.DataFrame({"第二表字段": [1]}).to_excel(writer, sheet_name="第二表", index=False)

    with TestClient(app) as client:
        upload = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.xlsx", workbook.read_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                "file_b": ("b.xlsx", workbook.read_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            },
        )
        session_id = upload.text.split('name="session_id" value="', 1)[1].split('"', 1)[0]
        response = client.get(
            f"/uploads/{session_id}/mapping/A",
            params={"sheet": "第二表"},
        )

    assert response.status_code == 200
    assert "第二表字段" in response.text
    assert "第一表字段" not in response.text


def test_model_page_reports_connection_result(tmp_path: Path) -> None:
    class FakeLlmClient:
        async def test_connection(self) -> str:
            return "连接成功"

        async def aclose(self) -> None:
            pass

    app = create_app(settings(tmp_path), start_worker=False, llm_client=FakeLlmClient())

    with TestClient(app) as client:
        page = client.get("/model")
        response = client.post("/model/test")

    assert page.status_code == 200
    assert "test-model" in page.text
    assert response.status_code == 200
    assert "连接成功" in response.text


def test_create_job_requires_configured_model(tmp_path: Path) -> None:
    unconfigured = Settings(database_path=tmp_path / "app.db", llm_model="")
    app = create_app(unconfigured, start_worker=False)
    csv_data = "工单编号,诉求标题,市民诉求\nA1,标题,内容\n".encode()

    with TestClient(app) as client:
        upload = client.post(
            "/uploads/inspect",
            files={
                "file_a": ("a.csv", csv_data, "text/csv"),
                "file_b": ("b.csv", csv_data, "text/csv"),
            },
        )
        session_id = upload.text.split('name="session_id" value="', 1)[1].split('"', 1)[0]
        response = client.post(
            "/jobs",
            data={"session_id": session_id, "sheet_a": "CSV", "sheet_b": "CSV"},
        )

    assert response.status_code == 400
    assert "LLM_MODEL" in response.text
