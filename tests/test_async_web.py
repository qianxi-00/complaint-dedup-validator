from pathlib import Path

from fastapi.testclient import TestClient

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_web import create_async_app
from complaint_dedup.config import Settings


class RecordingProcessor:
    def __init__(self) -> None:
        self.mode = None
        self.records = None
        self.match_preset = None
        self.time_window_days = None

    async def create_job(
        self,
        name,
        records_a,
        records_b=None,
        *,
        mode,
        match_preset="balanced",
        time_window_days=0,
    ):
        self.mode = mode
        self.records = records_a
        self.match_preset = match_preset
        self.time_window_days = time_window_days
        return "single-job"


def test_single_file_upload_and_job_creation(tmp_path: Path) -> None:
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "app.db",
        llm_model="test-model",
        _env_file=None,
    )
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    processor = RecordingProcessor()
    app = create_async_app(
        settings,
        start_worker=False,
        database=database,
        processor=processor,
    )
    csv_data = "工单编号,诉求标题,市民诉求\nA1,甲公司欠薪,地址：金瓯路188号\n".encode()

    with TestClient(app) as client:
        inspection = client.post(
            "/uploads/inspect",
            data={"mode": "single"},
            files={"file_a": ("single.csv", csv_data, "text/csv")},
        )
        assert inspection.status_code == 200
        assert 'name="mode" value="single"' in inspection.text
        session_id = inspection.text.split('name="session_id" value="', 1)[1].split('"', 1)[0]
        response = client.post(
            "/jobs",
            data={
                "session_id": session_id,
                "mode": "single",
                "sheet_a": "CSV",
                "a_work_order_id": "工单编号",
                "a_title": "诉求标题",
                "a_appeal_text": "市民诉求",
                "match_preset": "strict",
                "time_window_days": "7",
            },
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/jobs/single-job"
    assert processor.mode == "single"
    assert len(processor.records) == 1
    assert processor.records[0].source == "S"
    assert processor.match_preset == "strict"
    assert processor.time_window_days == 7


def test_pair_endpoint_renders_active_filters_and_street_stats(tmp_path: Path) -> None:
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(
        settings,
        start_worker=False,
        database=database,
        processor=RecordingProcessor(),
    )

    with TestClient(app) as client:
        import asyncio

        asyncio.run(database.enqueue_job("job-1", "页面任务", mode="single", total_records=1))
        asyncio.run(
            database.add_records(
                "job-1",
                [{"source": "S", "source_row": 2, "title": "甲", "region": "江海区", "street": "外海街道"}],
            )
        )
        page = client.get("/jobs/job-1")
        pairs = client.get(
            "/jobs/job-1/pairs",
            params={
                "show_all": "true",
                "region": "江海区",
                "category": "欠薪",
                "recall_reason": "vector",
                "model_decision": "review",
                "review_status": "pending",
                "min_confidence": "0.6",
            },
        )

    assert page.status_code == 200
    assert "街道" in page.text
    assert "外海街道" in page.text
    assert pairs.status_code == 200
    assert 'value="江海区"' in pairs.text
    assert 'value="欠薪"' in pairs.text
    assert 'value="0.6"' in pairs.text
    assert 'name="show_all" value="true" checked' in pairs.text


def test_hidden_upload_fields_are_not_overridden_by_label_layout(tmp_path: Path) -> None:
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(
        settings,
        start_worker=False,
        database=database,
        processor=RecordingProcessor(),
    )

    with TestClient(app) as client:
        page = client.get("/")
        css = client.get("/static/app.css")

    assert "/static/app.css?v=" in page.text
    assert "[hidden]" in css.text
    assert "display: none !important" in css.text


def test_async_job_page_renders_failure_counters(tmp_path: Path) -> None:
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(
        settings,
        start_worker=False,
        database=database,
        processor=RecordingProcessor(),
    )

    with TestClient(app) as client:
        import asyncio

        asyncio.run(database.enqueue_job("job-1", "页面任务", mode="single", total_records=0))
        response = client.get("/jobs/job-1")

    assert response.status_code == 200
    assert "失败" in response.text


def test_pair_review_uses_compact_filter_and_bounded_text_layout(tmp_path: Path) -> None:
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(
        settings,
        start_worker=False,
        database=database,
        processor=RecordingProcessor(),
    )

    with TestClient(app) as client:
        import asyncio

        asyncio.run(database.enqueue_job("job-layout", "布局测试", mode="single", total_records=0))
        response = client.get("/jobs/job-layout/pairs")
        css = client.get("/static/app.css")

    assert response.status_code == 200
    assert 'class="pair-filter-bar"' in response.text
    assert 'class="pair-filter-grid"' in response.text
    assert 'class="checkbox-field"' in response.text
    assert 'class="pair-filter-actions"' in response.text
    assert ".record-text" in css.text
    assert "max-height:" in css.text
    assert ".recall-badges" in css.text


def test_missing_job_status_stops_polling_with_not_found(tmp_path: Path) -> None:
    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(settings, start_worker=False, database=database, processor=RecordingProcessor())

    with TestClient(app) as client:
        response = client.get("/jobs/missing/status")

    assert response.status_code == 404


def test_candidate_review_uses_chinese_filters_and_ten_row_pages(tmp_path: Path) -> None:
    import asyncio

    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(settings, start_worker=False, database=database, processor=RecordingProcessor())

    with TestClient(app) as client:
        asyncio.run(database.enqueue_job("job-page", "分页测试", mode="single", total_records=13, pipeline_version="pair_v1"))
        record_ids = asyncio.run(
            database.add_records(
                "job-page",
                [
                    {"source": "S", "source_row": index + 2, "title": f"工单{index}"}
                    for index in range(13)
                ],
            )
        )
        asyncio.run(
            database.upsert_candidate_pairs(
                "job-page",
                [
                    {
                        "record_a_id": record_ids[0],
                        "record_b_id": record_ids[index],
                        "recall_reason": "hybrid_vector,same_category",
                    }
                    for index in range(1, 13)
                ],
            )
        )
        page = client.get("/jobs/job-page")
        filtered = client.get(
            "/jobs/job-page/pairs",
            params={"recall_reason": "same_category"},
        )

    assert page.status_code == 200
    assert page.text.count('class="pair-id"') == 10
    assert "共 12 条" in page.text
    assert "下一页" in page.text
    assert 'value="hybrid_vector"' in page.text and "向量相似召回" in page.text
    assert 'value="same_phone"' in page.text and "联系电话一致" in page.text
    assert 'value="same_category"' in page.text and "事项分类一致" in page.text
    assert "重置筛选" in page.text
    assert filtered.status_code == 200
    assert filtered.text.count('class="pair-id"') == 10
    assert 'value="same_category"' in filtered.text
    assert "selected" in filtered.text


def test_not_duplicate_filter_overrides_default_hidden_state(tmp_path: Path) -> None:
    import asyncio

    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(settings, start_worker=False, database=database, processor=RecordingProcessor())

    with TestClient(app) as client:
        asyncio.run(database.enqueue_job("job-hidden", "不可合并筛选", mode="single", total_records=2))
        record_ids = asyncio.run(
            database.add_records(
                "job-hidden",
                [
                    {"source": "S", "source_row": 2, "title": "甲"},
                    {"source": "S", "source_row": 3, "title": "乙"},
                ],
            )
        )
        asyncio.run(
            database.upsert_candidate_pairs(
                "job-hidden",
                [{"record_a_id": record_ids[0], "record_b_id": record_ids[1], "recall_reason": "hybrid_vector"}],
            )
        )
        pair = asyncio.run(database.list_candidate_pairs("job-hidden"))[0]
        asyncio.run(
            database.save_judgements(
                "job-hidden",
                {pair["id"]: {"decision": "not_duplicate", "confidence": 0.96}},
            )
        )
        hidden = client.get("/jobs/job-hidden/pairs")
        filtered = client.get(
            "/jobs/job-hidden/pairs",
            params={"model_decision": "not_duplicate", "min_confidence": ""},
        )

    assert 'class="pair-id"' not in hidden.text
    assert filtered.text.count('class="pair-id"') == 1


def test_job_page_explains_event_groups_and_hides_singletons(tmp_path: Path) -> None:
    import asyncio

    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(settings, start_worker=False, database=database, processor=RecordingProcessor())

    with TestClient(app) as client:
        asyncio.run(database.enqueue_job("job-groups", "事件组测试", mode="single", total_records=2, pipeline_version="pair_v1"))
        asyncio.run(
            database.add_records(
                "job-groups",
                [
                    {"source": "S", "source_row": 2, "title": "甲"},
                    {"source": "S", "source_row": 3, "title": "乙"},
                ],
            )
        )
        asyncio.run(database.rebuild_event_groups("job-groups"))
        response = client.get("/jobs/job-groups")

    assert response.status_code == 200
    assert 'id="groups-panel"' in response.text
    assert "人工确认重复后形成的最终合并集合" in response.text
    assert "暂无已合并事件组" in response.text


def test_event_cluster_job_uses_event_workspace_with_filters_and_detail(tmp_path: Path) -> None:
    import asyncio

    settings = Settings(database_mode="sqlite", database_path=tmp_path / "app.db", _env_file=None)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    app = create_async_app(settings, start_worker=False, database=database, processor=RecordingProcessor())

    with TestClient(app) as client:
        asyncio.run(database.enqueue_job("job-events", "事件工作台", mode="single", total_records=2, pipeline_version="event_cluster_v2"))
        record_ids = asyncio.run(database.add_records("job-events", [
            {"source": "S", "source_row": 2, "work_order_id": "A1", "title": "甲公司欠薪", "appeal_text": "未发工资", "region": "江海区", "street": "外海街道", "category_level_1": "劳动保障", "category": "拖欠工资"},
            {"source": "S", "source_row": 3, "work_order_id": "A2", "title": "甲公司拖欠工资", "appeal_text": "仍未发工资", "region": "江海区", "street": "外海街道", "category_level_1": "劳动保障", "category": "拖欠工资"},
        ]))
        event_id = asyncio.run(database.replace_candidate_events("job-events", [{
            "name": "江海区｜外海街道｜甲公司｜拖欠工资",
            "status": "review",
            "confidence": 0.96,
            "evidence": ["主体、地点和问题一致"],
            "members": [{"record_id": record_id, "confidence": 0.95, "role": "member"} for record_id in record_ids],
        }]))[0]
        page = client.get("/jobs/job-events")
        filtered = client.get(
            "/jobs/job-events/events",
            params={
                "region": "江海区",
                "category_level_1": "劳动保障",
                "event_id": "",
                "status": "review",
            },
        )
        detail = client.get(f"/jobs/job-events/events/{event_id}")

    assert 'id="events-panel"' in page.text
    assert "事件复核工作台" in page.text
    assert "召回审计" in page.text
    assert 'hx-trigger="change delay:150ms, submit"' in page.text
    assert filtered.status_code == 200
    assert 'name="region"' in filtered.text and 'value="江海区"' in filtered.text
    assert 'name="category_level_1"' in filtered.text and 'value="劳动保障"' in filtered.text
    assert 'option value="review" selected' in filtered.text
    assert "共 1 个事件" in filtered.text
    assert "甲公司欠薪" in detail.text and "甲公司拖欠工资" in detail.text
