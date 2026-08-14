import asyncio
import json
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.engine import URL

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_exporter import export_job
from complaint_dedup.async_pipeline import AsyncJobProcessor
from complaint_dedup.async_worker import AsyncJobRunner
from complaint_dedup.config import Settings
from complaint_dedup.embedding_client import EmbeddingClient
from complaint_dedup.file_inspection import inspect_input_file
from complaint_dedup.llm_client import LlmClient
from complaint_dedup.milvus_store import MilvusVectorStore
from complaint_dedup.rerank_client import RerankClient
from complaint_dedup.ui_labels import label, stage_label
from complaint_dedup.web import _load_records


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Jinja2Templates(directory=PROJECT_ROOT / "templates")
TEMPLATES.env.filters["zh"] = label
TEMPLATES.env.filters["stage_zh"] = stage_label

PAIR_PAGE_SIZE = 10
GROUP_PAGE_SIZE = 10


def create_async_app(
    settings: Settings,
    *,
    start_worker: bool = False,
    database: AsyncDatabase | None = None,
    processor: AsyncJobProcessor | None = None,
) -> FastAPI:
    config = settings
    database = database or AsyncDatabase(
        _database_url(config),
        pool_size=config.db_pool_size,
        max_overflow=config.db_max_overflow,
    )
    runtime_dir = config.database_path.parent
    uploads_dir = runtime_dir / "uploads"
    results_dir = runtime_dir / "results"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await database.initialize()
        owned_clients = []
        active_processor = processor
        runner = None
        if active_processor is None:
            llm = LlmClient(
                base_url=str(config.llm_base_url),
                api_key=config.llm_api_key,
                model=config.llm_model,
                timeout_seconds=config.llm_timeout_seconds,
                max_retries=config.llm_max_retries,
                temperature=config.llm_temperature,
                max_tokens=config.llm_max_tokens,
                enable_thinking=config.llm_enable_thinking,
                concurrency=max(
                    config.llm_extraction_concurrency,
                    config.llm_judgement_concurrency,
                ),
                max_connections=config.http_max_connections,
                max_keepalive_connections=config.http_max_keepalive_connections,
            )
            embedding = EmbeddingClient(
                url=str(config.embedding_url),
                model=config.embedding_model,
                timeout_seconds=config.embedding_timeout,
                max_retries=config.embedding_retries,
                concurrency=config.embedding_concurrency,
                max_connections=config.http_max_connections,
                max_keepalive_connections=config.http_max_keepalive_connections,
            )
            rerank = RerankClient(
                url=str(config.rerank_url),
                model=config.rerank_model,
                timeout_seconds=config.rerank_timeout,
                max_retries=config.rerank_retries,
                concurrency=config.rerank_concurrency,
                max_connections=config.http_max_connections,
                max_keepalive_connections=config.http_max_keepalive_connections,
            )
            vector_store = await MilvusVectorStore.connect(
                host=config.milvus_host,
                port=config.milvus_port,
                user=config.milvus_user,
                password=config.milvus_password,
                db_name=config.milvus_db,
                collection_prefix=config.milvus_collection,
                dimension=config.embedding_dimension,
                concurrency=config.milvus_concurrency,
            )
            owned_clients.extend((llm, embedding, rerank, vector_store))
            active_processor = AsyncJobProcessor(
                database=database,
                llm_client=llm,
                embedding_client=embedding,
                rerank_client=rerank,
                vector_store=vector_store,
                extraction_batch_size=config.llm_extraction_batch_size,
                judgement_batch_size=config.llm_judgement_batch_size,
                extraction_concurrency=config.llm_extraction_concurrency,
                judgement_concurrency=config.llm_judgement_concurrency,
                vector_top_k=config.event_vector_top_k if config.pipeline_version == "event_cluster_v2" else config.vector_top_k,
                rerank_top_n=config.event_rerank_top_n if config.pipeline_version == "event_cluster_v2" else config.rerank_top_n,
                max_candidates_per_record=config.max_candidates_per_record,
                max_inflight_batches_per_job=config.max_inflight_batches_per_job,
                embedding_batch_size=config.embedding_batch_size,
                rerank_enabled=config.rerank_enabled,
                pipeline_version=config.pipeline_version,
                event_llm_concurrency=config.event_llm_concurrency,
                event_raw_group_limit=config.event_raw_group_limit,
                event_component_max_size=config.event_component_max_size,
                auto_merge_enabled=config.auto_merge_enabled,
                auto_merge_confidence=config.auto_merge_confidence,
                auto_merge_max_members=config.auto_merge_max_members,
            )
        app.state.processor = active_processor
        app.state.llm_client = getattr(active_processor, "llm_client", None)
        if start_worker:
            runner = AsyncJobRunner(
                database=database,
                processor=active_processor,
                concurrency=config.job_concurrency,
                lease_seconds=config.job_lease_seconds,
                heartbeat_seconds=config.job_heartbeat_seconds,
            )
            await runner.start()
        app.state.runner = runner
        yield
        if runner:
            await runner.stop()
        for client in reversed(owned_clients):
            await client.close() if hasattr(client, "close") else await client.aclose()
        await database.close()

    app = FastAPI(title="投诉重复验证台", lifespan=lifespan)
    app.state.settings = config
    app.state.database = database
    app.state.processor = processor
    app.state.uploads_dir = uploads_dir
    app.state.results_dir = results_dir
    app.mount("/static", StaticFiles(directory=PROJECT_ROOT / "static"), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {"jobs": await database.list_jobs(), "settings": config},
        )

    @app.get("/jobs", response_class=HTMLResponse)
    async def history(request: Request):
        return TEMPLATES.TemplateResponse(
            request, "history.html", {"jobs": await database.list_jobs()}
        )

    @app.get("/model", response_class=HTMLResponse)
    @app.get("/settings", response_class=HTMLResponse)
    async def model_page(request: Request):
        return TEMPLATES.TemplateResponse(
            request, "model.html", {"settings": config, "result": None}
        )

    @app.post("/model/test", response_class=HTMLResponse)
    async def test_model(request: Request):
        client = app.state.llm_client
        if client is None:
            result = "连接测试不可用：当前使用注入的任务处理器"
        else:
            try:
                result = await client.test_connection()
            except Exception as exc:
                result = f"连接失败：{exc}"
        return TEMPLATES.TemplateResponse(
            request,
            "partials/model_status.html",
            {"settings": config, "result": result},
        )

    @app.post("/uploads/inspect", response_class=HTMLResponse)
    async def inspect_uploads(
        request: Request,
        mode: str = Form("cross"),
        file_a: UploadFile = File(...),
        file_b: UploadFile | None = File(None),
    ):
        if mode not in {"single", "cross"}:
            raise HTTPException(400, "分析模式无效")
        if mode == "cross" and file_b is None:
            raise HTTPException(400, "双文件模式必须上传文件 B")
        session_id = uuid.uuid4().hex
        session_dir = uploads_dir / session_id
        session_dir.mkdir(parents=True)
        uploads = [("A", file_a)] if mode == "single" else [("A", file_a), ("B", file_b)]
        paths: dict[str, Path] = {}
        inspections = {}
        for source, upload in uploads:
            suffix = Path(upload.filename or "").suffix.lower()
            if suffix not in {".xlsx", ".xls", ".csv"}:
                raise HTTPException(400, f"文件 {source} 格式不支持")
            path = session_dir / f"{source}{suffix}"
            path.write_bytes(await upload.read())
            paths[source] = path
            inspections[source] = await asyncio.to_thread(inspect_input_file, path)
        metadata = {"mode": mode, "file_a": str(paths["A"])}
        if "B" in paths:
            metadata["file_b"] = str(paths["B"])
        (session_dir / "session.json").write_text(
            json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
        )
        return TEMPLATES.TemplateResponse(
            request,
            "partials/inspection.html",
            {
                "session_id": session_id,
                "mode": mode,
                "a": inspections["A"],
                "b": inspections.get("B"),
            },
        )

    @app.get("/uploads/{session_id}/mapping/{source}", response_class=HTMLResponse)
    async def sheet_mapping(request: Request, session_id: str, source: str, sheet: str):
        source = source.upper()
        metadata_path = uploads_dir / session_id / "session.json"
        if not metadata_path.exists() or source not in {"A", "B"}:
            raise HTTPException(404, "上传会话或来源不存在")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        path_value = metadata.get(f"file_{source.lower()}")
        if not path_value:
            raise HTTPException(404, "上传文件不存在")
        inspection = await asyncio.to_thread(inspect_input_file, Path(path_value), preview_rows=0)
        selected = next((item for item in inspection.sheets if item.name == sheet), None)
        if selected is None:
            raise HTTPException(400, "工作表不存在")
        return TEMPLATES.TemplateResponse(
            request,
            "partials/mapping_fields.html",
            {
                "session_id": session_id,
                "prefix": source.lower(),
                "file": inspection,
                "selected": selected,
            },
        )

    @app.post("/jobs")
    async def create_job_route(
        session_id: str = Form(...),
        mode: str = Form("cross"),
        job_name: str = Form("投诉重复分析"),
        sheet_a: str = Form(...),
        sheet_b: str = Form(""),
        a_work_order_id: str = Form(""),
        a_received_at: str = Form(""),
        a_title: str = Form(""),
        a_category_level_1: str = Form(""),
        a_category_level_2: str = Form(""),
        a_category_level_3: str = Form(""),
        a_category_level_4: str = Form(""),
        a_category: str = Form(""),
        a_appeal_text: str = Form(""),
        b_work_order_id: str = Form(""),
        b_received_at: str = Form(""),
        b_title: str = Form(""),
        b_category_level_1: str = Form(""),
        b_category_level_2: str = Form(""),
        b_category_level_3: str = Form(""),
        b_category_level_4: str = Form(""),
        b_category: str = Form(""),
        b_appeal_text: str = Form(""),
        match_preset: str = Form("balanced"),
        time_window_days: int = Form(0),
    ):
        if not config.llm_model:
            raise HTTPException(400, "模型未配置：请先在 .env 中填写 LLM_MODEL")
        if match_preset not in {"strict", "balanced", "loose"}:
            raise HTTPException(400, "匹配档位无效")
        if time_window_days < 0:
            raise HTTPException(400, "时间窗口不能小于 0")
        metadata_path = uploads_dir / session_id / "session.json"
        if not metadata_path.exists():
            raise HTTPException(404, "上传会话不存在")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        mode = metadata.get("mode", mode)
        map_a = {
            "work_order_id": a_work_order_id,
            "received_at": a_received_at,
            "title": a_title,
            "category_level_1": a_category_level_1,
            "category_level_2": a_category_level_2,
            "category_level_3": a_category_level_3,
            "category_level_4": a_category_level_4,
            "category": a_category,
            "appeal_text": a_appeal_text,
        }
        records_a = await asyncio.to_thread(
            _load_records,
            Path(metadata["file_a"]),
            sheet_a,
            map_a,
            "S" if mode == "single" else "A",
        )
        records_b = None
        if mode == "cross":
            map_b = {
                "work_order_id": b_work_order_id,
                "received_at": b_received_at,
                "title": b_title,
                "category_level_1": b_category_level_1,
                "category_level_2": b_category_level_2,
                "category_level_3": b_category_level_3,
                "category_level_4": b_category_level_4,
                "category": b_category,
                "appeal_text": b_appeal_text,
            }
            records_b = await asyncio.to_thread(
                _load_records, Path(metadata["file_b"]), sheet_b, map_b, "B"
            )
        if len(records_a) + len(records_b or []) > config.max_total_rows:
            raise HTTPException(400, "所选工作表行数超过配置上限")
        job_id = await app.state.processor.create_job(
            job_name,
            records_a,
            records_b,
            mode=mode,
            match_preset=match_preset,
            time_window_days=time_window_days,
        )
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_detail(request: Request, job_id: str):
        try:
            job = await database.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(404, "任务不存在") from exc
        if job["pipeline_version"] == "event_cluster_v2":
            return await _event_workspace_response(request, database, job)
        pair_total = await database.count_pairs(job_id)
        group_total = await database.count_groups(job_id, merged_only=True)
        pairs = await database.list_pair_details(job_id, limit=PAIR_PAGE_SIZE)
        return TEMPLATES.TemplateResponse(
            request,
            "job.html",
            {
                "job": job,
                "pairs": pairs,
                "pair_page": 1,
                "pair_total": pair_total,
                "pair_page_count": max(1, (pair_total + PAIR_PAGE_SIZE - 1) // PAIR_PAGE_SIZE),
                "pair_has_next": pair_total > PAIR_PAGE_SIZE,
                "groups": await database.list_groups(
                    job_id, limit=GROUP_PAGE_SIZE, merged_only=True
                ),
                "group_total": group_total,
                "group_page": 1,
                "group_page_count": max(
                    1, (group_total + GROUP_PAGE_SIZE - 1) // GROUP_PAGE_SIZE
                ),
                "group_has_next": group_total > GROUP_PAGE_SIZE,
                "region_stats": await database.list_region_stats(job_id),
                "pair_filters": {},
                "show_all": False,
                "pair_query": "show_all=false",
            },
        )

    @app.get("/jobs/{job_id}/events", response_class=HTMLResponse)
    async def events(
        request: Request,
        job_id: str,
        page: int = 1,
        region: str = "",
        street: str = "",
        category_level_1: str = "",
        category_level_2: str = "",
        category_level_3: str = "",
        category_level_4: str = "",
        category: str = "",
        event_id: str = "",
        status: str = "",
        min_confidence: str = "",
        show_singletons: bool = False,
        show_rejected: bool = False,
    ):
        try:
            await database.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(404, "任务不存在") from exc
        confidence = _parse_confidence(min_confidence)
        parsed_event_id = _parse_optional_int(event_id, "事件")
        filters = {
            "region": region.strip(),
            "street": street.strip(),
            "category_level_1": category_level_1.strip(),
            "category_level_2": category_level_2.strip(),
            "category_level_3": category_level_3.strip(),
            "category_level_4": category_level_4.strip(),
            "category": category.strip(),
            "event_id": parsed_event_id,
            "status": status.strip(),
            "min_confidence": confidence,
            "include_singletons": show_singletons,
            "include_rejected": show_rejected,
        }
        page = max(page, 1)
        total = await database.count_candidate_events(job_id, **filters)
        page_count = max(1, (total + PAIR_PAGE_SIZE - 1) // PAIR_PAGE_SIZE)
        page = min(page, page_count)
        options = await database.list_candidate_event_filter_options(
            job_id, region=region, street=street
        )
        query_values = {
            **{key: value for key, value in filters.items() if value not in ("", None, False)},
            "show_singletons": str(show_singletons).lower(),
            "show_rejected": str(show_rejected).lower(),
        }
        return TEMPLATES.TemplateResponse(
            request,
            "partials/events.html",
            {
                "job_id": job_id,
                "events": await database.list_candidate_events(
                    job_id,
                    limit=PAIR_PAGE_SIZE,
                    offset=(page - 1) * PAIR_PAGE_SIZE,
                    **filters,
                ),
                "event_total": total,
                "event_page": page,
                "event_page_count": page_count,
                "event_has_next": page < page_count,
                "event_filters": filters,
                "event_options": options,
                "event_stats": await database.list_candidate_event_stats(job_id, **filters),
                "event_query": urlencode(query_values),
            },
        )

    @app.get("/jobs/{job_id}/events/{event_id}", response_class=HTMLResponse)
    async def event_detail(request: Request, job_id: str, event_id: int):
        try:
            event = await database.get_candidate_event(job_id, event_id)
        except KeyError as exc:
            raise HTTPException(404, "候选事件不存在") from exc
        return TEMPLATES.TemplateResponse(
            request, "event_detail.html", {"job_id": job_id, "event": event}
        )

    @app.get("/jobs/{job_id}/event-filter-options")
    async def event_filter_options(job_id: str, region: str = "", street: str = ""):
        return await database.list_candidate_event_filter_options(
            job_id, region=region, street=street
        )

    @app.post("/jobs/{job_id}/events/{event_id}/confirm")
    async def confirm_event(job_id: str, event_id: int, note: str = Form("")):
        await database.confirm_candidate_event(job_id, event_id, note or None)
        return RedirectResponse(f"/jobs/{job_id}/events/{event_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/{event_id}/reopen")
    async def reopen_event(job_id: str, event_id: int, note: str = Form("")):
        await database.reopen_candidate_event(job_id, event_id, note or None)
        return RedirectResponse(f"/jobs/{job_id}/events/{event_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/{event_id}/name")
    async def rename_event(job_id: str, event_id: int, name: str = Form(...)):
        await database.rename_candidate_event(job_id, event_id, name)
        return RedirectResponse(f"/jobs/{job_id}/events/{event_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/{event_id}/split")
    async def split_event(job_id: str, event_id: int, record_ids: list[int] = Form(...), name: str = Form(...)):
        await database.split_candidate_event(job_id, event_id, record_ids, name=name)
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/merge")
    async def merge_events(job_id: str, event_ids: list[int] = Form(...), name: str = Form(...)):
        event_id = await database.merge_candidate_events(job_id, event_ids, name=name)
        return RedirectResponse(f"/jobs/{job_id}/events/{event_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/{event_id}/members/{record_id}/move")
    async def move_event_member(job_id: str, event_id: int, record_id: int, target_event_id: int = Form(...)):
        await database.move_candidate_event_member(job_id, record_id, source_event_id=event_id, target_event_id=target_event_id)
        return RedirectResponse(f"/jobs/{job_id}/events/{target_event_id}", status_code=303)

    @app.post("/jobs/{job_id}/events/{event_id}/members/{record_id}/exclude")
    async def exclude_event_member(job_id: str, event_id: int, record_id: int, note: str = Form("")):
        await database.exclude_candidate_event_member(job_id, event_id, record_id, note=note or None)
        return RedirectResponse(f"/jobs/{job_id}/events/{event_id}", status_code=303)

    @app.get("/jobs/{job_id}/status", response_class=HTMLResponse)
    async def job_status(request: Request, job_id: str):
        try:
            job = await database.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        return TEMPLATES.TemplateResponse(
            request,
            "partials/job_status.html",
            {"job": job},
        )

    @app.get("/jobs/{job_id}/pairs", response_class=HTMLResponse)
    async def pairs(
        request: Request,
        job_id: str,
        page: int = 1,
        show_all: bool = False,
        region: str = "",
        category: str = "",
        recall_reason: str = "",
        model_decision: str = "",
        review_status: str = "",
        min_confidence: str = "",
    ):
        page = max(page, 1)
        confidence_value: float | None = None
        if min_confidence.strip():
            try:
                confidence_value = float(min_confidence)
            except ValueError as exc:
                raise HTTPException(400, "最低置信度必须是 0 到 1 之间的数字") from exc
            if not 0 <= confidence_value <= 1:
                raise HTTPException(400, "最低置信度必须是 0 到 1 之间的数字")
        filters = {
            "region": region.strip(),
            "category": category.strip(),
            "recall_reason": recall_reason.strip(),
            "model_decision": model_decision.strip(),
            "review_status": review_status.strip(),
            "min_confidence": confidence_value,
        }
        query_values = {
            "show_all": str(show_all).lower(),
            **{key: value for key, value in filters.items() if value not in ("", None)},
        }
        include_not_duplicate = show_all or filters["model_decision"] == "not_duplicate"
        pair_total = await database.count_pairs(
            job_id, include_not_duplicate=include_not_duplicate, **filters
        )
        page_count = max(1, (pair_total + PAIR_PAGE_SIZE - 1) // PAIR_PAGE_SIZE)
        page = min(page, page_count)
        return TEMPLATES.TemplateResponse(
            request,
            "partials/pairs.html",
            {
                "job_id": job_id,
                "pairs": await database.list_pair_details(
                    job_id,
                    limit=PAIR_PAGE_SIZE,
                    offset=(page - 1) * PAIR_PAGE_SIZE,
                    include_not_duplicate=include_not_duplicate,
                    **filters,
                ),
                "pair_page": page,
                "pair_total": pair_total,
                "pair_page_count": page_count,
                "pair_has_next": page < page_count,
                "pair_filters": filters,
                "show_all": show_all,
                "pair_query": urlencode(query_values),
            },
        )

    @app.get("/jobs/{job_id}/groups", response_class=HTMLResponse)
    async def event_group_page(request: Request, job_id: str, page: int = 1):
        group_total = await database.count_groups(job_id, merged_only=True)
        page_count = max(1, (group_total + GROUP_PAGE_SIZE - 1) // GROUP_PAGE_SIZE)
        page = min(max(page, 1), page_count)
        return TEMPLATES.TemplateResponse(
            request,
            "partials/event_groups.html",
            {
                "job_id": job_id,
                "groups": await database.list_groups(
                    job_id,
                    limit=GROUP_PAGE_SIZE,
                    offset=(page - 1) * GROUP_PAGE_SIZE,
                    merged_only=True,
                ),
                "group_total": group_total,
                "group_page": page,
                "group_page_count": page_count,
                "group_has_next": page < page_count,
            },
        )

    @app.post("/jobs/{job_id}/pairs/{pair_id}/review")
    async def review(job_id: str, pair_id: int, decision: str = Form(...), note: str = Form("")):
        try:
            await database.review_pair(job_id, pair_id, decision, note or None)
        except (KeyError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/{job_id}/pause")
    async def pause(job_id: str):
        await database.request_pause(job_id, True)
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/{job_id}/resume")
    async def resume(job_id: str):
        try:
            await database.resume_job(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="任务不存在") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/{job_id}/groups/{group_id}/name")
    async def rename_group(job_id: str, group_id: int, name: str = Form(...)):
        try:
            await database.rename_event_group(job_id, group_id, name)
        except (KeyError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}/export")
    async def export(job_id: str):
        output = await export_job(database, job_id, results_dir / job_id / "result.xlsx")
        return FileResponse(output, filename=f"{job_id}-result.xlsx")

    return app


def _database_url(settings: Settings) -> str:
    if settings.database_mode == "sqlite":
        return f"sqlite+aiosqlite:///{settings.database_path}"
    return URL.create(
        drivername="postgresql+asyncpg",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    ).render_as_string(hide_password=False)


def _parse_confidence(value: str) -> float | None:
    if not value.strip():
        return None
    try:
        confidence = float(value)
    except ValueError as exc:
        raise HTTPException(400, "最低置信度必须是 0 到 1 之间的数字") from exc
    if not 0 <= confidence <= 1:
        raise HTTPException(400, "最低置信度必须是 0 到 1 之间的数字")
    return confidence


def _parse_optional_int(value: str, field_name: str) -> int | None:
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except ValueError as exc:
        raise HTTPException(400, f"{field_name}参数无效") from exc


async def _event_workspace_response(
    request: Request, database: AsyncDatabase, job: dict
) -> HTMLResponse:
    job_id = job["id"]
    total = await database.count_candidate_events(job_id)
    options = await database.list_candidate_event_filter_options(job_id)
    return TEMPLATES.TemplateResponse(
        request,
        "job.html",
        {
            "job": job,
            "events": await database.list_candidate_events(job_id, limit=PAIR_PAGE_SIZE),
            "event_total": total,
            "event_page": 1,
            "event_page_count": max(1, (total + PAIR_PAGE_SIZE - 1) // PAIR_PAGE_SIZE),
            "event_has_next": total > PAIR_PAGE_SIZE,
            "event_filters": {},
            "event_options": options,
            "event_stats": await database.list_candidate_event_stats(job_id),
            "event_query": "show_singletons=false&show_rejected=false",
            "region_stats": await database.list_region_stats(job_id),
        },
    )
