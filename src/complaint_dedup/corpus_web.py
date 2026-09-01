from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_exporter import export_corpus
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository, EventFilters
from complaint_dedup.corpus_worker import CorpusBatchWorker
from complaint_dedup.llm_client import build_llm_client
from complaint_dedup.ui_labels import format_datetime, label, stage_label


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Jinja2Templates(directory=PROJECT_ROOT / "templates")
TEMPLATES.env.globals.update(label=label, stage_label=stage_label)
TEMPLATES.env.filters["urlencode"] = lambda value: urlencode(value, doseq=True)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise HTTPException(400, "日期格式应为 YYYY-MM-DD") from exc

def _parse_checkbox(value: str | None) -> bool:
    """Accept missing, empty, and standard HTML checkbox values."""
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def create_corpus_app(
    settings: Settings,
    *,
    database: AsyncDatabase | None = None,
    embedded_worker: bool | None = None,
) -> FastAPI:
    database = database or AsyncDatabase(
        corpus_database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    repository = CorpusRepository(database)
    start_embedded_worker = (
        settings.database_mode == "sqlite"
        if embedded_worker is None
        else embedded_worker
    )
    normalization_llm = (
        build_llm_client(
            settings,
            model=settings.llm_judgement_model or settings.llm_model,
            concurrency=settings.normalization_llm_concurrency,
        )
        if start_embedded_worker
        and settings.normalization_llm_enabled
        and settings.llm_model
        else None
    )
    processor = CorpusProcessor(
        repository,
        dictionary_review_required=settings.dictionary_review_required,
        normalization_llm_client=normalization_llm,
        normalization_llm_enabled=settings.normalization_llm_enabled,
        normalization_llm_min_confidence=settings.normalization_llm_min_confidence,
        normalization_llm_batch_size=settings.normalization_llm_batch_size,
    )
    runtime_dir = settings.database_path.parent
    upload_dir = runtime_dir / "corpus_uploads"
    result_dir = runtime_dir / "corpus_results"
    upload_dir.mkdir(parents=True, exist_ok=True)
    result_dir.mkdir(parents=True, exist_ok=True)
    worker = CorpusBatchWorker(repository, processor, settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await database.initialize()
        if start_embedded_worker:
            await worker.start()
        try:
            yield
        finally:
            if start_embedded_worker:
                await worker.stop()
            if normalization_llm is not None:
                await normalization_llm.aclose()
            await database.close()

    app = FastAPI(title="投诉事件归一化工作台", lifespan=lifespan)
    TEMPLATES.env.filters["localtime"] = lambda value: format_datetime(
        value, settings.app_timezone
    )
    app.state.database = database
    app.state.repository = repository
    app.state.processor = processor
    app.state.worker = worker
    app.state.settings = settings
    app.mount("/static", StaticFiles(directory=PROJECT_ROOT / "static"), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_index.html",
            {
                "batches": await repository.list_batches(limit=10),
                "batch_count": await repository.count_batches(),
                "event_count": await repository.count_events(),
            },
        )

    @app.get("/batches", response_class=HTMLResponse)
    async def batch_history(request: Request, page: int = 1):
        page = max(page, 1)
        page_size = 10
        total = await repository.count_batches()
        page_count = max(1, (total + page_size - 1) // page_size)
        page = min(page, page_count)
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_batches.html",
            {
                "batches": await repository.list_batches(
                    limit=page_size, offset=(page - 1) * page_size
                ),
                "page": page,
                "page_count": page_count,
            },
        )

    @app.post("/batches")
    async def create_batch(
        mode: str = Form(...),
        name: str = Form(""),
        file_history: UploadFile | None = File(None),
        file_daily: UploadFile | None = File(None),
    ):
        if mode not in {
            "bootstrap_history",
            "bootstrap_compare",
            "daily_increment",
        }:
            raise HTTPException(400, "批次类型无效")
        if mode in {"bootstrap_history", "bootstrap_compare"} and file_history is None:
            raise HTTPException(400, "历史冷启动必须上传历史文件 B")
        if mode in {"daily_increment", "bootstrap_compare"} and file_daily is None:
            raise HTTPException(400, "该模式必须上传当天文件 A")

        input_files: dict[str, dict[str, str]] = {}
        if mode in {"bootstrap_history", "bootstrap_compare"}:
            history_path, history_hash = await _save_upload(file_history, upload_dir)
            input_files["history"] = {
                "path": str(history_path),
                "file_hash": history_hash,
                "file_name": file_history.filename or history_path.name,
            }
        if mode in {"bootstrap_compare", "daily_increment"}:
            daily_path, daily_hash = await _save_upload(file_daily, upload_dir)
            input_files["daily"] = {
                "path": str(daily_path),
                "file_hash": daily_hash,
                "file_name": file_daily.filename or daily_path.name,
            }
        batch_name = name or {
            "bootstrap_history": "历史库冷启动",
            "bootstrap_compare": "首次联合比对",
            "daily_increment": "每日新增",
        }[mode]
        batch_id = await repository.create_batch(
            batch_name, mode, input_files=input_files
        )
        return RedirectResponse(f"/batches/{batch_id}", status_code=303)

    @app.get("/batches/{batch_id}", response_class=HTMLResponse)
    async def batch_detail(request: Request, batch_id: str):
        try:
            batch = await repository.get_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(404, "批次不存在") from exc
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_batch.html",
            {
                "batch": batch,
                "records": await repository.records_for_batch(
                    batch_id, limit=10
                ),
            },
        )

    @app.get("/batches/{batch_id}/data")
    async def batch_data(batch_id: str):
        try:
            batch = await repository.get_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(404, "批次不存在") from exc
        return JSONResponse(jsonable_encoder(batch))

    @app.get("/batches/{batch_id}/status", response_class=HTMLResponse)
    async def batch_status(request: Request, batch_id: str):
        try:
            batch = await repository.get_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(404, "批次不存在") from exc
        return TEMPLATES.TemplateResponse(
            request,
            "partials/corpus_batch_progress.html",
            {"batch": batch},
        )

    @app.post("/batches/{batch_id}/approve")
    async def approve_batch(batch_id: str):
        try:
            await repository.request_batch_action(batch_id, "approve")
        except (KeyError, ValueError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return RedirectResponse(f"/batches/{batch_id}", status_code=303)

    @app.post("/batches/{batch_id}/commit")
    async def commit_batch(batch_id: str):
        try:
            await repository.request_batch_action(batch_id, "commit")
        except (KeyError, ValueError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return RedirectResponse(f"/batches/{batch_id}", status_code=303)

    @app.get("/events", response_class=HTMLResponse)
    async def event_list(
        request: Request,
        region: str = "",
        street: str = "",
        processing_department: str = "",
        completed_from: str | None = None,
        completed_to: str | None = None,
        missing_completed: str | None = None,
        event_name: str = "",
        sort: str = "updated_desc",
        has_daily: str | None = None,
        hide_singletons: str | None = None,
        page: int = 1,
    ):
        has_daily = _parse_checkbox(has_daily)
        hide_singletons = _parse_checkbox(hide_singletons)
        missing_completed = _parse_checkbox(missing_completed)
        filters = EventFilters(
            region=region,
            street=street,
            event_name=event_name,
            processing_department=processing_department,
            completed_from=_parse_date(completed_from),
            completed_to=_parse_date(completed_to),
            missing_completed=missing_completed,
            has_daily_records=has_daily,
            hide_singletons=hide_singletons,
        )
        page = max(page, 1)
        page_size = 10
        items, total = await repository.list_event_summaries(
            filters=filters,
            sort=sort,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        max_page = max(1, (total + page_size - 1) // page_size)
        if page > max_page:
            page = max_page
            items, total = await repository.list_event_summaries(
                filters=filters,
                sort=sort,
                limit=page_size,
                offset=(page - 1) * page_size,
            )
        filter_options = await repository.event_filter_options(
            region=region, street=street
        )
        query = {
            "region": region,
            "street": street,
            "processing_department": processing_department,
            "completed_from": completed_from or "",
            "completed_to": completed_to or "",
            "missing_completed": "1" if missing_completed else "",
            "event_name": event_name,
            "sort": sort,
            "has_daily": "1" if has_daily else "",
            "hide_singletons": "1" if hide_singletons else "",
        }
        # Do not emit empty boolean query parameters. FastAPI rejects values
        # such as ``has_daily=`` instead of treating them as false.
        query = {key: value for key, value in query.items() if value != ""}
        page_count = max(1, (total + page_size - 1) // page_size)
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_events.html",
            {
                "events": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "page_count": page_count,
                "singleton_count": await repository.count_singleton_events(
                    filters=filters,
                ),
                "filtered_export_url": f"/exports/corpus?{urlencode({**query, 'scope': 'filtered'})}",
                "previous_url": f"/events?{urlencode({**query, 'page': page - 1})}",
                "next_url": f"/events?{urlencode({**query, 'page': page + 1})}",
                "filters": {
                    "region": region,
                    "street": street,
                    "processing_department": processing_department,
                    "completed_from": completed_from or "",
                    "completed_to": completed_to or "",
                    "missing_completed": missing_completed,
                    "event_name": event_name,
                    "sort": sort,
                    "has_daily": has_daily,
                    "hide_singletons": hide_singletons,
                },
                "filter_options": filter_options,
            },
        )

    @app.get("/events/options")
    async def event_options(q: str = "", exclude_event_id: int | None = None):
        return JSONResponse(
            await repository.search_event_options(
                q, exclude_event_id=exclude_event_id
            )
        )

    @app.get("/events/{event_id}", response_class=HTMLResponse)
    async def event_detail(request: Request, event_id: int, page: int = 1):
        try:
            event = await repository.get_event(event_id)
        except KeyError as exc:
            raise HTTPException(404, "事件不存在") from exc
        page = max(page, 1)
        page_size = 10
        total = await repository.count_event_records(event_id)
        page_count = max(1, (total + page_size - 1) // page_size)
        page = min(page, page_count)
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_event_detail.html",
            {
                "event": event,
                "records": await repository.event_records(
                    event_id,
                    limit=page_size,
                    offset=(page - 1) * page_size,
                ),
                "total": total,
                "page": page,
                "page_count": page_count,
            },
        )

    @app.post("/events/{event_id}/name")
    async def rename_event(event_id: int, name: str = Form(...)):
        try:
            await repository.rename_event(event_id, name, reviewed_by="本机管理员")
        except KeyError as exc:
            raise HTTPException(404, "事件不存在") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(f"/events/{event_id}", status_code=303)

    @app.post("/events/{event_id}/members/{record_id}/exclude")
    async def exclude_event_member(
        event_id: int,
        record_id: int,
        target_name: str = Form(""),
    ):
        try:
            target_id = await repository.exclude_record(
                event_id,
                record_id,
                target_name=target_name,
                reviewed_by="本机管理员",
            )
        except KeyError as exc:
            raise HTTPException(404, "事件或工单不存在") from exc
        return RedirectResponse(f"/events/{target_id}", status_code=303)

    @app.get("/exports/corpus")
    async def download_corpus(
        scope: str = "all",
        region: str = "",
        street: str = "",
        processing_department: str = "",
        completed_from: str | None = None,
        completed_to: str | None = None,
        missing_completed: str | None = None,
        event_name: str = "",
        has_daily: str | None = None,
        hide_singletons: str | None = None,
    ):
        filters = None
        if scope == "filtered":
            filters = EventFilters(
                region=region,
                street=street,
                event_name=event_name,
                processing_department=processing_department,
                completed_from=_parse_date(completed_from),
                completed_to=_parse_date(completed_to),
                missing_completed=_parse_checkbox(missing_completed),
                has_daily_records=_parse_checkbox(has_daily),
                hide_singletons=_parse_checkbox(hide_singletons),
            )
        elif scope != "all":
            raise HTTPException(400, "导出范围无效")
        output = result_dir / (
            "投诉事件筛选结果.xlsx" if filters is not None else "投诉事件归一化结果.xlsx"
        )
        await export_corpus(repository, output, filters=filters)
        return FileResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=output.name,
        )

    return app


async def _save_upload(upload: UploadFile | None, directory: Path) -> tuple[Path, str]:
    if upload is None:
        raise HTTPException(400, "缺少上传文件")
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix not in {".xlsx", ".xls", ".csv"}:
        raise HTTPException(400, "仅支持 xlsx、xls 和 csv")
    payload = await upload.read()
    digest = hashlib.sha256(payload).hexdigest()
    path = directory / f"{uuid.uuid4().hex}{suffix}"
    await asyncio.to_thread(path.write_bytes, payload)
    return path, digest
