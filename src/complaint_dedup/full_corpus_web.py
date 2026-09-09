from __future__ import annotations

import hashlib
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from complaint_dedup import licensing
from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.full_corpus import (
    ActiveJobConflictError,
    EventFilters,
    FullCorpusService,
    job_status_label,
)
from complaint_dedup.full_corpus_worker import FullCorpusWorker
from complaint_dedup.logging_setup import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_full_corpus_app(
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
    templates = Jinja2Templates(directory=PROJECT_ROOT / "templates")
    local_timezone = ZoneInfo(settings.app_timezone)
    should_start_worker = settings.database_mode == "sqlite" if embedded_worker is None else embedded_worker

    def local_datetime(value):
        if value in (None, ""):
            return "-"
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return value
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(local_timezone).strftime("%Y-%m-%d %H:%M")

    templates.env.filters["localtime"] = local_datetime
    templates.env.filters["endday"] = lambda value: value - timedelta(days=1) if value else value

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        setup_logging(
            log_dir=settings.log_dir,
            process_name="api",
            level=settings.log_level,
            retention_days=settings.log_retention_days,
        )
        await database.initialize()
        await licensing.ensure_license_ok(database)
        service_instance = FullCorpusService(database)
        worker = FullCorpusWorker(service_instance, settings)
        app.state.database = database
        app.state.full_service = service_instance
        app.state.worker = worker
        if should_start_worker:
            await worker.start()
        try:
            yield
        finally:
            if should_start_worker:
                await worker.stop()
            await database.close()

    app = FastAPI(title="投诉全量工单比对系统", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=PROJECT_ROOT / "static"), name="static")

    @app.middleware("http")
    async def license_gate(request: Request, call_next):
        try:
            licensing.ensure_not_expired()
        except licensing.LicenseExpiredError as exc:
            if "application/json" in request.headers.get("accept", ""):
                return JSONResponse(
                    {"error": "服务授权已到期", "expire_date": licensing.expire_date().isoformat()},
                    status_code=403,
                )
            return HTMLResponse(
                "<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>服务已到期</title>"
                f"<body><main style='max-width:720px;margin:15vh auto;font-family:sans-serif'>"
                f"<h1>服务授权已到期</h1><p>{exc}</p></main></body></html>",
                status_code=403,
            )
        return await call_next(request)

    def service(request: Request) -> FullCorpusService:
        return request.app.state.full_service

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/favicon.ico")
    async def favicon():
        return Response(status_code=204)

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        current = service(request)
        jobs = await current.list_jobs(limit=10)
        for job in jobs:
            job["status_label"] = job_status_label(job.get("status"))
        return templates.TemplateResponse(
            request,
            "full_index.html",
            {
                "sync_runs": await current.list_sync_runs(limit=10),
                "comparisons": await current.list_comparisons(limit=10),
                "jobs": jobs,
                "order_count": await current.count_current_orders(),
                "job_id": request.query_params.get("job_id", ""),
            },
        )

    @app.get("/jobs/{job_id}")
    async def job_status(request: Request, job_id: str):
        job = await service(request).get_job(job_id)
        if job is None:
            return JSONResponse({"error": "任务不存在"}, status_code=404)
        job["status_code"] = job.get("status")
        job["status"] = job_status_label(job.get("status"))
        job.pop("progress", None)
        return JSONResponse(jsonable_encoder(job))

    @app.post("/sync")
    async def sync_full_file(request: Request, file: UploadFile = File(...)):
        if not file.filename:
            return HTMLResponse("缺少全量 Excel 文件", status_code=400)
        safe_name = Path(file.filename).name
        if Path(safe_name).suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            return HTMLResponse("仅支持 xlsx、xls 和 csv 文件", status_code=400)
        upload_dir = Path(settings.database_path).parent / "full_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        payload = await file.read()
        target = upload_dir / f"{uuid.uuid4().hex}-{safe_name}"
        target.write_bytes(payload)
        try:
            job_id = await service(request).create_job(
                "sync",
                {
                    "path": str(target),
                    "file_name": safe_name,
                    "file_hash": hashlib.sha256(payload).hexdigest(),
                },
            )
        except ActiveJobConflictError as exc:
            target.unlink(missing_ok=True)
            return HTMLResponse(str(exc), status_code=409)
        return RedirectResponse(f"/?job_id={job_id}", status_code=303)

    @app.post("/comparisons")
    async def create_comparison(
        request: Request,
        time_field: str = Form("completed_at"),
        target_from: str = Form(""),
        target_to: str = Form(""),
        reference_from: str = Form(""),
        reference_to: str = Form(""),
    ):
        if time_field not in {"completed_at", "received_at"}:
            return HTMLResponse("创建比对任务失败：比对字段必须是 completed_at 或 received_at", status_code=400)
        current = service(request)
        latest = await current.latest_local_date(time_field)
        if latest is None:
            return HTMLResponse("当前全量库没有可用于窗口比对的日期", status_code=400)
        try:
            start = date.fromisoformat(target_from) if target_from else latest
            end = date.fromisoformat(target_to) if target_to else start
            reference_start = _parse_optional_date(reference_from)
            reference_end = _parse_optional_date(reference_to)
            if start > end:
                raise ValueError("待比对开始日期不能晚于结束日期")
            if (reference_start is None) != (reference_end is None):
                raise ValueError("被比对时间段必须同时提供开始和结束日期")
            if reference_start and reference_end:
                if reference_start > reference_end:
                    raise ValueError("被比对开始日期不能晚于结束日期")
                if reference_start <= end and start <= reference_end:
                    raise ValueError("待比对和被比对时间段不能重叠")
        except ValueError as exc:
            return HTMLResponse(f"创建比对任务失败：{exc}", status_code=400)
        try:
            job_id = await current.create_job(
                "comparison",
                {
                    "time_field": time_field,
                    "target_from": start.isoformat(),
                    "target_to": end.isoformat(),
                    "reference_from": reference_start.isoformat() if reference_start else None,
                    "reference_to": reference_end.isoformat() if reference_end else None,
                },
            )
        except ActiveJobConflictError as exc:
            return HTMLResponse(str(exc), status_code=409)
        return RedirectResponse(f"/?job_id={job_id}", status_code=303)

    @app.get("/comparisons", response_class=HTMLResponse)
    async def comparison_library(
        request: Request,
        comparison_id: str = "",
        region: str = "",
        street: str = "",
        processing_department: str = "",
        completed_from: str | None = None,
        completed_to: str | None = None,
        missing_completed: str | None = None,
        event_name: str = "",
        sort: str = "updated_desc",
        has_target: str | None = None,
        hide_singletons: str | None = None,
        keyword: str = "",
        search_all: str | None = None,
        page: int = 1,
        task_page: int = 1,
    ):
        current = service(request)
        task_page = max(task_page, 1)
        comparisons = await current.list_comparisons(limit=10, offset=(task_page - 1) * 10)
        task_count = await current.count_comparisons()
        task_page_count = max(1, (task_count + 9) // 10)
        if comparison_id:
            selected = await current.get_comparison(comparison_id)
            if selected is None:
                return HTMLResponse("比对任务不存在", status_code=404)
        else:
            selected = comparisons[0] if comparisons else None
        selected_id = str(selected["id"]) if selected else ""
        try:
            filters = EventFilters(
                region=region,
                street=street,
                event_name=event_name,
                processing_department=processing_department,
                completed_from=_parse_optional_date(completed_from),
                completed_to=_parse_optional_date(completed_to),
                missing_completed=_parse_checkbox(missing_completed),
                has_target_records=_parse_checkbox(has_target),
                hide_singletons=_parse_checkbox(hide_singletons),
                keyword=keyword,
                search_all=_parse_checkbox(search_all),
            )
        except ValueError as exc:
            return HTMLResponse(f"筛选条件无效：{exc}", status_code=400)
        page = max(page, 1)
        events: list[dict] = []
        total = singleton_count = 0
        filter_options = {"regions": [], "streets": [], "processing_departments": []}
        if selected:
            events, total = await current.list_event_summaries(
                selected_id,
                filters=filters,
                sort=sort,
                limit=20,
                offset=(page - 1) * 20,
            )
            singleton_count = await current.count_singleton_events(selected_id, filters=filters)
            filter_options = await current.event_filter_options(
                selected_id, region=region, street=street
            )
        page_count = max(1, (total + 19) // 20)
        if page > page_count:
            page = page_count
            if selected:
                events, total = await current.list_event_summaries(
                    selected_id,
                    filters=filters,
                    sort=sort,
                    limit=20,
                    offset=(page - 1) * 20,
                )
        query = {
            "comparison_id": selected_id,
            "region": region,
            "street": street,
            "processing_department": processing_department,
            "completed_from": completed_from or "",
            "completed_to": completed_to or "",
            "missing_completed": "1" if filters.missing_completed else "",
            "event_name": event_name,
            "sort": sort,
            "has_target": "1" if filters.has_target_records else "",
            "hide_singletons": "1" if filters.hide_singletons else "",
            "keyword": filters.keyword,
            "search_all": "1" if filters.search_all else "",
        }
        previous_url = _query_url("/comparisons", {**query, "page": page - 1}) if page > 1 else ""
        next_url = _query_url("/comparisons", {**query, "page": page + 1}) if page < page_count else ""
        return templates.TemplateResponse(
            request,
            "full_comparisons.html",
            {
                "comparisons": comparisons,
                "comparison": selected,
                "events": events,
                "total": total,
                "singleton_count": singleton_count,
                "filters": filters,
                "sort": sort,
                "filter_options": filter_options,
                "page": page,
                "page_count": page_count,
                "task_page": task_page,
                "task_page_count": task_page_count,
                "query": query,
                "all_export_url": _query_url(f"/comparisons/{selected_id}/export", {"scope": "all"}) if selected else "#",
                "filtered_export_url": _query_url(f"/comparisons/{selected_id}/export", {**query, "scope": "filtered"}) if selected else "#",
                "previous_url": previous_url,
                "next_url": next_url,
            },
        )

    @app.get("/comparisons/{comparison_id}")
    async def comparison_redirect(comparison_id: str):
        return RedirectResponse(f"/comparisons?comparison_id={comparison_id}", status_code=303)

    @app.get("/events")
    async def events_redirect():
        return RedirectResponse("/comparisons", status_code=303)

    @app.get("/events/{event_id}", response_class=HTMLResponse)
    async def event_detail(request: Request, event_id: int, page: int = 1):
        current = service(request)
        page = max(page, 1)
        event, records, total = await current.list_event_records(
            event_id, limit=20, offset=(page - 1) * 20
        )
        if event is None:
            return HTMLResponse("事件不存在", status_code=404)
        page_count = max(1, (total + 19) // 20)
        if page > page_count:
            page = page_count
            _, records, _ = await current.list_event_records(
                event_id, limit=20, offset=(page - 1) * 20
            )
        return templates.TemplateResponse(
            request,
            "full_event_detail.html",
            {"event": event, "records": records, "total": total, "page": page, "page_count": page_count},
        )

    @app.post("/events/{event_id}/name")
    async def rename_event(event_id: int, name: str = Form(...)):
        try:
            await app.state.full_service.update_event_name(event_id, name)
        except (KeyError, ValueError) as exc:
            return HTMLResponse(str(exc), status_code=400)
        return RedirectResponse(f"/events/{event_id}", status_code=303)

    @app.post("/events/{event_id}/members/{record_key}/exclude")
    async def exclude_event_member(event_id: int, record_key: str, target_name: str = Form("")):
        try:
            target_id = await app.state.full_service.exclude_event_member(
                event_id, record_key, target_name=target_name
            )
        except KeyError as exc:
            return HTMLResponse("事件或工单不存在", status_code=404)
        return RedirectResponse(f"/events/{target_id}", status_code=303)

    @app.get("/comparisons/{comparison_id}/export")
    async def export_comparison(
        request: Request,
        comparison_id: str,
        scope: str = "all",
        region: str = "",
        street: str = "",
        processing_department: str = "",
        completed_from: str | None = None,
        completed_to: str | None = None,
        missing_completed: str | None = None,
        event_name: str = "",
        has_target: str | None = None,
        hide_singletons: str | None = None,
        keyword: str = "",
        search_all: str | None = None,
    ):
        current = service(request)
        if await current.get_comparison(comparison_id) is None:
            return HTMLResponse("比对任务不存在", status_code=404)
        if scope not in {"all", "filtered"}:
            return HTMLResponse("导出范围无效", status_code=400)
        try:
            filters = None if scope == "all" else EventFilters(
                region=region,
                street=street,
                processing_department=processing_department,
                completed_from=_parse_optional_date(completed_from),
                completed_to=_parse_optional_date(completed_to),
                missing_completed=_parse_checkbox(missing_completed),
                event_name=event_name,
                has_target_records=_parse_checkbox(has_target),
                hide_singletons=_parse_checkbox(hide_singletons),
                keyword=keyword,
                search_all=_parse_checkbox(search_all),
            )
        except ValueError as exc:
            return HTMLResponse(f"筛选条件无效：{exc}", status_code=400)
        from complaint_dedup.full_corpus_exporter import export_comparison_workbook

        output = Path(settings.database_path).parent / "full_exports" / f"comparison-{comparison_id}-{scope}.xlsx"
        await export_comparison_workbook(current, comparison_id, output, filters=filters)
        return FileResponse(output, filename=output.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    @app.get("/exports/corpus")
    async def legacy_export(
        request: Request,
        comparison_id: str = "",
        scope: str = "all",
        region: str = "",
        street: str = "",
        processing_department: str = "",
        completed_from: str | None = None,
        completed_to: str | None = None,
        missing_completed: str | None = None,
        event_name: str = "",
        has_target: str | None = None,
        hide_singletons: str | None = None,
        keyword: str = "",
        search_all: str | None = None,
    ):
        current = service(request)
        if not comparison_id:
            latest = await current.list_comparisons(limit=1)
            comparison_id = str(latest[0]["id"]) if latest else ""
        if not comparison_id:
            return HTMLResponse("暂无可导出的比对任务", status_code=404)
        if await current.get_comparison(comparison_id) is None:
            return HTMLResponse("比对任务不存在", status_code=404)
        if scope not in {"all", "filtered"}:
            return HTMLResponse("导出范围无效", status_code=400)
        try:
            filters = None if scope == "all" else EventFilters(
                region=region,
                street=street,
                processing_department=processing_department,
                completed_from=_parse_optional_date(completed_from),
                completed_to=_parse_optional_date(completed_to),
                missing_completed=_parse_checkbox(missing_completed),
                event_name=event_name,
                has_target_records=_parse_checkbox(has_target),
                hide_singletons=_parse_checkbox(hide_singletons),
                keyword=keyword,
                search_all=_parse_checkbox(search_all),
            )
        except ValueError as exc:
            return HTMLResponse(f"筛选条件无效：{exc}", status_code=400)
        from complaint_dedup.full_corpus_exporter import export_comparison_workbook

        output = Path(settings.database_path).parent / "full_exports" / f"comparison-{comparison_id}-{scope}.xlsx"
        await export_comparison_workbook(current, comparison_id, output, filters=filters)
        return FileResponse(output, filename=output.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return app


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _local_date(value, timezone: ZoneInfo) -> date:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(timezone).date()


def _parse_checkbox(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _query_url(path: str, values: dict[str, object]) -> str:
    query = {key: value for key, value in values.items() if value not in (None, "")}
    return f"{path}?{urlencode(query)}"
