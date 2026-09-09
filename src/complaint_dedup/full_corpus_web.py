from __future__ import annotations

import hashlib
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from complaint_dedup import licensing
from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_io import load_records_auto
from complaint_dedup.full_corpus import FullCorpusService, WindowOverlapError
from complaint_dedup.logging_setup import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_full_corpus_app(settings, *, database: AsyncDatabase | None = None) -> FastAPI:
    database = database or AsyncDatabase(
        corpus_database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    templates = Jinja2Templates(directory=PROJECT_ROOT / "templates")
    local_timezone = ZoneInfo(settings.app_timezone)

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
        app.state.database = database
        app.state.full_service = FullCorpusService(database)
        yield
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
                    {
                        "error": "服务授权已到期",
                        "expire_date": licensing.expire_date().isoformat(),
                    },
                    status_code=403,
                )
            return HTMLResponse(
                f"<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>服务已到期</title>"
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

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        current = service(request)
        sync_runs = await current.list_sync_runs()
        comparisons = await current.list_comparisons()
        orders = await current.list_current_orders()
        return templates.TemplateResponse(
            request,
            "full_index.html",
            {"sync_runs": sync_runs[:10], "comparisons": comparisons[:10], "order_count": len(orders)},
        )

    @app.post("/sync")
    async def sync_full_file(request: Request, file: UploadFile = File(...)):
        if not file.filename:
            return HTMLResponse("缺少全量 Excel 文件", status_code=400)
        upload_dir = Path(settings.database_path).parent / "full_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        safe_name = Path(file.filename).name
        if Path(safe_name).suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            return HTMLResponse("仅支持 xlsx、xls 和 csv 文件", status_code=400)
        target = upload_dir / f"{uuid.uuid4().hex}-{safe_name}"
        payload = await file.read()
        target.write_bytes(payload)
        try:
            records = load_records_auto(target)
            if len(records) > settings.max_total_rows:
                return HTMLResponse(
                    f"文件共 {len(records)} 行，超过上限 {settings.max_total_rows} 行",
                    status_code=400,
                )
            result = await service(request).sync_records(
                records,
                file_name=safe_name,
                file_hash=hashlib.sha256(payload).hexdigest(),
            )
        except Exception as exc:
            return HTMLResponse(f"同步失败：{exc}", status_code=400)
        return RedirectResponse(f"/?sync_id={result.sync_id}", status_code=303)

    @app.post("/comparisons")
    async def create_comparison(
        request: Request,
        time_field: str = Form("completed_at"),
        target_from: str = Form(""),
        target_to: str = Form(""),
        reference_from: str = Form(""),
        reference_to: str = Form(""),
    ):
        current = service(request)
        rows = await current.list_current_orders()
        field_values = [
            row.get(time_field)
            for row in rows
            if not row.get("missing_in_latest_upload")
            and row.get(time_field) is not None
        ]
        if not field_values:
            return HTMLResponse("当前全量库没有可用于窗口比对的日期", status_code=400)
        latest = max(_local_date(value, local_timezone) for value in field_values)
        try:
            start = date.fromisoformat(target_from) if target_from else latest
            end = date.fromisoformat(target_to) if target_to else start
            result = await current.compare(
                time_field=time_field,
                target_from=start,
                target_to=end,
                reference_from=date.fromisoformat(reference_from) if reference_from else None,
                reference_to=date.fromisoformat(reference_to) if reference_to else None,
            )
        except (ValueError, WindowOverlapError) as exc:
            return HTMLResponse(f"创建比对任务失败：{exc}", status_code=400)
        return RedirectResponse(f"/comparisons/{result.comparison_id}", status_code=303)

    @app.get("/comparisons/{comparison_id}", response_class=HTMLResponse)
    async def comparison_detail(
        request: Request,
        comparison_id: str,
        saved: str = "",
    ):
        current = service(request)
        comparison = await current.get_comparison(comparison_id)
        if comparison is None:
            return HTMLResponse("比对任务不存在", status_code=404)
        events = await current.list_comparison_events(comparison_id)
        record_options: dict[str, dict[str, Any]] = {}
        for event in events:
            for member in event["members"]:
                snapshot = member["snapshot"]
                record_options[member["record_key"]] = {
                    "record_key": member["record_key"],
                    "label": snapshot.get("work_order_id") or member["record_key"],
                    "title": snapshot.get("title_raw") or "",
                }
        return templates.TemplateResponse(
            request,
            "full_comparison_detail.html",
            {
                "comparison": comparison,
                "target_to_label": _local_date_label(
                    comparison.get("target_to"),
                    local_timezone,
                    subtract_day=True,
                ),
                "events": events,
                "record_options": list(record_options.values()),
                "saved": saved == "1",
            },
        )

    @app.post("/comparisons/{comparison_id}/cannot-links")
    async def add_cannot_link(
        request: Request,
        comparison_id: str,
        left_record_key: str = Form(...),
        right_record_key: str = Form(...),
        reason: str = Form("人工确认不是同一事件"),
    ):
        current = service(request)
        if await current.get_comparison(comparison_id) is None:
            return HTMLResponse("比对任务不存在", status_code=404)
        try:
            await current.add_cannot_link(
                comparison_id,
                left_record_key,
                right_record_key,
                reason=reason.strip() or "人工确认不是同一事件",
            )
        except ValueError as exc:
            return HTMLResponse(f"保存禁止关系失败：{exc}", status_code=400)
        return RedirectResponse(
            f"/comparisons/{comparison_id}?saved=1",
            status_code=303,
        )

    @app.get("/comparisons/{comparison_id}/export")
    async def export_comparison(request: Request, comparison_id: str):
        current = service(request)
        comparison = await current.get_comparison(comparison_id)
        if comparison is None:
            return HTMLResponse("比对任务不存在", status_code=404)
        from complaint_dedup.full_corpus_exporter import export_comparison_workbook

        output = Path(settings.database_path).parent / "full_exports" / f"comparison-{comparison_id}.xlsx"
        await export_comparison_workbook(current, comparison_id, output)
        return FileResponse(output, filename=output.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return app


def _local_date_label(value, timezone: ZoneInfo, *, subtract_day: bool = False) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    local = value.astimezone(timezone)
    if subtract_day:
        local -= timedelta(days=1)
    return local.date().isoformat()


def _local_date(value, timezone: ZoneInfo) -> date:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return value.astimezone(timezone).date()
