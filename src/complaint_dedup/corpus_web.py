from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_exporter import export_corpus
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_worker import CorpusBatchWorker
from complaint_dedup.dictionary_seed_exporter import export_dictionary_seed
from complaint_dedup.ui_labels import label, stage_label


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Jinja2Templates(directory=PROJECT_ROOT / "templates")
TEMPLATES.env.globals.update(label=label, stage_label=stage_label)


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
    processor = CorpusProcessor(
        repository,
        dictionary_review_required=settings.dictionary_review_required,
    )
    runtime_dir = settings.database_path.parent
    upload_dir = runtime_dir / "corpus_uploads"
    result_dir = runtime_dir / "corpus_results"
    upload_dir.mkdir(parents=True, exist_ok=True)
    result_dir.mkdir(parents=True, exist_ok=True)
    worker = CorpusBatchWorker(repository, processor, settings)
    start_embedded_worker = (
        settings.database_mode == "sqlite"
        if embedded_worker is None
        else embedded_worker
    )

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
            await database.close()

    app = FastAPI(title="投诉事件归一化工作台", lifespan=lifespan)
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
                "batches": await repository.list_batches(),
                "dictionary": await repository.active_dictionary_version(),
                "events": await repository.list_events(),
            },
        )

    @app.get("/batches", response_class=HTMLResponse)
    async def batch_history(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_batches.html",
            {"batches": await repository.list_batches()},
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
            "correction",
        }:
            raise HTTPException(400, "批次类型无效")
        if mode in {"bootstrap_history", "bootstrap_compare"} and file_history is None:
            raise HTTPException(400, "历史冷启动必须上传历史文件 B")
        if mode in {"daily_increment", "correction", "bootstrap_compare"} and file_daily is None:
            raise HTTPException(400, "该模式必须上传当天文件 A")

        input_files: dict[str, dict[str, str]] = {}
        if mode in {"bootstrap_history", "bootstrap_compare"}:
            history_path, history_hash = await _save_upload(file_history, upload_dir)
            input_files["history"] = {
                "path": str(history_path),
                "file_hash": history_hash,
                "file_name": file_history.filename or history_path.name,
            }
        if mode in {"bootstrap_compare", "daily_increment", "correction"}:
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
            "correction": "补录或更正",
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
        version = None
        if batch.get("dictionary_version_id"):
            version = await repository.get_dictionary_version(
                int(batch["dictionary_version_id"])
            )
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_batch.html",
            {
                "batch": batch,
                "dictionary": version,
                "records": await repository.records_for_batch(batch_id),
            },
        )

    @app.get("/batches/{batch_id}/status", response_class=HTMLResponse)
    async def batch_status(request: Request, batch_id: str):
        try:
            batch = await repository.get_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(404, "批次不存在") from exc
        version = None
        if batch.get("dictionary_version_id"):
            version = await repository.get_dictionary_version(
                int(batch["dictionary_version_id"])
            )
        return TEMPLATES.TemplateResponse(
            request,
            "partials/corpus_batch_progress.html",
            {"batch": batch, "dictionary": version},
        )

    @app.get("/batches/{batch_id}/dictionary", response_class=HTMLResponse)
    async def dictionary_review(
        request: Request,
        batch_id: str,
        dimension: str = "anchor",
        status: str = "",
        page: int = 1,
    ):
        if dimension not in {"street", "anchor", "issue"}:
            raise HTTPException(400, "词典维度无效")
        batch = await repository.get_batch(batch_id)
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise HTTPException(404, "该批次没有候选词典")
        page = max(page, 1)
        page_size = 20
        items, total = await repository.list_dictionary_items(
            int(version_id),
            dimension=dimension,
            status=status,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_dictionary.html",
            {
                "batch": batch,
                "dictionary": await repository.get_dictionary_version(int(version_id)),
                "items": items,
                "summary": await repository.dictionary_review_summary(int(version_id)),
                "dimension": dimension,
                "status": status,
                "page": page,
                "page_size": page_size,
                "total": total,
            },
        )

    @app.post(
        "/batches/{batch_id}/dictionary/{dimension}/{item_id}/{action}"
    )
    async def review_dictionary_item(
        batch_id: str,
        dimension: str,
        item_id: int,
        action: str,
        name: str = Form(""),
        target_id: int = Form(0),
        alias_ids: list[int] = Form(default=[]),
    ):
        batch = await repository.get_batch(batch_id)
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise HTTPException(404, "该批次没有候选词典")
        try:
            if action == "merge":
                await repository.merge_dictionary_item(
                    int(version_id),
                    dimension=dimension,
                    item_id=item_id,
                    target_id=target_id,
                    reviewed_by="本机管理员",
                )
            elif action == "split":
                await repository.split_dictionary_item(
                    int(version_id),
                    dimension=dimension,
                    item_id=item_id,
                    new_name=name,
                    alias_ids=alias_ids,
                    reviewed_by="本机管理员",
                )
            else:
                await repository.review_dictionary_item(
                    int(version_id),
                    dimension=dimension,
                    item_id=item_id,
                    action=action,
                    name=name,
                    reviewed_by="本机管理员",
                )
        except (KeyError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(
            f"/batches/{batch_id}/dictionary?dimension={dimension}", status_code=303
        )

    @app.get(
        "/batches/{batch_id}/dictionary/{dimension}/{item_id}",
        response_class=HTMLResponse,
    )
    async def dictionary_item_detail(
        request: Request, batch_id: str, dimension: str, item_id: int
    ):
        batch = await repository.get_batch(batch_id)
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise HTTPException(404, "该批次没有候选词典")
        try:
            detail = await repository.get_dictionary_item(
                int(version_id), dimension=dimension, item_id=item_id
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(404, str(exc)) from exc
        options, _ = await repository.list_dictionary_items(
            int(version_id), dimension=dimension, limit=200, offset=0
        )
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_dictionary_item.html",
            {
                "batch": batch,
                "dimension": dimension,
                "detail": detail,
                "target_options": [row for row in options if row["id"] != item_id],
            },
        )

    @app.post("/batches/{batch_id}/dictionary/bulk-approve")
    async def bulk_approve_dictionary(
        batch_id: str,
        min_evidence: int = Form(2),
    ):
        batch = await repository.get_batch(batch_id)
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise HTTPException(404, "该批次没有候选词典")
        await repository.bulk_approve_dictionary_items(
            int(version_id),
            min_evidence=min_evidence,
            reviewed_by="本机管理员",
        )
        return RedirectResponse(
            f"/batches/{batch_id}/dictionary?dimension=anchor", status_code=303
        )

    @app.get("/batches/{batch_id}/dictionary/export")
    async def download_dictionary_seed(batch_id: str):
        batch = await repository.get_batch(batch_id)
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise HTTPException(404, "该批次没有候选词典")
        output = result_dir / f"词典种子_{batch_id[:8]}.xlsx"
        await export_dictionary_seed(repository, int(version_id), output)
        return FileResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename="dictionary_seed_v1.xlsx",
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
        issue: str = "",
        event_name: str = "",
        page: int = 1,
    ):
        page = max(page, 1)
        page_size = 10
        items, total = await repository.list_event_summaries(
            region=region,
            street=street,
            issue=issue,
            event_name=event_name,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        filter_options = await repository.event_filter_options(
            region=region, street=street
        )
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_events.html",
            {
                "events": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "filters": {
                    "region": region,
                    "street": street,
                    "issue": issue,
                    "event_name": event_name,
                },
                "filter_options": filter_options,
            },
        )

    @app.get("/events/{event_id}", response_class=HTMLResponse)
    async def event_detail(request: Request, event_id: int):
        try:
            event = await repository.get_event(event_id)
        except KeyError as exc:
            raise HTTPException(404, "事件不存在") from exc
        return TEMPLATES.TemplateResponse(
            request,
            "corpus_event_detail.html",
            {
                "event": event,
                "records": await repository.event_records(event_id),
                "event_options": await repository.list_events(),
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
    async def download_corpus():
        output = result_dir / "投诉事件归一化结果.xlsx"
        await export_corpus(repository, output)
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
