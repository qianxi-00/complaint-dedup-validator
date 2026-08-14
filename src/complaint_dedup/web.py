import json
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from complaint_dedup.config import Settings, get_settings
from complaint_dedup.database import connect_database, initialize_database
from complaint_dedup.evaluator import evaluate_explicit_pairs, export_evaluation, load_explicit_pairs
from complaint_dedup.exporter import export_job
from complaint_dedup.file_inspection import inspect_input_file
from complaint_dedup.llm_client import LlmClient
from complaint_dedup.pipeline import InputRecord, JobProcessor
from complaint_dedup.ui_labels import label, stage_label
from complaint_dedup.worker import JobRunner


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Jinja2Templates(directory=PROJECT_ROOT / "templates")
TEMPLATES.env.filters["zh"] = label
TEMPLATES.env.filters["stage_zh"] = stage_label


def create_app(
    settings: Settings | None = None,
    *,
    start_worker: bool = True,
    llm_client=None,
) -> FastAPI:
    config = settings or get_settings()
    initialize_database(config.database_path)
    runtime_dir = config.database_path.parent
    uploads_dir = runtime_dir / "uploads"
    results_dir = runtime_dir / "results"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    client = llm_client or LlmClient(
        base_url=str(config.llm_base_url),
        api_key=config.llm_api_key,
        model=config.llm_model,
        timeout_seconds=config.llm_timeout_seconds,
        max_retries=config.llm_max_retries,
        temperature=config.llm_temperature,
        max_tokens=config.llm_max_tokens,
        enable_thinking=config.llm_enable_thinking,
    )
    processor = JobProcessor(
        config.database_path,
        client,
        config.llm_extraction_batch_size,
        config.llm_judgement_batch_size,
        config.max_candidates_per_record,
        config.broad_key_max_matches,
    )
    runner = JobRunner(config.database_path, processor)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_worker:
            await runner.start()
        yield
        if start_worker:
            await runner.stop()
        if hasattr(client, "aclose"):
            await client.aclose()

    app = FastAPI(title="投诉重复验证台", lifespan=lifespan)
    app.state.settings = config
    app.state.processor = processor
    app.state.runner = runner
    app.state.uploads_dir = uploads_dir
    app.state.results_dir = results_dir
    static_dir = PROJECT_ROOT / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {"jobs": _list_jobs(config.database_path), "settings": config},
        )

    @app.get("/jobs", response_class=HTMLResponse)
    def jobs(request: Request):
        return TEMPLATES.TemplateResponse(
            request, "history.html", {"jobs": _list_jobs(config.database_path)}
        )

    @app.get("/model", response_class=HTMLResponse)
    def model_status(request: Request):
        return TEMPLATES.TemplateResponse(
            request, "model.html", {"settings": config, "result": None}
        )

    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(request: Request):
        return TEMPLATES.TemplateResponse(
            request, "model.html", {"settings": config, "result": None}
        )

    @app.post("/model/test", response_class=HTMLResponse)
    async def test_model(request: Request):
        try:
            result = await client.test_connection()
        except Exception as exc:
            result = f"连接失败：{exc}"
        return TEMPLATES.TemplateResponse(
            request, "partials/model_status.html", {"settings": config, "result": result}
        )

    @app.post("/evaluate/pairs")
    async def evaluate_pairs_route(request: Request):
        payload = await request.json()
        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        if not isinstance(pairs, list) or not pairs:
            raise HTTPException(400, "pairs 必须是非空数组")
        try:
            results = await evaluate_explicit_pairs(
                client,
                pairs,
                batch_size=config.llm_judgement_batch_size,
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from exc
        return JSONResponse({"pairs": [item.model_dump() for item in results]})

    @app.post("/evaluate/upload")
    async def evaluate_upload(file: UploadFile = File(...)):
        suffix = Path(file.filename or "").suffix.lower()
        if suffix not in {".xlsx", ".xls", ".csv"}:
            raise HTTPException(400, "评测文件仅支持 xlsx、xls、csv")
        evaluation_dir = runtime_dir / "evaluations" / uuid.uuid4().hex
        evaluation_dir.mkdir(parents=True, exist_ok=True)
        input_path = evaluation_dir / f"pairs{suffix}"
        input_path.write_bytes(await file.read())
        try:
            pairs = load_explicit_pairs(input_path)
            results = await evaluate_explicit_pairs(
                client, pairs, batch_size=config.llm_judgement_batch_size
            )
            output = export_evaluation(pairs, results, evaluation_dir / "evaluation.xlsx")
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from exc
        return FileResponse(output, filename="complaint-pair-evaluation.xlsx")

    @app.post("/uploads/inspect", response_class=HTMLResponse)
    async def inspect_uploads(
        request: Request,
        file_a: UploadFile = File(...),
        file_b: UploadFile = File(...),
    ):
        session_id = uuid.uuid4().hex
        session_dir = uploads_dir / session_id
        session_dir.mkdir(parents=True)
        paths = []
        for source, upload in (("A", file_a), ("B", file_b)):
            suffix = Path(upload.filename or "").suffix.lower()
            if suffix not in {".xlsx", ".xls", ".csv"}:
                raise HTTPException(400, f"文件 {source} 格式不支持")
            path = session_dir / f"{source}{suffix}"
            path.write_bytes(await upload.read())
            paths.append(path)
        inspections = [inspect_input_file(path) for path in paths]
        (session_dir / "session.json").write_text(
            json.dumps({"file_a": str(paths[0]), "file_b": str(paths[1])}, ensure_ascii=False),
            encoding="utf-8",
        )
        return TEMPLATES.TemplateResponse(
            request,
            "partials/inspection.html",
            {"session_id": session_id, "a": inspections[0], "b": inspections[1]},
        )

    @app.get("/uploads/{session_id}/mapping/{source}", response_class=HTMLResponse)
    def sheet_mapping(
        request: Request,
        session_id: str,
        source: str,
        sheet: str | None = None,
        sheet_a: str | None = None,
        sheet_b: str | None = None,
    ):
        source = source.upper()
        if source not in {"A", "B"}:
            raise HTTPException(400, "来源必须是 A 或 B")
        sheet = sheet or sheet_a or sheet_b
        if not sheet:
            raise HTTPException(400, "未指定工作表")
        metadata_path = uploads_dir / session_id / "session.json"
        if not metadata_path.exists():
            raise HTTPException(404, "上传会话不存在")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        path = Path(metadata[f"file_{source.lower()}"])
        inspection = inspect_input_file(path, preview_rows=0)
        selected = next((item for item in inspection.sheets if item.name == sheet), None)
        if selected is None:
            raise HTTPException(400, "工作表不存在")
        return TEMPLATES.TemplateResponse(
            request,
            "partials/mapping_fields.html",
            {"session_id": session_id, "prefix": source.lower(), "file": inspection, "selected": selected},
        )

    @app.post("/jobs")
    def create_job_route(
        session_id: str = Form(...),
        job_name: str = Form("跨表重复投诉分析"),
        sheet_a: str = Form(...),
        sheet_b: str = Form(...),
        match_preset: str = Form("balanced"),
        time_window_days: int = Form(0),
        a_work_order_id: str = Form(""),
        a_received_at: str = Form(""),
        a_title: str = Form(""),
        a_category: str = Form(""),
        a_appeal_text: str = Form(""),
        b_work_order_id: str = Form(""),
        b_received_at: str = Form(""),
        b_title: str = Form(""),
        b_category: str = Form(""),
        b_appeal_text: str = Form(""),
    ):
        if not config.llm_model:
            raise HTTPException(400, "模型未配置：请先在 .env 中填写 LLM_MODEL")
        session_dir = uploads_dir / session_id
        metadata_path = session_dir / "session.json"
        if not metadata_path.exists():
            raise HTTPException(404, "上传会话不存在")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        map_a = {
            "work_order_id": a_work_order_id,
            "received_at": a_received_at,
            "title": a_title,
            "category": a_category,
            "appeal_text": a_appeal_text,
        }
        map_b = {
            "work_order_id": b_work_order_id,
            "received_at": b_received_at,
            "title": b_title,
            "category": b_category,
            "appeal_text": b_appeal_text,
        }
        records_a = _load_records(Path(metadata["file_a"]), sheet_a, map_a, "A")
        records_b = _load_records(Path(metadata["file_b"]), sheet_b, map_b, "B")
        if len(records_a) + len(records_b) > config.max_total_rows:
            raise HTTPException(400, "所选工作表合计行数超过配置上限")
        job_id = processor.create_job(
            job_name,
            records_a,
            records_b,
            match_preset=match_preset,
            time_window_days=time_window_days,
        )
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str):
        try:
            job = processor.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(404, "任务不存在") from exc
        return TEMPLATES.TemplateResponse(
            request,
            "job.html",
            {
                "job": job,
                "pairs": processor.list_pairs(job_id),
                "pair_page": 1,
                "pair_has_next": processor.count_pairs(job_id) > 50,
                "groups": processor.list_groups(job_id),
            },
        )

    @app.get("/jobs/{job_id}/status", response_class=HTMLResponse)
    def job_status(request: Request, job_id: str):
        try:
            job = processor.get_job(job_id)
        except KeyError as exc:
            raise HTTPException(404, "任务不存在") from exc
        return TEMPLATES.TemplateResponse(
            request, "partials/job_status.html", {"job": job}
        )

    @app.post("/jobs/{job_id}/pause")
    def pause(job_id: str):
        processor.request_pause(job_id, True)
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.post("/jobs/{job_id}/resume")
    def resume(job_id: str):
        processor.request_pause(job_id, False)
        with connect_database(config.database_path) as connection:
            connection.execute(
                "UPDATE jobs SET status = 'queued', stage = 'queued' WHERE id = ?", (job_id,)
            )
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}/pairs", response_class=HTMLResponse)
    def pair_list(request: Request, job_id: str, page: int = 1):
        page = max(page, 1)
        return TEMPLATES.TemplateResponse(
            request,
            "partials/pairs.html",
            {
                "job_id": job_id,
                "pairs": processor.list_pairs(job_id, limit=50, offset=(page - 1) * 50),
                "pair_page": page,
                "pair_has_next": processor.count_pairs(job_id) > page * 50,
            },
        )

    @app.post("/jobs/{job_id}/pairs/{pair_id}/review")
    def review(job_id: str, pair_id: int, decision: str = Form(...), note: str = Form("")):
        with connect_database(config.database_path) as connection:
            pair = connection.execute(
                "SELECT job_id FROM candidate_pairs WHERE id = ?", (pair_id,)
            ).fetchone()
        if pair is None or pair["job_id"] != job_id:
            raise HTTPException(404, "候选对不存在")
        try:
            processor.review_pair(pair_id, decision, note or None, job_id=job_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

    @app.get("/jobs/{job_id}/export")
    def export(job_id: str):
        output = results_dir / job_id / "result.xlsx"
        export_job(config.database_path, job_id, output)
        return FileResponse(output, filename=f"{job_id}-result.xlsx")

    @app.delete("/jobs/{job_id}")
    def delete_job(job_id: str):
        with connect_database(config.database_path) as connection:
            connection.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        shutil.rmtree(results_dir / job_id, ignore_errors=True)
        return {"deleted": True}

    return app


def _list_jobs(database_path: Path) -> list[dict]:
    with connect_database(database_path) as connection:
        rows = connection.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    return [dict(row) for row in rows]


def _load_records(path: Path, sheet: str, mapping: dict[str, str], source: str) -> list[InputRecord]:
    if path.suffix.lower() == ".csv":
        inspection = inspect_input_file(path)
        frame = pd.read_csv(path, encoding=inspection.encoding, dtype=object)
    else:
        engine = "openpyxl" if path.suffix.lower() == ".xlsx" else "xlrd"
        frame = pd.read_excel(path, sheet_name=sheet, engine=engine, dtype=object)
    frame = frame.where(pd.notna(frame), None)

    def value(row, key):
        column = mapping.get(key)
        return row.get(column) if column else None

    records = []
    for index, row in frame.iterrows():
        records.append(
            InputRecord(
                source=source,
                source_row=index + 2,
                work_order_id=_text(value(row, "work_order_id")),
                received_at=_text(value(row, "received_at")),
                title=_text(value(row, "title")),
                category=_text(value(row, "category")),
                appeal_text=_text(value(row, "appeal_text")),
                category_level_1=_text(value(row, "category_level_1")),
                category_level_2=_text(value(row, "category_level_2")),
                category_level_3=_text(value(row, "category_level_3")),
                category_level_4=_text(value(row, "category_level_4")),
                raw_fields={str(key): _text(item) for key, item in row.to_dict().items()},
            )
        )
    return records


def _text(value) -> str | None:
    return None if value is None else str(value)
