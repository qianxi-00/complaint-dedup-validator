import json
from pathlib import Path

import pandas as pd

from complaint_dedup.database import connect_database


SHEETS = ["结果总览", "候选对", "事件组", "抽取失败", "模型失败"]


def export_job(database_path: str | Path, job_id: str, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with connect_database(database_path) as connection:
        job = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if job is None:
            raise KeyError(job_id)
        pairs = connection.execute(
            """
            SELECT p.id AS candidate_pair_id, p.candidate_key, p.llm_decision, p.confidence,
                   p.evidence_json, p.hard_conflicts_json, p.event_name_suggestion,
                   p.judgement_status, p.judgement_error,
                   rv.decision AS review_decision, rv.note AS review_note,
                   a.work_order_id AS a_work_order_id, a.title AS a_title,
                   a.appeal_text AS a_appeal_text, a.raw_json AS a_raw_json,
                   b.work_order_id AS b_work_order_id, b.title AS b_title,
                   b.appeal_text AS b_appeal_text, b.raw_json AS b_raw_json
            FROM candidate_pairs p
            JOIN records a ON a.id = p.record_a_id
            JOIN records b ON b.id = p.record_b_id
            LEFT JOIN reviews rv ON rv.candidate_pair_id = p.id
            WHERE p.job_id = ? ORDER BY p.id
            """,
            (job_id,),
        ).fetchall()
        groups = connection.execute(
            """
            SELECT g.id AS event_group_id, g.name AS event_name,
                   r.source, r.source_row, r.work_order_id, r.title, r.category,
                   r.appeal_text, r.raw_json
            FROM event_groups g
            JOIN event_members m ON m.event_group_id = g.id
            JOIN records r ON r.id = m.record_id
            WHERE g.job_id = ? ORDER BY g.id, r.source, r.source_row
            """,
            (job_id,),
        ).fetchall()
        extraction_failures = connection.execute(
            "SELECT source, source_row, work_order_id, title, extraction_error, raw_json FROM records WHERE job_id = ? AND extraction_status = 'failed'",
            (job_id,),
        ).fetchall()
        model_failures = connection.execute(
            """
            SELECT batch_type, batch_index, attempts, request_json,
                   response_json, error_message
            FROM llm_batches
            WHERE job_id = ? AND status = 'failed'
            ORDER BY batch_type, batch_index
            """,
            (job_id,),
        ).fetchall()

    summary = pd.DataFrame(
        [
            {"指标": "任务ID", "值": job["id"]},
            {"指标": "状态", "值": job["status"]},
            {"指标": "总工单数", "值": job["total_records"]},
            {"指标": "已抽取", "值": job["extracted_records"]},
            {"指标": "候选对", "值": job["candidate_count"]},
            {"指标": "已二审", "值": job["judged_count"]},
            {"指标": "抽取失败", "值": job["extraction_failure_count"]},
            {"指标": "模型失败", "值": job["judgement_failure_count"]},
        ]
    )
    frames = {
        "结果总览": summary,
        "候选对": _frame(pairs),
        "事件组": _frame(groups),
        "抽取失败": _frame(extraction_failures),
        "模型失败": _frame(model_failures),
    }
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet in SHEETS:
            frames[sheet].to_excel(writer, sheet_name=sheet, index=False)
            worksheet = writer.sheets[sheet]
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
    return output


def _frame(rows) -> pd.DataFrame:
    data = [dict(row) for row in rows]
    if not data:
        return pd.DataFrame()
    for row in data:
        for key, value in list(row.items()):
            if key.endswith("_json") and value:
                try:
                    row[key] = json.dumps(json.loads(value), ensure_ascii=False)
                except (TypeError, json.JSONDecodeError):
                    pass
            if isinstance(row[key], str) and row[key].startswith(("=", "+", "-", "@")):
                row[key] = "'" + row[key]
    return pd.DataFrame(data)
