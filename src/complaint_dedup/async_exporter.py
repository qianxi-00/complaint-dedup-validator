import asyncio
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, select

from complaint_dedup.async_database import (
    AsyncDatabase,
    candidate_event_members,
    candidate_events,
    candidate_pairs,
    event_groups,
    event_members,
    jobs,
    job_batches,
    records,
    reviews,
)


PAIR_SHEETS = ["结果总览", "地区统计", "工单明细", "候选对", "事件组", "抽取失败", "模型失败"]
EVENT_SHEETS = ["结果总览", "地区事项统计", "工单明细", "候选事件", "正式事件组", "召回审计", "抽取失败", "模型失败"]


async def export_job(
    database: AsyncDatabase,
    job_id: str,
    output_path: str | Path,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    async with database.engine.connect() as connection:
        job = (
            await connection.execute(select(jobs).where(jobs.c.id == job_id))
        ).mappings().first()
        if job is None:
            raise KeyError(job_id)
        member_count = (
            select(func.count(event_members.c.record_id))
            .where(event_members.c.event_group_id == event_groups.c.id)
            .correlate(event_groups)
            .scalar_subquery()
        )
        detail_rows = (
            await connection.execute(
                select(
                    records,
                    event_groups.c.id.label("event_group_id"),
                    event_groups.c.name.label("event_name"),
                    event_groups.c.first_received_at,
                    event_groups.c.last_received_at,
                    member_count.label("event_member_count"),
                )
                .select_from(
                    records.outerjoin(
                        event_members, event_members.c.record_id == records.c.id
                    ).outerjoin(
                        event_groups, event_groups.c.id == event_members.c.event_group_id
                    )
                )
                .where(records.c.job_id == job_id)
                .order_by(records.c.id)
            )
        ).mappings().all()
        candidate_event_rows = (
            await connection.execute(
                select(
                    candidate_events,
                    candidate_event_members.c.record_id,
                    candidate_event_members.c.confidence.label("member_confidence"),
                    candidate_event_members.c.assignment_source,
                )
                .select_from(
                    candidate_events.join(
                        candidate_event_members,
                        candidate_event_members.c.candidate_event_id == candidate_events.c.id,
                    )
                )
                .where(candidate_events.c.job_id == job_id)
                .order_by(candidate_events.c.id, candidate_event_members.c.record_id)
            )
        ).mappings().all()
        left_record = records.alias("left_export_record")
        right_record = records.alias("right_export_record")
        pair_rows = (
            await connection.execute(
                select(
                    candidate_pairs,
                    reviews.c.decision.label("review_decision"),
                    reviews.c.note.label("review_note"),
                    left_record.c.region.label("a_region"),
                    right_record.c.region.label("b_region"),
                )
                .select_from(
                    candidate_pairs.join(
                        left_record, left_record.c.id == candidate_pairs.c.record_a_id
                    )
                    .join(right_record, right_record.c.id == candidate_pairs.c.record_b_id)
                    .outerjoin(reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id)
                )
                .where(candidate_pairs.c.job_id == job_id)
                .order_by(candidate_pairs.c.id)
            )
        ).mappings().all()
        failed_batches = (
            await connection.execute(
                select(job_batches)
                .where(job_batches.c.job_id == job_id, job_batches.c.status == "failed")
                .order_by(job_batches.c.stage, job_batches.c.batch_index)
            )
        ).mappings().all()

    candidate_by_record = {
        int(row["record_id"]): dict(row) for row in candidate_event_rows
    }
    details = [
        _detail_row(
            dict(row),
            candidate_event=candidate_by_record.get(int(row["id"])),
        )
        for row in detail_rows
    ]
    visible_pairs = [
        _pair_row(dict(row))
        for row in pair_rows
        if row["llm_decision"] != "not_duplicate" or row["review_decision"] is not None
    ]
    region_stats = _region_stats(details, pair_rows)
    groups = _group_rows(details)
    summary = [
        {"指标": "任务ID", "值": job["id"]},
        {"指标": "任务模式", "值": "单文件整表去重" if job["mode"] == "single" else "双文件跨表比对"},
        {"指标": "状态", "值": job["status"]},
        {"指标": "总工单数", "值": job["total_records"]},
        {"指标": "候选对", "值": job["candidate_count"]},
        {"指标": "已二审", "值": job["judged_count"]},
        {"指标": "事件组", "值": len({row["事件组ID"] for row in details if row["事件组ID"] is not None})},
    ]
    extraction_failures = [
        row for row in details if row.get("抽取状态") == "failed"
    ]
    legacy_frames = {
        "结果总览": pd.DataFrame(summary),
        "地区统计": pd.DataFrame(region_stats),
        "工单明细": pd.DataFrame(details),
        "候选对": pd.DataFrame(visible_pairs),
        "事件组": pd.DataFrame(groups),
        "抽取失败": pd.DataFrame(extraction_failures),
        "模型失败": pd.DataFrame(
            [
                {
                    "阶段": row["stage"],
                    "批次": row["batch_index"],
                    "尝试次数": row["attempts"],
                    "记录ID": "；".join(str(value) for value in row["item_ids"]),
                    "错误": row["error_message"],
                }
                for row in failed_batches
            ]
        ),
    }
    if job["pipeline_version"] == "event_cluster_v2":
        event_rows = _candidate_event_rows(candidate_event_rows)
        frames = {
            "结果总览": pd.DataFrame(summary),
            "地区事项统计": pd.DataFrame(_event_region_stats(details, event_rows)),
            "工单明细": pd.DataFrame(details),
            "候选事件": pd.DataFrame(event_rows),
            "正式事件组": pd.DataFrame(groups),
            "召回审计": pd.DataFrame([_pair_row(dict(row)) for row in pair_rows]),
            "抽取失败": pd.DataFrame(extraction_failures),
            "模型失败": legacy_frames["模型失败"],
        }
    else:
        frames = legacy_frames
    await asyncio.to_thread(_write_workbook, output, frames)
    return output


def _write_workbook(output: Path, frames: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet in frames:
            _escape_formula_values(frames[sheet]).to_excel(
                writer, sheet_name=sheet, index=False
            )
            worksheet = writer.sheets[sheet]
            worksheet.freeze_panes = "A2"
            if worksheet.max_column:
                worksheet.auto_filter.ref = worksheet.dimensions


def _escape_formula_values(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.map(
        lambda value: "'" + value
        if isinstance(value, str) and value.startswith(("=", "+", "-", "@"))
        else value
    )


def _detail_row(
    row: dict[str, Any], *, candidate_event: dict[str, Any] | None = None
) -> dict[str, Any]:
    first = row.get("first_received_at")
    last = row.get("last_received_at")
    result = {
        "候选事件ID": candidate_event.get("id") if candidate_event else None,
        "候选事件名称": candidate_event.get("name") if candidate_event else None,
        "事件状态": candidate_event.get("status") if candidate_event else None,
        "事件置信度": candidate_event.get("confidence") if candidate_event else None,
        "成员置信度": candidate_event.get("member_confidence") if candidate_event else None,
        "事件组ID": row.get("event_group_id"),
        "事件名称": row.get("event_name"),
        "合并来源": _merge_source(candidate_event, row),
        "合并状态": "已合并" if int(row.get("event_member_count") or 0) > 1 else "单例",
        "地区": row.get("region") or "未知地区",
        "首次受理时间": first,
        "最后受理时间": last,
        "时间范围": f"{first} 至 {last}" if first and last else first or last,
        "来源": row.get("source"),
        "原始行号": row.get("source_row"),
        "工单编号": row.get("work_order_id"),
        "受理时间": row.get("received_at"),
        "诉求标题": row.get("title"),
        "事项分类": row.get("category"),
        "事项分类一级": row.get("category_level_1"),
        "事项分类二级": row.get("category_level_2"),
        "事项分类三级": row.get("category_level_3"),
        "事项分类四级": row.get("category_level_4"),
        "市民诉求": row.get("appeal_text"),
        "抽取状态": row.get("extraction_status"),
    }
    for key, value in (row.get("raw_json") or {}).items():
        result[f"原始字段｜{key}"] = value
    return result


def _merge_source(candidate_event: dict[str, Any] | None, row: dict[str, Any]) -> str:
    if not candidate_event:
        return "单例" if int(row.get("event_member_count") or 0) <= 1 else "人工"
    if candidate_event.get("status") == "auto_merged":
        return "自动"
    if candidate_event.get("status") == "confirmed":
        return "人工"
    return "单例" if candidate_event.get("status") == "singleton" else "待复核"


def _candidate_event_rows(rows: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        event_id = int(row["id"])
        item = grouped.setdefault(
            event_id,
            {
                "候选事件ID": event_id,
                "候选事件名称": row["name"],
                "状态": row["status"],
                "事件置信度": row["confidence"],
                "地区": row["region"],
                "街道": row["street"],
                "事项分类一级": row["category_level_1"],
                "事项分类二级": row["category_level_2"],
                "事项分类三级": row["category_level_3"],
                "事项分类四级": row["category_level_4"],
                "成员数": 0,
                "最低成员置信度": None,
                "工单记录ID": [],
            },
        )
        item["成员数"] += 1
        item["工单记录ID"].append(str(row["record_id"]))
        confidence = row.get("member_confidence")
        if confidence is not None:
            item["最低成员置信度"] = min(
                confidence,
                item["最低成员置信度"] if item["最低成员置信度"] is not None else confidence,
            )
    for item in grouped.values():
        item["工单记录ID"] = "；".join(item["工单记录ID"])
    return list(grouped.values())


def _event_region_stats(
    details: list[dict[str, Any]], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in details:
        key = (row.get("地区") or "未知地区", row.get("事项分类一级") or "未分类")
        grouped.setdefault(
            key,
            {"地区": key[0], "事项分类一级": key[1], "工单数": 0, "候选事件数": 0, "自动合并": 0, "人工确认": 0, "待复核": 0},
        )["工单数"] += 1
    for event in events:
        key = (event.get("地区") or "未知地区", event.get("事项分类一级") or "未分类")
        item = grouped.setdefault(
            key,
            {"地区": key[0], "事项分类一级": key[1], "工单数": 0, "候选事件数": 0, "自动合并": 0, "人工确认": 0, "待复核": 0},
        )
        item["候选事件数"] += 1
        status_key = {"auto_merged": "自动合并", "confirmed": "人工确认", "review": "待复核"}.get(event["状态"])
        if status_key:
            item[status_key] += 1
    return list(grouped.values())


def _pair_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "候选对ID": row.get("id"),
        "A记录ID": row.get("record_a_id"),
        "B记录ID": row.get("record_b_id"),
        "召回依据": "；".join(row.get("recall_reasons") or []),
        "向量分数": row.get("vector_score"),
        "重排分数": row.get("rerank_score"),
        "模型结论": row.get("llm_decision"),
        "模型置信度": row.get("confidence"),
        "人工结论": row.get("review_decision"),
        "人工备注": row.get("review_note"),
        "事件名称建议": row.get("event_name_suggestion"),
    }


def _region_stats(details: list[dict[str, Any]], pairs: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in details:
        region = row["地区"]
        grouped.setdefault(
            region,
            {
                "地区": region,
                "样本数": 0,
                "模型建议重复": 0,
                "人工确认重复": 0,
                "待人工复核": 0,
            },
        )["样本数"] += 1
    for row in pairs:
        regions = {str(row.get("a_region") or "未知地区"), str(row.get("b_region") or "未知地区")}
        for region in regions:
            item = grouped.setdefault(region, {"地区": region, "样本数": 0, "模型建议重复": 0, "人工确认重复": 0, "待人工复核": 0})
            if row["llm_decision"] == "duplicate":
                item["模型建议重复"] += 1
            if row["llm_decision"] == "review":
                item["待人工复核"] += 1
            if row["review_decision"] == "duplicate":
                item["人工确认重复"] += 1
    return list(grouped.values())


def _group_rows(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[Any, int] = {}
    for row in details:
        if row["事件组ID"] is None:
            continue
        counts[row["事件组ID"]] = counts.get(row["事件组ID"], 0) + 1
    return [
        {
            "事件组ID": row["事件组ID"],
            "事件名称": row["事件名称"],
            "地区": row["地区"],
            "成员数": counts[row["事件组ID"]],
            "工单编号": row["工单编号"],
            "诉求标题": row["诉求标题"],
            "首次受理时间": row["首次受理时间"],
            "最后受理时间": row["最后受理时间"],
        }
        for row in details
        if row["事件组ID"] is not None
    ]
