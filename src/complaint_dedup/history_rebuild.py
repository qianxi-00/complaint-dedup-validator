from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_schema import corpus_records
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.config import Settings


def _input_record(row: dict[str, Any]) -> InputRecord:
    received_at = row.get("received_at")
    if isinstance(received_at, datetime):
        received_at = received_at.isoformat()
    raw_fields = dict(row.get("raw_json") or {})
    raw_fields.setdefault("所属部门", row.get("department"))
    raw_fields.setdefault("处理部门", row.get("processing_department"))
    if row.get("completed_at") is not None:
        completed_at = row["completed_at"]
        raw_fields.setdefault(
            "办结时间",
            completed_at.isoformat()
            if isinstance(completed_at, datetime)
            else str(completed_at),
        )
    return InputRecord(
        source="B",
        source_row=int(row["source_row"] or 0),
        work_order_id=str(row["work_order_id"] or ""),
        title=row.get("title_raw"),
        category=row.get("category_level_4") or row.get("final_category"),
        category_level_1=row.get("category_level_1"),
        category_level_2=row.get("category_level_2"),
        category_level_3=row.get("category_level_3"),
        category_level_4=row.get("category_level_4"),
        appeal_text=row.get("appeal_text"),
        received_at=received_at,
        data_source=str(row.get("data_source") or "history"),
        raw_fields=raw_fields,
    )


async def _large_event_samples(
    repository: CorpusRepository, generation_id: int, *, limit: int = 10
) -> list[dict[str, Any]]:
    from complaint_dedup.corpus_schema import corpus_event_members, corpus_records, events

    async with repository.database.engine.connect() as connection:
        rows = (
            await connection.execute(
                select(
                    events.c.id,
                    events.c.event_name,
                    events.c.event_key_version,
                    func.count(corpus_event_members.c.record_id).label("size"),
                    func.count(func.distinct(corpus_records.c.phone_exact)).label("phones"),
                )
                .select_from(
                    events.join(
                        corpus_event_members,
                        corpus_event_members.c.event_id == events.c.id,
                    ).join(
                        corpus_records,
                        corpus_records.c.id == corpus_event_members.c.record_id,
                    )
                )
                .where(events.c.generation_id == generation_id)
                .where(
                    events.c.status == "active",
                    corpus_records.c.committed.is_(True),
                )
                .group_by(events.c.id, events.c.event_name, events.c.event_key_version)
                .order_by(func.count(corpus_event_members.c.record_id).desc())
                .limit(limit)
            )
        ).mappings().all()
    return [dict(row) for row in rows]


async def rebuild_active_generation(
    database: AsyncDatabase,
    *,
    report_path: Path | None = None,
    rebuilt_by: str = "rebuild-cli",
) -> tuple[int, int, dict[str, Any]]:
    repository = CorpusRepository(database)
    processor = CorpusProcessor(repository)
    old = await repository.active_generation()
    if old is None:
        raise ValueError("没有可重建的活动历史库")
    old_id = int(old["id"])

    async with database.engine.connect() as connection:
        source_rows = (
            await connection.execute(
                select(corpus_records)
                .where(
                    corpus_records.c.generation_id == old_id,
                    corpus_records.c.committed.is_(True),
                )
                .order_by(corpus_records.c.id)
            )
        ).mappings().all()

    if not source_rows:
        raise ValueError("活动历史库没有已提交工单")

    records = [_input_record(dict(row)) for row in source_rows]
    dictionary_version_id = await repository.create_dictionary_version(
        f"rebuild-{old_id}",
        status="candidate",
        source_record_count=len(records),
    )
    batch_id = await repository.create_batch(
        f"历史重建-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "bootstrap_history",
        total_records=len(records),
        dictionary_version_id=dictionary_version_id,
    )
    new_id = await repository.create_generation(batch_id, dictionary_version_id)
    await repository.update_batch_setup(
        batch_id,
        total_records=len(records),
        dictionary_version_id=dictionary_version_id,
        generation_id=new_id,
    )

    await processor.stage_records(
        name=f"历史重建-{old_id}",
        batch_type="bootstrap_history",
        file_name=f"rebuild-{old_id}.xlsx",
        file_hash=f"rebuild-{old_id}-".ljust(64, "0"),
        records=records,
        batch_id=batch_id,
        dictionary_version_id_override=dictionary_version_id,
    )
    await processor._assign_exact_events(batch_id, frozen=True)
    await repository.assign_linked_records(batch_id)
    await repository.assign_strong_signal_records(batch_id)
    await processor._assign_safe_singletons(batch_id)
    await repository.mark_generation_records_committed(new_id)

    record_map = await repository.generation_record_id_map(old_id, new_id)
    conflict_splits = await repository.split_conflicting_events(new_id, record_map)
    old_metrics = await repository.generation_metrics(old_id)
    new_metrics = await repository.generation_metrics(new_id)
    report = {
        "status": "review_required",
        "rebuilt_by": rebuilt_by,
        "old_generation_id": old_id,
        "new_generation_id": new_id,
        "record_id_map_size": len(record_map),
        "cannot_link_conflict_splits": conflict_splits,
        "old_metrics": old_metrics,
        "new_metrics": new_metrics,
        "old_samples": await _large_event_samples(repository, old_id),
        "new_samples": await _large_event_samples(repository, new_id),
        "activation_guard": "人工审核通过后执行 activate",
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    return old_id, new_id, report


async def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="重建投诉事件历史代次")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--activate-generation", type=int, metavar="ID")
    action.add_argument("--rollback-generation", type=int, metavar="ID")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("runtime/history-rebuild-report.json"),
        help="重建审核报告路径",
    )
    args = parser.parse_args(argv)
    settings = Settings()
    database = AsyncDatabase(corpus_database_url(settings))
    repository = CorpusRepository(database)
    try:
        if args.activate_generation is not None:
            await repository.activate_generation(args.activate_generation)
            print(f"已激活历史代次: {args.activate_generation}")
            return 0
        if args.rollback_generation is not None:
            await repository.activate_generation(args.rollback_generation)
            print(f"已回切历史代次: {args.rollback_generation}")
            return 0

        old_id, new_id, report = await rebuild_active_generation(
            database, report_path=args.report
        )
        print(
            f"历史重建完成，旧代次={old_id}，新代次={new_id}，"
            f"状态={report['status']}，报告={args.report}"
        )
        return 0
    finally:
        await database.close()
