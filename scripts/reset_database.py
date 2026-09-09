"""Destructively replace the old complaint-dedup schema with the new baseline."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import create_async_engine

from complaint_dedup.config import get_settings
from complaint_dedup.corpus_database import corpus_database_url


TABLES_TO_DROP = (
    "user_sessions",
    "users",
    "dictionary_review_actions",
    "corpus_review_actions",
    "event_snapshots",
    "cannot_links",
    "record_links",
    "issue_mentions",
    "corpus_event_members",
    "events",
    "normalization_decisions",
    "batch_records",
    "corpus_records",
    "issue_aliases",
    "canonical_issues",
    "anchor_aliases",
    "canonical_anchors",
    "street_aliases",
    "canonical_streets",
    "dictionary_extraction_runs",
    "daily_batches",
    "dictionary_versions",
    "corpus_sources",
    "corpus_generations",
    "event_review_actions",
    "candidate_event_members",
    "candidate_events",
    "event_members",
    "event_groups",
    "reviews",
    "candidate_pairs",
    "job_batches",
    "records",
    "jobs",
    "license_state",
    "work_order_cannot_links",
    "comparison_event_members",
    "comparison_events",
    "comparison_record_members",
    "comparison_runs",
    "work_order_versions",
    "sync_runs",
    "work_orders",
)


async def drop_existing_database() -> None:
    settings = get_settings()
    url = corpus_database_url(settings)
    engine = create_async_engine(url, pool_pre_ping=True)
    try:
        async with engine.begin() as connection:
            existing = await connection.run_sync(
                lambda sync_connection: set(sa.inspect(sync_connection).get_table_names())
            )
            for name in TABLES_TO_DROP:
                if name not in existing:
                    continue
                await connection.execute(sa.text(f'DROP TABLE IF EXISTS "{name}" CASCADE'))
            if "alembic_version" in existing:
                await connection.execute(sa.text('DROP TABLE IF EXISTS "alembic_version"'))
    finally:
        await engine.dispose()

def main() -> int:
    parser = argparse.ArgumentParser(
        description="删除旧投诉判重数据库并初始化全量工单/时间窗口比对新基线。"
    )
    parser.add_argument(
        "--confirm-reset",
        action="store_true",
        help="确认已备份数据库且允许永久删除现有工单、任务和审计数据",
    )
    args = parser.parse_args()
    if not args.confirm_reset:
        parser.error("这是破坏性操作，请显式传入 --confirm-reset")

    settings = get_settings()
    url = corpus_database_url(settings)
    asyncio.run(drop_existing_database())
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "head")
    print("数据库已重置并初始化为全量工单/时间窗口比对新基线。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
