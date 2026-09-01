"""Add completion and processing department fields to corpus records."""

from datetime import datetime
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from alembic import op


revision = "20260817_0016"
down_revision = "20260817_0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("corpus_records")}
    if "completed_at" not in columns:
        op.add_column(
            "corpus_records",
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        )
    if "processing_department" not in columns:
        op.add_column(
            "corpus_records",
            sa.Column("processing_department", sa.Text(), nullable=True),
        )

    bind = op.get_bind()
    records = bind.execute(
        sa.text("SELECT id, department, raw_json FROM corpus_records")
    ).mappings().all()
    updates = []
    for row in records:
        raw = row["raw_json"] or {}
        if not isinstance(raw, dict):
            continue
        completed_at = _parse_completed_at(raw.get("办结时间") or raw.get("办结时间 "))
        processing_department = str(
            raw.get("处理部门") or raw.get("处理部门 ") or row["department"] or ""
        ).strip()
        updates.append(
            {
                "_id": row["id"],
                "_completed_at": completed_at,
                "_processing_department": processing_department or None,
            }
        )
    if updates:
        bind.execute(
            sa.text(
                "UPDATE corpus_records SET completed_at = :_completed_at, "
                "processing_department = :_processing_department WHERE id = :_id"
            ),
            updates,
        )

    indexes = {
        index["name"] for index in inspector.get_indexes("corpus_records")
    }
    if "ix_corpus_generation_completed_at" not in indexes:
        op.create_index(
            "ix_corpus_generation_completed_at",
            "corpus_records",
            ["generation_id", "completed_at"],
        )
    if "ix_corpus_generation_processing_department" not in indexes:
        op.create_index(
            "ix_corpus_generation_processing_department",
            "corpus_records",
            ["generation_id", "processing_department"],
        )


def downgrade() -> None:
    op.drop_index("ix_corpus_generation_processing_department", table_name="corpus_records")
    op.drop_index("ix_corpus_generation_completed_at", table_name="corpus_records")
    with op.batch_alter_table("corpus_records") as batch:
        batch.drop_column("processing_department")
        batch.drop_column("completed_at")


def _parse_completed_at(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed
