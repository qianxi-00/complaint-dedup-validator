"""Add replayable group identifiers to automated dedup audits."""

import sqlalchemy as sa
from alembic import op

from complaint_dedup import corpus_schema  # noqa: F401

revision = "20260910_0004"
down_revision = "20260910_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {
        column["name"]
        for column in sa.inspect(bind).get_columns("comparison_decisions")
    }
    fields = (
        (
            "card_ids",
            sa.Column(
                "card_ids",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
                comment="参与本次决策的完整事件卡 ID 列表",
            ),
        ),
        (
            "assigned_card_ids",
            sa.Column(
                "assigned_card_ids",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
                comment="模型代表卡分组后按规则分配的事件卡 ID 列表",
            ),
        ),
        (
            "final_event_key",
            sa.Column(
                "final_event_key",
                sa.String(1024),
                comment="该审计决策对应的最终事件键，跨多个事件时为空",
            ),
        ),
    )
    for name, column in fields:
        if name not in columns:
            op.add_column("comparison_decisions", column)
    if bind.dialect.name == "postgresql":
        comments = {
            "card_ids": "参与本次决策的完整事件卡 ID 列表",
            "assigned_card_ids": "模型代表卡分组后按规则分配的事件卡 ID 列表",
            "final_event_key": "该审计决策对应的最终事件键，跨多个事件时为空",
        }
        for name, comment in comments.items():
            op.execute(
                sa.text(
                    f'COMMENT ON COLUMN "comparison_decisions"."{name}" '
                    f"IS {_sql_literal(comment)}"
                )
            )


def downgrade() -> None:
    bind = op.get_bind()
    columns = {
        column["name"]
        for column in sa.inspect(bind).get_columns("comparison_decisions")
    }
    for name in ("final_event_key", "assigned_card_ids", "card_ids"):
        if name in columns:
            op.drop_column("comparison_decisions", name)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
