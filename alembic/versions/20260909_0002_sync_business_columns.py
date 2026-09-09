"""Add the business column snapshot to existing sync runs."""

import sqlalchemy as sa
from alembic import op


revision = "20260909_0002"
down_revision = "20260908_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("sync_runs")}
    if "business_columns" in columns:
        return

    op.add_column(
        "sync_runs",
        sa.Column(
            "business_columns",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'"),
            comment="上传文件业务列顺序",
        ),
    )
    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                "COMMENT ON COLUMN sync_runs.business_columns IS "
                "'上传文件业务列顺序'"
            )
        )


def downgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("sync_runs")}
    if "business_columns" in columns:
        op.drop_column("sync_runs", "business_columns")
