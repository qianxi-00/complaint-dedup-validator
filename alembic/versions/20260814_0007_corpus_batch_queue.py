"""Add durable input payloads for corpus batch workers."""

import sqlalchemy as sa
from alembic import op


revision = "20260814_0007"
down_revision = "20260814_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("daily_batches")}
    if "input_files" not in columns:
        op.add_column(
            "daily_batches",
            sa.Column("input_files", sa.JSON(), nullable=False, server_default="{}"),
        )


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("daily_batches")}
    if "input_files" in columns:
        op.drop_column("daily_batches", "input_files")
