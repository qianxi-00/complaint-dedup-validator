"""Add asynchronous job failure counters."""

from alembic import op
import sqlalchemy as sa


revision = "20260811_0003"
down_revision = "20260811_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "extraction_failure_count" not in columns:
        op.add_column(
            "jobs",
            sa.Column("extraction_failure_count", sa.Integer(), nullable=False, server_default="0"),
        )
    if "judgement_failure_count" not in columns:
        op.add_column(
            "jobs",
            sa.Column("judgement_failure_count", sa.Integer(), nullable=False, server_default="0"),
        )
    if "error_message" not in columns:
        op.add_column("jobs", sa.Column("error_message", sa.Text()))


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    for name in ("error_message", "judgement_failure_count", "extraction_failure_count"):
        if name in columns:
            op.drop_column("jobs", name)
