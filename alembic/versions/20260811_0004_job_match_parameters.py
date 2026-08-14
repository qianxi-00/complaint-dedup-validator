"""Persist job matching parameters."""

from alembic import op
import sqlalchemy as sa


revision = "20260811_0004"
down_revision = "20260811_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    if "match_preset" not in columns:
        op.add_column(
            "jobs",
            sa.Column("match_preset", sa.String(length=16), nullable=False, server_default="balanced"),
        )
    if "time_window_days" not in columns:
        op.add_column(
            "jobs",
            sa.Column("time_window_days", sa.Integer(), nullable=False, server_default="0"),
        )


def downgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("jobs")}
    for name in ("time_window_days", "match_preset"):
        if name in columns:
            op.drop_column("jobs", name)
