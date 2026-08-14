"""Add resumable job batches."""

from alembic import op
import sqlalchemy as sa


revision = "20260811_0002"
down_revision = "20260811_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "job_batches" in inspector.get_table_names():
        return
    op.create_table(
        "job_batches",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("batch_index", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("item_ids", sa.JSON(), nullable=False),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "stage", "batch_index", name="uq_job_stage_batch"),
    )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "job_batches" in inspector.get_table_names():
        op.drop_table("job_batches")
