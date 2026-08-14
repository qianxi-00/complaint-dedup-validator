"""Add event cluster v2 storage."""

from alembic import op
import sqlalchemy as sa


revision = "20260812_0005"
down_revision = "20260811_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    job_columns = {column["name"] for column in inspector.get_columns("jobs")}
    if "pipeline_version" not in job_columns:
        op.add_column(
            "jobs",
            sa.Column(
                "pipeline_version",
                sa.String(length=32),
                nullable=False,
                server_default="pair_v1",
            ),
        )
        op.execute(
            sa.text(
                "UPDATE jobs SET pipeline_version = 'pair_v1' "
                "WHERE pipeline_version IS NULL OR pipeline_version = ''"
            )
        )
        op.alter_column(
            "jobs",
            "pipeline_version",
            existing_type=sa.String(length=32),
            server_default="event_cluster_v2",
        )
    record_columns = {column["name"] for column in inspector.get_columns("records")}
    for name in (
        "category_level_1",
        "category_level_2",
        "category_level_3",
        "category_level_4",
        "normalized_title",
        "event_signature",
        "normalized_location",
    ):
        if name not in record_columns:
            op.add_column("records", sa.Column(name, sa.Text(), nullable=True))

    tables = set(inspector.get_table_names())
    if "candidate_events" not in tables:
        op.create_table(
        "candidate_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="review"),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        sa.Column("region", sa.String(length=255), nullable=True),
        sa.Column("street", sa.String(length=255), nullable=True),
        sa.Column("category_level_1", sa.Text(), nullable=True),
        sa.Column("category_level_2", sa.Text(), nullable=True),
        sa.Column("category_level_3", sa.Text(), nullable=True),
        sa.Column("category_level_4", sa.Text(), nullable=True),
        sa.Column("final_event_group_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["final_event_group_id"], ["event_groups.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        )
    if "candidate_event_members" not in tables:
        op.create_table(
        "candidate_event_members",
        sa.Column("candidate_event_id", sa.Integer(), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("role", sa.String(length=32), nullable=False, server_default="member"),
        sa.Column("assignment_source", sa.String(length=32), nullable=False, server_default="model"),
        sa.ForeignKeyConstraint(["candidate_event_id"], ["candidate_events.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["record_id"], ["records.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("candidate_event_id", "record_id"),
        sa.UniqueConstraint("record_id", name="uq_candidate_event_record"),
        )
    if "event_review_actions" not in tables:
        op.create_table(
        "event_review_actions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("candidate_event_id", sa.Integer(), nullable=True),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["candidate_event_id"], ["candidate_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        )


def downgrade() -> None:
    op.drop_table("event_review_actions")
    op.drop_table("candidate_event_members")
    op.drop_table("candidate_events")
    for name in (
        "normalized_location",
        "event_signature",
        "normalized_title",
        "category_level_4",
        "category_level_3",
        "category_level_2",
        "category_level_1",
    ):
        op.drop_column("records", name)
    op.drop_column("jobs", "pipeline_version")
