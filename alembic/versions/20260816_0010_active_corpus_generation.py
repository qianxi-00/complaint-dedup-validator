"""Track active corpus generations for atomic history replacement."""

import sqlalchemy as sa
from alembic import op


revision = "20260816_0010"
down_revision = "20260814_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "corpus_generations" not in tables:
        op.create_table(
            "corpus_generations",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("generation_key", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="building"),
            sa.Column("source_batch_id", sa.String(length=64), nullable=True),
            sa.Column("dictionary_version_id", sa.Integer(), nullable=True),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["source_batch_id"], ["daily_batches.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["dictionary_version_id"], ["dictionary_versions.id"], ondelete="SET NULL"),
            sa.UniqueConstraint("generation_key", name="uq_corpus_generation_key"),
        )
    columns = {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in ("daily_batches", "corpus_sources", "corpus_records", "events")
        if table in tables
    }
    if "generation_id" not in columns.get("daily_batches", set()):
        with op.batch_alter_table("daily_batches") as batch:
            batch.add_column(sa.Column("generation_id", sa.Integer(), nullable=True))
            batch.create_foreign_key(
                "fk_daily_batches_generation_id",
                "corpus_generations",
                ["generation_id"],
                ["id"],
                ondelete="SET NULL",
            )
    if "generation_id" not in columns.get("corpus_sources", set()):
        with op.batch_alter_table("corpus_sources") as batch:
            batch.add_column(sa.Column("generation_id", sa.Integer(), nullable=True))
            batch.create_foreign_key(
                "fk_corpus_sources_generation_id",
                "corpus_generations",
                ["generation_id"],
                ["id"],
                ondelete="SET NULL",
            )
    if "generation_id" not in columns.get("corpus_records", set()):
        with op.batch_alter_table("corpus_records") as batch:
            batch.add_column(sa.Column("generation_id", sa.Integer(), nullable=True))
            batch.create_foreign_key(
                "fk_corpus_records_generation_id",
                "corpus_generations",
                ["generation_id"],
                ["id"],
                ondelete="SET NULL",
            )
    if "generation_id" not in columns.get("events", set()):
        with op.batch_alter_table("events") as batch:
            batch.add_column(sa.Column("generation_id", sa.Integer(), nullable=True))
            batch.create_foreign_key(
                "fk_events_generation_id",
                "corpus_generations",
                ["generation_id"],
                ["id"],
                ondelete="RESTRICT",
            )


def downgrade() -> None:
    for table, constraint in (
        ("events", "fk_events_generation_id"),
        ("corpus_records", "fk_corpus_records_generation_id"),
        ("corpus_sources", "fk_corpus_sources_generation_id"),
        ("daily_batches", "fk_daily_batches_generation_id"),
    ):
        with op.batch_alter_table(table) as batch:
            batch.drop_constraint(constraint, type_="foreignkey")
            batch.drop_column("generation_id")
    op.drop_table("corpus_generations")
