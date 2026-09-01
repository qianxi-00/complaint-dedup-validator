"""Scope corpus source and record de-duplication to a history generation."""

from alembic import op


revision = "20260817_0014"
down_revision = "20260816_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SQLite's metadata.create_all path uses unnamed autoindexes for column
    # uniqueness, so the PostgreSQL constraint rewrite is intentionally left
    # to the production dialect.  SQLite tests use corpus_schema metadata.
    if op.get_bind().dialect.name == "sqlite":
        return
    with op.batch_alter_table("corpus_sources") as batch:
        batch.drop_constraint("corpus_sources_file_hash_key", type_="unique")
        batch.create_unique_constraint(
            "uq_corpus_source_generation_hash", ["generation_id", "file_hash"]
        )
    with op.batch_alter_table("corpus_records") as batch:
        batch.drop_constraint("uq_corpus_source_row_hash", type_="unique")
        batch.create_unique_constraint(
            "uq_corpus_source_generation_row_hash",
            ["generation_id", "source_file_hash", "source_row", "row_hash"],
        )


def downgrade() -> None:
    if op.get_bind().dialect.name == "sqlite":
        return
    with op.batch_alter_table("corpus_records") as batch:
        batch.drop_constraint("uq_corpus_source_generation_row_hash", type_="unique")
        batch.create_unique_constraint(
            "uq_corpus_source_row_hash", ["source_file_hash", "source_row", "row_hash"]
        )
    with op.batch_alter_table("corpus_sources") as batch:
        batch.drop_constraint("uq_corpus_source_generation_hash", type_="unique")
        batch.create_unique_constraint("corpus_sources_file_hash_key", ["file_hash"])
