"""Add occurrence identity to transaction-like complaint events."""

import sqlalchemy as sa
from alembic import op


revision = "20260817_0015"
down_revision = "20260817_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    record_columns = {
        column["name"] for column in inspector.get_columns("corpus_records")
    }
    if "occurrence_key" not in record_columns:
        op.add_column(
            "corpus_records",
            sa.Column(
                "occurrence_key",
                sa.String(length=128),
                nullable=False,
                server_default="",
            ),
        )
    if "occurrence_identifiers" not in record_columns:
        op.add_column(
            "corpus_records",
            sa.Column(
                "occurrence_identifiers",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            ),
        )
    event_columns = {column["name"] for column in inspector.get_columns("events")}
    if "occurrence_key" not in event_columns:
        op.add_column(
            "events",
            sa.Column(
                "occurrence_key",
                sa.String(length=128),
                nullable=False,
                server_default="",
            ),
        )
    constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in sa.inspect(op.get_bind()).get_unique_constraints("events")
    }
    if "occurrence_key" not in constraints.get("uq_corpus_event_generation_key", ()):
        with op.batch_alter_table("events") as batch:
            batch.drop_constraint("uq_corpus_event_generation_key", type_="unique")
            batch.create_unique_constraint(
                "uq_corpus_event_generation_key",
                [
                    "generation_id",
                    "street_id",
                    "anchor_id",
                    "issue_id",
                    "occurrence_key",
                    "event_key_version",
                ],
            )


def downgrade() -> None:
    with op.batch_alter_table("events") as batch:
        batch.drop_constraint("uq_corpus_event_generation_key", type_="unique")
        batch.create_unique_constraint(
            "uq_corpus_event_generation_key",
            [
                "generation_id",
                "street_id",
                "anchor_id",
                "issue_id",
                "event_key_version",
            ],
        )
        batch.drop_column("occurrence_key")
    with op.batch_alter_table("corpus_records") as batch:
        batch.drop_column("occurrence_identifiers")
        batch.drop_column("occurrence_key")
