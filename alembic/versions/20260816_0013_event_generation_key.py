"""Scope corpus event identity to an active corpus generation."""

import sqlalchemy as sa
from alembic import op


revision = "20260816_0013"
down_revision = "20260816_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("events")
    }
    with op.batch_alter_table("events") as batch:
        if "uq_event_key_version" in constraints:
            batch.drop_constraint("uq_event_key_version", type_="unique")
        if "uq_corpus_event_generation_key" not in constraints:
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


def downgrade() -> None:
    bind = op.get_bind()
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("events")
    }
    with op.batch_alter_table("events") as batch:
        if "uq_corpus_event_generation_key" in constraints:
            batch.drop_constraint("uq_corpus_event_generation_key", type_="unique")
        if "uq_event_key_version" not in constraints:
            batch.create_unique_constraint(
                "uq_event_key_version",
                ["street_id", "anchor_id", "issue_id", "event_key_version"],
            )
