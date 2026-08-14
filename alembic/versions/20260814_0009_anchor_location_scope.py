"""Include address qualifiers in canonical anchor identity."""

import sqlalchemy as sa
from alembic import op


revision = "20260814_0009"
down_revision = "20260814_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("canonical_anchors")}
    if "location_signature" not in columns:
        op.add_column(
            "canonical_anchors",
            sa.Column(
                "location_signature",
                sa.String(length=768),
                nullable=False,
                server_default="",
            ),
        )
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("canonical_anchors")
    }
    with op.batch_alter_table("canonical_anchors") as batch:
        if "uq_anchor_version" in constraints:
            batch.drop_constraint("uq_anchor_version", type_="unique")
        if "uq_anchor_scope_version" not in constraints:
            batch.create_unique_constraint(
                "uq_anchor_scope_version",
                [
                    "street_id",
                    "canonical_name",
                    "anchor_type",
                    "location_signature",
                    "dictionary_version_id",
                ],
            )


def downgrade() -> None:
    bind = op.get_bind()
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("canonical_anchors")
    }
    with op.batch_alter_table("canonical_anchors") as batch:
        if "uq_anchor_scope_version" in constraints:
            batch.drop_constraint("uq_anchor_scope_version", type_="unique")
        if "uq_anchor_version" not in constraints:
            batch.create_unique_constraint(
                "uq_anchor_version",
                ["street_id", "canonical_name", "anchor_type", "dictionary_version_id"],
            )
        batch.drop_column("location_signature")
