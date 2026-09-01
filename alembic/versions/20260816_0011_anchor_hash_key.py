"""Use a bounded hash for canonical anchor uniqueness."""

import hashlib

import sqlalchemy as sa
from alembic import op


revision = "20260816_0011"
down_revision = "20260816_0010"
branch_labels = None
depends_on = None


def _anchor_hash(canonical_name: str, anchor_type: str, location_signature: str) -> str:
    value = "\x1f".join((canonical_name, anchor_type, location_signature))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("canonical_anchors")}
    if "anchor_key_hash" not in columns:
        op.add_column(
            "canonical_anchors",
            sa.Column("anchor_key_hash", sa.String(length=64), nullable=True),
        )

    anchors = sa.table(
        "canonical_anchors",
        sa.column("id", sa.Integer()),
        sa.column("canonical_name", sa.Text()),
        sa.column("anchor_type", sa.String()),
        sa.column("location_signature", sa.String()),
        sa.column("anchor_key_hash", sa.String()),
    )
    rows = bind.execute(
        sa.select(
            anchors.c.id,
            anchors.c.canonical_name,
            anchors.c.anchor_type,
            anchors.c.location_signature,
        ).where(anchors.c.anchor_key_hash.is_(None))
    )
    for row in rows:
        bind.execute(
            anchors.update()
            .where(anchors.c.id == row.id)
            .values(
                anchor_key_hash=_anchor_hash(
                    str(row.canonical_name or ""),
                    str(row.anchor_type or "unknown"),
                    str(row.location_signature or ""),
                )
            )
        )

    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("canonical_anchors")
    }
    with op.batch_alter_table("canonical_anchors") as batch:
        if "uq_anchor_scope_version" in constraints:
            batch.drop_constraint("uq_anchor_scope_version", type_="unique")
        batch.alter_column(
            "anchor_key_hash",
            existing_type=sa.String(length=64),
            nullable=False,
        )
        if "uq_anchor_hash_version" not in constraints:
            batch.create_unique_constraint(
                "uq_anchor_hash_version",
                ["street_id", "anchor_key_hash", "dictionary_version_id"],
            )


def downgrade() -> None:
    bind = op.get_bind()
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("canonical_anchors")
    }
    with op.batch_alter_table("canonical_anchors") as batch:
        if "uq_anchor_hash_version" in constraints:
            batch.drop_constraint("uq_anchor_hash_version", type_="unique")
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
        batch.drop_column("anchor_key_hash")
