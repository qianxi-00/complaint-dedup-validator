"""Use a bounded hash for canonical anchor alias uniqueness."""

import hashlib

import sqlalchemy as sa
from alembic import op


revision = "20260816_0012"
down_revision = "20260816_0011"
branch_labels = None
depends_on = None


def _alias_hash(alias: str) -> str:
    return hashlib.sha256(alias.encode("utf-8")).hexdigest()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("anchor_aliases")}
    if "alias_key_hash" not in columns:
        op.add_column(
            "anchor_aliases",
            sa.Column("alias_key_hash", sa.String(length=64), nullable=True),
        )

    aliases = sa.table(
        "anchor_aliases",
        sa.column("id", sa.Integer()),
        sa.column("alias", sa.Text()),
        sa.column("alias_key_hash", sa.String()),
    )
    rows = bind.execute(
        sa.select(aliases.c.id, aliases.c.alias).where(aliases.c.alias_key_hash.is_(None))
    )
    for row in rows:
        bind.execute(
            aliases.update()
            .where(aliases.c.id == row.id)
            .values(alias_key_hash=_alias_hash(str(row.alias or "")))
        )

    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("anchor_aliases")
    }
    with op.batch_alter_table("anchor_aliases") as batch:
        if "uq_anchor_alias" in constraints:
            batch.drop_constraint("uq_anchor_alias", type_="unique")
        batch.alter_column(
            "alias_key_hash",
            existing_type=sa.String(length=64),
            nullable=False,
        )
        if "uq_anchor_alias_hash" not in constraints:
            batch.create_unique_constraint(
                "uq_anchor_alias_hash",
                ["anchor_id", "alias_key_hash"],
            )


def downgrade() -> None:
    bind = op.get_bind()
    constraints = {
        item["name"] for item in sa.inspect(bind).get_unique_constraints("anchor_aliases")
    }
    with op.batch_alter_table("anchor_aliases") as batch:
        if "uq_anchor_alias_hash" in constraints:
            batch.drop_constraint("uq_anchor_alias_hash", type_="unique")
        if "uq_anchor_alias" not in constraints:
            batch.create_unique_constraint("uq_anchor_alias", ["anchor_id", "alias"])
        batch.drop_column("alias_key_hash")
