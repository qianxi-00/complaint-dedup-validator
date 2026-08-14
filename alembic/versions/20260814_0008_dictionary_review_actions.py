"""Add dictionary review audit actions."""

from alembic import op

from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401


revision = "20260814_0008"
down_revision = "20260814_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    metadata.tables["dictionary_review_actions"].create(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    op.drop_table("dictionary_review_actions")
