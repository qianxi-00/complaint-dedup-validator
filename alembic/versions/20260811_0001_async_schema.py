"""Create asynchronous complaint dedup schema."""

from alembic import op

from complaint_dedup.async_database import metadata


revision = "20260811_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    metadata.create_all(op.get_bind())


def downgrade() -> None:
    metadata.drop_all(op.get_bind())
