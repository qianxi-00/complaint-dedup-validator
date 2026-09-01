"""Create asynchronous complaint dedup schema."""

import sys
from pathlib import Path

from alembic import op

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from legacy_schema import legacy_metadata  # noqa: E402


revision = "20260811_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    legacy_metadata.create_all(op.get_bind())


def downgrade() -> None:
    legacy_metadata.drop_all(op.get_bind())
