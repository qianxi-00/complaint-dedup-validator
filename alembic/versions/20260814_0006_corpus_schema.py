"""Add persistent corpus, dictionaries, and frozen events."""

from alembic import op

from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401


revision = "20260814_0006"
down_revision = "20260812_0005"
branch_labels = None
depends_on = None


_CORPUS_TABLES = (
    "dictionary_review_actions",
    "corpus_review_actions",
    "event_snapshots",
    "cannot_links",
    "record_links",
    "issue_mentions",
    "corpus_event_members",
    "events",
    "normalization_decisions",
    "batch_records",
    "corpus_records",
    "issue_aliases",
    "canonical_issues",
    "anchor_aliases",
    "canonical_anchors",
    "street_aliases",
    "canonical_streets",
    "dictionary_extraction_runs",
    "daily_batches",
    "dictionary_versions",
    "corpus_sources",
)


def upgrade() -> None:
    metadata.create_all(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for table_name in _CORPUS_TABLES:
        op.drop_table(table_name)
