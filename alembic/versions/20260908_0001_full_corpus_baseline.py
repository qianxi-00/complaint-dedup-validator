"""Initialize the full-corpus/window-comparison database baseline."""

import sqlalchemy as sa
from alembic import op

from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401

revision = "20260908_0001"
down_revision = None
branch_labels = None
depends_on = None

_TABLES = (
    "license_state",
    "work_orders",
    "sync_runs",
    "work_order_versions",
    "comparison_runs",
    "comparison_record_members",
    "comparison_events",
    "comparison_event_members",
    "work_order_cannot_links",
)

# The old application created these tables across its job, daily-import,
# dictionary, generation, and event-review workflows. They are intentionally
# removed before creating the new baseline schema.
_LEGACY_TABLES = (
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
    "corpus_generations",
    "event_review_actions",
    "candidate_event_members",
    "candidate_events",
    "event_members",
    "event_groups",
    "reviews",
    "candidate_pairs",
    "job_batches",
    "records",
    "jobs",
    "user_sessions",
    "users",
)


def upgrade() -> None:
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    for name in _LEGACY_TABLES:
        if name not in existing:
            continue
        if bind.dialect.name == "postgresql":
            op.execute(sa.text(f'DROP TABLE IF EXISTS "{name}" CASCADE'))
        else:
            op.drop_table(name)
    metadata.create_all(
        bind,
        tables=[metadata.tables[name] for name in _TABLES],
        checkfirst=True,
    )
    if bind.dialect.name == "postgresql":
        for table in (metadata.tables[name] for name in _TABLES):
            if table.comment:
                op.execute(
                    sa.text(
                        f'COMMENT ON TABLE "{table.name}" IS {_sql_literal(table.comment)}'
                    )
                )
            for column in table.columns:
                if column.comment:
                    op.execute(
                        sa.text(
                            f'COMMENT ON COLUMN "{table.name}"."{column.name}" '
                            f'IS {_sql_literal(column.comment)}'
                        )
                    )


def downgrade() -> None:
    bind = op.get_bind()
    for name in reversed(_TABLES):
        table = metadata.tables[name]
        table.drop(bind, checkfirst=True)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
