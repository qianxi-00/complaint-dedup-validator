from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)

from complaint_dedup.async_database import metadata


corpus_sources = Table(
    "corpus_sources",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("file_name", Text, nullable=False),
    Column("file_hash", String(64), nullable=False),
    Column("source_type", String(32), nullable=False),
    Column("column_mapping", JSON, nullable=False, default=dict),
    Column("business_columns", JSON, nullable=False, default=list),
    Column("row_count", Integer, nullable=False, default=0),
    Column("generation_id", Integer, ForeignKey("corpus_generations.id", ondelete="SET NULL")),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("generation_id", "file_hash", name="uq_corpus_source_generation_hash"),
)

daily_batches = Table(
    "daily_batches",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("name", String(255), nullable=False),
    Column("batch_type", String(32), nullable=False),
    Column("status", String(32), nullable=False, default="uploaded"),
    Column("stage", String(32), nullable=False, default="uploaded"),
    Column("input_files", JSON, nullable=False, default=dict),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="SET NULL")),
    Column("generation_id", Integer, ForeignKey("corpus_generations.id", ondelete="SET NULL")),
    Column("total_records", Integer, nullable=False, default=0),
    Column("matched_records", Integer, nullable=False, default=0),
    Column("new_events", Integer, nullable=False, default=0),
    Column("review_records", Integer, nullable=False, default=0),
    Column("pause_requested", Boolean, nullable=False, default=False),
    Column("lease_owner", String(128)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("committed_at", DateTime(timezone=True)),
)

corpus_generations = Table(
    "corpus_generations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("generation_key", String(64), nullable=False, unique=True),
    Column("status", String(32), nullable=False, default="building"),
    Column("source_batch_id", String(64), ForeignKey("daily_batches.id", ondelete="SET NULL")),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="SET NULL")),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("activated_at", DateTime(timezone=True)),
)

dictionary_versions = Table(
    "dictionary_versions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("version", String(64), nullable=False, unique=True),
    Column("status", String(32), nullable=False, default="candidate"),
    Column("source_record_count", Integer, nullable=False, default=0),
    Column("approved_by", String(255)),
    Column("approved_at", DateTime(timezone=True)),
    Column("change_reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

dictionary_extraction_runs = Table(
    "dictionary_extraction_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Integer, ForeignKey("corpus_sources.id", ondelete="CASCADE"), nullable=False),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="CASCADE"), nullable=False),
    Column("source_file_hash", String(64), nullable=False),
    Column("parser_version", String(64), nullable=False),
    Column("model_name", String(255)),
    Column("status", String(32), nullable=False, default="candidate"),
    Column("candidate_counts", JSON, nullable=False, default=dict),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
)

dictionary_review_actions = Table(
    "dictionary_review_actions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "dictionary_version_id",
        Integer,
        ForeignKey("dictionary_versions.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("dimension", String(32), nullable=False),
    Column("item_id", Integer, nullable=False),
    Column("action", String(32), nullable=False),
    Column("before_json", JSON, nullable=False, default=dict),
    Column("after_json", JSON, nullable=False, default=dict),
    Column("reviewed_by", String(255)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

canonical_streets = Table(
    "canonical_streets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("region", String(255)),
    Column("canonical_name", String(255), nullable=False),
    Column("review_status", String(32), nullable=False, default="candidate"),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="CASCADE"), nullable=False),
    UniqueConstraint("region", "canonical_name", "dictionary_version_id", name="uq_canonical_street_version"),
)

street_aliases = Table(
    "street_aliases",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("street_id", Integer, ForeignKey("canonical_streets.id", ondelete="CASCADE"), nullable=False),
    Column("alias", String(255), nullable=False),
    Column("evidence_count", Integer, nullable=False, default=0),
    Column("review_status", String(32), nullable=False, default="candidate"),
    UniqueConstraint("street_id", "alias", name="uq_street_alias"),
)

canonical_anchors = Table(
    "canonical_anchors",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("street_id", Integer, ForeignKey("canonical_streets.id", ondelete="CASCADE")),
    Column("canonical_name", Text, nullable=False),
    Column("anchor_type", String(32), nullable=False, default="unknown"),
    Column("location_signature", String(768), nullable=False, default=""),
    Column("anchor_key_hash", String(64), nullable=False),
    Column("road", String(255)),
    Column("house_no", String(64)),
    Column("building", String(64)),
    Column("shop_no", String(64)),
    Column("floor", String(64)),
    Column("direction", String(64)),
    Column("parent_anchor_id", Integer, ForeignKey("canonical_anchors.id", ondelete="SET NULL")),
    Column("ambiguity_flags", JSON, nullable=False, default=list),
    Column("review_status", String(32), nullable=False, default="candidate"),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="CASCADE"), nullable=False),
    UniqueConstraint(
        "street_id",
        "anchor_key_hash",
        "dictionary_version_id",
        name="uq_anchor_hash_version",
    ),
)

anchor_aliases = Table(
    "anchor_aliases",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("anchor_id", Integer, ForeignKey("canonical_anchors.id", ondelete="CASCADE"), nullable=False),
    Column("alias", Text, nullable=False),
    Column("alias_key_hash", String(64), nullable=False),
    Column("evidence_count", Integer, nullable=False, default=0),
    Column("sample_record_ids", JSON, nullable=False, default=list),
    Column("review_status", String(32), nullable=False, default="candidate"),
    UniqueConstraint("anchor_id", "alias_key_hash", name="uq_anchor_alias_hash"),
)

canonical_issues = Table(
    "canonical_issues",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("canonical_name", Text, nullable=False),
    Column("category_level_1", Text),
    Column("category_level_2", Text),
    Column("category_level_3", Text),
    Column("category_level_4", Text),
    Column("negative_aliases", JSON, nullable=False, default=list),
    Column("independent_issue", Boolean, nullable=False, default=False),
    Column("review_status", String(32), nullable=False, default="candidate"),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="CASCADE"), nullable=False),
    UniqueConstraint("canonical_name", "dictionary_version_id", name="uq_issue_version"),
)

issue_aliases = Table(
    "issue_aliases",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("issue_id", Integer, ForeignKey("canonical_issues.id", ondelete="CASCADE"), nullable=False),
    Column("alias", Text, nullable=False),
    Column("evidence_count", Integer, nullable=False, default=0),
    Column("review_status", String(32), nullable=False, default="candidate"),
    UniqueConstraint("issue_id", "alias", name="uq_issue_alias"),
)

corpus_records = Table(
    "corpus_records",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Integer, ForeignKey("corpus_sources.id", ondelete="RESTRICT"), nullable=False),
    Column("source_batch_id", String(64), ForeignKey("daily_batches.id", ondelete="SET NULL")),
    Column("source_file_hash", String(64), nullable=False),
    Column("generation_id", Integer, ForeignKey("corpus_generations.id", ondelete="SET NULL")),
    Column("source_row", Integer, nullable=False),
    Column("row_hash", String(64), nullable=False),
    Column("occurrence_key", String(128), nullable=False, default=""),
    Column("occurrence_identifiers", JSON, nullable=False, default=list),
    Column("data_source", String(32), nullable=False),
    Column("work_order_id", String(255)),
    Column("received_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("title_raw", Text),
    Column("title_normalized", Text),
    Column("title_noise_tokens", JSON, nullable=False, default=list),
    Column("appeal_text", Text),
    Column("category_level_1", Text),
    Column("category_level_2", Text),
    Column("category_level_3", Text),
    Column("category_level_4", Text),
    Column("final_category", Text),
    Column("department", Text),
    Column("processing_department", Text),
    Column("processing_region", String(255)),
    Column("phone_exact", String(32)),
    Column("phone_mask_pattern", String(32)),
    Column("phone_is_valid", Boolean, nullable=False, default=False),
    Column("address_raw", Text),
    Column("address_line", Text),
    Column("region", String(255)),
    Column("street_id", Integer, ForeignKey("canonical_streets.id", ondelete="SET NULL")),
    Column("street_raw", String(255)),
    Column("road", String(255)),
    Column("house_no", String(64)),
    Column("building", String(64)),
    Column("unit", String(64)),
    Column("room", String(64)),
    Column("shop_no", String(64)),
    Column("floor", String(64)),
    Column("direction", String(64)),
    Column("anchor_raw", Text),
    Column("anchor_type", String(32)),
    Column("anchor_id", Integer, ForeignKey("canonical_anchors.id", ondelete="SET NULL")),
    Column("issue_id", Integer, ForeignKey("canonical_issues.id", ondelete="SET NULL")),
    Column("parse_source", String(32)),
    Column("parse_confidence", Float),
    Column("anchor_resolution_status", String(32), nullable=False, default="unknown"),
    Column("issue_resolution_status", String(32), nullable=False, default="unknown"),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="SET NULL")),
    Column("parser_version", String(64), nullable=False),
    Column("raw_json", JSON, nullable=False, default=dict),
    Column("committed", Boolean, nullable=False, default=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "generation_id",
        "source_file_hash",
        "source_row",
        "row_hash",
        name="uq_corpus_source_generation_row_hash",
    ),
)

batch_records = Table(
    "batch_records",
    metadata,
    Column("batch_id", String(64), ForeignKey("daily_batches.id", ondelete="CASCADE"), primary_key=True),
    Column("record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), primary_key=True),
    Column("status", String(32), nullable=False, default="uploaded"),
)

normalization_decisions = Table(
    "normalization_decisions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), nullable=False),
    Column("dimension", String(32), nullable=False),
    Column("raw_value", Text),
    Column("canonical_id", Integer),
    Column("method", String(32), nullable=False),
    Column("score", Float),
    Column("evidence", JSON, nullable=False, default=dict),
    Column("dictionary_version_id", Integer, ForeignKey("dictionary_versions.id", ondelete="SET NULL")),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("street_id", Integer, ForeignKey("canonical_streets.id", ondelete="RESTRICT"), nullable=False),
    Column("anchor_id", Integer, ForeignKey("canonical_anchors.id", ondelete="RESTRICT"), nullable=False),
    Column("issue_id", Integer, ForeignKey("canonical_issues.id", ondelete="RESTRICT"), nullable=False),
    Column("occurrence_key", String(128), nullable=False, default=""),
    Column("generation_id", Integer, ForeignKey("corpus_generations.id", ondelete="RESTRICT")),
    Column("event_key_version", String(64), nullable=False),
    Column("event_revision", Integer, nullable=False, default=1),
    Column("event_name", Text, nullable=False),
    Column("name_source", String(32), nullable=False, default="program"),
    Column("status", String(32), nullable=False, default="active"),
    Column("is_frozen", Boolean, nullable=False, default=False),
    Column("first_received_at", DateTime(timezone=True)),
    Column("last_received_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "generation_id",
        "street_id",
        "anchor_id",
        "issue_id",
        "occurrence_key",
        "event_key_version",
        name="uq_corpus_event_generation_key",
    ),
)

corpus_event_members = Table(
    "corpus_event_members",
    metadata,
    Column("event_id", Integer, ForeignKey("events.id", ondelete="CASCADE"), primary_key=True),
    Column("record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), primary_key=True, unique=True),
    Column("assignment_source", String(32), nullable=False),
    Column("assigned_at", DateTime(timezone=True), nullable=False),
)

issue_mentions = Table(
    "issue_mentions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), nullable=False),
    Column("segment_no", Integer, nullable=False),
    Column("text", Text, nullable=False),
    Column("issue_id", Integer, ForeignKey("canonical_issues.id", ondelete="SET NULL")),
    Column("is_primary", Boolean, nullable=False, default=False),
    Column("is_independent", Boolean, nullable=False, default=True),
    UniqueConstraint("record_id", "segment_no", name="uq_issue_segment"),
)

record_links = Table(
    "record_links",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), nullable=False),
    Column("target_record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE")),
    Column("link_type", String(32), nullable=False),
    Column("raw_value", Text),
    Column("score", Float),
    Column("evidence", JSON, nullable=False, default=dict),
    Column("revoked", Boolean, nullable=False, default=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

cannot_links = Table(
    "cannot_links",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("left_record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), nullable=False),
    Column("right_record_id", Integer, ForeignKey("corpus_records.id", ondelete="CASCADE"), nullable=False),
    Column("reason", Text, nullable=False),
    Column("source", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("left_record_id", "right_record_id", name="uq_corpus_cannot_link"),
)

event_snapshots = Table(
    "event_snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False),
    Column("event_revision", Integer, nullable=False),
    Column("event_name", Text, nullable=False),
    Column("member_ids", JSON, nullable=False, default=list),
    Column("reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("event_id", "event_revision", name="uq_event_snapshot_revision"),
)

corpus_review_actions = Table(
    "corpus_review_actions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("batch_id", String(64), ForeignKey("daily_batches.id", ondelete="SET NULL")),
    Column("event_id", Integer, ForeignKey("events.id", ondelete="SET NULL")),
    Column("record_id", Integer, ForeignKey("corpus_records.id", ondelete="SET NULL")),
    Column("action", String(32), nullable=False),
    Column("details", JSON, nullable=False, default=dict),
    Column("reviewed_by", String(255)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


Index("ix_corpus_event_key", corpus_records.c.street_id, corpus_records.c.anchor_id, corpus_records.c.issue_id)
Index("ix_corpus_generation_completed_at", corpus_records.c.generation_id, corpus_records.c.completed_at)
Index(
    "ix_corpus_generation_processing_department",
    corpus_records.c.generation_id,
    corpus_records.c.processing_department,
)
Index("ix_corpus_work_order", corpus_records.c.work_order_id)
Index("ix_corpus_phone_exact", corpus_records.c.phone_exact)
Index("ix_corpus_title_normalized", corpus_records.c.title_normalized)
Index("ix_corpus_address_parts", corpus_records.c.road, corpus_records.c.house_no, corpus_records.c.building)
