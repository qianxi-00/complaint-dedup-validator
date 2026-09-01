from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401


def test_corpus_schema_registers_all_long_lived_tables():
    expected = {
        "corpus_sources",
        "corpus_generations",
        "corpus_records",
        "daily_batches",
        "batch_records",
        "dictionary_extraction_runs",
        "dictionary_review_actions",
        "dictionary_versions",
        "canonical_streets",
        "street_aliases",
        "canonical_anchors",
        "anchor_aliases",
        "canonical_issues",
        "issue_aliases",
        "normalization_decisions",
        "events",
        "corpus_event_members",
        "issue_mentions",
        "record_links",
        "cannot_links",
        "event_snapshots",
        "corpus_review_actions",
    }
    assert expected <= set(metadata.tables)


def test_corpus_record_keeps_resolution_and_source_fields():
    table = metadata.tables["corpus_records"]
    expected = {
        "source_file_hash",
        "source_row",
        "row_hash",
        "work_order_id",
        "title_raw",
        "title_normalized",
        "title_noise_tokens",
        "phone_exact",
        "phone_mask_pattern",
        "phone_is_valid",
        "address_raw",
        "address_line",
        "parse_source",
        "parse_confidence",
        "anchor_resolution_status",
        "issue_resolution_status",
        "dictionary_version_id",
        "parser_version",
        "raw_json",
        "generation_id",
        "occurrence_key",
        "occurrence_identifiers",
    }
    assert expected <= set(table.c.keys())


def test_event_key_is_unique_within_key_version():
    table = metadata.tables["events"]
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert (
        "generation_id",
        "street_id",
        "anchor_id",
        "issue_id",
        "occurrence_key",
        "event_key_version",
    ) in unique_columns


def test_anchor_identity_uses_bounded_hash_key():
    table = metadata.tables["canonical_anchors"]
    assert "location_signature" in table.c
    assert "anchor_key_hash" in table.c
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert (
        "street_id",
        "anchor_key_hash",
        "dictionary_version_id",
    ) in unique_columns


def test_anchor_alias_identity_uses_bounded_hash_key():
    table = metadata.tables["anchor_aliases"]
    assert "alias_key_hash" in table.c
    unique_columns = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert ("anchor_id", "alias_key_hash") in unique_columns
    assert ("anchor_id", "alias") not in unique_columns
