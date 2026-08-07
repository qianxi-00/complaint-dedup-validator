from pathlib import Path

from complaint_dedup.database import connect_database, initialize_database
from complaint_dedup.worker import JobRunner


def test_recovery_preserves_user_requested_pause(tmp_path: Path) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    with connect_database(database) as connection:
        connection.execute(
            """
            INSERT INTO jobs (id, status, stage, pause_requested)
            VALUES ('paused-job', 'running', 'judging', 1)
            """
        )
        connection.execute(
            """
            INSERT INTO llm_batches (
                job_id, batch_type, batch_index, status, attempts
            ) VALUES ('paused-job', 'judgement', 0, 'running', 1)
            """
        )

    JobRunner(database, processor=None)._recover_interrupted()

    with connect_database(database) as connection:
        job = connection.execute(
            "SELECT status, stage, pause_requested FROM jobs WHERE id = 'paused-job'"
        ).fetchone()
        batch = connection.execute(
            "SELECT status FROM llm_batches WHERE job_id = 'paused-job'"
        ).fetchone()

    assert dict(job) == {
        "status": "paused",
        "stage": "judging",
        "pause_requested": 1,
    }
    assert batch["status"] == "pending"


def test_recovery_requeues_interrupted_unpaused_job(tmp_path: Path) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    with connect_database(database) as connection:
        connection.execute(
            """
            INSERT INTO jobs (id, status, stage, pause_requested)
            VALUES ('running-job', 'running', 'extracting', 0)
            """
        )

    JobRunner(database, processor=None)._recover_interrupted()

    with connect_database(database) as connection:
        job = connection.execute(
            "SELECT status, stage, pause_requested FROM jobs WHERE id = 'running-job'"
        ).fetchone()

    assert dict(job) == {
        "status": "queued",
        "stage": "queued",
        "pause_requested": 0,
    }
