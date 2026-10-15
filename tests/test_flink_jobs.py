"""The flink-job CLI: the keeper's record, Flink's view of it, cancel by id."""

import pytest

from pipeline import flink_cluster, flink_jobs


def test_there_is_no_submit_command_the_keeper_owns_submission(capsys):
    with pytest.raises(SystemExit):
        flink_jobs.parse_args(["submit"])


def test_cancel_is_by_id(monkeypatch):
    cancelled = []
    monkeypatch.setattr(flink_cluster, "cancel", cancelled.append)

    assert flink_jobs.main(["cancel", "a" * 32]) == 0
    assert cancelled == ["a" * 32]


RECORD = {
    "generation": 2,
    "epoch": 4,
    "failures": 1,
    "attempts": {
        "1": {"job_id": None, "submitted_at": None},
        "2": {"job_id": "b" * 32, "submitted_at": None},
    },
}


def test_status_shows_every_attempt_owed_ones_included(monkeypatch, capsys):
    monkeypatch.setattr(flink_jobs, "read_record", lambda: (RECORD, 9))
    monkeypatch.setattr(flink_cluster, "job_state", lambda job_id: "RUNNING")
    monkeypatch.setattr(flink_cluster, "completed_checkpoints", lambda job_id: 12)
    monkeypatch.setattr(flink_cluster, "restored_checkpoint", lambda job_id: 7)
    monkeypatch.setattr(
        flink_cluster,
        "jobs_overview",
        lambda: [
            {"id": "b" * 32, "name": "pageview-windows-g2", "state": "RUNNING"},
            {"id": "c" * 32, "name": "pageview-windows-g1", "state": "RUNNING"},
            {"id": "d" * 32, "name": "someone-elses-job", "state": "RUNNING"},
        ],
    )

    assert flink_jobs.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "generation 2" in out
    assert "g1 pageview-windows-g1 (superseded): no id yet — still owed" in out
    assert "restored from checkpoint 7" in out
    assert "unrecorded: pageview-windows-g1 " + "c" * 32 in out
    assert "someone-elses-job" not in out


def test_a_job_flink_does_not_know_is_reported_as_not_found(monkeypatch, capsys):
    def not_found(job_id):
        raise flink_cluster.NotFound("404")

    monkeypatch.setattr(flink_jobs, "read_record", lambda: (RECORD, 9))
    monkeypatch.setattr(flink_cluster, "job_state", not_found)
    monkeypatch.setattr(flink_cluster, "jobs_overview", lambda: [])

    assert flink_jobs.main(["status"]) == 0
    assert "not found" in capsys.readouterr().out


def test_an_unreachable_zookeeper_is_reported_not_raised(monkeypatch, capsys):
    def down():
        raise RuntimeError("ZooKeeper unavailable")

    monkeypatch.setattr(flink_jobs, "read_record", down)

    assert flink_jobs.main(["status"]) == 1
    assert "ZooKeeper unavailable" in capsys.readouterr().out
