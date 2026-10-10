"""The flink-job CLI: submit with defaults, list, cancel by id."""

from pipeline import flink_cluster, flink_jobs
from pipeline.flink_job import Settings


def test_submit_uses_the_readers_default_settings(monkeypatch, capsys):
    submitted = []
    monkeypatch.setattr(
        flink_cluster, "submit", lambda settings: submitted.append(settings) or "j" * 32
    )

    assert flink_jobs.main(["submit"]) == 0
    assert submitted == [Settings()]
    assert "submitted " + "j" * 32 in capsys.readouterr().out


def test_cancel_is_by_id(monkeypatch):
    cancelled = []
    monkeypatch.setattr(flink_cluster, "cancel", cancelled.append)

    assert flink_jobs.main(["cancel", "a" * 32]) == 0
    assert cancelled == ["a" * 32]


def test_an_unreachable_cluster_is_reported_not_raised(monkeypatch, capsys):
    def down(path, **kwargs):
        raise flink_cluster.FlinkError("connection refused")

    monkeypatch.setattr(flink_cluster, "request", down)

    assert flink_jobs.main(["status"]) == 1
    assert "connection refused" in capsys.readouterr().out
