import logging

import pytest

from pipeline import kafka_consumer


class FakeMessage:
    def __init__(self, value, partition=0, offset=0):
        self.value = value
        self.partition = partition
        self.offset = offset


class FakeConsumer:
    def __init__(self, messages):
        self._messages = messages
        self.commits = 0

    def __iter__(self):
        return iter(self._messages)

    def commit(self):
        self.commits += 1


def messages(count):
    return [FakeMessage({"n": n}, partition=n % 4, offset=n) for n in range(count)]


def test_consumes_every_message(caplog):
    consumer = FakeConsumer(messages(3))

    with caplog.at_level(logging.INFO, logger="consumer"):
        kafka_consumer.consume_forever(consumer, commit_every=10, crash_after=None)

    assert sum("Consumed" in r.message for r in caplog.records) == 3


def test_commits_once_per_batch():
    consumer = FakeConsumer(messages(10))

    kafka_consumer.consume_forever(consumer, commit_every=5, crash_after=None)

    assert consumer.commits == 2


def test_does_not_commit_a_partial_batch():
    # The uncommitted remainder is exactly what replays after a crash.
    consumer = FakeConsumer(messages(7))

    kafka_consumer.consume_forever(consumer, commit_every=5, crash_after=None)

    assert consumer.commits == 1


def test_crash_injection_exits_without_committing(monkeypatch):
    consumer = FakeConsumer(messages(10))
    exits = []

    def die(code):
        # os._exit really does terminate, so the fake has to stop the loop too.
        exits.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(kafka_consumer.os, "_exit", die)

    with pytest.raises(SystemExit):
        kafka_consumer.consume_forever(consumer, commit_every=5, crash_after=3)

    # Crashed at 3 with commit_every=5, so nothing was ever committed.
    assert exits == [1]
    assert consumer.commits == 0


def test_crash_after_a_commit_leaves_the_committed_work_alone(monkeypatch):
    consumer = FakeConsumer(messages(10))

    def die(code):
        raise SystemExit(code)

    monkeypatch.setattr(kafka_consumer.os, "_exit", die)

    with pytest.raises(SystemExit):
        kafka_consumer.consume_forever(consumer, commit_every=5, crash_after=7)

    # One batch of five was committed before the crash at seven; two replay.
    assert consumer.commits == 1


@pytest.mark.parametrize("commit_every", [1, 3, 5])
def test_commit_every_controls_the_duplicate_window(commit_every):
    consumer = FakeConsumer(messages(9))

    kafka_consumer.consume_forever(
        consumer, commit_every=commit_every, crash_after=None
    )

    assert consumer.commits == 9 // commit_every


def test_consumer_group_defaults_to_the_shared_group():
    # Four consumers divide the partitions because they share one group.
    assert kafka_consumer.CONSUMER_GROUP == "pipeline"


def test_consumer_group_can_be_isolated(monkeypatch):
    # The smoke test relies on this to avoid observing someone else's topology.
    import importlib

    from pipeline import config

    monkeypatch.setenv("CONSUMER_GROUP", "smoke-topology-abc123")
    assert importlib.reload(config).CONSUMER_GROUP == "smoke-topology-abc123"
    monkeypatch.delenv("CONSUMER_GROUP")
    importlib.reload(config)


def test_crash_on_a_commit_boundary_reports_a_full_batch_pending(monkeypatch, caplog):
    """processed % commit_every would say zero here; a whole batch is pending."""
    consumer = FakeConsumer(messages(10))

    def die(code):
        raise SystemExit(code)

    monkeypatch.setattr(kafka_consumer.os, "_exit", die)

    with caplog.at_level(logging.WARNING, logger="consumer"), pytest.raises(SystemExit):
        kafka_consumer.consume_forever(consumer, commit_every=5, crash_after=5)

    assert "5 uncommitted" in caplog.text
    assert consumer.commits == 0


@pytest.mark.parametrize(
    ("crash_after", "commit_every", "expected"),
    [(1, 5, 1), (3, 5, 3), (5, 5, 5), (6, 5, 1), (10, 5, 5), (4, 1, 1)],
)
def test_uncommitted_count_is_never_zero(
    monkeypatch, caplog, crash_after, commit_every, expected
):
    consumer = FakeConsumer(messages(20))

    def die(code):
        raise SystemExit(code)

    monkeypatch.setattr(kafka_consumer.os, "_exit", die)

    with caplog.at_level(logging.WARNING, logger="consumer"), pytest.raises(SystemExit):
        kafka_consumer.consume_forever(
            consumer, commit_every=commit_every, crash_after=crash_after
        )

    assert f"{expected} uncommitted" in caplog.text
